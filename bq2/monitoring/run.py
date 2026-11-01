"""
Run the four live checks on one replayed slice (local mode) and decide the gate.

    python -m monitoring.run 2022-09-08               # writes out/monitor/checks/<slice>.json
    python -m monitoring.run 2022-09-11 --triage      # plus the triage note in out/monitor/notes/

Per slice:
    raw slice (out/stream/)  ->  curate (dedup + FX join)  ->  score with the live model
    ->  recon, quality, drift, volume  ->  gate (code, never the agent)

A slice is one day. Drift and volume compare it with the other normal days of its kind
(normal_days): weekdays 2 and 5-9 Sep (1 Sep is month-start), weekends 3-4 Sep, never the slice's
own day. The normal days are scored once and cached in out/live_model/baseline.parquet.
"""
from __future__ import annotations

import argparse
import json
from functools import lru_cache
from pathlib import Path

import pandas as pd
import xgboost as xgb

from monitoring import checks
from monitoring.curate import curate_slice, load_currency_lookup, load_fx
from monitoring.features import live_features
from monitoring.replay import REPO, STREAM_DIR, TAIL_START, load_slice, slice_bounds

LIVE_DIR = REPO / "out" / "live_model"
CHECKS_DIR = REPO / "out" / "monitor" / "checks"
NORMAL_WEEKDAYS = ["2022-09-02", "2022-09-05", "2022-09-06", "2022-09-07", "2022-09-08", "2022-09-09"]
NORMAL_WEEKEND_DAYS = ["2022-09-03", "2022-09-04"]
BASELINE_COLS = ["ts", "from_bank", "payment_format", "payment_currency", "amount_paid_usd",
                 "is_laundering", "score"]
NOTES_DIR = REPO / "out" / "monitor" / "notes"


@lru_cache(maxsize=1)
def live_model() -> tuple[xgb.Booster, float, pd.DataFrame, pd.DataFrame]:
    booster = xgb.Booster()
    booster.load_model(LIVE_DIR / "model.json")
    threshold = json.loads((LIVE_DIR / "score_threshold.json").read_text())["threshold"]
    return (booster, threshold, pd.read_parquet(LIVE_DIR / "sender_features.parquet"),
            pd.read_parquet(LIVE_DIR / "receiver_features.parquet"))


@lru_cache(maxsize=1)
def reference_tables() -> tuple[pd.DataFrame, pd.DataFrame]:
    return load_fx(), load_currency_lookup()


def curate(raw: pd.DataFrame, fx: pd.DataFrame | None = None) -> pd.DataFrame:
    default_fx, lookup = reference_tables()
    return curate_slice(raw, default_fx if fx is None else fx, lookup)


def score(curated_day: pd.DataFrame) -> pd.Series:
    """Live scores for curated rows; the frame must hold the day's earlier rows too (so-far-today counts)."""
    if curated_day.empty:                         # e.g. every row lost to a missing FX rate
        return pd.Series(index=curated_day.index, dtype="float32")
    booster, _, snd, rcv = live_model()
    X = live_features(curated_day, snd, rcv)
    return pd.Series(booster.predict(xgb.DMatrix(X, enable_categorical=True)), index=curated_day.index)


def normal_days(day: str) -> list[str]:
    """The normal days a slice is compared with: the other ones of its kind, never its own day."""
    weekend = pd.Timestamp(day).dayofweek >= 5
    return [d for d in (NORMAL_WEEKEND_DAYS if weekend else NORMAL_WEEKDAYS) if d != day]


def score_slice(sid: str, fx: pd.DataFrame | None = None, out_dir: Path = STREAM_DIR) -> pd.DataFrame:
    """The slice's curated rows with a 'score' column."""
    curated = curate(load_slice(sid, out_dir), fx)
    curated["score"] = score(curated).astype("float32")
    return curated


def baseline(out_dir: Path = STREAM_DIR) -> pd.DataFrame:
    """Scored rows of every normal day, cached."""
    path = LIVE_DIR / "baseline.parquet"
    if path.exists():
        return pd.read_parquet(path)
    parts = []
    for day in NORMAL_WEEKDAYS + NORMAL_WEEKEND_DAYS:
        curated = score_slice(day, out_dir=out_dir)
        parts.append(curated[BASELINE_COLS])
    base = pd.concat(parts, ignore_index=True)
    base.to_parquet(path, index=False)
    return base


def run_checks(sid: str, fx: pd.DataFrame | None = None, out_dir: Path = STREAM_DIR,
               write: bool = True) -> dict:
    """All four checks plus the gate for one slice. fx overrides the FX table."""
    raw = load_slice(sid, out_dir)
    duplicates = int(raw.duplicated().sum())
    scored = score_slice(sid, fx, out_dir)
    start, end = slice_bounds(sid)
    _, threshold, _, _ = live_model()

    base = baseline(out_dir)
    base_day = base["ts"].dt.strftime("%Y-%m-%d")
    base_slice = base[base_day.isin(normal_days(sid))]
    base_volume = base_slice.groupby(base_day[base_slice.index]).size()

    results = [
        {**checks.check_recon(raw.drop_duplicates(), scored), "duplicates_dropped": duplicates},
        checks.check_quality(scored, start, end),
        checks.check_drift(base_slice, scored),
        checks.check_volume(scored, base_volume, threshold),
    ]
    gate = checks.gate(results)
    out = {"slice": sid, "gate": gate, "path": "build_marts" if gate == "pass" else "quarantine",
           "results": results, "facts": slice_facts(sid, scored, base_slice, base_volume, threshold)}
    if write:
        CHECKS_DIR.mkdir(parents=True, exist_ok=True)
        (CHECKS_DIR / f"{sid}.json").write_text(json.dumps(out, indent=2, default=str))
    return out


def slice_facts(sid: str, scored: pd.DataFrame, base_slice: pd.DataFrame, base_volume: pd.Series,
                score_threshold: float, top_n: int = 5, shifts_n: int = 3) -> dict:
    """The report card for the agent: calendar and normal-day context, the day's top alerts and the
    biggest shifts in mix from the normal days. These are the numbers it may quote besides the checks'."""
    start, _ = slice_bounds(sid)
    fmt = scored["payment_format"].value_counts(normalize=True)
    normal_fmt = base_slice["payment_format"].value_counts(normalize=True)
    top = fmt.index[0] if len(fmt) else None
    alerts = scored[scored["score"] >= score_threshold].nlargest(top_n, "score")
    top_alerts = [{"time": r.ts.strftime("%H:%M"), "from_bank": r.from_bank, "to_bank": r.to_bank,
                   "payment_format": r.payment_format, "payment_currency": r.payment_currency,
                   "amount_usd": round(float(r.amount_paid_usd), 2), "score": round(float(r.score), 3)}
                  for r in alerts.itertuples()]
    shifts = []
    for col in ("payment_format", "payment_currency"):
        today = scored[col].value_counts(normalize=True)
        normal = base_slice[col].value_counts(normalize=True)
        for value in today.index.union(normal.index):
            t, n = 100 * float(today.get(value, 0.0)), 100 * float(normal.get(value, 0.0))
            shifts.append({"what": f"{col}: {value}", "today_pct": round(t, 1), "normal_pct": round(n, 1)})
    shifts.sort(key=lambda x: abs(x["today_pct"] - x["normal_pct"]), reverse=True)
    return {
        "weekday": start.day_name(), "day_of_month": start.day,
        "is_month_start": start.day == 1, "is_weekend": start.dayofweek >= 5,
        "inside_simulation_window_1_to_10_sep": start < TAIL_START,
        "normal_days_compared": len(base_volume),
        "normal_rows_per_day": int(round(base_volume.mean())),
        "normal_laundering_rate_pct": round(100 * float(base_slice["is_laundering"].mean()), 3),
        "top_payment_format": top,
        "top_format_share_pct": round(100 * float(fmt.iloc[0]), 1) if top else None,
        "top_format_normal_share_pct": round(100 * float(normal_fmt.get(top, 0.0)), 1) if top else None,
        "top_alerts": top_alerts,
        "biggest_shifts": shifts[:shifts_n],
    }


def triage_slice(sid: str, llm=None, escalate: bool = True) -> dict:
    """Triage note for a slice whose checks ran, written to out/monitor/notes/<slice>.json.
    A slice that passed every check gets "All checks passed." without calling the model;
    a failed one gets the agent's note, escalated if its severity is high."""
    checked = json.loads((CHECKS_DIR / f"{sid}.json").read_text())
    if checked["gate"] == "pass":
        note = {"slice": sid, "severity": "low", "verdict": "expected", "summary": "All checks passed.",
                "evidence": [], "suggested_action": "None.", "unverified_numbers": [], "usage": None,
                "label": "Written by code, no model call"}
    else:
        from agent.triage_agent import triage
        facts = {**checked["facts"], "gate_decision": checked["path"]}
        note = triage(sid, checked["results"], facts, llm=llm)
    NOTES_DIR.mkdir(parents=True, exist_ok=True)
    (NOTES_DIR / f"{sid}.json").write_text(json.dumps(note, indent=2, default=str))
    if escalate:
        from monitoring.escalations import send_escalation
        send_escalation(note, checked["path"])
    return note


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("slice", help="slice id, e.g. 2022-09-08 or 2022-09-11")
    ap.add_argument("--triage", action="store_true", help="also write the triage note (calls the model if a check failed)")
    args = ap.parse_args()
    print(json.dumps(run_checks(args.slice), indent=2, default=str))
    if args.triage:
        print(json.dumps(triage_slice(args.slice), indent=2, default=str))

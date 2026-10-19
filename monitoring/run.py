"""
Run the four live checks on one replayed slice (local mode) and decide the gate.

    python -m monitoring.run 2022-09-08T10            # writes out/monitor/checks/<slice>.json
    python -m monitoring.run 2022-09-11 --triage      # plus the agent note in out/monitor/notes/

Per slice:
    raw slice (out/stream/)  ->  curate (dedup + FX join)  ->  score with the live model
    ->  recon, quality, drift, anomaly  ->  gate (code, never the agent)

The drift baseline is the same hour on normal weekdays: 2, 5, 6, 7, 8 and 9 Sep (1 Sep is
month-start). The anomaly volume baseline is the same hour on the same day type, so a weekend slice
is compared with 3-4 Sep. Both are scored once and cached in out/live_model/baseline.parquet.
For the day-long tail slice, the baseline is whole weekdays.
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
from monitoring.replay import REPO, STREAM_DIR, TAIL_START, list_slices, load_slice, slice_bounds

LIVE_DIR = REPO / "out" / "live_model"
CHECKS_DIR = REPO / "out" / "monitor" / "checks"
BASELINE_DAYS = ["2022-09-02", "2022-09-05", "2022-09-06", "2022-09-07", "2022-09-08", "2022-09-09"]
WEEKEND_DAYS = ["2022-09-03", "2022-09-04"]
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


def score_slice(sid: str, fx: pd.DataFrame | None = None, out_dir: Path = STREAM_DIR) -> pd.DataFrame:
    """The slice's curated rows with a 'score' column, scored using only that day's slices up to this one."""
    day = sid[:10]
    so_far = [s for s in list_slices(out_dir) if s[:10] == day and s <= sid]
    curated = curate(pd.concat([load_slice(s, out_dir) for s in so_far], ignore_index=True), fx)
    curated["score"] = score(curated).astype("float32")
    start, end = slice_bounds(sid)
    return curated[(curated["ts"] >= start) & (curated["ts"] < end)].reset_index(drop=True)


def baseline(out_dir: Path = STREAM_DIR) -> pd.DataFrame:
    """Scored rows of the baseline days, cached."""
    path = LIVE_DIR / "baseline.parquet"
    if path.exists():
        return pd.read_parquet(path)
    parts = []
    for day in BASELINE_DAYS + WEEKEND_DAYS:      # a whole day at once gives the same so-far-today counts
        sids = [s for s in list_slices(out_dir) if s[:10] == day]
        curated = curate(pd.concat([load_slice(s, out_dir) for s in sids], ignore_index=True))
        curated["score"] = score(curated).astype("float32")
        parts.append(curated[BASELINE_COLS])
    base = pd.concat(parts, ignore_index=True)
    base.to_parquet(path, index=False)
    return base


def run_checks(sid: str, fx: pd.DataFrame | None = None, out_dir: Path = STREAM_DIR,
               write: bool = True) -> dict:
    """All four checks plus the gate for one slice. fx overrides the FX table (used by the weekend demo)."""
    raw = load_slice(sid, out_dir)
    duplicates = int(raw.duplicated().sum())
    scored = score_slice(sid, fx, out_dir)
    start, end = slice_bounds(sid)
    _, threshold, _, _ = live_model()

    base = baseline(out_dir)
    weekend = base["ts"].dt.dayofweek >= 5
    weekdays = base[~weekend]
    if "T" in sid:
        base_slice = weekdays[weekdays["ts"].dt.hour == start.hour]
        same_type = base[(weekend == (start.dayofweek >= 5)) & (base["ts"].dt.hour == start.hour)]
        base_volume = same_type.groupby(same_type["ts"].dt.floor("h")).size()
    else:
        base_slice = weekdays
        base_volume = weekdays.groupby(weekdays["ts"].dt.normalize()).size()
    flagged = weekdays[weekdays["score"] >= threshold]
    base_bank_share = flagged["from_bank"].value_counts(normalize=True)

    results = [
        {**checks.check_recon(raw.drop_duplicates(), scored), "duplicates_dropped": duplicates},
        checks.check_quality(scored, start, end),
        checks.check_drift(base_slice, scored),
        checks.check_anomaly(scored, base_volume, base_bank_share, threshold),
    ]
    gate = checks.gate(results)
    out = {"slice": sid, "gate": gate, "path": "build_marts" if gate == "pass" else "quarantine",
           "results": results, "facts": slice_facts(sid, scored, base_slice, base_volume)}
    if write:
        CHECKS_DIR.mkdir(parents=True, exist_ok=True)
        (CHECKS_DIR / f"{sid}.json").write_text(json.dumps(out, indent=2, default=str))
    return out


def slice_facts(sid: str, scored: pd.DataFrame, base_slice: pd.DataFrame, base_volume: pd.Series) -> dict:
    """Calendar and baseline context for the agent: the numbers it may quote besides the checks'."""
    start, _ = slice_bounds(sid)
    fmt = scored["payment_format"].value_counts(normalize=True)
    normal_fmt = base_slice["payment_format"].value_counts(normalize=True)
    top = fmt.index[0] if len(fmt) else None
    return {
        "weekday": start.day_name(), "day_of_month": start.day,
        "hour": start.hour if "T" in sid else "whole day",
        "is_month_start": start.day == 1, "is_weekend": start.dayofweek >= 5,
        "inside_simulation_window_1_to_10_sep": start < TAIL_START,
        "normal_rows_same_slice_type": int(round(base_volume.mean())),
        "normal_laundering_rate_pct": round(100 * float(base_slice["is_laundering"].mean()), 3),
        "top_payment_format": top,
        "top_format_share_pct": round(100 * float(fmt.iloc[0]), 1) if top else None,
        "top_format_normal_share_pct": round(100 * float(normal_fmt.get(top, 0.0)), 1) if top else None,
    }


def triage_slice(sid: str, llm=None, alert: bool = True) -> dict:
    """Agent note for a slice whose checks ran: written to out/monitor/notes/<slice>.json, alerted if high."""
    from agent.triage_agent import triage
    from monitoring.alerts import send_alert
    checked = json.loads((CHECKS_DIR / f"{sid}.json").read_text())
    facts = {**checked["facts"], "gate_decision": checked["path"]}
    note = triage(sid, checked["results"], facts, llm=llm)
    NOTES_DIR.mkdir(parents=True, exist_ok=True)
    (NOTES_DIR / f"{sid}.json").write_text(json.dumps(note, indent=2, default=str))
    if alert:
        send_alert(note, checked["path"])
    return note


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("slice", help="slice id, e.g. 2022-09-08T10 or 2022-09-11")
    ap.add_argument("--triage", action="store_true", help="also ask the agent for a note (calls the model)")
    args = ap.parse_args()
    print(json.dumps(run_checks(args.slice), indent=2, default=str))
    if args.triage:
        print(json.dumps(triage_slice(args.slice), indent=2, default=str))

"""
The snapshot: one DuckDB file that the Helical dashboard reads, synced from S3 (the source of truth).

    monitor/ -> monitor_slices, monitor_daily, monitor_format_mix, monitor_reports,
                monitor_shifts, monitor_top_alerts, monitor_escalations   (the monitor page)
    marts/   -> mart_<name> for each Athena mart, model_<name> for marts/model/*.csv   (the analysis page)

`through` keeps only the slices up to that day, so during the replay the monitor page grows one day
at a time even though S3 already holds every day. The file is written in DuckDB's 1.5 format, the
newest that Helical's driver (duckdb_jdbc 1.5.6) reads, and swapped in whole when complete.
"""
from __future__ import annotations

import io
import json
import os
import re
from pathlib import Path

import duckdb
import pandas as pd

SNAPSHOT_PATH = Path(__file__).resolve().parents[1] / "out" / "snapshot" / "aml.duckdb"
STORAGE_VERSION = "v1.5.0"
CHECKS = ["recon", "quality", "drift", "volume"]
MARTS = {"channel_lift", "currency_risk", "bank_concentration", "corridors", "amount_hour", "typology_profile"}
MODEL_TABLE = re.compile(r"[a-z0-9_]{1,40}")     # table names come from S3 keys: only plain ones pass


def _json(store, prefix: str, through: str | None) -> list[dict]:
    out = []
    for key in store.keys(prefix):
        day = Path(key).stem
        if key.endswith(".json") and (through is None or day <= through):
            out.append(json.loads(store.get(key)))
    return out


def monitor_tables(store, through: str | None = None) -> dict[str, pd.DataFrame]:
    reports = {r["slice"]: r for r in _json(store, "monitor/reports/", through)}
    slices = []
    for c in _json(store, "monitor/checks/", through):
        status = {r["check"]: r["status"] for r in c["results"]}
        slices.append({"slice": c["slice"], "weekday": reports.get(c["slice"], {}).get("weekday"),
                       "quarantined": c["path"] == "quarantine", **{k: status.get(k) for k in CHECKS}})

    daily = _json(store, "monitor/daily/", through)
    mix = [{"day": d["day"], "payment_format": f, "pct": pct}
           for d in daily for f, pct in d["format_mix_pct"].items()]

    shifts, alerts = [], []
    for r in reports.values():
        shifts += [{"slice": r["slice"], **s} for s in r["biggest_shifts"]]
        alerts += [{"slice": r["slice"], "rank": i, **a} for i, a in enumerate(r["top_alerts"], 1)]

    escalations = []
    if store.keys("monitor/escalations.log"):
        for line in store.get("monitor/escalations.log").decode().splitlines():
            e = json.loads(line) if line.strip() else None
            if e and (through is None or e["slice"] <= through):
                escalations.append({**e, "failed_checks": ", ".join(e["failed_checks"])})

    def frame(records, columns):
        return pd.DataFrame(records, columns=columns)

    return {
        "monitor_slices": frame(slices, ["slice", "weekday", "quarantined", *CHECKS]),
        "monitor_daily": frame([{k: d[k] for k in ("day", "weekday", "transfers", "alerts", "laundering_rate_pct")}
                                for d in daily], ["day", "weekday", "transfers", "alerts", "laundering_rate_pct"]),
        "monitor_format_mix": frame(mix, ["day", "payment_format", "pct"]),
        "monitor_reports": frame([{"slice": r["slice"], "weekday": r["weekday"], "quarantined": r["quarantined"],
                                   "failed_checks": ", ".join(r["failed_checks"]), "summary": r["summary"]}
                                  for r in reports.values()],
                                 ["slice", "weekday", "quarantined", "failed_checks", "summary"]),
        "monitor_shifts": frame(shifts, ["slice", "what", "today_pct", "normal_pct"]),
        "monitor_top_alerts": frame(alerts, ["slice", "rank", "time", "from_bank", "to_bank", "payment_format",
                                             "payment_currency", "amount_usd", "score"]),
        "monitor_escalations": frame(escalations, ["escalated_at", "slice", "weekday", "failed_checks", "summary"]),
    }


def analysis_tables(store) -> dict[str, pd.DataFrame]:
    """Each known Athena mart (all its part files) and each plainly named marts/model/ CSV."""
    parts: dict[str, list[pd.DataFrame]] = {}
    out = {}
    for key in store.keys("marts/"):
        bits = key.split("/")
        if len(bits) != 3 or not bits[2]:
            continue                                   # folder markers
        if bits[1] == "model":
            if key.endswith(".csv") and MODEL_TABLE.fullmatch(Path(key).stem):
                out[f"model_{Path(key).stem}"] = pd.read_csv(io.BytesIO(store.get(key)))
        elif bits[1] in MARTS:
            parts.setdefault(bits[1], []).append(pd.read_parquet(io.BytesIO(store.get(key))))
    for name, frames in parts.items():
        out[f"mart_{name}"] = pd.concat(frames, ignore_index=True)
    return out


def build_snapshot(store, path, through: str | None = None) -> dict[str, int]:
    """Write every table to <path> and return the row count of each."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tables = {**monitor_tables(store, through), **analysis_tables(store)}
    tmp = path.with_name(path.name + ".tmp")
    tmp.unlink(missing_ok=True)
    con = duckdb.connect()
    try:
        con.execute(f"ATTACH '{tmp}' AS snap (STORAGE_VERSION '{STORAGE_VERSION}')")
        for name, df in tables.items():
            con.register("frame", df)
            con.execute(f'CREATE TABLE snap."{name}" AS SELECT * FROM frame')
            con.unregister("frame")
        con.execute("DETACH snap")
    finally:
        con.close()
    os.replace(tmp, path)
    return {name: len(df) for name, df in tables.items()}


if __name__ == "__main__":
    import argparse

    from pipeline.aws import S3

    ap = argparse.ArgumentParser(description="Sync the Helical snapshot from S3.")
    ap.add_argument("--through", help="last slice to include (YYYY-MM-DD); default: all")
    args = ap.parse_args()
    counts = build_snapshot(S3(os.environ.get("AML_BUCKET", "aml-bq2")), SNAPSHOT_PATH, args.through)
    for name, n in counts.items():
        print(f"{name:28} {n:>7,}")
    print(f"-> {SNAPSHOT_PATH}")

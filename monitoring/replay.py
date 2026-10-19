"""
Replay HI-Small as a live feed: one slice per simulated hour (local mode).

The dataset is a finished simulation of 1-10 Sep 2022, so "live" means replaying it in time
order. Each hour of 1-10 Sep becomes its own slice. The 11-18 Sep tail is simulator leftovers
(57-73% laundering); only 11 Sep is replayed, as one day-long slice, for the quarantine demo.

Slices are raw: bank and account ids stay text (leading zeros), duplicates are kept, and there
are no derived columns. Recon compares them against the curated data.

    python -m monitoring.replay build      # cut every slice into out/stream/
    python -m monitoring.replay next       # print the next slice id and move the cursor
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv

REPO = Path(__file__).resolve().parent.parent
RAW_TRANS = Path(os.environ.get("AML_DATA_DIR", "~/aml_data")).expanduser() / "HI-Small_Trans.csv"
STREAM_DIR = REPO / "out" / "stream"

RAW_NAMES = ["ts", "from_bank", "from_account", "to_bank", "to_account", "amount_received",
             "receiving_currency", "amount_paid", "payment_currency", "payment_format", "is_laundering"]
ID_COLS = ["from_bank", "from_account", "to_bank", "to_account"]

FIRST_DAY = pd.Timestamp("2022-09-01")
TAIL_START = pd.Timestamp("2022-09-11")     # tail: only this day is replayed, as one slice
TAIL_END = pd.Timestamp("2022-09-12")


def read_raw(path: Path = RAW_TRANS) -> pd.DataFrame:
    """The Kaggle transactions CSV with snake_case names and text ids."""
    opts = pacsv.ConvertOptions(column_types={**{c: pa.string() for c in ID_COLS}, "ts": pa.timestamp("s")},
                                timestamp_parsers=["%Y/%m/%d %H:%M"])
    tbl = pacsv.read_csv(path, read_options=pacsv.ReadOptions(column_names=RAW_NAMES, skip_rows=1),
                         convert_options=opts)
    df = tbl.to_pandas()
    df["ts"] = df["ts"].astype("datetime64[ns]")
    return df


def slice_id(ts: pd.Series) -> pd.Series:
    """'YYYY-MM-DDTHH' for 1-10 Sep, 'YYYY-MM-DD' for the tail day, None outside the replay."""
    ids = ts.dt.strftime("%Y-%m-%dT%H").where((ts >= FIRST_DAY) & (ts < TAIL_START))
    return ids.fillna(ts.dt.strftime("%Y-%m-%d").where((ts >= TAIL_START) & (ts < TAIL_END)))


def slice_bounds(sid: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """[start, end) of a slice, for the quality check's timestamp rule."""
    start = pd.Timestamp(sid.replace("T", " ") + (":00" if "T" in sid else ""))
    return start, start + (pd.Timedelta("1h") if "T" in sid else pd.Timedelta("1D"))


def write_slices(df: pd.DataFrame, out_dir: Path = STREAM_DIR) -> list[str]:
    """Write out_dir/slice=<id>/part-0.parquet for every slice; returns the ids in replay order."""
    ids = slice_id(df["ts"])
    keep = ids.notna()
    for sid, part in df[keep].groupby(ids[keep], sort=True):
        d = out_dir / f"slice={sid}"
        d.mkdir(parents=True, exist_ok=True)
        part.sort_values("ts", kind="stable").to_parquet(d / "part-0.parquet", index=False)
    return list_slices(out_dir)


def list_slices(out_dir: Path = STREAM_DIR) -> list[str]:
    return sorted(p.name.removeprefix("slice=") for p in out_dir.glob("slice=*") if p.is_dir())


def load_slice(sid: str, out_dir: Path = STREAM_DIR) -> pd.DataFrame:
    return pd.read_parquet(out_dir / f"slice={sid}" / "part-0.parquet")


def next_slice(out_dir: Path = STREAM_DIR) -> str | None:
    """The next slice after the cursor in out_dir/_cursor.json, which it then records. None at the end."""
    cursor = out_dir / "_cursor.json"
    last = json.loads(cursor.read_text())["last"] if cursor.exists() else None
    todo = [s for s in list_slices(out_dir) if last is None or s > last]
    if not todo:
        return None
    cursor.write_text(json.dumps({"last": todo[0]}))
    return todo[0]


def reset_cursor(out_dir: Path = STREAM_DIR, start_after: str | None = None) -> None:
    """Restart the replay; start_after='2022-09-07T23' begins it at 8 Sep 00:00."""
    cursor = out_dir / "_cursor.json"
    if start_after is None:
        cursor.unlink(missing_ok=True)
    else:
        cursor.write_text(json.dumps({"last": start_after}))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["build", "next", "reset"])
    ap.add_argument("--start-after", help="for reset: slice id to resume after, e.g. 2022-09-07T23")
    args = ap.parse_args()
    if args.command == "build":
        ids = write_slices(read_raw())
        print(f"{len(ids)} slices in {STREAM_DIR}: {ids[0]} .. {ids[-1]}")
    elif args.command == "next":
        print(next_slice() or "end of replay")
    else:
        reset_cursor(start_after=args.start_after)
        print("cursor reset")

"""
Live monitoring checks run on every replayed slice (Live monitoring tab of the plan).

Each check returns a dict with at least {"check", "status", ...numbers}.
status is "pass" or "fail"; there is no warning level. The Airflow gate decides from these alone;
the check report (report.py) only states them.

Pure pandas / numpy so the same code runs in a notebook, a test, or an Airflow task.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

KNOWN_CURRENCIES = {
    "US Dollar", "Euro", "Yuan", "Yen", "Rupee", "Ruble", "Bitcoin", "Shekel",
    "Canadian Dollar", "Australian Dollar", "Swiss Franc", "UK Pound",
    "Brazil Real", "Saudi Riyal", "Mexican Peso",
}
KNOWN_FORMATS = {"ACH", "Bitcoin", "Cash", "Cheque", "Credit Card", "Reinvestment", "Wire"}
KEY_COLUMNS = ["ts", "from_bank", "from_account", "to_bank", "to_account",
               "amount_paid", "payment_currency", "payment_format", "is_laundering"]
AMOUNT_BANDS = [0, 100, 1_000, 10_000, 50_000, 100_000, 1_000_000, np.inf]
PSI_FAIL = 0.25                          # common rule of thumb for PSI
VOLUME_Z_FAIL = 3.0                      # spreads from the normal days' mean


# --------------------------------------------------------------------------- #
# 1. Reconciliation: nothing lost or invented between raw and curated
# --------------------------------------------------------------------------- #
def check_recon(raw: pd.DataFrame, curated: pd.DataFrame, known_duplicates: int = 0) -> dict:
    raw_rows, cur_rows = len(raw), len(curated)
    raw_usd_rows = raw["amount_paid"].sum()
    cur_amt = curated["amount_paid"].sum()
    raw_l, cur_l = int(raw["is_laundering"].sum()), int(curated["is_laundering"].sum())
    row_gap = raw_rows - known_duplicates - cur_rows
    ok = row_gap == 0 and np.isclose(raw_usd_rows, cur_amt, rtol=1e-9) and raw_l == cur_l
    out = {"check": "recon", "status": "pass" if ok else "fail",
           "raw_rows": raw_rows, "curated_rows": cur_rows, "row_gap": int(row_gap),
           "raw_laundering": raw_l, "curated_laundering": cur_l,
           "amount_gap": float(raw_usd_rows - cur_amt)}
    if not ok and "ts" in raw:
        # Name the days the missing rows fall on: points straight at weekend FX gaps
        missing = raw.merge(curated[["ts", "from_account", "to_account", "amount_paid"]],
                            how="left", indicator=True,
                            on=["ts", "from_account", "to_account", "amount_paid"])
        missing = missing[missing["_merge"] == "left_only"]
        days = pd.to_datetime(missing["ts"], format="%Y/%m/%d %H:%M", errors="coerce").dt.day_name()
        out["missing_by_weekday"] = days.value_counts().to_dict()
    return out


# --------------------------------------------------------------------------- #
# 2. Quality: hard rules every row must satisfy
# --------------------------------------------------------------------------- #
def check_quality(df: pd.DataFrame, slice_start=None, slice_end=None) -> dict:
    problems = {
        "null_key_values": int(df[KEY_COLUMNS].isna().any(axis=1).sum()),
        "unknown_currency": int((~df["payment_currency"].isin(KNOWN_CURRENCIES)).sum()),
        "unknown_format": int((~df["payment_format"].isin(KNOWN_FORMATS)).sum()),
        "non_positive_amount": int((df["amount_paid"] <= 0).sum()),
    }
    if "amount_paid_usd" in df:
        problems["missing_usd_amount"] = int(df["amount_paid_usd"].isna().sum())
    if slice_start is not None and slice_end is not None:
        ts = pd.to_datetime(df["ts"])
        problems["outside_slice_window"] = int(((ts < slice_start) | (ts >= slice_end)).sum())
    bad = sum(problems.values())
    return {"check": "quality", "status": "pass" if bad == 0 else "fail",
            "rows": len(df), "bad_rows": bad, **problems}


# --------------------------------------------------------------------------- #
# 3. Drift: population stability index against a baseline
# --------------------------------------------------------------------------- #
def psi(expected: pd.Series, actual: pd.Series, eps: float = 1e-4) -> float:
    """PSI between two distributions given as counts or shares indexed by category."""
    idx = expected.index.union(actual.index)
    e = expected.reindex(idx, fill_value=0).astype(float)
    a = actual.reindex(idx, fill_value=0).astype(float)
    e = e / e.sum() + eps
    a = a / a.sum() + eps
    return float(((a - e) * np.log(a / e)).sum())


def check_drift(baseline: pd.DataFrame, slice_df: pd.DataFrame,
                score_col: str | None = "score") -> dict:
    """baseline = the other normal days of the slice's kind (see run.normal_days)."""
    amt = "amount_paid_usd" if "amount_paid_usd" in slice_df else "amount_paid"
    metrics = {
        "psi_payment_format": psi(baseline["payment_format"].value_counts(),
                                  slice_df["payment_format"].value_counts()),
        "psi_currency": psi(baseline["payment_currency"].value_counts(),
                            slice_df["payment_currency"].value_counts()),
        "psi_amount_band": psi(pd.cut(baseline[amt], AMOUNT_BANDS).value_counts(),
                               pd.cut(slice_df[amt], AMOUNT_BANDS).value_counts()),
    }
    if score_col and score_col in baseline and score_col in slice_df:
        bins = np.linspace(0, 1, 11)
        metrics["psi_model_score"] = psi(pd.cut(baseline[score_col], bins, include_lowest=True).value_counts(),
                                         pd.cut(slice_df[score_col], bins, include_lowest=True).value_counts())
    worst = max(metrics, key=metrics.get)
    return {"check": "drift", "status": "fail" if metrics[worst] > PSI_FAIL else "pass", "worst_metric": worst,
            **{k: round(v, 3) for k, v in metrics.items()}}


# --------------------------------------------------------------------------- #
# 4. Volume: how many transfers, against the normal days
# --------------------------------------------------------------------------- #
def check_volume(slice_df: pd.DataFrame, baseline_volume: pd.Series, score_threshold: float,
                 z_fail: float = VOLUME_Z_FAIL, min_rel_std: float = 0.10) -> dict:
    """
    baseline_volume: transfers per day on the normal days the slice is compared with.
    score_threshold: the alert threshold; the day's alert count is reported, never judged.
    min_rel_std: floor on the volume spread, as a share of the baseline mean. A weekend day is
        compared with a single other weekend day, whose spread is zero.
    The hourly version also failed a slice for a "hot bank" (6+ alerts and 5x its normal share).
    Dropped 1 Nov: over a whole day it fired on 10 of 11 days for 6-9 alerts out of thousands.
    """
    n = len(slice_df)
    spread = max(baseline_volume.std(ddof=0), min_rel_std * baseline_volume.mean(), 1.0)
    vol_z = float((n - baseline_volume.mean()) / spread)
    laundering_rate = float(slice_df["is_laundering"].mean()) if "is_laundering" in slice_df else None
    return {"check": "volume", "status": "fail" if abs(vol_z) > z_fail else "pass",
            "rows": n, "normal_rows": round(float(baseline_volume.mean())), "volume_z": round(vol_z, 2),
            "flagged": int((slice_df["score"] >= score_threshold).sum()),
            "laundering_rate_pct": None if laundering_rate is None else round(100 * laundering_rate, 2)}


def gate(results: list[dict]) -> str:
    """Pass only if nothing failed."""
    return "fail" if any(r["status"] == "fail" for r in results) else "pass"

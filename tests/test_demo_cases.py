"""
The three live-monitor demo cases, run on the real replay slices.

Needs `python -m monitoring.replay build` and `python scripts/train_live_model.py` first;
skipped otherwise. The first run also scores and caches the baseline (a few minutes).
"""
import pandas as pd
import pytest

from monitoring import run
from monitoring.checks import PSI_FAIL
from monitoring.replay import STREAM_DIR

pytestmark = pytest.mark.skipif(
    not (STREAM_DIR / "slice=2022-09-11").exists() or not (run.LIVE_DIR / "model.json").exists(),
    reason="replay slices or live model missing")


def by_check(out):
    return {r["check"]: r for r in out["results"]}


def test_weekend_fx_gap_fails_recon_on_saturday():
    """A plain ECB join has no weekend rates: recon catches the lost rows and names the day."""
    fx, _ = run.reference_tables()
    weekday_fx = fx[pd.to_datetime(fx["rate_date"], format="%Y/%m/%d").dt.dayofweek < 5]
    out = run.run_checks("2022-09-03T12", fx=weekday_fx, write=False)
    recon = by_check(out)["recon"]
    assert recon["status"] == "fail" and recon["curated_rows"] == 0
    assert set(recon["missing_by_weekday"]) == {"Saturday"}
    assert out["path"] == "quarantine"


def test_month_start_batch_is_flagged_by_drift():
    """1 Sep 00:00: the Reinvestment batch shifts the payment-format mix far from a normal midnight."""
    drift = by_check(run.run_checks("2022-09-01T00", write=False))["drift"]
    assert drift["status"] == "fail"
    assert drift["psi_payment_format"] > PSI_FAIL


def test_tail_day_is_quarantined():
    """11 Sep: 396 simulator leftovers, 58.6% laundering, nothing like a normal day."""
    out = run.run_checks("2022-09-11", write=False)
    anomaly = by_check(out)["anomaly"]
    assert anomaly["rows"] == 396 and anomaly["laundering_rate_pct"] == pytest.approx(58.6, abs=0.05)
    assert out["path"] == "quarantine"



@pytest.mark.parametrize("sid", ["2022-09-08T10", "2022-09-10T16"])   # a weekday and a Saturday hour
def test_normal_hours_pass_the_gate(sid):
    """Guard against checks that fail everything: then the demo cases above would pass for no reason."""
    out = run.run_checks(sid, write=False)
    assert out["path"] == "build_marts", {r["check"]: r["status"] for r in out["results"]}

"""
The live monitor on the real daily slices: only the three natural failures fail.

Needs `python -m monitoring.replay build` and `python scripts/train_live_model.py` first;
skipped otherwise. The first run also scores and caches the normal days (a few minutes).
"""
import pytest

from monitoring import run
from monitoring.checks import PSI_FAIL
from monitoring.replay import STREAM_DIR

pytestmark = pytest.mark.skipif(
    not (STREAM_DIR / "slice=2022-09-11").exists() or not (run.LIVE_DIR / "model.json").exists(),
    reason="replay slices or live model missing")


def by_check(out):
    return {r["check"]: r for r in out["results"]}


def test_month_start_batch_is_flagged_by_drift():
    """1 Sep: the Reinvestment batch shifts the payment-format mix far from a normal weekday."""
    out = run.run_checks("2022-09-01", write=False)
    drift = by_check(out)["drift"]
    assert drift["status"] == "fail" and drift["psi_payment_format"] > PSI_FAIL
    assert out["path"] == "quarantine"


def test_tail_day_is_quarantined():
    """11 Sep: 396 simulator leftovers, 58.6% laundering, nothing like a normal day."""
    out = run.run_checks("2022-09-11", write=False)
    volume = by_check(out)["volume"]
    assert volume["status"] == "fail" and volume["rows"] == 396
    assert volume["laundering_rate_pct"] == pytest.approx(58.6, abs=0.05)
    assert out["path"] == "quarantine"


def test_busy_friday_fails_on_volume():
    """2 Sep: 754,449 transfers, the busiest normal weekday, 3.45 spreads above the others."""
    out = run.run_checks("2022-09-02", write=False)
    assert by_check(out)["volume"]["status"] == "fail" and by_check(out)["drift"]["status"] == "pass"
    assert out["path"] == "quarantine"


@pytest.mark.parametrize("sid", [f"2022-09-{d:02d}" for d in range(3, 11)])
def test_normal_days_pass_the_gate(sid):
    """Guard against checks that fail everything: then the cases above would pass for no reason."""
    out = run.run_checks(sid, write=False)
    assert out["path"] == "build_marts", {r["check"]: r for r in out["results"] if r["status"] == "fail"}

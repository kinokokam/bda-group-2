import pandas as pd
import pytest

from monitoring.checks import check_drift, check_volume
from monitoring.run import normal_days


def transfers(ach: int, cheque: int) -> pd.DataFrame:
    """Transfers that differ only in payment format, so only the format PSI can move."""
    n = ach + cheque
    return pd.DataFrame({"payment_format": ["ACH"] * ach + ["Cheque"] * cheque,
                         "payment_currency": ["US Dollar"] * n, "amount_paid_usd": [500.0] * n})


NORMAL = transfers(500, 500)


@pytest.mark.parametrize("ach, status", [
    (500, "pass"),      # same mix: PSI 0
    (690, "pass"),      # PSI about 0.15: was a "warn" until 26 Oct, now simply passes
    (900, "fail"),      # PSI about 0.88, over the 0.25 line
])
def test_drift_is_pass_or_fail_only(ach, status):
    out = check_drift(NORMAL, transfers(ach, 1000 - ach), score_col=None)
    assert out["status"] == status


@pytest.mark.parametrize("day, compared_with", [
    ("2022-09-01", ["2022-09-02", "2022-09-05", "2022-09-06", "2022-09-07", "2022-09-08", "2022-09-09"]),
    ("2022-09-02", ["2022-09-05", "2022-09-06", "2022-09-07", "2022-09-08", "2022-09-09"]),
    ("2022-09-06", ["2022-09-02", "2022-09-05", "2022-09-07", "2022-09-08", "2022-09-09"]),
    ("2022-09-03", ["2022-09-04"]),
    ("2022-09-04", ["2022-09-03"]),
    ("2022-09-10", ["2022-09-03", "2022-09-04"]),
    ("2022-09-11", ["2022-09-03", "2022-09-04"]),
])
def test_a_day_is_compared_with_the_other_normal_days_of_its_kind(day, compared_with):
    assert normal_days(day) == compared_with


NORMAL_VOLUME = pd.Series([480_000, 482_000, 484_000, 650_000, 750_000])     # five normal weekdays


def scored(rows: int, bank_flags: int = 0) -> pd.DataFrame:
    """rows transfers; the first bank_flags are flagged and all sent by one bank."""
    return pd.DataFrame({"score": [0.99] * bank_flags + [0.1] * (rows - bank_flags),
                         "from_bank": ["0210575"] * rows, "is_laundering": [0] * rows})


@pytest.mark.parametrize("rows, status", [
    (520_000, "pass"),    # about the normal mean
    (950_000, "fail"),    # mean 569,200, spread 111,400: 3.42 spreads above
    (400, "fail"),        # nearly empty, like the 11 Sep tail
])
def test_volume_fails_beyond_three_spreads(rows, status):
    assert check_volume(scored(rows), NORMAL_VOLUME, score_threshold=0.95)["status"] == status


def test_flags_piling_up_at_one_bank_do_not_fail_the_day():
    """The per-day hot-bank rule was dropped on 1 Nov: it flagged 6-9 transfers out of thousands."""
    out = check_volume(scored(520_000, bank_flags=500), NORMAL_VOLUME, score_threshold=0.95)
    assert out["status"] == "pass" and out["flagged"] == 500

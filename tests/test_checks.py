import pandas as pd
import pytest

from monitoring.checks import check_drift
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

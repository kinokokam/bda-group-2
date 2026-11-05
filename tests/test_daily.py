import pytest

from monitoring.daily import daily_row

VOLUME = {"check": "volume", "status": "pass", "rows": 520000, "normal_rows": 516945, "volume_z": 0.06,
          "flagged": 2210, "laundering_rate_pct": 0.09}


def checked(path="build_marts"):
    return {"slice": "2022-09-05", "gate": "pass" if path == "build_marts" else "fail", "path": path,
            "results": [{"check": "recon", "status": "pass"}, VOLUME],
            "facts": {"weekday": "Monday", "format_mix_pct": {"Cheque": 41.0, "ACH": 14.0}}}


def test_a_passed_day_becomes_one_row_of_the_monitors_daily_charts():
    assert daily_row(checked()) == {"day": "2022-09-05", "weekday": "Monday", "transfers": 520000,
                                    "alerts": 2210, "laundering_rate_pct": 0.09,
                                    "format_mix_pct": {"Cheque": 41.0, "ACH": 14.0}}


def test_a_quarantined_day_never_reaches_the_daily_charts():
    with pytest.raises(ValueError, match="quarantined"):
        daily_row(checked("quarantine"))

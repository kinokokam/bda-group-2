import json

import pytest

from monitoring import escalations, run
from monitoring.report import write_report

PASSED = [{"check": "recon", "status": "pass", "raw_rows": 100, "curated_rows": 100, "row_gap": 0},
          {"check": "quality", "status": "pass", "rows": 100, "bad_rows": 0},
          {"check": "drift", "status": "pass", "worst_metric": "psi_amount_band", "psi_payment_format": 0.01,
           "psi_amount_band": 0.02},
          {"check": "volume", "status": "pass", "rows": 100, "normal_rows": 100, "volume_z": 0.0}]
FACTS = {"weekday": "Thursday", "inside_simulation_window_1_to_10_sep": True,
         "top_alerts": [{"time": "14:33", "from_bank": "070", "score": 0.99}],
         "biggest_shifts": [{"what": "payment_currency: Euro", "today_pct": 30.0, "normal_pct": 20.0},
                            {"what": "payment_format: Reinvestment", "today_pct": 43.1, "normal_pct": 0.0}]}


def checked(results, facts=FACTS, sid="2022-09-01"):
    gate = "fail" if any(r["status"] == "fail" for r in results) else "pass"
    return {"slice": sid, "gate": gate, "path": "quarantine" if gate == "fail" else "build_marts",
            "results": results, "facts": facts}


def failing(**changes):
    """PASSED with the named checks replaced: changes maps check name -> its failed result."""
    return [changes.get(r["check"], r) for r in PASSED]


def test_a_day_that_passes_says_so_in_one_line():
    report = write_report(checked(PASSED, sid="2022-09-05"))
    assert report["summary"] == "Passed all 4 checks."
    assert report["quarantined"] is False and report["failed_checks"] == []


def test_failed_drift_and_volume_are_reported_with_their_numbers():
    """The 1 Sep wording agreed on 2 Nov, with the real 1 Nov numbers."""
    results = failing(
        drift={"check": "drift", "status": "fail", "worst_metric": "psi_payment_format",
               "psi_payment_format": 3.866, "psi_amount_band": 0.201},
        volume={"check": "volume", "status": "fail", "rows": 1114919, "normal_rows": 556529, "volume_z": 5.15})
    report = write_report(checked(results))
    assert report["summary"] == (
        "Failed 2 of 4 checks: drift and volume. "
        "Drift: payment format PSI 3.866 (fails above 0.25); Reinvestment was 43.1% of transfers, "
        "against 0.0% on normal days. "
        "Volume: 1,114,919 transfers, against a normal 556,529 a day (z 5.15; fails beyond ±3). "
        "The day is quarantined.")
    assert report["quarantined"] is True and report["failed_checks"] == ["drift", "volume"]
    assert report["weekday"] == "Thursday" and report["top_alerts"] == FACTS["top_alerts"]


def test_a_day_outside_the_simulation_says_so():
    results = failing(volume={"check": "volume", "status": "fail", "rows": 396, "normal_rows": 207406,
                              "volume_z": -9.98})
    facts = {**FACTS, "inside_simulation_window_1_to_10_sep": False}
    assert write_report(checked(results, facts, "2022-09-11"))["summary"] == (
        "Failed 1 of 4 checks: volume. "
        "Volume: 396 transfers, against a normal 207,406 a day (z -9.98; fails beyond ±3). "
        "The date is outside the 1–10 Sep simulation. The day is quarantined.")


def test_drift_on_amounts_quotes_no_format_shift():
    results = failing(drift={"check": "drift", "status": "fail", "worst_metric": "psi_amount_band",
                             "psi_payment_format": 0.01, "psi_amount_band": 1.769})
    assert write_report(checked(results))["summary"] == (
        "Failed 1 of 4 checks: drift. Drift: amount band PSI 1.769 (fails above 0.25). The day is quarantined.")


def test_failed_recon_and_quality_are_reported_with_their_numbers():
    results = failing(
        recon={"check": "recon", "status": "fail", "raw_rows": 1000, "curated_rows": 988, "row_gap": 12},
        quality={"check": "quality", "status": "fail", "rows": 988, "bad_rows": 3})
    assert write_report(checked(results))["summary"] == (
        "Failed 2 of 4 checks: recon and quality. "
        "Recon: 1,000 raw rows became 988 curated rows (12 missing). "
        "Quality: 3 of 988 rows broke a rule. "
        "The day is quarantined.")


def test_three_failed_checks_are_listed_with_commas():
    results = failing(
        recon={"check": "recon", "status": "fail", "raw_rows": 10, "curated_rows": 9, "row_gap": 1},
        quality={"check": "quality", "status": "fail", "rows": 9, "bad_rows": 1},
        volume={"check": "volume", "status": "fail", "rows": 9, "normal_rows": 100, "volume_z": -4.0})
    assert write_report(checked(results))["summary"].startswith(
        "Failed 3 of 4 checks: recon, quality and volume. ")


@pytest.fixture
def monitor_dirs(tmp_path, monkeypatch):
    monkeypatch.setattr(run, "CHECKS_DIR", tmp_path / "checks")
    monkeypatch.setattr(run, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(escalations, "ESCALATIONS_LOG", tmp_path / "escalations.log")
    (tmp_path / "checks").mkdir()

    def save(out):
        (tmp_path / "checks" / f"{out['slice']}.json").write_text(json.dumps(out))
    return tmp_path, save


def test_a_quarantined_day_is_reported_and_escalated(monitor_dirs):
    tmp, save = monitor_dirs
    save(checked(failing(volume={"check": "volume", "status": "fail", "rows": 9, "normal_rows": 100,
                                 "volume_z": -4.0}), sid="2022-09-02"))
    report = run.report_slice("2022-09-02")
    assert json.loads((tmp / "reports" / "2022-09-02.json").read_text()) == report
    [entry] = escalations.read_escalations()
    assert entry["slice"] == "2022-09-02" and entry["failed_checks"] == ["volume"]
    assert entry["summary"] == report["summary"]


def test_a_day_that_passes_is_not_escalated(monitor_dirs):
    tmp, save = monitor_dirs
    save(checked(PASSED, sid="2022-09-05"))
    run.report_slice("2022-09-05")
    assert escalations.read_escalations() == []

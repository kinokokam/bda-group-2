from monitoring.escalations import read_escalations, send_escalation

REPORT = {"slice": "2022-09-11", "weekday": "Sunday", "quarantined": True,
          "failed_checks": ["drift", "volume"], "summary": "Failed 2 of 4 checks: drift and volume."}


def test_a_quarantined_report_is_logged(tmp_path):
    log = tmp_path / "escalations.log"
    assert send_escalation(REPORT, log) is True
    [entry] = read_escalations(log)
    assert entry["slice"] == "2022-09-11" and entry["failed_checks"] == ["drift", "volume"]
    assert entry["summary"] == REPORT["summary"] and "escalated_at" in entry


def test_a_report_that_was_not_quarantined_is_not_logged(tmp_path):
    log = tmp_path / "escalations.log"
    assert send_escalation({**REPORT, "quarantined": False, "failed_checks": []}, log) is False
    assert read_escalations(log) == []


def test_escalations_append(tmp_path):
    log = tmp_path / "escalations.log"
    send_escalation(REPORT, log)
    send_escalation({**REPORT, "slice": "2022-09-01"}, log)
    assert [a["slice"] for a in read_escalations(log)] == ["2022-09-11", "2022-09-01"]


def test_rerunning_a_day_replaces_its_escalation(tmp_path):
    """Airflow may run a day again (retry, backfill); the log keeps one entry per day."""
    log = tmp_path / "escalations.log"
    send_escalation(REPORT, log)
    send_escalation({**REPORT, "slice": "2022-09-01"}, log)
    send_escalation({**REPORT, "summary": "Failed 1 of 4 checks: volume."}, log)
    assert [(a["slice"], a["summary"]) for a in read_escalations(log)] == [
        ("2022-09-01", REPORT["summary"]), ("2022-09-11", "Failed 1 of 4 checks: volume.")]


def test_a_day_that_passes_on_rerun_loses_its_escalation(tmp_path):
    log = tmp_path / "escalations.log"
    send_escalation(REPORT, log)
    send_escalation({**REPORT, "quarantined": False, "failed_checks": []}, log)
    assert read_escalations(log) == []

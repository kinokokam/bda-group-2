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

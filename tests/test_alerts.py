from monitoring.alerts import read_alerts, send_alert

NOTE = {"slice": "2022-09-11", "severity": "high", "verdict": "data issue",
        "summary": "396 transfers, 58.59% laundering.", "suggested_action": "Keep quarantined.",
        "unverified_numbers": [], "label": "AI-generated note, review before acting"}


def test_high_severity_is_logged_with_the_gate_path(tmp_path):
    log = tmp_path / "alerts.log"
    assert send_alert(NOTE, "quarantine", log) is True
    [entry] = read_alerts(log)
    assert entry["slice"] == "2022-09-11" and entry["path"] == "quarantine" and entry["verdict"] == "data issue"
    assert entry["label"].startswith("AI-generated")


def test_low_and_medium_are_not_logged(tmp_path):
    log = tmp_path / "alerts.log"
    for sev in ("low", "medium"):
        assert send_alert({**NOTE, "severity": sev}, "build_marts", log) is False
    assert read_alerts(log) == []


def test_alerts_append(tmp_path):
    log = tmp_path / "alerts.log"
    send_alert(NOTE, "quarantine", log)
    send_alert({**NOTE, "slice": "2022-09-01T00"}, "quarantine", log)
    assert [a["slice"] for a in read_alerts(log)] == ["2022-09-11", "2022-09-01T00"]

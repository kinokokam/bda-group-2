from monitoring.escalations import read_escalations, send_escalation

NOTE = {"slice": "2022-09-11", "severity": "high", "verdict": "data issue",
        "summary": "396 transfers, 58.59% laundering.", "suggested_action": "Keep quarantined.",
        "unverified_numbers": [], "label": "AI-generated note, review before acting"}


def test_high_severity_is_logged_with_the_gate_path(tmp_path):
    log = tmp_path / "escalations.log"
    assert send_escalation(NOTE, "quarantine", log) is True
    [entry] = read_escalations(log)
    assert entry["slice"] == "2022-09-11" and entry["path"] == "quarantine" and entry["verdict"] == "data issue"
    assert entry["label"].startswith("AI-generated")


def test_low_and_medium_are_not_logged(tmp_path):
    log = tmp_path / "escalations.log"
    for sev in ("low", "medium"):
        assert send_escalation({**NOTE, "severity": sev}, "build_marts", log) is False
    assert read_escalations(log) == []


def test_escalations_append(tmp_path):
    log = tmp_path / "escalations.log"
    send_escalation(NOTE, "quarantine", log)
    send_escalation({**NOTE, "slice": "2022-09-01"}, "quarantine", log)
    assert [a["slice"] for a in read_escalations(log)] == ["2022-09-11", "2022-09-01"]

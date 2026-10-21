import json

import pytest

from agent.triage_agent import FakeLLM, triage
from monitoring import alerts, run

CHECKS = [{"check": "drift", "status": "fail", "psi_payment_format": 7.04},
          {"check": "anomaly", "status": "fail", "rows": 344208, "volume_z": 67.48}]
FACTS = {"day_of_month": 1, "is_month_start": True, "top_format_share_pct": 68.6}
NOTE = {"severity": "low", "verdict": "expected",
        "summary": "The 1st of the month brings a batch of 344208 transfers, 68.6% Reinvestment.",
        "evidence": ["344208", "68.6", "7.04"], "suggested_action": "Release the slice after review."}


def test_note_carries_usage_label_and_no_unverified_numbers():
    fake = FakeLLM(NOTE, input_tokens=812, output_tokens=95)
    out = triage("2022-09-01T00", CHECKS, FACTS, llm=fake)
    assert out["verdict"] == "expected" and out["slice"] == "2022-09-01T00"
    assert out["usage"] == {"input_tokens": 812, "output_tokens": 95, "total_tokens": 907}
    assert out["unverified_numbers"] == [] and out["label"].startswith("AI-generated")
    assert "344208" in fake.calls[0][1][1]                  # the checks reached the model


def test_made_up_numbers_are_flagged():
    note = {**NOTE, "summary": "Volume is 15 times normal.", "evidence": ["15"]}
    assert triage("2022-09-01T00", CHECKS, FACTS, llm=FakeLLM(note))["unverified_numbers"] == ["15"]


@pytest.mark.parametrize("severity, alerted", [("high", 1), ("low", 0)])
def test_triage_slice_writes_the_note_and_alerts_only_high(tmp_path, monkeypatch, severity, alerted):
    monkeypatch.setattr(run, "CHECKS_DIR", tmp_path / "checks")
    monkeypatch.setattr(run, "NOTES_DIR", tmp_path / "notes")
    monkeypatch.setattr(alerts, "ALERTS_LOG", tmp_path / "alerts.log")
    (tmp_path / "checks").mkdir()
    (tmp_path / "checks" / "2022-09-11.json").write_text(json.dumps(
        {"slice": "2022-09-11", "path": "quarantine", "results": CHECKS, "facts": FACTS}))
    run.triage_slice("2022-09-11", llm=FakeLLM({**NOTE, "severity": severity}))
    assert json.loads((tmp_path / "notes" / "2022-09-11.json").read_text())["severity"] == severity
    assert len(alerts.read_alerts()) == alerted


def test_numbers_match_with_or_without_trailing_zeros():
    note = {**NOTE, "summary": "100% ACH.", "evidence": ["100"]}
    assert triage("2022-09-11", CHECKS, {"top_format_share_pct": 100.0}, llm=FakeLLM(note))["unverified_numbers"] == []

import json

import pytest

from agent.triage_agent import FakeLLM, triage
from monitoring import escalations, run

CHECKS = [{"check": "drift", "status": "fail", "psi_payment_format": 7.04},
          {"check": "volume", "status": "fail", "rows": 344208, "volume_z": 67.48}]
FACTS = {"day_of_month": 1, "is_month_start": True, "top_format_share_pct": 68.6}
NOTE = {"severity": "low", "verdict": "expected",
        "summary": "The 1st of the month brings a batch of 344208 transfers, 68.6% Reinvestment.",
        "evidence": ["344208", "68.6", "7.04"], "suggested_action": "Release the slice after review."}


def test_note_carries_usage_label_and_no_unverified_numbers():
    fake = FakeLLM(NOTE, input_tokens=812, output_tokens=95)
    out = triage("2022-09-01", CHECKS, FACTS, llm=fake)
    assert out["verdict"] == "expected" and out["slice"] == "2022-09-01"
    assert out["usage"] == {"input_tokens": 812, "output_tokens": 95, "total_tokens": 907}
    assert out["unverified_numbers"] == [] and out["label"].startswith("AI-generated")
    assert "344208" in fake.calls[0][1][1]                  # the checks reached the model


def test_made_up_numbers_are_flagged():
    note = {**NOTE, "summary": "Volume is 15 times normal.", "evidence": ["15"]}
    assert triage("2022-09-01", CHECKS, FACTS, llm=FakeLLM(note))["unverified_numbers"] == ["15"]


@pytest.fixture
def monitor_dirs(tmp_path, monkeypatch):
    monkeypatch.setattr(run, "CHECKS_DIR", tmp_path / "checks")
    monkeypatch.setattr(run, "NOTES_DIR", tmp_path / "notes")
    monkeypatch.setattr(escalations, "ESCALATIONS_LOG", tmp_path / "escalations.log")
    (tmp_path / "checks").mkdir()

    def checked(sid, gate):
        results = CHECKS if gate == "fail" else [{**c, "status": "pass"} for c in CHECKS]
        (tmp_path / "checks" / f"{sid}.json").write_text(json.dumps(
            {"slice": sid, "gate": gate, "path": "quarantine" if gate == "fail" else "build_marts",
             "results": results, "facts": FACTS}))
    return tmp_path, checked


def test_a_day_that_passes_gets_a_note_without_calling_claude(monitor_dirs):
    tmp, checked = monitor_dirs
    checked("2022-09-05", "pass")
    fake = FakeLLM({**NOTE, "severity": "high"})
    note = run.triage_slice("2022-09-05", llm=fake)
    assert fake.calls == []
    assert note["summary"] == "All checks passed." and note["severity"] == "low"
    assert json.loads((tmp / "notes" / "2022-09-05.json").read_text()) == note
    assert escalations.read_escalations() == []


@pytest.mark.parametrize("severity, escalated", [("high", 1), ("low", 0)])
def test_a_failed_day_gets_a_claude_note_escalated_only_if_high(monitor_dirs, severity, escalated):
    tmp, checked = monitor_dirs
    checked("2022-09-11", "fail")
    fake = FakeLLM({**NOTE, "severity": severity})
    run.triage_slice("2022-09-11", llm=fake)
    assert len(fake.calls) == 1
    assert json.loads((tmp / "notes" / "2022-09-11.json").read_text())["severity"] == severity
    assert len(escalations.read_escalations()) == escalated


def test_numbers_match_with_or_without_trailing_zeros():
    note = {**NOTE, "summary": "100% ACH.", "evidence": ["100"]}
    assert triage("2022-09-11", CHECKS, {"top_format_share_pct": 100.0}, llm=FakeLLM(note))["unverified_numbers"] == []


def test_a_date_range_is_not_a_negative_number():
    note = {**NOTE, "summary": "Dated outside 1-10 September.", "evidence": []}
    assert triage("2022-09-11", CHECKS, {"inside_1_to_10": False}, llm=FakeLLM(note))["unverified_numbers"] == []


def test_numbers_quoted_from_the_report_card_are_verified():
    facts = {**FACTS, "top_alerts": [{"from_bank": "070", "amount_usd": 12345.67, "score": 0.991}]}
    note = {**NOTE, "summary": "The top alert is US$12345.67 from bank 070, score 0.991.", "evidence": ["12345.67"]}
    assert triage("2022-09-01", CHECKS, facts, llm=FakeLLM(note))["unverified_numbers"] == []

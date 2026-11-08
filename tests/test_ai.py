"""The investigator brief (frontend/ai.py), with a stubbed Bedrock client: no AWS calls."""

import sys
from pathlib import Path
from types import SimpleNamespace

import anthropic
import httpx2
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "frontend"))

import ai  # noqa: E402
import metrics  # noqa: E402

ALERT = {
    "txn_id": "2022-09-10-11-0000042", "event_time": "2022-09-10 11:57:00", "score": 0.7241, "label": 1,
    "from_bank": "011", "from_account": "801F05EF0", "to_bank": "012", "to_account": "80016D280",
    "amount_paid_usd": 12104.04, "amount": 12104.04, "payment_currency": "US Dollar", "payment_format": "ACH",
    "is_cross_bank": 1, "is_cross_currency": 0, "is_structuring_threshold": 0,
    "sender_tx_count_24h": 3, "receiver_tx_count_24h": 1, "raw_margin": 5.1,
}
REASONS = metrics.parse_contributors(
    '[{"feature": "payment_format_code", "value": 0.0, "contribution": 2.1},'
    ' {"feature": "sender_burst_ratio_1h_24h", "value": 18.5, "contribution": 0.9}]',
    ALERT,
)


def history(rows: int, account: str = "801F05EF0") -> pd.DataFrame:
    return pd.DataFrame({
        "run_id": ["backfill"] * rows,
        "event_time": [f"2022-09-10 11:{59 - i:02d}:00" for i in range(rows)],
        "direction": ["Sent" if i % 2 == 0 else "Received" for i in range(rows)],
        "from_account": [account if i % 2 == 0 else "8OTHER000" for i in range(rows)],
        "to_account": ["8OTHER000" if i % 2 == 0 else account for i in range(rows)],
        "amount_paid_usd": [1080.0] * rows, "amount": [1000.0] * rows, "payment_currency": ["Euro"] * rows,
        "payment_format": ["ACH"] * rows, "score": [0.05] * rows, "is_alert": [i == 0 for i in range(rows)],
        "label": [1] * rows,
    })


def test_prompt_holds_the_alert_its_factors_and_both_histories():
    prompt = ai.brief_prompt(ALERT, REASONS, history(3), history(2, "80016D280"))
    assert "12,104.04 US Dollar" in prompt and "risk score: 72% (High priority)" in prompt
    assert "flags: cross-bank" in prompt
    assert "Payment format = ACH: +2.10" in prompt
    assert '<sender_history account="801F05EF0">' in prompt and '<receiver_history account="80016D280">' in prompt
    assert "1,000.00 Euro" in prompt                     # amounts in their own currency


def test_factors_that_are_easy_to_misread_carry_their_meaning():
    prompt = ai.brief_prompt(ALERT, REASONS, history(1), history(1))
    assert "Sender burst, hour against day = 18.50 (a ratio, not a count" in prompt
    assert set(ai.FACTOR_NOTES) <= set(metrics.FEATURE_LABELS)     # notes only for real model features


def test_prompt_never_carries_the_outcome():
    prompt = ai.brief_prompt(ALERT, REASONS, history(3), history(3)).lower()
    assert "label" not in prompt and "laundering = " not in prompt and "is_laundering" not in prompt
    assert "raw_margin" not in prompt


def test_a_transfer_to_the_same_account_says_so():
    own = history(1).assign(to_account="801F05EF0")      # Sent from the account to itself
    sender = ai.brief_prompt(ALERT, REASONS, own, history(0)).split("<sender_history")[1]
    assert "Sent to itself (same account)" in sender


def test_long_histories_are_trimmed_and_say_so():
    prompt = ai.brief_prompt(ALERT, REASONS, history(60), history(0))
    sender = prompt.split("<sender_history")[1].split("</sender_history>")[0]
    assert sender.count(" | ") // 5 == ai.HISTORY_ROWS
    assert f"60 transfers on record; the {ai.HISTORY_ROWS} most recent are listed" in sender
    assert "No other transfers on record." in prompt.split("<receiver_history")[1]


def test_history_runs_oldest_first_with_each_transfer_placed_against_the_alert():
    # history(4): 11:59, 11:58, 11:57, 11:56; the alert is at 11:57.
    sender = ai.brief_prompt(ALERT, REASONS, history(4), history(0)).split("<sender_history")[1]
    times = [line.split(" (")[0] for line in sender.splitlines() if line.startswith("2022-")]
    assert times == sorted(times)
    assert "2022-09-10 11:56:00 (-1 min)" in sender
    assert "2022-09-10 11:57:00 (same minute)" in sender
    assert "2022-09-10 11:59:00 (+2 min)" in sender


def test_the_alerts_own_row_is_marked_in_both_histories():
    own = pd.DataFrame({
        "run_id": ["r"], "event_time": [ALERT["event_time"]], "direction": ["Sent"],
        "from_account": [ALERT["from_account"]], "to_account": [ALERT["to_account"]],
        "amount_paid_usd": [ALERT["amount_paid_usd"]], "amount": [ALERT["amount"]],
        "payment_currency": ["US Dollar"], "payment_format": ["ACH"], "score": [ALERT["score"]], "is_alert": [True],
    })
    received = own.assign(direction="Received")
    prompt = ai.brief_prompt(ALERT, REASONS, pd.concat([history(3), own]), received)
    assert prompt.count("THIS ALERT") == 2
    assert prompt.split("<sender_history")[1].split("</sender_history>")[0].count("THIS ALERT") == 1


def test_counts_are_computed_not_left_to_the_model():
    sender = ai.brief_prompt(ALERT, REASONS, history(4), history(0)).split("<sender_history")[1]
    # Before 11:57: only 11:56 (Received from 8OTHER000). After: 11:58 (Received) and 11:59 (Sent).
    assert ("Counted in the 24 hours before the alert, not including it: 0 sent, 1 received, 1 distinct counterparties. "
            "After the alert: 2 transfers (1 sent, 1 received).") in sender
    assert "Counted by day, all transfers on record including this alert: 2022-09-10: 2 sent, 2 received." in sender


@pytest.mark.parametrize("minutes, label", [(0, "same minute"), (-12, "-12 min"), (210, "+3.5 h"), (-2880, "-2.0 d")])
def test_offset_labels(minutes, label):
    assert ai.offset_label(minutes) == label


def test_alert_without_factors_says_so():
    empty = metrics.parse_contributors(None)
    assert "No factors were recorded" in ai.brief_prompt(ALERT, empty, history(1), history(1))


class StubMessages:
    def __init__(self, response=None, error=None):
        self.response, self.error, self.request = response, error, None

    def create(self, **request):
        self.request = request
        if self.error:
            raise self.error
        return self.response


def stub_client(**kwargs):
    return SimpleNamespace(messages=StubMessages(**kwargs))


def reply(*texts, stop_reason="end_turn"):
    content = [SimpleNamespace(type="text", text=text) for text in texts]
    return SimpleNamespace(content=content, stop_reason=stop_reason)


def test_explain_sends_the_brief_request_and_joins_text():
    client = stub_client(response=reply("**Why it scored high**", "ACH carried most of the risk."))
    assert ai.explain("prompt", client=client) == "**Why it scored high**\n\nACH carried most of the risk."
    request = client.messages.request
    assert request["model"] == ai.MODEL and request["system"] == ai.SYSTEM
    assert request["messages"] == [{"role": "user", "content": "prompt"}]
    assert not {"temperature", "top_p", "top_k"} & set(request)


def api_error(cls, status):
    response = httpx2.Response(status, request=httpx2.Request("POST", "https://bedrock.example/invoke"))
    return cls("error", response=response, body=None)


@pytest.mark.parametrize("error, says", [
    (api_error(anthropic.PermissionDeniedError, 403), "No access"),
    (api_error(anthropic.RateLimitError, 429), "throttling"),
    (api_error(anthropic.InternalServerError, 500), r"error \(500\)"),
])
def test_explain_turns_failures_into_a_message_for_the_analyst(error, says):
    with pytest.raises(ai.BriefUnavailable, match=says):
        ai.explain("prompt", client=stub_client(error=error))


def test_explain_reports_a_refusal_or_an_empty_reply():
    with pytest.raises(ai.BriefUnavailable, match="declined"):
        ai.explain("prompt", client=stub_client(response=reply("", stop_reason="refusal")))
    with pytest.raises(ai.BriefUnavailable, match="empty"):
        ai.explain("prompt", client=stub_client(response=reply("  ")))


def test_the_brief_can_be_turned_off(monkeypatch):
    monkeypatch.setenv("BQ1_AI", "off")
    assert not ai.available()
    monkeypatch.delenv("BQ1_AI")
    assert ai.available()

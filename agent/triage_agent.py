"""
Claude triage agent for one replayed slice (Live monitoring tab of the plan).

The checks in monitoring/checks.py decide pass or fail. This agent only reads their
results plus a few fixed, read-only facts, and writes a short structured note:
severity, verdict, a three-sentence summary, the numbers it used, a suggested action.

Install:  pip install langchain-core pydantic  plus ONE of:
          langchain-aws        (Amazon Bedrock, uses your AWS profile)
          langchain-anthropic  (Anthropic API, uses ANTHROPIC_API_KEY from .env)
Keys and model IDs come from environment variables only; never hard-code them.
"""
from __future__ import annotations

import json
import os
import re
from typing import Literal

from pydantic import BaseModel, Field


class Triage(BaseModel):
    severity: Literal["low", "medium", "high"]
    verdict: Literal["expected", "investigate", "data issue"]
    summary: str = Field(description="At most three short sentences in plain English.")
    evidence: list[str] = Field(description="Each number used, copied exactly from the input.")
    suggested_action: str


SYSTEM = """You are an anti-money-laundering data analyst reviewing one hourly slice of
transactions that has just been checked by an automated pipeline.
You receive the check results and some baseline facts as JSON.
Rules:
- Use only numbers that appear in the input. Never invent figures.
- Decide whether the slice shows expected behaviour (for example a month-start batch),
  something a risk team should investigate, or a data problem.
- The pass/fail status is already decided by code. Explain it; do not override it.
- Keep the summary to three short sentences a non-technical risk manager can follow."""


def get_llm():
    provider = os.environ.get("AGENT_PROVIDER", "bedrock")
    if provider == "bedrock":
        from langchain_aws import ChatBedrock
        return ChatBedrock(model_id=os.environ["BEDROCK_MODEL_ID"],
                           model_kwargs={"max_tokens": 400, "temperature": 0})
    from langchain_anthropic import ChatAnthropic
    return ChatAnthropic(model=os.environ["ANTHROPIC_MODEL"], max_tokens=400, temperature=0)


def numbers_in(text: str) -> set[str]:
    return set(re.findall(r"-?\d+(?:\.\d+)?", text.replace(",", "")))


def triage(slice_id: str, check_results: list[dict], facts: dict) -> dict:
    """facts: small, pre-computed context, e.g. baseline laundering rate, weekday volume,
    day of month, which categories moved. Fetched by fixed read-only queries upstream."""
    payload = {"slice": slice_id, "checks": check_results, "facts": facts}
    llm = get_llm().with_structured_output(Triage)
    note: Triage = llm.invoke([("system", SYSTEM), ("human", json.dumps(payload, default=str))])

    # Guardrail: every number the agent quotes must exist in what it was given.
    allowed = numbers_in(json.dumps(payload, default=str))
    unknown = sorted(numbers_in(note.summary + " " + " ".join(note.evidence)) - allowed)
    out = note.model_dump()
    out.update({"slice": slice_id, "unverified_numbers": unknown,
                "label": "AI-generated note, review before acting"})
    return out


if __name__ == "__main__":
    # Dry run with the real 11 Sep numbers from the plan (needs model credentials in .env)
    example_checks = [
        {"check": "recon", "status": "pass", "raw_rows": 396, "curated_rows": 396},
        {"check": "quality", "status": "pass", "bad_rows": 0},
        {"check": "drift", "status": "fail", "psi_payment_format": 8.8},
        {"check": "anomaly", "status": "fail", "rows": 396, "laundering_rate_pct": 58.59},
    ]
    example_facts = {"baseline_laundering_rate_pct": 0.1, "normal_weekday_rows": 482771,
                     "slice_ach_share_pct": 100, "first_scheme_hop_before_slice": True}
    print(json.dumps(triage("2022-09-11", example_checks, example_facts), indent=2))

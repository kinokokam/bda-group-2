"""
Claude triage agent for one replayed slice (Live monitoring tab of the plan).

The checks in monitoring/checks.py decide pass or fail. This agent only reads their
results plus a few fixed, read-only facts, and writes a short structured note:
severity, verdict, a three-sentence summary, the numbers it used, a suggested action.

Install:  pip install langchain-core pydantic python-dotenv  plus ONE of:
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

try:  # read settings from .env in the repo root, if python-dotenv is installed
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


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
        # Converse API: works with inference profiles such as global.anthropic.claude-haiku-4-5-...
        import boto3
        from langchain_aws import ChatBedrockConverse
        session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"),
                                # BEDROCK_REGION: where Bedrock calls enter, separate from AWS_REGION
                                # (S3/Athena). On 19 Oct the global Haiku 4.5 profile failed from
                                # ap-southeast-1 with ServiceUnavailable but worked from us-east-1.
                                region_name=os.environ.get("BEDROCK_REGION",
                                                           os.environ.get("AWS_REGION", "ap-southeast-1")))
        return ChatBedrockConverse(client=session.client("bedrock-runtime"),
                                   model=os.environ["BEDROCK_MODEL_ID"], provider="anthropic",
                                   max_tokens=400, temperature=0)
    from langchain_anthropic import ChatAnthropic
    return ChatAnthropic(model=os.environ["ANTHROPIC_MODEL"], max_tokens=400, temperature=0)


class FakeLLM:
    """Stands in for Claude in tests: always returns the same note and made-up token counts.
    Mirrors the slice of the LangChain interface triage() uses."""

    def __init__(self, note: dict, input_tokens: int = 500, output_tokens: int = 120):
        self.note, self.usage = note, {"input_tokens": input_tokens, "output_tokens": output_tokens,
                                       "total_tokens": input_tokens + output_tokens}
        self.calls: list = []

    def with_structured_output(self, schema, include_raw: bool = False):
        self.schema = schema
        return self

    def invoke(self, messages):
        self.calls.append(messages)
        raw = type("AIMessage", (), {"usage_metadata": self.usage})()
        return {"raw": raw, "parsed": self.schema(**self.note), "parsing_error": None}


def numbers_in(text: str) -> set[str]:
    """Numbers in the text, with trailing decimal zeros dropped so 100.0 in the input matches 100 in a note."""
    found = re.findall(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    return {n.rstrip("0").rstrip(".") if "." in n else n for n in found}


def triage(slice_id: str, check_results: list[dict], facts: dict, llm=None) -> dict:
    """facts: small, pre-computed context, e.g. baseline laundering rate, weekday volume,
    day of month, which categories moved. Fetched by fixed read-only queries upstream.
    llm: defaults to get_llm(); tests pass a FakeLLM."""
    payload = {"slice": slice_id, "checks": check_results, "facts": facts}
    structured = (llm or get_llm()).with_structured_output(Triage, include_raw=True)
    reply = structured.invoke([("system", SYSTEM), ("human", json.dumps(payload, default=str))])
    if reply.get("parsed") is None:
        raise ValueError(f"agent reply did not parse: {reply.get('parsing_error')}")
    note: Triage = reply["parsed"]
    usage = getattr(reply["raw"], "usage_metadata", None) or {}

    # Guardrail: every number the agent quotes must exist in what it was given.
    allowed = numbers_in(json.dumps(payload, default=str))
    unknown = sorted(numbers_in(note.summary + " " + " ".join(note.evidence)) - allowed)
    out = note.model_dump()
    out.update({"slice": slice_id, "unverified_numbers": unknown,
                "usage": {k: usage.get(k) for k in ("input_tokens", "output_tokens", "total_tokens")},
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

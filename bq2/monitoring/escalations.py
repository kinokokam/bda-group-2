"""
send_escalation: record a quarantined slice's check report in out/escalations.log (local mode).

In a real bank this would email or page the risk team; here the dashboard shows it (Slack is an
open decision). Each escalation is one JSON line, so the monitor page and the tests can read the
log back. Every quarantined slice escalates, once however often it is rerun: the monitor states
what it measured and the risk team decides what it means. ("Alert" means a flagged transfer, not this.)
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from monitoring.replay import REPO

ESCALATIONS_LOG = REPO / "out" / "escalations.log"


def send_escalation(report: dict, log: Path | None = None) -> bool:
    """Log the report if its slice was quarantined. Returns True when an escalation was written.
    Idempotent per slice: a rerun replaces that slice's earlier entry, or removes it if the slice now passes."""
    log = log or ESCALATIONS_LOG
    kept = [e for e in read_escalations(log) if e.get("slice") != report.get("slice")]
    escalate = bool(report.get("quarantined"))
    if escalate:
        kept.append({
            "escalated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            **{k: report.get(k) for k in ("slice", "weekday", "failed_checks", "summary")},
        })
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text("".join(json.dumps(e) + "\n" for e in kept), encoding="utf-8")
    return escalate


def read_escalations(log: Path | None = None) -> list[dict]:
    log = log or ESCALATIONS_LOG
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines() if line.strip()]

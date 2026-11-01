"""
send_escalation: append a high-severity triage note to out/escalations.log (local mode).

In a real bank this would email or page the risk team; here the dashboard shows it (Slack is an
open decision). Each escalation is one JSON line, so the monitor page and the tests can read the
log back. Only severity "high" escalates; the gate's pass/quarantine decision is recorded beside
it but never changes what escalates. ("Alert" means a flagged transfer, not this.)
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from monitoring.replay import REPO

ESCALATIONS_LOG = REPO / "out" / "escalations.log"


def send_escalation(note: dict, path: str | None = None, log: Path | None = None) -> bool:
    """Log the note if its severity is high. path: the gate's choice ('build_marts' or 'quarantine').
    Returns True when an escalation was written."""
    if note.get("severity") != "high":
        return False
    entry = {
        "escalated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "slice": note.get("slice"),
        "path": path,
        **{k: note.get(k) for k in ("severity", "verdict", "summary", "suggested_action",
                                     "unverified_numbers", "label")},
    }
    log = log or ESCALATIONS_LOG
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")
    return True


def read_escalations(log: Path | None = None) -> list[dict]:
    log = log or ESCALATIONS_LOG
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines() if line.strip()]

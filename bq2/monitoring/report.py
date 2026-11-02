"""
write_report: the check report for one slice, filled in from a fixed template (decided 2 Nov).

The report states what the checks measured and nothing else: no verdict, severity or suggested
action. Deciding what the numbers mean is the risk team's job. No model writes it, so the same
checks always give the same report. Airflow runs it as the report_slice task.
"""
from __future__ import annotations

from monitoring.checks import PSI_FAIL, VOLUME_Z_FAIL

PSI_NAMES = {"psi_payment_format": "payment format", "psi_currency": "currency",
             "psi_amount_band": "amount band", "psi_model_score": "risk score"}
SHIFT_COLUMNS = {"psi_payment_format": "payment_format", "psi_currency": "payment_currency"}


def _listed(names: list[str]) -> str:
    return names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]


def _recon(r: dict, facts: dict) -> str:
    return (f"Recon: {r['raw_rows']:,} raw rows became {r['curated_rows']:,} curated rows "
            f"({r['row_gap']:,} missing).")


def _quality(r: dict, facts: dict) -> str:
    return f"Quality: {r['bad_rows']:,} of {r['rows']:,} rows broke a rule."


def _drift(r: dict, facts: dict) -> str:
    worst = r["worst_metric"]
    text = f"Drift: {PSI_NAMES[worst]} PSI {r[worst]} (fails above {PSI_FAIL})"
    column = SHIFT_COLUMNS.get(worst)
    shift = next((s for s in facts.get("biggest_shifts", []) if s["what"].startswith(f"{column}: ")), None)
    if shift:
        value = shift["what"].split(": ", 1)[1]
        text += (f"; {value} was {shift['today_pct']}% of transfers, "
                 f"against {shift['normal_pct']}% on normal days")
    return text + "."


def _volume(r: dict, facts: dict) -> str:
    return (f"Volume: {r['rows']:,} transfers, against a normal {r['normal_rows']:,} a day "
            f"(z {r['volume_z']}; fails beyond ±{VOLUME_Z_FAIL:g}).")


SENTENCES = {"recon": _recon, "quality": _quality, "drift": _drift, "volume": _volume}


def write_report(checked: dict) -> dict:
    """checked: what run.run_checks wrote for the slice (results, gate path, facts)."""
    results, facts = checked["results"], checked["facts"]
    failed = [r for r in results if r["status"] == "fail"]
    quarantined = checked["path"] == "quarantine"
    if failed:
        parts = [f"Failed {len(failed)} of {len(results)} checks: {_listed([r['check'] for r in failed])}."]
        parts += [SENTENCES[r["check"]](r, facts) for r in failed]
        if not facts["inside_simulation_window_1_to_10_sep"]:
            parts.append("The date is outside the 1–10 Sep simulation.")
        if quarantined:
            parts.append("The day is quarantined.")
        summary = " ".join(parts)
    else:
        summary = f"Passed all {len(results)} checks."
    return {"slice": checked["slice"], "weekday": facts["weekday"], "quarantined": quarantined,
            "failed_checks": [r["check"] for r in failed], "summary": summary,
            "biggest_shifts": facts.get("biggest_shifts", []), "top_alerts": facts.get("top_alerts", [])}

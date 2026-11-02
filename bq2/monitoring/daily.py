"""
daily_row: one row of the monitor's daily charts for a day that passed the gate.

A quarantined day never gets a row, so it stays out of the monitor's daily charts. The
exposure analysis (Part 1) is a separate one-off study of all 1-10 Sep and is not affected.
"""
from __future__ import annotations


def daily_row(checked: dict) -> dict:
    """checked: what run.run_checks wrote for the day."""
    if checked["path"] == "quarantine":
        raise ValueError(f"{checked['slice']} was quarantined; it has no row in the daily charts")
    volume = next(r for r in checked["results"] if r["check"] == "volume")
    return {"day": checked["slice"], "weekday": checked["facts"]["weekday"], "transfers": volume["rows"],
            "alerts": volume["flagged"], "laundering_rate_pct": volume["laundering_rate_pct"],
            "format_mix_pct": checked["facts"]["format_mix_pct"]}

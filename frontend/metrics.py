"""Pure helpers behind the views: windows, priority, reasons, evaluation figures.

Kept free of Streamlit and boto3 so the tests can exercise them directly.
"""

import json
import re
from datetime import datetime
from typing import Optional

import pandas as pd

from queries import BACKFILL_RUN, HIGH_PRIORITY, MEDIUM_PRIORITY, PIPELINE_FX_RATES

# `pipeline.sh predemo` names runs by the minute they started.
DEMO_RUN = re.compile(r"^\d{8}-\d{4}$")

# HI-Small account ids are nine hex digits; the looser pattern only has to keep
# free text out of the query, the literal quoting in `athena` does the rest.
ACCOUNT_ID = re.compile(r"^[0-9A-Za-z]{1,32}$")

PRIORITIES = ["High", "Medium", "Low"]

# The converted currencies as the data spells them, for captions.
USD_RATED_NAMES = ["US Dollar", "Euro", "UK Pound", "Swiss Franc", "Canadian Dollar", "Australian Dollar"]

CONTRIBUTOR_COLUMNS = ["feature", "reason", "value", "contribution"]

# Model feature names, as in `xgboost_features.json`, in an analyst's words.
FEATURE_LABELS = {
    "amount_paid_usd": "Amount paid (USD)",
    "amount_received_usd": "Amount received (USD)",
    "paid_fx_rate": "Payment currency rate",
    "recv_fx_rate": "Receiving currency rate",
    "fx_amount_discrepancy": "Gap between paid and received (USD)",
    "is_structuring_threshold": "Just under $10,000",
    "is_cross_currency": "Cross-currency",
    "is_cross_bank": "Cross-bank",
    "is_round_amount": "Round amount",
    "tx_hour": "Hour of day",
    "tx_dayofweek": "Day of week",
    "tx_dayofmonth": "Day of month",
    "is_weekend": "Weekend",
    "sender_tx_count_1h": "Sender transfers, last hour",
    "sender_volume_1h": "Sender value, last hour",
    "sender_tx_count_24h": "Sender transfers, last 24 hours",
    "sender_volume_24h": "Sender value, last 24 hours",
    "sender_tx_count_7d": "Sender transfers, last 7 days",
    "sender_volume_7d": "Sender value, last 7 days",
    "sender_burst_ratio_1h_24h": "Sender burst, hour against day",
    "sender_burst_ratio_24h_7d": "Sender burst, day against week",
    "sender_time_delta_sec": "Time since sender's last transfer",
    "receiver_tx_count_24h": "Receiver transfers, last 24 hours",
    "receiver_volume_24h": "Receiver value, last 24 hours",
    "receiver_time_delta_sec": "Time since receiver's last transfer",
    "payment_format_code": "Payment format",
    "payment_currency_code": "Payment currency",
    "receiving_currency_code": "Receiving currency",
    "from_bank_code": "Sending bank",
    "to_bank_code": "Receiving bank",
}

# A coded feature's value is an index into `category_maps.json`; the alert row
# carries the readable original, so show that instead.
CODED_FEATURE_COLUMNS = {
    "payment_format_code": "payment_format",
    "payment_currency_code": "payment_currency",
    "receiving_currency_code": "receiving_currency",
    "from_bank_code": "from_bank",
    "to_bank_code": "to_bank",
}


def default_run(run_ids: list[str]) -> Optional[str]:
    """Pick the data window to show first.

    Parameters
    ----------
    run_ids : list[str]
        Every run present in the gold table.

    Returns
    -------
    Optional[str]
        The newest live feed, else the history, else whatever is there, or
        None when the table is empty.
    """
    demos = sorted(run for run in run_ids if DEMO_RUN.match(run))
    if demos:
        return demos[-1]
    if BACKFILL_RUN in run_ids:
        return BACKFILL_RUN
    return run_ids[0] if run_ids else None


def window_label(run_id: str, first_event: Optional[str] = None, last_event: Optional[str] = None) -> str:
    """Name a data window the way an analyst would.

    Parameters
    ----------
    run_id : str
        Run id from the gold table.
    first_event, last_event : str, optional
        Earliest and latest transaction times in it.

    Returns
    -------
    str
        `Live feed, started 31 Oct 18:54` for a feed, `History, 2022-09-08 to
        2022-09-10` for the backfill, otherwise the id itself.
    """
    if DEMO_RUN.match(run_id):
        started = datetime.strptime(run_id, "%Y%m%d-%H%M")
        return f"Live feed, started {started:%d %b %H:%M}"
    if run_id == BACKFILL_RUN:
        if first_event and last_event:
            return f"History, {first_event[:10]} to {last_event[:10]}"
        return "History"
    return run_id


def priority(score: Optional[float]) -> str:
    """Place an alert's score in a review tier.

    Parameters
    ----------
    score : Optional[float]
        Calibrated score, an estimated probability of laundering.

    Returns
    -------
    str
        `High`, `Medium` or `Low`.
    """
    if score is not None and not pd.isna(score):
        if score >= HIGH_PRIORITY:
            return "High"
        if score >= MEDIUM_PRIORITY:
            return "Medium"
    return "Low"


def is_usd_rated(currency: Optional[str]) -> bool:
    """Report whether the scoring job converted this currency to dollars."""
    return bool(currency) and str(currency).upper() in PIPELINE_FX_RATES


def native_amount(amount_usd: Optional[float], currency: Optional[str]) -> Optional[float]:
    """Undo the scoring job's conversion, giving the amount in its own currency.

    Exact for every currency, because the job's rate is known: one of
    `PIPELINE_FX_RATES`, or 1.0 for the rest.

    Parameters
    ----------
    amount_usd : Optional[float]
        The gold table's `amount_paid_usd`.
    currency : Optional[str]
        The transfer's payment currency.

    Returns
    -------
    Optional[float]
        Amount in `currency`, rounded to cents; None when the amount is missing.
    """
    if amount_usd is None or pd.isna(amount_usd):
        return None
    rate = PIPELINE_FX_RATES.get(str(currency or "").upper(), 1.0)
    return round(float(amount_usd) / rate, 2)


def add_native_amounts(frame: pd.DataFrame) -> pd.DataFrame:
    """Add an `amount` column in each row's own currency, beside `payment_currency`."""
    frame = frame.copy()
    frame["amount"] = [
        native_amount(amount, currency)
        for amount, currency in zip(frame["amount_paid_usd"], frame["payment_currency"])
    ]
    return frame


def display_value(value) -> str:
    """Format a feature value for reading: `9,500` rather than `9500.0`."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return str(value)
    if float(value).is_integer():
        return f"{int(value):,}"
    return f"{value:,.2f}"


def readable_value(feature: str, value, alert: Optional[dict]) -> str:
    """Show a contributor's value as the analyst would recognise it.

    A coded feature's value is a category index, meaningless on its own, so the
    original is taken from the alert row; the gold table does not keep every
    original (no receiving currency), and those show a dash.

    Parameters
    ----------
    feature : str
        Model feature name.
    value
        Value Flink recorded.
    alert : dict, optional
        The alert's row.

    Returns
    -------
    str
        Display text.
    """
    if feature in CODED_FEATURE_COLUMNS:
        original = (alert or {}).get(CODED_FEATURE_COLUMNS[feature])
        return "–" if original is None or pd.isna(original) else str(original)
    return display_value(value)


def feature_label(name: str) -> str:
    """Return a feature's readable name, or the name itself if unknown."""
    return FEATURE_LABELS.get(name, name)


def is_account_id(text: str) -> bool:
    """Report whether text looks like an account id.

    Parameters
    ----------
    text : str
        What the analyst typed.

    Returns
    -------
    bool
        True for 1 to 32 letters and digits.
    """
    return bool(ACCOUNT_ID.match(text.strip()))


def evaluation(scored: int, alerts: int, laundering: int, caught: int) -> dict[str, Optional[float]]:
    """Derive the headline figures for a run against known outcomes.

    Parameters
    ----------
    scored : int
        Transfers scored.
    alerts : int
        Transfers at or above the alert threshold.
    laundering : int
        Transfers known to be laundering.
    caught : int
        Alerts that are known laundering.

    Returns
    -------
    dict[str, Optional[float]]
        `alert_rate`, `precision`, `recall`, `base_rate` and `lift`, each None
        where its denominator is zero.
    """
    alert_rate = alerts / scored if scored else None
    precision = caught / alerts if alerts else None
    recall = caught / laundering if laundering else None
    base_rate = laundering / scored if scored else None
    lift = precision / base_rate if precision is not None and base_rate else None
    return {
        "alert_rate": alert_rate,
        "precision": precision,
        "recall": recall,
        "base_rate": base_rate,
        "lift": lift,
    }


def parse_contributors(raw: Optional[str], alert: Optional[dict] = None) -> pd.DataFrame:
    """Unpack the `top_contributors` column of one alert.

    Flink writes the five features with the largest absolute contribution to
    the model's raw score, as JSON. Non-alerts carry `"[]"`.

    Parameters
    ----------
    raw : Optional[str]
        The column's value.
    alert : dict, optional
        The alert's row; when given, coded features show the original value
        (`ACH` rather than its index).

    Returns
    -------
    pd.DataFrame
        `feature`, `reason` (readable name), `value` and `contribution`,
        largest absolute contribution first; empty when there is nothing.
    """
    empty = pd.DataFrame(columns=CONTRIBUTOR_COLUMNS)
    if not raw:
        return empty
    try:
        entries = json.loads(raw)
    except json.JSONDecodeError:
        return empty
    if not entries:
        return empty

    frame = pd.DataFrame(entries).reindex(columns=["feature", "value", "contribution"])
    frame["reason"] = frame["feature"].map(feature_label)
    frame["value"] = [readable_value(feature, value, alert) for feature, value in zip(frame["feature"], frame["value"])]
    order = frame["contribution"].abs().sort_values(ascending=False).index
    return frame.loc[order, CONTRIBUTOR_COLUMNS].reset_index(drop=True)

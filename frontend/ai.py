"""Investigator brief: Claude Haiku 4.5 on Amazon Bedrock explains one alert.

The analyst asks for it on the alert queue. The prompt holds only what that
page already shows: the alert, the five factors behind its score and both
accounts' recent transfers. It is built from named fields, so the outcome
label never reaches the model, and the brief states what the data shows
without deciding whether the transfer is laundering.

Credentials come from boto's default chain, as the Athena queries do: the
`AWS_PROFILE` in the shell on a laptop, the instance role on EC2. The SDK is
held below 2.0, whose API version Bedrock accepts only for Claude 4.7 and later.
"""

import functools
import importlib.util
import os

import pandas as pd

import metrics

DEFAULT_MODEL = "global.anthropic.claude-haiku-4-5-20251001-v1:0"
MODEL = os.environ.get("BEDROCK_MODEL", DEFAULT_MODEL)
REGION = os.environ.get("BEDROCK_REGION", "ap-southeast-1")
MODEL_LABEL = "Claude Haiku 4.5" if MODEL == DEFAULT_MODEL else MODEL

HISTORY_ROWS = 25
# A brief is three short sections; this is room for them, not a target.
MAX_TOKENS = 1024

SYSTEM = """You help an anti-money-laundering investigator review one alert raised by a transaction-scoring model.

You receive the alert, the five factors that moved its risk score most, and the recent transfers of the sending and receiving accounts. Work only from that data. Do not invent figures, accounts or events, and do not decide whether the transfer is laundering: that is the investigator's call.

Each history lists transfers oldest first, each with its time relative to the alert (minus means before the alert, plus after). The row marked "THIS ALERT" is the alerted transfer itself. Rules for the histories:
- Take counts only from the "Counted" lines; do not count rows yourself.
- A transfer happened after the alert only if its offset starts with "+". Quote offsets exactly as written (for example "-1 min"), never your own arithmetic.
- Money was passed on from the alert only if the receiver's "After the alert" count is above zero. Transfers before the alert cannot be onward movement of its money; if the count is zero, say nothing was passed on within the data.
- Give the date and time of any transfer or sequence you describe, copied from its row.
- Do not name or invent amount thresholds. The alert's flags already say whether its amount is just under $10,000.

Write three short sections with these headings:
**Why it scored high** - the factors that drove the score, in plain words, with their values.
**What the histories show** - patterns in the two accounts' transfers that matter for this alert, such as bursts of transfers, many counterparties, round amounts or amounts just under 10,000, money passed straight on, or money flowing back. Say plainly when a history is short or shows nothing unusual.
**What to check next** - two or three concrete checks the investigator can make.

Keep the whole brief under 200 words. Quote amounts in the currency given."""


class BriefUnavailable(RuntimeError):
    """The model could not be reached or refused the request; the message says why, for the analyst."""


def available() -> bool:
    """Report whether the brief is offered: on unless `BQ1_AI=off` or the SDK is missing."""
    if os.environ.get("BQ1_AI", "").strip().lower() == "off":
        return False
    return importlib.util.find_spec("anthropic") is not None


def _percent(value) -> str:
    return "–" if value is None or pd.isna(value) else f"{float(value):.0%}"


def _amount(value, currency) -> str:
    if value is None or pd.isna(value):
        return f"– {currency}"
    return f"{float(value):,.2f} {currency}"


def _alert_block(alert: dict) -> str:
    flags = [
        name
        for name, column in (
            ("cross-bank", "is_cross_bank"),
            ("cross-currency", "is_cross_currency"),
            ("just under $10,000", "is_structuring_threshold"),
        )
        if alert.get(column) is not None and not pd.isna(alert.get(column)) and alert.get(column) == 1
    ]
    amount = alert.get("amount")
    if amount is None or pd.isna(amount):
        amount = metrics.native_amount(alert.get("amount_paid_usd"), alert.get("payment_currency"))
    lines = [
        f"time: {alert.get('event_time')}",
        f"sender: account {alert.get('from_account')} at bank {alert.get('from_bank')}",
        f"receiver: account {alert.get('to_account')} at bank {alert.get('to_bank')}",
        f"amount: {_amount(amount, alert.get('payment_currency'))}",
        f"payment format: {alert.get('payment_format')}",
        f"flags: {', '.join(flags) if flags else 'none'}",
        f"risk score: {_percent(alert.get('score'))} ({metrics.priority(alert.get('score'))} priority)",
        f"sender transfers in the last 24 hours: {alert.get('sender_tx_count_24h')}",
        f"receiver transfers in the last 24 hours: {alert.get('receiver_tx_count_24h')}",
    ]
    return "<alert>\n" + "\n".join(lines) + "\n</alert>"


# What a factor's number means, where its name alone could be misread (on
# 8 Nov the model took a burst ratio of 7 for seven transfers). Formulas are
# those of bq1/flink/features.py.
FACTOR_NOTES = {
    "sender_burst_ratio_1h_24h": "a ratio, not a count: the sender's value in the last hour over its hourly average for the last 24 hours",
    "sender_burst_ratio_24h_7d": "a ratio, not a count: the sender's value in the last 24 hours over its daily average for the last 7 days",
    "sender_time_delta_sec": "seconds since the sender's previous transfer",
    "receiver_time_delta_sec": "seconds since the receiver's previous transfer",
    "sender_tx_count_1h": "transfers the sender made in the last hour, this one included",
    "sender_tx_count_24h": "transfers the sender made in the last 24 hours, this one included",
    "sender_tx_count_7d": "transfers the sender made in the last 7 days, this one included",
    "receiver_tx_count_24h": "transfers the receiver got in the last 24 hours, this one included",
    "tx_dayofweek": "1 is Sunday, 7 is Saturday",
    "tx_hour": "hour of the day, 0 to 23",
    "is_structuring_threshold": "1 means the amount is between 9,000 and 9,999.99",
    "is_cross_bank": "1 means the two accounts are at different banks",
    "is_cross_currency": "1 means paid and received in different currencies",
    "is_round_amount": "1 means a whole multiple of 1,000",
    "is_weekend": "1 means a Saturday or Sunday",
}


def _factors_block(reasons: pd.DataFrame) -> str:
    if reasons.empty:
        body = "No factors were recorded for this alert."
    else:
        body = "\n".join(
            f"- {row.reason} = {row.value}"
            + (f" ({FACTOR_NOTES[row.feature]})" if row.feature in FACTOR_NOTES else "")
            + f": {float(row.contribution):+.2f}"
            for row in reasons.itertuples()
        )
    return (
        "<factors>\nLargest effect first. A positive effect pushed the score toward suspicion, "
        "a negative one away from it.\n" + body + "\n</factors>"
    )


def offset_label(minutes: float) -> str:
    """Say how far a transfer is from the alert: `same minute`, `-12 min`, `+3.5 h`, `-2.0 d`."""
    if abs(minutes) < 1:
        return "same minute"
    sign = "+" if minutes > 0 else "-"
    size = abs(minutes)
    if size < 60:
        return f"{sign}{size:.0f} min"
    if size < 48 * 60:
        return f"{sign}{size / 60:.1f} h"
    return f"{sign}{size / 1440:.1f} d"


def _counts_line(history: pd.DataFrame, minutes: pd.Series) -> str:
    """The figures the brief may quote, counted here rather than by the model."""
    before = history[(minutes < 0) & (minutes >= -24 * 60)]
    after = history[minutes > 0]
    counterparties = pd.concat([
        before.loc[before["direction"] == "Sent", "to_account"],
        before.loc[before["direction"] == "Received", "from_account"],
    ]).nunique()
    sent = int((before["direction"] == "Sent").sum())
    received = int((before["direction"] == "Received").sum())
    by_day = []
    for day, rows in history.groupby(history["event_time"].astype(str).str[:10], sort=True):
        parts = [f"{n} {word}" for word, n in (("sent", int((rows["direction"] == "Sent").sum())),
                                                ("received", int((rows["direction"] == "Received").sum()))) if n]
        by_day.append(f"{day}: {', '.join(parts)}")
    return (
        f"Counted in the 24 hours before the alert, not including it: {sent} sent, {received} received, "
        f"{counterparties} distinct counterparties. After the alert: {len(after)} transfers "
        f"({int((after['direction'] == 'Sent').sum())} sent, {int((after['direction'] == 'Received').sum())} received).\n"
        f"Counted by day, all transfers on record including this alert: {'; '.join(by_day)}."
    )


def _history_block(tag: str, account: str, history: pd.DataFrame, alert_time, alert_txn: str = "") -> str:
    if history.empty:
        return f'<{tag} account="{account}">\nNo other transfers on record.\n</{tag}>'
    minutes = (pd.to_datetime(history["event_time"]) - alert_time).dt.total_seconds() / 60
    # The newest transfers, then put in time order: listed newest first, the
    # model reverses before and after.
    shown = history.head(HISTORY_ROWS).assign(minutes=minutes).sort_values("event_time", kind="stable")
    lines = []
    for row in shown.itertuples():
        counterparty = row.to_account if row.direction == "Sent" else row.from_account
        preposition = "to" if row.direction == "Sent" else "from"
        if counterparty == account:
            counterparty = "itself (same account)"
        amount = getattr(row, "amount", None)
        if amount is None or pd.isna(amount):
            amount = metrics.native_amount(row.amount_paid_usd, row.payment_currency)
        alerted = "alert" if bool(row.is_alert) else "no alert"
        # The history query has no transfer id, so the alert's own row is the one at its
        # minute with its counterparties and amount.
        is_this_alert = (
            abs(row.minutes) < 1 and bool(row.is_alert) and alert_txn
            and f"{row.from_account}>{row.to_account}>{float(row.amount_paid_usd):.2f}" == alert_txn
        )
        lines.append(
            f"{row.event_time} ({offset_label(row.minutes)}) | {row.direction} {preposition} {counterparty} | "
            f"{_amount(amount, row.payment_currency)} | {row.payment_format} | risk {_percent(row.score)} | {alerted}"
            + (" | THIS ALERT" if is_this_alert else "")
        )
    note = (
        f"{len(history)} transfers on record; the {len(shown)} most recent are listed, oldest first, each with "
        f"its time relative to the alert (minus = before).\n{_counts_line(history, minutes)}"
    )
    return f'<{tag} account="{account}">\n{note}\n' + "\n".join(lines) + f"\n</{tag}>"


def brief_prompt(alert: dict, reasons: pd.DataFrame, sender: pd.DataFrame, receiver: pd.DataFrame) -> str:
    """Build the user message for one alert from named fields only.

    Parameters
    ----------
    alert : dict
        The alert's row from the queue, with `amount` in its own currency.
    reasons : pd.DataFrame
        `metrics.parse_contributors` output: `reason`, `value`, `contribution`.
    sender, receiver : pd.DataFrame
        Each account's transfers from `data.account_history`, newest first.

    Returns
    -------
    str
        The alert, its factors and both histories, each in its own tag.
    """
    alert_time = pd.to_datetime(alert.get("event_time"))
    usd = alert.get("amount_paid_usd")
    alert_txn = "" if usd is None or pd.isna(usd) else f"{alert.get('from_account')}>{alert.get('to_account')}>{float(usd):.2f}"
    return "\n\n".join([
        "Explain this alert for the investigator.",
        _alert_block(alert),
        _factors_block(reasons),
        _history_block("sender_history", str(alert.get("from_account")), sender, alert_time, alert_txn),
        _history_block("receiver_history", str(alert.get("to_account")), receiver, alert_time, alert_txn),
    ])


@functools.lru_cache(maxsize=1)
def _client():
    """One Bedrock client for the process, on boto's default credential chain."""
    import anthropic

    return anthropic.AnthropicBedrock(aws_region=REGION)


def explain(prompt: str, client=None) -> str:
    """Ask the model for the brief.

    Parameters
    ----------
    prompt : str
        From `brief_prompt`.
    client : optional
        Bedrock client; tests pass a stub.

    Returns
    -------
    str
        The brief as Markdown.

    Raises
    ------
    BriefUnavailable
        If Bedrock could not be reached, refused the credentials, throttled or
        returned an error, or the model declined.
    """
    import anthropic
    from botocore.exceptions import BotoCoreError

    try:
        response = (client or _client()).messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
    except (anthropic.AuthenticationError, anthropic.PermissionDeniedError) as error:
        raise BriefUnavailable(
            f"No access to {MODEL_LABEL} on Bedrock in {REGION}. Check the AWS profile or instance role."
        ) from error
    except anthropic.RateLimitError as error:
        raise BriefUnavailable("Bedrock is throttling requests. Try again in a minute.") from error
    except anthropic.APIStatusError as error:
        raise BriefUnavailable(f"Bedrock returned an error ({error.status_code}).") from error
    except (anthropic.APIConnectionError, BotoCoreError) as error:
        raise BriefUnavailable(f"Could not reach Bedrock in {REGION}: {error}") from error

    if response.stop_reason == "refusal":
        raise BriefUnavailable("The model declined to write a brief for this alert.")
    text = "\n\n".join(block.text for block in response.content if block.type == "text").strip()
    if not text:
        raise BriefUnavailable("The model returned an empty brief.")
    return text

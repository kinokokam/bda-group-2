"""Overview: the analyst's landing page for the selected data window.

What is waiting, what to open first, where alerts come from and which
accounts keep appearing. Uses no outcome labels; those live on the model
performance page. During a demo on EC2 the alert figures come live from the
Kafka stream; otherwise everything is read through Athena.
"""

from datetime import datetime

import pandas as pd
import streamlit as st

import data
import metrics
import theme
from theme import Kpi

REFRESH = "15s"
# Live alerts are in memory, so a fast refresh costs nothing; the Athena
# figures beside them stay behind their own cache.
LIVE_REFRESH = "2s"
TOP_ALERTS = 5
TOP_ACCOUNTS = 5

run_id = st.session_state["run_id"]
is_history = run_id == metrics.BACKFILL_RUN

ALERT_COLUMNS = {
    "priority": theme.PRIORITY_PILL,
    "score": st.column_config.NumberColumn("Risk", format="percent"),
    "event_time": st.column_config.TextColumn("Time"),
    "from_account": st.column_config.TextColumn("Sender"),
    "to_account": st.column_config.TextColumn("Receiver"),
    "amount": st.column_config.NumberColumn("Amount", format="localized"),
    "payment_currency": st.column_config.TextColumn("Currency"),
    "payment_format": st.column_config.TextColumn("Format"),
}

ACCOUNT_COLUMNS = {
    "from_account": st.column_config.TextColumn("Account"),
    "from_bank": st.column_config.TextColumn("Bank"),
    "alerts": st.column_config.NumberColumn("Alerts"),
    "max_score": st.column_config.NumberColumn("Highest risk", format="percent"),
    "counterparties": st.column_config.NumberColumn("Receivers", help="Distinct accounts it sent to."),
}


def load() -> dict:
    """Gather the overview's figures from the live stream, or from Athena alone.

    Live, every alert figure comes from the Kafka stream; only the count of
    transfers screened and the repeat accounts need transfers that did not
    alert, so they still come from Athena, about a minute behind.
    """
    alerts = data.live_alerts(run_id)
    if alerts is None:
        frames = data.overview(run_id, by_hour=is_history)
        totals = frames["totals"].iloc[0]
        return {
            "scored": int(totals["scored"]),
            "alerts": int(totals["alerts"]),
            "high": int(totals["high"]),
            "medium": int(totals["medium"]),
            "accounts_alerted": int(totals["accounts_alerted"]),
            "latest": f"Latest transaction {totals['last_event'] or '–'}",
            "screened_help": None,
            "queue": frames["queue"],
            "activity": frames["activity"],
            "formats": frames["formats"],
            "watch": frames["watch"],
        }

    summary = metrics.alert_summary(alerts)
    screened = data.screened(run_id).iloc[0]
    return {
        **summary,
        "scored": int(screened["scored"]),
        "latest": f"Latest alert {summary['last_alert'] or '–'}",
        "screened_help": "Counted from the gold table, which updates about once a minute.",
        "queue": alerts,
        "activity": metrics.alerts_by_period(alerts, data.period_width(is_history)),
        "formats": metrics.alerts_by_format(alerts),
        "watch": data.watch_list(run_id),
    }


def render() -> None:
    """Draw the overview for the selected window."""
    figures = load()
    scored = figures["scored"]
    alerts = figures["alerts"]
    high = figures["high"]
    medium = figures["medium"]
    is_live = figures["screened_help"] is not None
    latest = str(figures["latest"])

    theme.hero(
        "Overview",
        f"{latest} · refreshed {datetime.now():%H:%M:%S} · {NOTE}",
        [
            Kpi("Alerts to review", f"{alerts:,}", "⚑",
                pill="Live" if is_live else (f"{alerts / scored:.2%} of screened" if scored else "")),
            Kpi("High priority", f"{high:,}", "!", pill=f"{metrics.HIGH_PRIORITY:.0%}+ risk", tone="amber",
                help=f"Estimated likelihood of laundering of {metrics.HIGH_PRIORITY:.0%} or more. Review these first."),
            Kpi("Accounts involved", f"{figures['accounts_alerted']:,}", "◉", pill="Senders",
                help="Distinct senders with an alert."),
            Kpi("Transactions screened", f"{scored:,}", "▦", pill="~1 min behind" if is_live else "",
                help=figures["screened_help"] or ""),
        ],
        badge=theme.window_badge(run_id),
    )

    if alerts == 0:
        st.info("No alerts in this window yet.")
        return

    queue = metrics.add_native_amounts(figures["queue"])
    queue["priority"] = queue["score"].map(metrics.priority)

    first, tiers = st.columns([3, 2])
    with first, theme.card("review-first"):
        st.subheader("Review first")
        st.dataframe(
            theme.with_priority_pills(queue.head(TOP_ALERTS)[list(ALERT_COLUMNS)]),
            hide_index=True,
            column_config=ALERT_COLUMNS,
            width="stretch",
        )
        st.page_link("views/alert_queue.py", label="Open the full alert queue", icon=":material/arrow_forward:")
    with tiers, theme.card("by-priority"):
        st.subheader("Alerts by priority")
        by_priority = pd.DataFrame(
            {"priority": metrics.PRIORITIES, "alerts": [high, medium, alerts - high - medium]}
        )
        st.bar_chart(by_priority, x="priority", y="alerts", height=230, sort=False, x_label="Priority", y_label="Alerts")
        st.caption(
            f"High: {metrics.HIGH_PRIORITY:.0%}+ estimated likelihood · "
            f"Medium: {metrics.MEDIUM_PRIORITY:.0%}–{metrics.HIGH_PRIORITY:.0%} · Low: below that"
        )

    timeline, formats = st.columns([3, 2])
    unit = "hour" if is_history else "minute"
    with timeline, theme.card("per-period"):
        st.subheader(f"Alerts per {unit}")
        activity = figures["activity"]
        activity = activity.set_index(activity["period"].map(lambda period: metrics.period_label(period, is_history)))
        st.line_chart(activity[["alerts"]], height=260, x_label=unit.capitalize(), y_label="Alerts")
    with formats, theme.card("by-format"):
        st.subheader("Alerts by payment format")
        by_format = figures["formats"]
        st.bar_chart(by_format, x="payment_format", y="alerts", horizontal=True, sort="-alerts", height=260,
                     x_label="Payment format", y_label="Alerts")

    with theme.card("repeat-accounts"):
        st.subheader("Repeat accounts")
        watched = figures["watch"]
        if watched.empty:
            st.caption("No account has more than one alert yet.")
        else:
            st.dataframe(
                watched.head(TOP_ACCOUNTS)[list(ACCOUNT_COLUMNS)],
                hide_index=True,
                column_config=ACCOUNT_COLUMNS,
                width="stretch",
            )
        st.page_link("views/watch_list.py", label="Open the watch-list", icon=":material/arrow_forward:")


if is_history:
    NOTE = "history, fixed"
    render()
elif data.live_alerts(run_id) is not None:
    NOTE = f"alerts live every {LIVE_REFRESH}, screened counts about a minute behind"
    st.fragment(render, run_every=LIVE_REFRESH)()
else:
    NOTE = f"updates every {REFRESH}, new transactions arrive about once a minute"
    st.fragment(render, run_every=REFRESH)()

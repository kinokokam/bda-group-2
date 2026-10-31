"""Overview: the analyst's landing page for the selected data window.

What is waiting, what to open first, where alerts come from and which
accounts keep appearing. Uses no outcome labels; those live on the model
performance page.
"""

from datetime import datetime

import pandas as pd
import streamlit as st

import data
import metrics

REFRESH = "15s"
TOP_ALERTS = 5
TOP_ACCOUNTS = 5

run_id = st.session_state["run_id"]
is_history = run_id == metrics.BACKFILL_RUN

ALERT_COLUMNS = {
    "priority": st.column_config.TextColumn("Priority"),
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


def render() -> None:
    """Draw the overview for the selected window."""
    frames = data.overview(run_id, by_hour=is_history)
    totals = frames["totals"].iloc[0]
    scored = int(totals["scored"])
    alerts = int(totals["alerts"])
    high = int(totals["high"])
    medium = int(totals["medium"])

    st.caption(
        f"Latest transaction {totals['last_event'] or '–'} · refreshed {datetime.now():%H:%M:%S}"
    )

    to_review, urgent, accounts, value, screened = st.columns(5)
    to_review.metric("Alerts to review", f"{alerts:,}")
    urgent.metric(
        "High priority",
        f"{high:,}",
        help=f"Estimated likelihood of laundering of {metrics.HIGH_PRIORITY:.0%} or more. Review these first.",
    )
    accounts.metric("Accounts involved", f"{int(totals['accounts_alerted']):,}", help="Distinct senders with an alert.")
    unrated = int(totals["alerts_unrated"])
    value.metric(
        "Value under alert (USD)",
        f"${float(totals['alert_value_usd']):,.0f}",
        help=f"Alerts paid in {', '.join(metrics.USD_RATED_NAMES)}. "
        f"{unrated:,} alerts in other currencies have no USD rate and are not included.",
    )
    screened.metric(
        "Transactions screened",
        f"{scored:,}",
        help=f"{alerts / scored:.2%} raised an alert." if scored else None,
    )

    if alerts == 0:
        st.info("No alerts in this window yet.")
        return

    queue = metrics.add_native_amounts(frames["queue"])
    queue["priority"] = queue["score"].map(metrics.priority)

    first, tiers = st.columns([3, 2])
    with first:
        st.subheader("Review first")
        st.dataframe(
            queue.head(TOP_ALERTS)[list(ALERT_COLUMNS)],
            hide_index=True,
            column_config=ALERT_COLUMNS,
            width="stretch",
        )
        st.page_link("views/alert_queue.py", label="Open the full alert queue", icon=":material/arrow_forward:")
    with tiers:
        st.subheader("Alerts by priority")
        by_priority = pd.DataFrame(
            {"priority": metrics.PRIORITIES, "alerts": [high, medium, alerts - high - medium]}
        )
        st.bar_chart(by_priority, x="priority", y="alerts", height=230, sort=False)
        st.caption(
            f"High: {metrics.HIGH_PRIORITY:.0%}+ estimated likelihood · "
            f"Medium: {metrics.MEDIUM_PRIORITY:.0%}–{metrics.HIGH_PRIORITY:.0%} · Low: below that"
        )

    timeline, formats = st.columns([3, 2])
    unit = "hour" if is_history else "minute"
    with timeline:
        st.subheader(f"Alerts per {unit}")
        activity = frames["activity"].set_index("period")
        st.line_chart(activity[["alerts"]], height=260)
    with formats:
        st.subheader("Alerts by payment format")
        by_format = frames["formats"]
        st.bar_chart(by_format, x="payment_format", y="alerts", horizontal=True, sort="-alerts", height=260)

    st.subheader("Repeat accounts")
    watched = frames["watch"]
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


st.header("Overview")
if is_history:
    render()
else:
    st.caption(f"Updates every {REFRESH}. New transactions arrive in batches about once a minute.")
    st.fragment(render, run_every=REFRESH)()

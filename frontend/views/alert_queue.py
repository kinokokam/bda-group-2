"""Alert queue: every alert in the window, ranked by risk rather than arrival.

First-in, first-out queues bury the riskiest transfers behind older, milder
ones; ranking puts them on top. Selecting one shows why it scored high and
what both accounts have been doing.
"""

from datetime import datetime

import pandas as pd
import streamlit as st

import data
import metrics

run_id = st.session_state["run_id"]

QUEUE_COLUMNS = [
    "priority", "score", "event_time", "from_account", "to_account",
    "amount", "payment_currency", "payment_format",
    "sender_tx_count_24h", "receiver_tx_count_24h",
]

COLUMN_CONFIG = {
    "priority": st.column_config.TextColumn("Priority"),
    "score": st.column_config.NumberColumn("Risk", format="percent", help="Estimated likelihood of laundering."),
    "event_time": st.column_config.TextColumn("Time"),
    "from_account": st.column_config.TextColumn("Sender"),
    "to_account": st.column_config.TextColumn("Receiver"),
    "amount": st.column_config.NumberColumn("Amount", format="localized", help="In the transfer's own currency."),
    "payment_currency": st.column_config.TextColumn("Currency"),
    "payment_format": st.column_config.TextColumn("Format"),
    "sender_tx_count_24h": st.column_config.NumberColumn("Sender transfers (24h)"),
    "receiver_tx_count_24h": st.column_config.NumberColumn("Receiver transfers (24h)"),
    "direction": st.column_config.TextColumn("Direction"),
    "is_alert": st.column_config.CheckboxColumn("Alert"),
    "reason": st.column_config.TextColumn("Factor"),
    "value": st.column_config.TextColumn("Value"),
    "contribution": st.column_config.NumberColumn("Effect", format="%+.2f"),
}


def show_history(account: str) -> None:
    """Draw an account's recent transfers with a count of its alerts."""
    history = metrics.add_native_amounts(data.account_history(run_id, account))
    if history.empty:
        st.caption("No other transfers on record.")
        return
    alert_count = int(history["is_alert"].fillna(False).sum())
    st.caption(f"{len(history):,} transfers on record (newest 200), {alert_count} of them alerts.")
    columns = ["event_time", "direction", "from_account", "to_account", "amount", "payment_currency", "payment_format", "score", "is_alert"]
    st.dataframe(history[columns], hide_index=True, column_config=COLUMN_CONFIG, height=260, width="stretch")


st.header("Alert queue")
heading, refresh = st.columns([4, 1])
heading.caption(f"Highest risk first · loaded {datetime.now():%H:%M:%S}")
if refresh.button("Refresh", icon=":material/refresh:", width="stretch"):
    data.alert_queue.clear()

alerts = data.alert_queue(run_id)
if alerts.empty:
    st.info("No alerts in this window yet.")
    st.stop()

alerts = metrics.add_native_amounts(alerts)
alerts["priority"] = alerts["score"].map(metrics.priority)
chosen = st.segmented_control(
    "Priority", metrics.PRIORITIES, selection_mode="multi", default=metrics.PRIORITIES, label_visibility="collapsed"
)
shown = alerts[alerts["priority"].isin(chosen)].reset_index(drop=True)

selection = st.dataframe(
    shown[QUEUE_COLUMNS],
    hide_index=True,
    column_config=COLUMN_CONFIG,
    on_select="rerun",
    selection_mode="single-row",
    height=360,
    width="stretch",
)
st.caption(f"{len(shown):,} of {len(alerts):,} alerts shown (at most 500). Select one to investigate it.")

rows = selection.selection.rows
# A selection can outlive a filter change that shortened the table.
if not rows or rows[0] >= len(shown):
    st.stop()

alert = shown.iloc[rows[0]]
st.divider()
st.subheader(f"Transfer {alert['txn_id']}")

summary, reasons = st.columns([2, 3])
with summary:
    risk, tier = st.columns(2)
    risk.metric("Risk", f"{alert['score']:.0%}")
    tier.metric("Priority", alert["priority"])
    st.markdown(
        f"**{alert['from_account']}** (bank {alert['from_bank']}) → "
        f"**{alert['to_account']}** (bank {alert['to_bank']})\n\n"
        f"{alert['amount']:,.2f} {alert['payment_currency']}"
        + (f" (${alert['amount_paid_usd']:,.2f})" if metrics.is_usd_rated(alert["payment_currency"])
           and alert["payment_currency"].upper() not in ("USD", "US DOLLAR") else "")
        + f" by {alert['payment_format']} at {alert['event_time']}"
    )
    flags = [
        name
        for name, column in (
            ("cross-bank", "is_cross_bank"),
            ("cross-currency", "is_cross_currency"),
            ("just under $10,000", "is_structuring_threshold"),
        )
        if pd.notna(alert[column]) and alert[column] == 1
    ]
    if flags:
        st.markdown("Flags: " + ", ".join(flags))

with reasons:
    st.markdown("**Why it was flagged**")
    contributors = metrics.parse_contributors(alert["top_contributors"], alert.to_dict())
    if contributors.empty:
        st.caption("No reasons recorded for this transfer.")
    else:
        st.bar_chart(contributors, x="reason", y="contribution", horizontal=True, sort=False, height=220)
        st.caption("The five factors that moved the risk most. Positive pushes toward suspicion, negative away.")
        st.dataframe(
            contributors[["reason", "value", "contribution"]],
            hide_index=True,
            column_config=COLUMN_CONFIG,
            width="stretch",
        )

sender, receiver = st.tabs([f"Sender {alert['from_account']}", f"Receiver {alert['to_account']}"])
with sender:
    show_history(alert["from_account"])
with receiver:
    show_history(alert["to_account"])

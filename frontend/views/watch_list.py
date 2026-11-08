"""Watch-list: sender accounts that keep raising alerts, and a lookup for any account.

Laundering schemes reuse accounts, so an account flagged more than once is worth
reviewing as a whole rather than one transfer at a time.
"""

import streamlit as st

import data
import metrics
import theme
from theme import Kpi

run_id = st.session_state["run_id"]

COLUMN_CONFIG = {
    "from_account": st.column_config.TextColumn("Account"),
    "from_bank": st.column_config.TextColumn("Bank"),
    "alerts": st.column_config.NumberColumn("Alerts"),
    "alerts_this_run": st.column_config.NumberColumn("In this window"),
    "max_score": st.column_config.NumberColumn("Highest risk", format="percent"),
    "transfers": st.column_config.NumberColumn("Transfers"),
    "counterparties": st.column_config.NumberColumn("Receivers", help="Distinct accounts it sent to."),
    "last_seen": st.column_config.TextColumn("Last seen"),
    "event_time": st.column_config.TextColumn("Time"),
    "direction": st.column_config.TextColumn("Direction"),
    "to_account": st.column_config.TextColumn("Receiver"),
    "amount": st.column_config.NumberColumn("Amount", format="localized", help="In the transfer's own currency."),
    "payment_currency": st.column_config.TextColumn("Currency"),
    "payment_format": st.column_config.TextColumn("Format"),
    "score": st.column_config.NumberColumn("Risk", format="percent"),
    "is_alert": st.column_config.CheckboxColumn("Alert"),
}

HISTORY_CONFIG = {**COLUMN_CONFIG, "from_account": st.column_config.TextColumn("Sender")}

watched = data.watch_list(run_id)
theme.hero(
    "Watch-list",
    "Senders with two or more alerts, counting the history before this window",
    [
        Kpi("Accounts on the watch-list", f"{len(watched):,}", "◉", pill="Top 200" if len(watched) >= 200 else ""),
        Kpi("Most alerts from one sender", f"{int(watched['alerts'].max()) if len(watched) else 0:,}", "⚑"),
        Kpi("Alerting again in this window", f"{int((watched['alerts_this_run'] > 0).sum()) if len(watched) else 0:,}",
            "↻", pill="This window", tone="amber", help="Watch-listed senders with an alert in the selected window."),
        Kpi("Highest risk among them", f"{watched['max_score'].max():.0%}" if len(watched) else "–", "!",
            help="The highest risk score any watch-listed sender has reached."),
    ],
    badge=theme.window_badge(run_id),
)

with theme.card("watch-list"):
    st.subheader("Repeat senders")
    if watched.empty:
        st.info("No account has raised more than one alert yet.")
    else:
        columns = ["from_account", "from_bank", "alerts", "alerts_this_run", "max_score", "transfers", "counterparties", "last_seen"]
        st.dataframe(watched[columns], hide_index=True, column_config=COLUMN_CONFIG, height=360, width="stretch")

with theme.card("lookup"):
    st.subheader("Look up an account")
    account = st.text_input("Account id", placeholder="e.g. 8000EBD30").strip()
    if account:
        if not metrics.is_account_id(account):
            st.warning("Account ids are letters and digits only.")
        else:
            history = metrics.add_native_amounts(data.account_history(run_id, account))
            if history.empty:
                st.info(f"No transfers for {account} on record.")
            else:
                alert_count = int(history["is_alert"].fillna(False).sum())
                st.caption(f"{len(history):,} transfers on record (newest 200), {alert_count} of them alerts.")
                columns = ["event_time", "direction", "from_account", "to_account", "amount", "payment_currency", "payment_format", "score", "is_alert"]
                st.dataframe(history[columns], hide_index=True, column_config=HISTORY_CONFIG, width="stretch")

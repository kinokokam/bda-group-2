"""Model performance: the alerts in a window measured against known outcomes.

For whoever owns the model. Reviewers work without outcomes, so nothing here
appears on the monitoring pages.
"""

from datetime import datetime

import pandas as pd
import streamlit as st

import data
import metrics
import theme
from theme import Kpi

REFRESH = "15s"

run_id = st.session_state["run_id"]
is_history = run_id == metrics.BACKFILL_RUN


def percent(value) -> str:
    """Format a share, or a dash when it is undefined."""
    return "–" if value is None else f"{value:.1%}"


def render_window() -> None:
    """Draw the outcome figures for the selected window."""
    frames = data.evaluation(run_id, by_hour=is_history)
    totals = frames["totals"].iloc[0]
    counts = {name: int(totals[name]) for name in ("scored", "alerts", "laundering", "caught")}
    figures = metrics.evaluation(**counts)

    theme.hero(
        "Model performance",
        f"Alerts measured against known outcomes, for model owners · refreshed {datetime.now():%H:%M:%S}",
        [
            Kpi("Laundering caught", f"{counts['caught']:,}", "✓", pill=f"Of {counts['laundering']:,}"),
            Kpi("Precision", percent(figures["precision"]), "◎", pill=f"Of {counts['alerts']:,} alerts",
                help="Share of alerts that were laundering."),
            Kpi("Recall", percent(figures["recall"]), "↺", help="Share of laundering that raised an alert."),
            Kpi("Lift", "–" if figures["lift"] is None else f"{figures['lift']:.0f}×", "↗",
                pill=f"Vs {percent(figures['base_rate'])} base rate", tone="amber",
                help="Precision against the base rate: how much better the queue is than reviewing "
                "transactions at random."),
        ],
        badge=theme.window_badge(run_id),
    )

    tiers, timeline = st.columns([2, 3])
    with tiers, theme.card("tiers"):
        st.subheader("Hit rate by priority")
        by_priority = (
            frames["priority"].set_index("priority").reindex(metrics.PRIORITIES).fillna(0).astype(int)
        )
        by_priority["hit_rate"] = (by_priority["caught"] / by_priority["alerts"].where(by_priority["alerts"] > 0))
        st.dataframe(
            theme.with_priority_pills(by_priority.reset_index()),
            hide_index=True,
            width="stretch",
            column_config={
                "priority": theme.PRIORITY_PILL,
                "alerts": st.column_config.NumberColumn("Alerts"),
                "caught": st.column_config.NumberColumn("Laundering"),
                "hit_rate": st.column_config.NumberColumn("Hit rate", format="percent"),
            },
        )
        st.caption("Higher tiers should hit more often; if they don't, the cut-offs need revisiting.")
    with timeline, theme.card("timeline"):
        unit = "hour" if is_history else "minute"
        st.subheader(f"Laundering caught and missed per {unit}")
        activity = frames["activity"]
        if activity.empty:
            st.caption("Nothing scored yet.")
        else:
            # Caught in the band green, missed in the BQ2 fail red.
            activity = activity.set_index(activity["period"].map(lambda period: metrics.period_label(period, is_history)))
            st.line_chart(activity[["caught", "missed"]], height=260, color=["#14574A", "#C93B3B"],
                          x_label=unit.capitalize(), y_label="Transfers")


def render_offline() -> None:
    """Draw the model's held-out test results and its operating threshold."""
    files = data.model_files()
    config, test = files["config"], files["test"]
    if not config and not test:
        return
    with theme.card("offline"):
        render_offline_card(config, test)


def render_offline_card(config: dict, test: dict) -> None:
    """Draw the held-out results inside their card."""
    st.subheader("Held-out test results")
    if config:
        st.caption(
            f"Model {config.get('model_version', '?')} · alerts at a risk of "
            f"{float(config.get('calibrated_threshold', 0)):.1%} or more"
        )
    if test:
        pr_auc, roc_auc, test_precision, test_recall = st.columns(4)
        pr_auc.metric("PR-AUC", f"{test['pr_auc']:.3f}")
        roc_auc.metric("ROC-AUC", f"{test['roc_auc']:.3f}")
        test_precision.metric("Test precision", percent(test.get("precision")))
        test_recall.metric("Test recall", percent(test.get("recall")))

        capacity = test.get("top_k_alert_capacity", {})
        if capacity:
            table = pd.DataFrame(
                {
                    "reviewed": [key.split("_")[1].replace("pct", "%") for key in capacity],
                    "recall": list(capacity.values()),
                }
            )
            st.dataframe(
                table,
                hide_index=True,
                column_config={
                    "reviewed": st.column_config.TextColumn("Top-ranked transactions reviewed"),
                    "recall": st.column_config.NumberColumn("Laundering found", format="percent"),
                },
            )
        st.caption(
            f"Measured on {test.get('test_sample_count', 0):,} later transactions the model never saw "
            f"in training, {test.get('test_positive_count', 0):,} of them laundering."
        )


if is_history:
    render_window()
else:
    st.fragment(render_window, run_every=REFRESH)()
st.caption("The monitoring pages never show outcomes, since reviewers work without them.")
render_offline()

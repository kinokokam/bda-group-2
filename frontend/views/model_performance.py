"""Model performance: the alerts in a window measured against known outcomes.

For whoever owns the model. Reviewers work without outcomes, so nothing here
appears on the monitoring pages.
"""

from datetime import datetime

import pandas as pd
import streamlit as st

import data
import metrics

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

    st.caption(f"Refreshed {datetime.now():%H:%M:%S}")

    caught, precision, recall, lift = st.columns(4)
    caught.metric("Laundering caught", f"{counts['caught']:,} of {counts['laundering']:,}")
    precision.metric("Precision", percent(figures["precision"]), help="Share of alerts that were laundering.")
    recall.metric("Recall", percent(figures["recall"]), help="Share of laundering that raised an alert.")
    lift.metric(
        "Lift",
        "–" if figures["lift"] is None else f"{figures['lift']:.0f}×",
        help=f"Precision against the base rate of {percent(figures['base_rate'])}: how much better "
        "the queue is than reviewing transactions at random.",
    )

    tiers, timeline = st.columns([2, 3])
    with tiers:
        st.subheader("Hit rate by priority")
        by_priority = (
            frames["priority"].set_index("priority").reindex(metrics.PRIORITIES).fillna(0).astype(int)
        )
        by_priority["hit_rate"] = (by_priority["caught"] / by_priority["alerts"].where(by_priority["alerts"] > 0))
        st.dataframe(
            by_priority.reset_index(),
            hide_index=True,
            width="stretch",
            column_config={
                "priority": st.column_config.TextColumn("Priority"),
                "alerts": st.column_config.NumberColumn("Alerts"),
                "caught": st.column_config.NumberColumn("Laundering"),
                "hit_rate": st.column_config.NumberColumn("Hit rate", format="percent"),
            },
        )
        st.caption("Higher tiers should hit more often; if they don't, the cut-offs need revisiting.")
    with timeline:
        unit = "hour" if is_history else "minute"
        st.subheader(f"Laundering caught and missed per {unit}")
        activity = frames["activity"]
        if activity.empty:
            st.caption("Nothing scored yet.")
        else:
            st.line_chart(activity.set_index("period")[["caught", "missed"]], height=260)


def render_offline() -> None:
    """Draw the model's held-out test results and its operating threshold."""
    files = data.model_files()
    config, test = files["config"], files["test"]
    if not config and not test:
        return

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


st.header("Model performance")
st.caption(
    "How the alerts compare with known outcomes. For model owners; the monitoring "
    "pages never show outcomes, since reviewers work without them."
)
if is_history:
    render_window()
else:
    st.fragment(render_window, run_every=REFRESH)()
st.divider()
render_offline()

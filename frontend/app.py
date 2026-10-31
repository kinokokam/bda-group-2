"""Analyst dashboard over the Pipeline B gold table.

Reads `aml_bq1_catalog.scored` through Athena and never writes. The sidebar
picks the data window and is shared by every view; the views live in `views/`
rather than `pages/` because `st.navigation` replaces Streamlit's folder
discovery.

Run from the repository root:
    AWS_PROFILE=bda streamlit run frontend/app.py
"""

import streamlit as st

import athena
import data
import metrics

st.set_page_config(page_title="AML alert triage", page_icon=":material/policy:", layout="wide")

with st.sidebar:
    st.title("AML alert triage")

    try:
        runs = data.runs()
    except Exception as error:
        st.error(
            "Could not load scored transactions. Check that `AWS_PROFILE` names a "
            f"profile with access to them (region {athena.REGION}).\n\n{error}"
        )
        st.stop()

    if runs.empty:
        st.warning("No scored transactions yet.")
        st.stop()

    labels = {
        row.run_id: metrics.window_label(row.run_id, row.first_event, row.last_event)
        for row in runs.itertuples()
    }
    run_ids = runs["run_id"].tolist()
    newest = metrics.default_run(run_ids)
    # Follow a feed that started after the page was opened; otherwise keep
    # whatever the analyst picked.
    is_new_feed = newest not in st.session_state.get("known_runs", run_ids)
    if st.session_state.get("run_id") not in run_ids or is_new_feed:
        st.session_state["run_id"] = newest
    st.session_state["known_runs"] = run_ids
    st.selectbox(
        "Data window",
        options=run_ids,
        key="run_id",
        format_func=labels.get,
        help="A live feed updates as transactions arrive; the history is the days before it.",
    )

    @st.fragment(run_every=f"{data.RUNS_TTL_SECONDS}s")
    def follow_new_feed() -> None:
        """Rerun the whole page when a feed appears, so the picker switches to it."""
        if metrics.default_run(data.runs()["run_id"].tolist()) not in st.session_state["known_runs"]:
            st.rerun()

    follow_new_feed()

navigation = st.navigation(
    {
        "Monitoring": [
            st.Page("views/overview.py", title="Overview", icon=":material/dashboard:", default=True),
            st.Page("views/alert_queue.py", title="Alert queue", icon=":material/notifications:"),
            st.Page("views/watch_list.py", title="Watch-list", icon=":material/visibility:"),
        ],
        "Model": [
            st.Page("views/model_performance.py", title="Model performance", icon=":material/analytics:"),
        ],
    }
)
navigation.run()

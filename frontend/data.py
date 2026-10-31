"""Cached loaders shared by the views.

Iceberg commits once per Flink checkpoint (every 60 s, `infra/apps.tf`), so
rows arrive in batches; a short cache keeps a refreshing page from querying
faster than anything can change, and every Athena query bills a 10 MB minimum.
"""

from typing import Optional

import pandas as pd
import streamlit as st

import athena
import queries

LIVE_TTL_SECONDS = 10
RUNS_TTL_SECONDS = 30
MODEL_TTL_SECONDS = 3600

# Minute buckets for a two-hour feed, hour buckets for the 2.5-day history.
MINUTE_WIDTH = 16
HOUR_WIDTH = 13


def period_width(by_hour: bool) -> int:
    """Return how many characters of `event_time` make one chart bucket."""
    return HOUR_WIDTH if by_hour else MINUTE_WIDTH


@st.cache_data(ttl=RUNS_TTL_SECONDS, show_spinner=False)
def runs() -> pd.DataFrame:
    """Return one row per run in the gold table, newest id first."""
    return athena.query(queries.RUNS)


@st.cache_data(ttl=LIVE_TTL_SECONDS, show_spinner=False)
def overview(run_id: str, by_hour: bool) -> dict[str, pd.DataFrame]:
    """Return everything the overview page draws, queried in parallel."""
    width = period_width(by_hour)
    return athena.query_many(
        {
            "totals": (queries.OVERVIEW, [run_id]),
            "activity": (queries.ACTIVITY.format(width=width), [run_id]),
            "formats": (queries.ALERTS_BY_FORMAT, [run_id]),
            "queue": (queries.ALERT_QUEUE, [run_id]),
            "watch": (queries.WATCH_LIST, [run_id, run_id]),
        }
    )


@st.cache_data(ttl=LIVE_TTL_SECONDS, show_spinner=False)
def alert_queue(run_id: str) -> pd.DataFrame:
    """Return the run's alerts, highest risk score first."""
    return athena.query(queries.ALERT_QUEUE, [run_id])


@st.cache_data(ttl=LIVE_TTL_SECONDS, show_spinner=False)
def account_history(run_id: str, account: str) -> pd.DataFrame:
    """Return an account's transfers across the history and the run, newest first."""
    return athena.query(queries.ACCOUNT_HISTORY, [account, run_id, account, account])


@st.cache_data(ttl=LIVE_TTL_SECONDS, show_spinner=False)
def watch_list(run_id: str) -> pd.DataFrame:
    """Return sender accounts with two or more alerts across the history and the run."""
    return athena.query(queries.WATCH_LIST, [run_id, run_id])


@st.cache_data(ttl=LIVE_TTL_SECONDS, show_spinner=False)
def evaluation(run_id: str, by_hour: bool) -> dict[str, pd.DataFrame]:
    """Return the outcome-based figures for the model performance page."""
    width = period_width(by_hour)
    return athena.query_many(
        {
            "totals": (queries.EVALUATION_TOTALS, [run_id]),
            "priority": (queries.EVALUATION_BY_PRIORITY, [run_id]),
            "activity": (queries.EVALUATION_ACTIVITY.format(width=width), [run_id]),
        }
    )


@st.cache_data(ttl=MODEL_TTL_SECONDS, show_spinner=False)
def model_files() -> dict[str, Optional[dict]]:
    """Return the model's `config.json` and offline `metrics.json`, None where unreadable."""
    files = {}
    for name, key in (("config", "model/config.json"), ("test", "evaluation/metrics.json")):
        try:
            files[name] = athena.read_json(key)
        except Exception:  # Optional panels; never fail the page over them.
            files[name] = None
    return files

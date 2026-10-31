import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "frontend"))

import athena  # noqa: E402
import metrics  # noqa: E402
import queries  # noqa: E402


def column(name, kind):
    return {"Name": name, "Type": kind}


def row(*values):
    return {"Data": [{} if value is None else {"VarCharValue": value} for value in values]}


COLUMNS = [column("run_id", "varchar"), column("scored", "bigint"), column("score", "double"), column("is_alert", "boolean")]
PAGES = [
    {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": COLUMNS},
                   "Rows": [row("run_id", "scored", "score", "is_alert"), row("backfill", "1241719", "0.91", "true")]}},
    {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": COLUMNS},
                   "Rows": [row("20261027-1400", None, None, None)]}},
]


class StubAthena:
    def __init__(self, states, pages=PAGES):
        self.states = list(states)
        self.pages = pages
        self.request = None
        self.stopped = False

    def start_query_execution(self, **request):
        self.request = request
        return {"QueryExecutionId": "q1"}

    def get_query_execution(self, QueryExecutionId):
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        return {"QueryExecution": {"Status": {"State": state, "StateChangeReason": "TABLE_NOT_FOUND"}}}

    def stop_query_execution(self, QueryExecutionId):
        self.stopped = True

    def get_paginator(self, name):
        assert name == "get_query_results"
        pages = self.pages

        class Paginator:
            def paginate(self, QueryExecutionId):
                return iter(pages)

        return Paginator()


def test_to_frame_skips_header_once_and_casts_types():
    frame = athena.to_frame(PAGES)
    assert frame["run_id"].tolist() == ["backfill", "20261027-1400"]
    assert frame["scored"].dtype == "Int64"
    assert frame.loc[0, "scored"] == 1241719
    assert pd.isna(frame.loc[1, "scored"])
    assert frame.loc[0, "score"] == pytest.approx(0.91)
    assert frame["is_alert"].dtype == "boolean"
    assert frame.loc[0, "is_alert"]
    assert pd.isna(frame.loc[1, "is_alert"])


def test_to_frame_empty_result_keeps_columns():
    page = {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": COLUMNS}, "Rows": [row("run_id", "scored", "score", "is_alert")]}}
    frame = athena.to_frame([page])
    assert frame.empty
    assert list(frame.columns) == ["run_id", "scored", "score", "is_alert"]


def test_literal_quotes_strings_and_escapes_quotes():
    assert athena.literal("8000EBD30") == "'8000EBD30'"
    assert athena.literal("x' OR '1'='1") == "'x'' OR ''1''=''1'"
    assert athena.literal(16) == "16"
    with pytest.raises(TypeError):
        athena.literal(True)


def test_query_binds_parameters_in_bq1_workgroup(monkeypatch):
    monkeypatch.setattr(athena, "POLL_SECONDS", 0)
    client = StubAthena(["QUEUED", "RUNNING", "SUCCEEDED"])
    frame = athena.query("SELECT 1 WHERE run_id = ?", ["backfill"], client=client)
    assert client.request["WorkGroup"] == "aml-bq1"
    assert client.request["QueryExecutionContext"] == {"Database": "aml_bq1_catalog"}
    assert client.request["ExecutionParameters"] == ["'backfill'"]
    assert len(frame) == 2


def test_query_without_parameters_sends_none():
    client = StubAthena(["SUCCEEDED"])
    athena.query("SELECT 1", client=client)
    assert "ExecutionParameters" not in client.request


def test_query_raises_with_athena_reason():
    with pytest.raises(athena.QueryFailed, match="TABLE_NOT_FOUND"):
        athena.query("SELECT 1", client=StubAthena(["FAILED"]))


def test_query_stops_on_timeout(monkeypatch):
    monkeypatch.setattr(athena, "POLL_SECONDS", 0)
    monkeypatch.setattr(athena, "TIMEOUT_SECONDS", -1)
    client = StubAthena(["RUNNING"])
    with pytest.raises(athena.QueryFailed, match="timed out"):
        athena.query("SELECT 1", client=client)
    assert client.stopped


def test_rates_match_the_scoring_job():
    path = Path(__file__).resolve().parents[1] / "bq1" / "flink" / "features.py"
    spec = importlib.util.spec_from_file_location("bq1_features", path)
    features = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(features)
    assert queries.PIPELINE_FX_RATES == features.FX_RATES


def test_named_usd_currencies_are_the_converted_ones():
    for name in metrics.USD_RATED_NAMES:
        assert metrics.is_usd_rated(name)
    assert not metrics.is_usd_rated("Ruble")
    assert not metrics.is_usd_rated("Yen")  # the job knows JPY, but the data says Yen
    assert not metrics.is_usd_rated(None)


@pytest.mark.parametrize("amount_usd, currency, native", [
    (1080.0, "Euro", 1000.0),               # converted at 1.08, so undo it
    (27585439848.38, "Ruble", 27585439848.38),  # never converted: already in rubles
    (500.0, "US Dollar", 500.0),
    (None, "Euro", None),
])
def test_native_amount_undoes_the_scoring_job_rate(amount_usd, currency, native):
    assert metrics.native_amount(amount_usd, currency) == native


def test_alert_value_only_sums_converted_currencies():
    assert queries.USD_RATED in queries.OVERVIEW
    assert "'EURO'" in queries.USD_RATED and "'RUBLE'" not in queries.USD_RATED


def test_display_value_formats_numbers_for_reading():
    assert metrics.display_value(9500.0) == "9,500"
    assert metrics.display_value(0.1234) == "0.12"
    assert metrics.display_value("ACH") == "ACH"


def test_evaluation_matches_the_measured_demo():
    # bq1/README_PIPELINE.md §6.3: 16,098 scored, 126 alerts, 35 of 66 caught.
    figures = metrics.evaluation(scored=16098, alerts=126, laundering=66, caught=35)
    assert figures["precision"] == pytest.approx(0.278, abs=1e-3)
    assert figures["recall"] == pytest.approx(0.530, abs=1e-3)
    assert round(figures["lift"]) == 68


def test_evaluation_with_nothing_scored_is_undefined_not_an_error():
    figures = metrics.evaluation(0, 0, 0, 0)
    assert set(figures.values()) == {None}


def test_parse_contributors_orders_by_absolute_contribution_with_readable_names():
    raw = ('[{"feature": "tx_hour", "value": 3.0, "contribution": 0.4},'
           ' {"feature": "amount_paid_usd", "value": 9500.0, "contribution": -1.2}]')
    frame = metrics.parse_contributors(raw)
    assert frame["feature"].tolist() == ["amount_paid_usd", "tx_hour"]
    assert frame["reason"].tolist() == ["Amount paid (USD)", "Hour of day"]
    assert frame["value"].tolist() == ["9,500", "3"]
    assert list(frame.columns) == ["feature", "reason", "value", "contribution"]


def test_parse_contributors_shows_the_category_not_its_code():
    raw = '[{"feature": "payment_format_code", "value": 0.0, "contribution": 2.1}]'
    frame = metrics.parse_contributors(raw, {"payment_format": "ACH"})
    assert frame.loc[0, "reason"] == "Payment format"
    assert frame.loc[0, "value"] == "ACH"


def test_parse_contributors_hides_codes_it_cannot_translate():
    # The gold table keeps no receiving currency, so its code has no original to show.
    raw = '[{"feature": "receiving_currency_code", "value": 3.0, "contribution": 0.5}]'
    assert metrics.parse_contributors(raw, {"payment_format": "ACH"}).loc[0, "value"] == "–"
    assert metrics.parse_contributors(raw).loc[0, "value"] == "–"


def test_alert_queue_selects_only_columns_the_gold_table_has():
    # GOLD_COLUMNS in bq1/flink/main.py; a missing one fails every query at runtime.
    gold = {"txn_id", "event_time", "event_date", "run_id", "from_bank", "from_account", "to_bank", "to_account",
            "amount_paid_usd", "amount_received_usd", "payment_currency", "payment_format", "is_cross_currency",
            "is_cross_bank", "is_structuring_threshold", "sender_tx_count_24h", "sender_volume_24h",
            "receiver_tx_count_24h", "raw_margin", "score", "is_alert", "label", "top_contributors"}
    selected = queries.ALERT_QUEUE.split("SELECT", 1)[1].split("FROM", 1)[0]
    assert {name.strip() for name in selected.split(",")} <= gold


def test_every_model_feature_has_a_readable_name():
    # The order Pipeline A writes to xgboost_features.json (models/train_xgboost.py).
    numeric = [
        "amount_paid_usd", "amount_received_usd", "paid_fx_rate", "recv_fx_rate", "fx_amount_discrepancy",
        "is_structuring_threshold", "is_cross_currency", "is_cross_bank", "is_round_amount", "tx_hour",
        "tx_dayofweek", "tx_dayofmonth", "is_weekend", "sender_tx_count_1h", "sender_volume_1h",
        "sender_tx_count_24h", "sender_volume_24h", "sender_tx_count_7d", "sender_volume_7d",
        "sender_burst_ratio_1h_24h", "sender_burst_ratio_24h_7d", "sender_time_delta_sec",
        "receiver_tx_count_24h", "receiver_volume_24h", "receiver_time_delta_sec",
    ]
    coded = [f"{name}_code" for name in ("payment_format", "payment_currency", "receiving_currency", "from_bank", "to_bank")]
    assert set(numeric + coded) == set(metrics.FEATURE_LABELS)
    assert metrics.feature_label("something_new") == "something_new"


@pytest.mark.parametrize("score, tier", [(0.72, "High"), (0.20, "High"), (0.16, "Medium"), (0.10, "Medium"),
                                         (0.084, "Low"), (None, "Low"), (float("nan"), "Low")])
def test_priority_tiers(score, tier):
    assert metrics.priority(score) == tier


def test_window_labels_read_like_an_analyst_tool():
    assert metrics.window_label("20261031-1854") == "Live feed, started 31 Oct 18:54"
    assert metrics.window_label("backfill", "2022-09-08 00:00:00", "2022-09-10 10:59:00") == \
        "History, 2022-09-08 to 2022-09-10"
    assert metrics.window_label("backfill") == "History"
    assert metrics.window_label("smoke") == "smoke"


def test_analyst_queries_never_read_outcomes():
    analyst = [queries.RUNS, queries.OVERVIEW, queries.ACTIVITY, queries.ALERTS_BY_FORMAT,
               queries.ALERT_QUEUE, queries.ACCOUNT_HISTORY, queries.WATCH_LIST]
    for sql in analyst:
        assert "label" not in sql.lower()
    assert "label" in queries.EVALUATION_TOTALS


def test_query_many_returns_each_result_by_name():
    results = athena.query_many({"a": ("SELECT 1", None), "b": ("SELECT 2", ["x"])},
                                client=StubAthena(["SUCCEEDED"]))
    assert set(results) == {"a", "b"}
    assert all(len(frame) == 2 for frame in results.values())


@pytest.mark.parametrize("raw", [None, "", "[]", "not json"])
def test_parse_contributors_empty_for_non_alerts(raw):
    assert metrics.parse_contributors(raw).empty


def test_default_run_prefers_newest_demo_then_backfill():
    assert metrics.default_run(["backfill", "20261027-1400", "20261028-0930"]) == "20261028-0930"
    assert metrics.default_run(["smoke", "backfill"]) == "backfill"
    assert metrics.default_run(["smoke"]) == "smoke"
    assert metrics.default_run([]) is None


def test_is_account_id_rejects_free_text():
    assert metrics.is_account_id("8000EBD30")
    assert metrics.is_account_id(" 8000EBD30 ")
    assert not metrics.is_account_id("8000EBD30' OR 1=1 --")
    assert not metrics.is_account_id("")

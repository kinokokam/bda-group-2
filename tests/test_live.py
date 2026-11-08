"""Live alerts from the Kafka alerts topic (frontend/live.py), with no broker or AWS."""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "frontend"))

import live  # noqa: E402
import metrics  # noqa: E402
import queries  # noqa: E402

RUN = "20261027-1430"


def message(txn_id, score, **fields):
    """A scored record as Flink writes it to the alerts topic, outcome included."""
    record = {
        "txn_id": txn_id, "event_time": "2022-09-10 11:01:00", "score": score, "is_alert": True,
        "from_bank": "011", "from_account": "A1", "to_bank": "012", "to_account": "B1",
        "amount_paid_usd": 100.0, "amount_received_usd": 100.0, "payment_currency": "US Dollar",
        "payment_format": "ACH", "is_cross_bank": 1, "is_cross_currency": 0, "is_structuring_threshold": 0,
        "sender_tx_count_24h": 3, "sender_volume_24h": 300.0, "receiver_tx_count_24h": 1,
        "label": 1, "raw_margin": 2.5, "run_id": RUN, "payment_format_code": 0,
        "top_contributors": [{"feature": "payment_format_code", "value": 0.0, "contribution": 2.1}],
    }
    record.update(fields)
    return record


def test_alert_row_keeps_only_alert_queue_fields_and_never_the_outcome():
    row = live.alert_row(message("t1", 0.3))
    assert list(row) == list(queries.ALERT_QUEUE_COLUMNS)
    assert "label" not in row and "raw_margin" not in row


def test_alert_row_contributors_read_like_the_gold_table():
    row = live.alert_row(message("t1", 0.3))
    assert isinstance(row["top_contributors"], str)
    reasons = metrics.parse_contributors(row["top_contributors"], row)
    assert reasons.loc[0, "reason"] == "Payment format"
    assert reasons.loc[0, "value"] == "ACH"
    assert live.alert_row(message("t2", 0.3, top_contributors=None))["top_contributors"] == "[]"


def test_store_keeps_one_copy_of_a_resent_alert_and_ranks_by_score():
    store = live.AlertStore()
    store.add(RUN, message("low", 0.08))
    store.add(RUN, message("high", 0.40))
    store.add(RUN, message("high", 0.40))           # resent after a Flink restart
    store.add(RUN, message("tie", 0.40, event_time="2022-09-10 11:00:00"))
    frame = store.frame(RUN)
    assert frame["txn_id"].tolist() == ["tie", "high", "low"]
    assert list(frame.columns) == list(queries.ALERT_QUEUE_COLUMNS)


def test_store_knows_a_run_before_its_first_alert():
    store = live.AlertStore()
    store.add_run(RUN)
    assert store.has(RUN) and not store.has("20261028-0900")
    empty = store.frame(RUN)
    assert empty.empty and list(empty.columns) == list(queries.ALERT_QUEUE_COLUMNS)


@pytest.mark.parametrize("topic, run_id", [
    ("alerts-20261027-1430", "20261027-1430"),
    ("transactions-20261027-1430", None),
    ("rejected-20261027-1430", None),
    ("alerts-smoke", None),
    ("alerts-backfill", None),
    ("__consumer_offsets", None),
])
def test_run_of_reads_only_demo_alert_topics(topic, run_id):
    assert live.run_of(topic) == run_id


class StubKafkaApi:
    def __init__(self, clusters):
        self.clusters = clusters

    def list_clusters_v2(self, ClusterNameFilter):
        return {"ClusterInfoList": self.clusters}

    def get_bootstrap_brokers(self, ClusterArn):
        return {"BootstrapBrokerStringSaslIam": f"b-1.{ClusterArn}:9098,b-2.{ClusterArn}:9098"}


def test_bootstrap_uses_the_active_cluster_of_that_exact_name():
    api = StubKafkaApi([
        {"ClusterName": "aml-bq1-old", "State": "ACTIVE", "ClusterArn": "old"},
        {"ClusterName": "aml-bq1", "State": "ACTIVE", "ClusterArn": "main"},
    ])
    assert live.bootstrap("aml-bq1", client=api) == "b-1.main:9098,b-2.main:9098"


def test_bootstrap_is_none_until_the_cluster_is_active():
    api = StubKafkaApi([{"ClusterName": "aml-bq1", "State": "CREATING", "ClusterArn": "main"}])
    assert live.bootstrap("aml-bq1", client=api) is None


@pytest.mark.parametrize("raw, parsed", [(b'{"a": 1}', {"a": 1}), (b"not json", None), (b"[1]", None), (None, None)])
def test_decode_accepts_only_json_objects(raw, parsed):
    assert live.decode(raw) == parsed


class Done(Exception):
    """Ends the hub's loop once the fake consumer runs out of messages."""


class Record:
    def __init__(self, topic, value):
        self.topic = topic
        self.value = value


class FakeConsumer:
    def __init__(self, topics, batches):
        self._topics = topics
        self.batches = list(batches)
        self.assigned = []

    def topics(self):
        return set(self._topics)

    def partitions_for_topic(self, topic):
        return {0}

    def assign(self, partitions):
        self.assigned = list(partitions)

    def assignment(self):
        return set(self.assigned)

    def poll(self, timeout_ms):
        if not self.batches:
            raise Done
        return {"partition": self.batches.pop(0)}


def test_hub_follows_only_alert_topics_and_stores_each_alert_once():
    topic = f"alerts-{RUN}"
    consumer = FakeConsumer(
        [topic, f"transactions-{RUN}", f"rejected-{RUN}"],
        [[Record(topic, message("t1", 0.3)), Record(topic, None)], [Record(topic, message("t1", 0.3))]],
    )
    hub = live.Hub("aml-bq1", "ap-southeast-1")
    with pytest.raises(Done):
        hub.follow(consumer)
    assert [tp.topic for tp in consumer.assigned] == [topic]
    assert hub.store.frame(RUN)["txn_id"].tolist() == ["t1"]


def test_alerts_fall_back_to_athena_without_a_cluster(monkeypatch):
    monkeypatch.setattr(live, "MSK_CLUSTER", "")
    assert live.alerts(RUN) is None
    assert live.runs() == []


def test_alerts_come_live_only_for_runs_the_connected_hub_has_seen(monkeypatch):
    hub = live.Hub("aml-bq1", "ap-southeast-1")
    hub.store.add(RUN, message("t1", 0.3))
    monkeypatch.setattr(live, "MSK_CLUSTER", "aml-bq1")
    monkeypatch.setattr(live, "_hub", hub)

    assert live.alerts(RUN) is None                  # not connected yet: Athena
    hub.connected = True
    assert live.alerts(RUN)["txn_id"].tolist() == ["t1"]
    assert live.alerts("backfill") is None           # history is never streamed
    assert live.runs() == [RUN]


def alerts_frame():
    rows = [
        message("a", 0.25, from_account="A", payment_currency="Euro", amount_paid_usd=108.0,
                event_time="2022-09-10 11:00:10", payment_format="ACH"),
        message("b", 0.15, from_account="A", payment_currency="Ruble", amount_paid_usd=5000.0,
                event_time="2022-09-10 11:00:50", payment_format="ACH"),
        message("c", 0.08, from_account="B", payment_currency="US Dollar", amount_paid_usd=50.0,
                event_time="2022-09-10 11:02:00", payment_format="Cheque"),
    ]
    return pd.DataFrame([live.alert_row(row) for row in rows])


def test_alert_summary_matches_the_overview_query():
    summary = metrics.alert_summary(alerts_frame())
    assert summary == {
        "alerts": 3, "high": 1, "medium": 1, "accounts_alerted": 2, "last_alert": "2022-09-10 11:02:00",
    }


def test_alert_summary_of_no_alerts():
    summary = metrics.alert_summary(pd.DataFrame(columns=list(queries.ALERT_QUEUE_COLUMNS)))
    assert summary["alerts"] == 0 and summary["accounts_alerted"] == 0 and summary["last_alert"] is None


def test_alerts_by_period_and_format_match_their_queries():
    frame = alerts_frame()
    by_minute = metrics.alerts_by_period(frame, 16)
    assert by_minute.to_dict("list") == {"period": ["2022-09-10 11:00", "2022-09-10 11:02"], "alerts": [2, 1]}
    assert metrics.alerts_by_format(frame).to_dict("list") == {"payment_format": ["ACH", "Cheque"], "alerts": [2, 1]}


def test_new_queries_and_fields_never_read_outcomes():
    assert "label" not in queries.SCREENED.lower()
    assert "label" not in queries.ALERT_QUEUE_COLUMNS

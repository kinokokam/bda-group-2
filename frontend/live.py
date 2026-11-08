"""Live alerts from the Kafka alerts topic, for the dashboard on EC2.

Flink writes every alert to `alerts-<run_id>` the moment it is scored, while
the gold table only commits at each 60-second checkpoint. Reading the topic
puts alerts on screen within seconds; everything that needs transfers that
did not alert, or the history, still comes from Athena.

One background thread per process (`Hub`) holds one consumer, assigned to every
`alerts-<run_id>` topic on the cluster and reading each from its start, so all
browser sessions share it and a page opened mid-demo still sees the whole run.
It never joins a consumer group, so it commits no offsets and needs no group
permissions. Alerts are kept by `txn_id`: the Flink Kafka sink is not
exactly-once, so a restart can send one again.

The brokers are private to the VPC, so only the EC2 instance can reach them.
Live alerts are on only where `MSK_CLUSTER` is set, which the instance's
service does; anywhere else, or until the hub connects, `alerts` returns None
and the views read Athena as before.
"""

import json
import logging
import os
import threading
import time
from typing import Callable, Optional

import pandas as pd

import athena
import metrics
import queries

LOGGER = logging.getLogger("live")

MSK_CLUSTER = os.environ.get("MSK_CLUSTER", "")
TOPIC_PREFIX = "alerts-"

RETRY_SECONDS = 30
TOPICS_EVERY_SECONDS = 5
POLL_MILLISECONDS = 500
BOOTSTRAP_TIMEOUT_MILLISECONDS = 10_000


def alert_row(message: dict) -> dict:
    """Keep the alert-queue fields of one Kafka message.

    The message is Flink's whole scored record, `label` included. Taking only
    `queries.ALERT_QUEUE_COLUMNS` keeps the outcome off the analyst pages, as
    the Athena queries do, and gives the same frame either way.

    Parameters
    ----------
    message : dict
        Decoded message from an alerts topic.

    Returns
    -------
    dict
        The alert-queue fields, with `top_contributors` as a JSON string, the
        form the gold table stores and `metrics.parse_contributors` reads.
    """
    row = {name: message.get(name) for name in queries.ALERT_QUEUE_COLUMNS}
    if not isinstance(row["top_contributors"], str):
        row["top_contributors"] = json.dumps(row["top_contributors"] or [])
    return row


def run_of(topic: str) -> Optional[str]:
    """Return the demo run an alerts topic belongs to.

    Parameters
    ----------
    topic : str
        Topic name, such as `alerts-20261027-1430`.

    Returns
    -------
    Optional[str]
        The run id, or None for any other topic.
    """
    if not topic.startswith(TOPIC_PREFIX):
        return None
    run_id = topic[len(TOPIC_PREFIX):]
    return run_id if metrics.DEMO_RUN.match(run_id) else None


def bootstrap(cluster: str, client=None) -> Optional[str]:
    """Find the cluster's private IAM broker list, as `pipeline.sh` does.

    Parameters
    ----------
    cluster : str
        MSK cluster name.
    client : botocore client, optional
        Kafka API client; tests pass a stub.

    Returns
    -------
    Optional[str]
        Comma-separated brokers on port 9098, or None when no cluster of that
        name is ACTIVE.
    """
    client = client or athena.session().client("kafka")
    found = client.list_clusters_v2(ClusterNameFilter=cluster)["ClusterInfoList"]
    active = [info for info in found if info.get("ClusterName") == cluster and info.get("State") == "ACTIVE"]
    if not active:
        return None
    brokers = client.get_bootstrap_brokers(ClusterArn=active[0]["ClusterArn"])
    return brokers.get("BootstrapBrokerStringSaslIam") or None


def decode(value: Optional[bytes]) -> Optional[dict]:
    """Parse one message value, or None when it is not a JSON object."""
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def token_provider(region: str):
    """Build the SASL/OAUTHBEARER token source MSK IAM authentication needs.

    Built on demand so the module imports without the Kafka packages; the
    class is the one in `bq1/lambdas/replay/handler.py`.

    Parameters
    ----------
    region : str
        Region the cluster is in.

    Returns
    -------
    AbstractTokenProvider
        Provider signing tokens with the instance role.
    """
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    from kafka.net.sasl.oauth import AbstractTokenProvider

    class IamTokenProvider(AbstractTokenProvider):
        """Supplies short-lived tokens; the client rejects any other type."""

        def token(self) -> str:
            signed_token, _expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
            return signed_token

    return IamTokenProvider()


class AlertStore:
    """Alerts by run, one per `txn_id`, safe to read while the hub writes."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._alerts: dict[str, dict[str, dict]] = {}

    def add_run(self, run_id: str) -> None:
        """Record a run whose topic exists, even before its first alert."""
        with self._lock:
            self._alerts.setdefault(run_id, {})

    def add(self, run_id: str, message: dict) -> None:
        """Keep one alert, replacing an earlier copy of the same transfer."""
        row = alert_row(message)
        key = str(row["txn_id"]) if row["txn_id"] else json.dumps(row, sort_keys=True, default=str)
        with self._lock:
            self._alerts.setdefault(run_id, {})[key] = row

    def runs(self) -> list[str]:
        """Return every run seen, newest first."""
        with self._lock:
            return sorted(self._alerts, reverse=True)

    def has(self, run_id: str) -> bool:
        """Report whether the run's topic has been seen."""
        with self._lock:
            return run_id in self._alerts

    def frame(self, run_id: str) -> pd.DataFrame:
        """Return the run's alerts ordered like `queries.ALERT_QUEUE`.

        Parameters
        ----------
        run_id : str
            Demo run id.

        Returns
        -------
        pd.DataFrame
            One row per alert, highest score first, then oldest first.
        """
        with self._lock:
            rows = list(self._alerts.get(run_id, {}).values())
        frame = pd.DataFrame(rows, columns=list(queries.ALERT_QUEUE_COLUMNS))
        frame["score"] = pd.to_numeric(frame["score"]).astype("float64")
        frame["amount_paid_usd"] = pd.to_numeric(frame["amount_paid_usd"]).astype("float64")
        return frame.sort_values(["score", "event_time", "txn_id"], ascending=[False, True, True],
                                 kind="stable").reset_index(drop=True)


class Hub(threading.Thread):
    """Background reader that keeps an `AlertStore` filled from every alerts topic.

    Parameters
    ----------
    cluster : str
        MSK cluster name.
    region : str
        Region the cluster is in.
    connect : callable, optional
        Returns a connected consumer; tests pass a fake.
    """

    def __init__(self, cluster: str, region: str, connect: Optional[Callable] = None) -> None:
        super().__init__(name="live-alerts", daemon=True)
        self.cluster = cluster
        self.region = region
        self.store = AlertStore()
        self.connected = False
        self.error = ""
        self._connect = connect or self._consumer

    def _consumer(self):
        """Connect a group-less consumer to the cluster's private brokers."""
        servers = bootstrap(self.cluster)
        if servers is None:
            raise RuntimeError(f"no ACTIVE MSK cluster named {self.cluster}")

        from kafka import KafkaConsumer

        return KafkaConsumer(
            bootstrap_servers=servers.split(","),
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            sasl_oauth_token_provider=token_provider(self.region),
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=decode,
            bootstrap_timeout_ms=BOOTSTRAP_TIMEOUT_MILLISECONDS,
        )

    def run(self) -> None:
        """Connect, follow the topics, and reconnect after any failure."""
        while True:
            try:
                consumer = self._connect()
                self.connected, self.error = True, ""
                self.follow(consumer)
            except Exception as error:  # Keep the thread alive; the views fall back to Athena.
                LOGGER.warning("live alerts unavailable: %s", error)
                self.connected, self.error = False, str(error)
                time.sleep(RETRY_SECONDS)

    def follow(self, consumer) -> None:
        """Assign every alerts topic and store what arrives, until an error.

        A topic added later, such as the next `predemo`'s, is picked up within
        `TOPICS_EVERY_SECONDS` and read from its start; partitions already
        assigned keep their position.

        Parameters
        ----------
        consumer : KafkaConsumer
            Connected consumer with no subscription.
        """
        from kafka import TopicPartition

        checked = float("-inf")
        while True:
            if time.monotonic() - checked >= TOPICS_EVERY_SECONDS:
                topics = sorted(topic for topic in consumer.topics() if run_of(topic))
                for topic in topics:
                    self.store.add_run(run_of(topic))
                partitions = [
                    TopicPartition(topic, partition)
                    for topic in topics
                    for partition in consumer.partitions_for_topic(topic)
                ]
                if partitions:
                    consumer.assign(partitions)
                checked = time.monotonic()

            if not consumer.assignment():
                time.sleep(POLL_MILLISECONDS / 1000)
                continue

            for records in consumer.poll(timeout_ms=POLL_MILLISECONDS).values():
                for record in records:
                    run_id = run_of(record.topic)
                    if run_id and record.value is not None:
                        self.store.add(run_id, record.value)


_hub: Optional[Hub] = None
_hub_lock = threading.Lock()


def hub() -> Optional[Hub]:
    """Return the process's hub, starting it on first use.

    Returns
    -------
    Optional[Hub]
        The running hub, or None where `MSK_CLUSTER` is unset.
    """
    global _hub
    if not MSK_CLUSTER:
        return None
    with _hub_lock:
        if _hub is None:
            _hub = Hub(MSK_CLUSTER, athena.REGION)
            _hub.start()
    return _hub


def alerts(run_id: str) -> Optional[pd.DataFrame]:
    """Return a run's alerts from the stream, or None to read Athena instead.

    Parameters
    ----------
    run_id : str
        Selected data window.

    Returns
    -------
    Optional[pd.DataFrame]
        The run's alerts, possibly none yet, when the hub is connected and has
        seen the run's topic; otherwise None.
    """
    live = hub()
    if live is None or not live.connected or not live.store.has(run_id):
        return None
    return live.store.frame(run_id)


def runs() -> list[str]:
    """Return the runs with an alerts topic, newest first; empty when not connected."""
    live = hub()
    if live is None or not live.connected:
        return []
    return live.store.runs()

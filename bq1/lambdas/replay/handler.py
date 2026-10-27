"""Replays pre-sorted hourly transaction files into MSK at a controlled pace.

Reads `bronze/hourly/YYYY-MM-DD-HH.csv`, which Phase 2 produced already sorted,
so rows stream straight through without buffering or sorting. Timestamps are
never rewritten; only the delay between sends changes.
"""

import csv
import json
import os
import time
from datetime import datetime, timedelta
from typing import Iterator

import boto3
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.net.sasl.oauth import AbstractTokenProvider

from features import dedupe_header

BUCKET = os.environ["BUCKET"]
REGION = os.environ["REGION"]
HOURLY_PREFIX = os.environ.get("HOURLY_PREFIX", "bronze/hourly/")

TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M"
HOUR_FORMAT = "%Y-%m-%dT%H:%M"

s3_client = boto3.client("s3", region_name=REGION)

def bootstrap_servers() -> str:
    """Return the cluster's IAM bootstrap endpoint from the environment.

    Read lazily and not resolved through the MSK API: this function runs in
    private subnets with no NAT, so AWS public endpoints are unreachable and
    such a call hangs until the invocation times out. `pipeline.sh` writes the
    real value in once the cluster is up.

    Returns
    -------
    str
        Comma-separated broker list for SASL/IAM on port 9098.

    Raises
    ------
    RuntimeError
        If the variable is absent or still the placeholder.
    """
    servers = os.environ.get("BOOTSTRAP", "")
    if not servers or servers == "pending":
        raise RuntimeError("BOOTSTRAP is unset; run a configure phase first")
    return servers


class IamTokenProvider(AbstractTokenProvider):
    """Supplies short-lived SASL/OAUTHBEARER tokens signed with the Lambda's role.

    The client rejects a provider that is not an AbstractTokenProvider instance,
    and reports the refusal only as a bootstrap timeout.
    """

    def token(self) -> str:
        """Return a fresh MSK authentication token.

        Returns
        -------
        str
            Signed token valid for the cluster's region.
        """
        signed_token, _expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
        return signed_token


def build_producer() -> KafkaProducer:
    """Create a batching producer authenticated to MSK with IAM.

    Returns
    -------
    KafkaProducer
        Producer configured for throughput rather than per-message durability.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers().split(","),
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=IamTokenProvider(),
        value_serializer=lambda payload: json.dumps(payload).encode(),
        key_serializer=lambda key: key.encode(),
        linger_ms=50,
        batch_size=256 * 1024,
        acks=1,
    )


def hours_between(start: datetime, end: datetime) -> Iterator[str]:
    """Yield the hourly file stems covering a half-open interval.

    Parameters
    ----------
    start : datetime
        First instant to include.
    end : datetime
        First instant to exclude.

    Yields
    ------
    str
        File stem such as `2022-09-10-11`.
    """
    cursor = start.replace(minute=0, second=0, microsecond=0)
    while cursor < end:
        yield cursor.strftime("%Y-%m-%d-%H")
        cursor += timedelta(hours=1)


def read_hour(stem: str) -> Iterator[dict[str, str]]:
    """Stream one hourly file from S3 as dictionaries.

    Parameters
    ----------
    stem : str
        File stem such as `2022-09-10-11`.

    Yields
    ------
    dict[str, str]
        One row, keys taken from the file's header.
    """
    response = s3_client.get_object(Bucket=BUCKET, Key=f"{HOURLY_PREFIX}{stem}.csv")
    lines = (line.decode() for line in response["Body"].iter_lines())
    reader = csv.reader(lines)
    header = dedupe_header(next(reader))
    for values in reader:
        yield dict(zip(header, values))


def create_topics(names: list[str]) -> dict:
    """Create topics that do not exist yet.

    Kafka auto-creation only fires on produce, so a consumer subscribing before
    the first send fails on missing topic metadata.

    Parameters
    ----------
    names : list[str]
        Topic names to create.

    Returns
    -------
    dict
        Names created and names already present.
    """
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers().split(","),
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=IamTokenProvider(),
    )
    try:
        existing = set(admin_client.list_topics())
        missing = [name for name in names if name not in existing]
        if missing:
            admin_client.create_topics(
                [
                    NewTopic(name=name, num_partitions=1, replication_factor=2)
                    for name in missing
                ]
            )
    finally:
        admin_client.close()
    return {"created": missing, "existing": sorted(set(names) & existing)}


def handler(event: dict, _context: object) -> dict:
    """Replay a window of transactions into a Kafka topic.

    Parameters
    ----------
    event : dict
        `start` and `end` as `YYYY-MM-DDTHH:MM`, `topic`, and `speed` — simulated
        seconds per real second, where 0 sends as fast as possible.
    _context : object
        Unused Lambda context.

    Returns
    -------
    dict
        Rows sent, rows the broker acknowledged, rows outside the window, and
        the elapsed wall time.

    Raises
    ------
    KeyError
        If `start`, `end` or `topic` is missing.
    RuntimeError
        If any record was not acknowledged by the broker.
    """
    if event.get("action") == "create-topics":
        return create_topics(event["topics"])

    start = datetime.strptime(event["start"], HOUR_FORMAT)
    end = datetime.strptime(event["end"], HOUR_FORMAT)
    topic = event["topic"]
    speed = float(event.get("speed", 0))

    producer = build_producer()
    pending = []
    sent = 0
    skipped = 0
    sequence = 0
    previous_epoch: float | None = None
    began = time.monotonic()

    for stem in hours_between(start, end):
        for row in read_hour(stem):
            sequence += 1
            moment = datetime.strptime(row["Timestamp"], TIMESTAMP_FORMAT)
            if moment < start or moment >= end:
                skipped += 1
                continue

            epoch = moment.timestamp()
            if speed > 0 and previous_epoch is not None:
                delay = (epoch - previous_epoch) / speed
                if delay > 0:
                    time.sleep(delay)
            previous_epoch = epoch

            row["txn_id"] = f"{stem}-{sequence:07d}"
            pending.append(producer.send(topic, key=row["Account"], value=row))
            sent += 1

    producer.flush()
    producer.close()

    failed = 0
    first_error = ""
    for future in pending:
        if not future.succeeded():
            failed += 1
            if not first_error:
                first_error = str(future.exception)

    if failed:
        raise RuntimeError(f"{failed} of {sent} records were not acknowledged: {first_error}")

    return {
        "sent": sent,
        "delivered": sent - failed,
        "skipped": skipped,
        "seconds": round(time.monotonic() - began, 1),
    }

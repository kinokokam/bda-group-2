"""Real-time AML scoring on Managed Service for Apache Flink.

Reads replayed transactions from MSK, rebuilds the rolling account features the
Spark ETL produced in batch, scores each record with the Pipeline A model, and
emits alerts, rejects and a full scored record.

Dependencies arrive as an extracted `pyArchives` bundle rather than on the
import path, so `bootstrap_dependency_path` runs before anything imports them.
"""

import importlib.util
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator, Optional

LOGGER = logging.getLogger("bq1")
LOGGER.setLevel(logging.INFO)

PROPERTIES_FILE = "/etc/flink/application_properties.json"
PROPERTY_GROUP = "BQ1"

MODEL_FILE = "xgboost_aml_model.json"
FEATURES_FILE = "xgboost_features.json"
CALIBRATOR_FILE = "calibrator.pkl"
CATEGORY_MAPS_FILE = "category_maps.json"
CONFIG_FILE = "config.json"

TOP_CONTRIBUTORS = 5


MARKER = Path("joblib") / "__init__.py"


def bootstrap_dependency_path() -> Optional[str]:
    """Add the extracted dependency bundle to the import path.

    Flink names the directory it extracts `pyArchives` into, so the bundle is
    located by probing for a package it must contain. Logs where it looked when
    it fails.

    Must be called from inside an operator's `open`, not only at module level:
    PyFlink cloudpickles operators by value, so the Python worker never imports
    this module and module-level setup does not reach it.

    Returns
    -------
    Optional[str]
        Directory added to `sys.path`, or None when already importable.
    """
    if importlib.util.find_spec("joblib") is not None:
        return None

    here = Path(__file__).resolve().parent
    bases = [here, Path.cwd(), *Path.cwd().parents[:2]]
    probes = [base / name for base in bases for name in ("deps.zip", "deps", ".")]
    for root in probes:
        if (root / MARKER).exists():
            resolved = str(root.resolve())
            if resolved not in sys.path:
                sys.path.insert(0, resolved)
            return resolved

    for base in bases:
        found = sorted(base.rglob(str(MARKER)), key=lambda path: len(path.parts))
        if found:
            resolved = str(found[0].parent.parent)
            if resolved not in sys.path:
                sys.path.insert(0, resolved)
            return resolved

    try:
        listing = sorted(item.name for item in Path.cwd().iterdir())
    except OSError as error:
        listing = [f"unreadable: {error}"]
    LOGGER.error(
        "bq1 dependency bundle not found. file=%s cwd=%s probed=%s cwd_entries=%s",
        __file__,
        Path.cwd(),
        [str(item) for item in probes],
        listing,
    )
    return None


bootstrap_dependency_path()

from pyflink.common import Duration, Row, Types, WatermarkStrategy  # noqa: E402
from pyflink.common.serialization import Encoder, SimpleStringSchema  # noqa: E402
from pyflink.common.watermark_strategy import TimestampAssigner  # noqa: E402
from pyflink.datastream import DataStream, StreamExecutionEnvironment  # noqa: E402
from pyflink.datastream.connectors.file_system import (  # noqa: E402
    FileSink,
    FileSource,
    RollingPolicy,
    StreamFormat,
)
from pyflink.datastream.connectors.kafka import (  # noqa: E402
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import (  # noqa: E402
    KeyedProcessFunction,
    ProcessFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ListStateDescriptor  # noqa: E402
from pyflink.table import StreamTableEnvironment  # noqa: E402

import features  # noqa: E402

REJECTED_FIELD = "_rejected"

GOLD_CATALOG = "iceberg"
GOLD_TABLE = "scored"
GOLD_DATABASE_DEFAULT = "aml_bq1_catalog"

GOLD_COLUMNS = (
    ("txn_id", "STRING", Types.STRING(), str),
    ("event_time", "STRING", Types.STRING(), str),
    ("event_date", "STRING", Types.STRING(), str),
    ("run_id", "STRING", Types.STRING(), str),
    ("from_bank", "STRING", Types.STRING(), str),
    ("from_account", "STRING", Types.STRING(), str),
    ("to_bank", "STRING", Types.STRING(), str),
    ("to_account", "STRING", Types.STRING(), str),
    ("amount_paid_usd", "DOUBLE", Types.DOUBLE(), float),
    ("amount_received_usd", "DOUBLE", Types.DOUBLE(), float),
    ("payment_currency", "STRING", Types.STRING(), str),
    ("payment_format", "STRING", Types.STRING(), str),
    ("is_cross_currency", "INT", Types.INT(), int),
    ("is_cross_bank", "INT", Types.INT(), int),
    ("is_structuring_threshold", "INT", Types.INT(), int),
    ("sender_tx_count_24h", "INT", Types.INT(), int),
    ("sender_volume_24h", "DOUBLE", Types.DOUBLE(), float),
    ("receiver_tx_count_24h", "INT", Types.INT(), int),
    ("raw_margin", "DOUBLE", Types.DOUBLE(), float),
    ("score", "DOUBLE", Types.DOUBLE(), float),
    ("is_alert", "BOOLEAN", Types.BOOLEAN(), bool),
    ("label", "INT", Types.INT(), int),
    ("top_contributors", "STRING", Types.STRING(), str),
)

GOLD_ROW_TYPE = Types.ROW_NAMED(
    [name for name, _sql, _field, _cast in GOLD_COLUMNS],
    [field for _name, _sql, field, _cast in GOLD_COLUMNS],
)


def is_rejected(record: str) -> bool:
    """Report whether a parsed record is a parse failure.

    Parameters
    ----------
    record : str
        Record as emitted by `ParseTransaction`.

    Returns
    -------
    bool
        True when the record carries the rejection flag.
    """
    return json.loads(record).get(REJECTED_FIELD, False) is True


def locate(filename: str) -> str:
    """Find a bundled data file on the interpreter path.

    Parameters
    ----------
    filename : str
        Basename as packaged into the application zip.

    Returns
    -------
    str
        Absolute path to the file.

    Raises
    ------
    FileNotFoundError
        If no directory on `sys.path` or the working directory holds it.
    """
    for root in [os.getcwd()] + sys.path:
        candidate = os.path.join(root, filename)
        if os.path.isfile(candidate):
            return candidate
    raise FileNotFoundError(f"{filename} not on sys.path")


def load_properties() -> dict[str, str]:
    """Read the BQ1 runtime property group, falling back to the environment.

    Returns
    -------
    dict[str, str]
        Property map. Environment defaults let the job run against a local
        Kafka without the Managed Flink properties file.
    """
    if os.path.isfile(PROPERTIES_FILE):
        with open(PROPERTIES_FILE) as handle:
            for group in json.load(handle):
                if group.get("PropertyGroupId") == PROPERTY_GROUP:
                    return group.get("PropertyMap", {})
    return {
        "bootstrap.servers": os.environ.get("MSK_BOOTSTRAP", ""),
        "source.topic": os.environ.get("SOURCE_TOPIC", "transactions"),
        "alerts.topic": os.environ.get("ALERTS_TOPIC", "alerts"),
        "rejected.topic": os.environ.get("REJECTED_TOPIC", "rejected"),
        "bucket": os.environ.get("S3_BUCKET_NAME", ""),
        "run.id": os.environ.get("RUN_ID", "manual"),
        "glue.database": os.environ.get("GLUE_DATABASE", GOLD_DATABASE_DEFAULT),
        "gold.from": os.environ.get("GOLD_FROM", ""),
        "source.mode": os.environ.get("SOURCE_MODE", "kafka"),
        "source.from": os.environ.get("SOURCE_FROM", ""),
        "source.to": os.environ.get("SOURCE_TO", ""),
    }


def kafka_iam_properties(bootstrap: str) -> dict[str, str]:
    """Build the client properties MSK requires for IAM authentication.

    Parameters
    ----------
    bootstrap : str
        Comma-separated broker list.

    Returns
    -------
    dict[str, str]
        Properties for the Kafka source and sink builders.
    """
    return {
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "AWS_MSK_IAM",
        "sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    }


class EventTimeAssigner(TimestampAssigner):
    """Takes event time from the transaction's own timestamp, never arrival time."""

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        """Return the record's event time in milliseconds.

        Parameters
        ----------
        value : str
            Raw record as JSON.
        record_timestamp : int
            Kafka's own timestamp, used only when the field is unreadable.

        Returns
        -------
        int
            Event time in milliseconds since the epoch.
        """
        try:
            return int(features.parse_timestamp(json.loads(value)["Timestamp"]).timestamp() * 1000)
        except (ValueError, KeyError, TypeError):
            return record_timestamp


class ParseTransaction(ProcessFunction):
    """Turns a raw Kafka message into derived fields, flagging failures."""

    def process_element(self, value: str, ctx: "ProcessFunction.Context") -> Iterator[str]:
        """Derive the history-free features for one record.

        A record that cannot be parsed is emitted on the same stream carrying
        `REJECTED_FIELD`, for a downstream filter to route. Callers must drop
        flagged records before anything reads feature fields.

        Parameters
        ----------
        value : str
            Raw JSON message from the replay Lambda.
        ctx : ProcessFunction.Context
            Unused; required by the interface.

        Yields
        ------
        str
            Derived record as JSON, or a flagged record on failure.
        """
        try:
            yield json.dumps(features.derive_row(json.loads(value)))
        except (ValueError, KeyError, TypeError) as error:
            yield json.dumps(
                {REJECTED_FIELD: True, "reason": str(error), "raw": value[:500]}
            )


class SenderFeatures(KeyedProcessFunction):
    """Rolling sender features from seven days of per-account history."""

    def __init__(self) -> None:
        self.history = None

    def open(self, runtime_context: RuntimeContext) -> None:
        """Declare the keyed history state.

        Parameters
        ----------
        runtime_context : RuntimeContext
            Flink-supplied context.

        Returns
        -------
        None
        """
        self.history = runtime_context.get_list_state(
            ListStateDescriptor("sender_history", Types.TUPLE([Types.DOUBLE(), Types.DOUBLE()]))
        )

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context") -> Iterator[str]:
        """Add the six sender aggregates, both burst ratios and the time delta.

        Parameters
        ----------
        value : str
            Derived record as JSON.
        ctx : KeyedProcessFunction.Context
            Flink-supplied context.

        Yields
        ------
        str
            Record with sender features added, as JSON.
        """
        row = json.loads(value)
        now = float(row["tx_epoch"])

        stored = list(self.history.get())
        previous_epoch = max((epoch for epoch, _ in stored if epoch < now), default=None)

        retained = [
            (epoch, amount)
            for epoch, amount in stored
            if epoch >= now - features.SEVEN_DAYS_SECONDS
        ]
        row["sender_time_delta_sec"] = (
            now - previous_epoch
            if previous_epoch is not None
            else features.NO_PRIOR_TRANSACTION_SECONDS
        )

        retained.append((now, float(row["amount_paid_usd"])))
        self.history.update(retained)

        count_1h, volume_1h = features.window_totals(retained, now, features.ONE_HOUR_SECONDS)
        count_24h, volume_24h = features.window_totals(retained, now, features.ONE_DAY_SECONDS)
        count_7d, volume_7d = features.window_totals(retained, now, features.SEVEN_DAYS_SECONDS)

        row["sender_tx_count_1h"] = count_1h
        row["sender_volume_1h"] = volume_1h
        row["sender_tx_count_24h"] = count_24h
        row["sender_volume_24h"] = volume_24h
        row["sender_tx_count_7d"] = count_7d
        row["sender_volume_7d"] = volume_7d
        row["sender_burst_ratio_1h_24h"] = features.burst_ratio(volume_1h, volume_24h, 24.0)
        row["sender_burst_ratio_24h_7d"] = features.burst_ratio(volume_24h, volume_7d, 7.0)
        yield json.dumps(row)


class ReceiverFeaturesAndScore(KeyedProcessFunction):
    """Adds receiver features, then scores and explains the record."""

    def __init__(self, run_id: str, is_scoring_enabled: bool = True) -> None:
        self.run_id = run_id
        self.is_scoring_enabled = is_scoring_enabled
        self.history = None
        self.booster = None
        self.calibrator = None
        self.feature_names = None
        self.category_maps = None
        self.threshold = 0.075
        self.numpy = None
        self.xgboost = None

    def open(self, runtime_context: RuntimeContext) -> None:
        """Load the model artifacts once and declare the keyed history state.

        Parameters
        ----------
        runtime_context : RuntimeContext
            Flink-supplied context.

        Returns
        -------
        None
        """
        self.history = runtime_context.get_list_state(
            ListStateDescriptor("receiver_history", Types.TUPLE([Types.DOUBLE(), Types.DOUBLE()]))
        )

        if not self.is_scoring_enabled:
            return

        bootstrap_dependency_path()

        import joblib
        import numpy
        import xgboost

        self.numpy = numpy
        self.xgboost = xgboost

        self.booster = xgboost.Booster()
        self.booster.load_model(locate(MODEL_FILE))
        self.calibrator = joblib.load(locate(CALIBRATOR_FILE))
        with open(locate(FEATURES_FILE)) as handle:
            self.feature_names = json.load(handle)
        with open(locate(CATEGORY_MAPS_FILE)) as handle:
            self.category_maps = json.load(handle)
        with open(locate(CONFIG_FILE)) as handle:
            self.threshold = float(json.load(handle)["calibrated_threshold"])

        LOGGER.info(
            "loaded model, %d features, threshold %.4f", len(self.feature_names), self.threshold
        )

    def _encode(self, row: dict) -> dict:
        """Map the five categorical strings to the codes the model was trained on.

        Parameters
        ----------
        row : dict
            Record carrying the raw categorical values.

        Returns
        -------
        dict
            The same record with `*_code` fields added. Unseen values become -1.
        """
        for column, mapping in self.category_maps.items():
            row[f"{column}_code"] = mapping.get(str(row.get(column, "")), -1)
        return row

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context") -> Iterator[str]:
        """Add receiver features, score, explain, and emit the scored record.

        Parameters
        ----------
        value : str
            Record carrying sender features, as JSON.
        ctx : KeyedProcessFunction.Context
            Flink-supplied context.

        Yields
        ------
        str
            Scored record as JSON.
        """
        row = json.loads(value)
        now = float(row["tx_epoch"])

        stored = list(self.history.get())
        previous_epoch = max((epoch for epoch, _ in stored if epoch < now), default=None)

        retained = [
            (epoch, amount)
            for epoch, amount in stored
            if epoch >= now - features.ONE_DAY_SECONDS
        ]
        row["receiver_time_delta_sec"] = (
            now - previous_epoch
            if previous_epoch is not None
            else features.NO_PRIOR_TRANSACTION_SECONDS
        )

        retained.append((now, float(row["amount_received_usd"])))
        self.history.update(retained)

        count_24h, volume_24h = features.window_totals(retained, now, features.ONE_DAY_SECONDS)
        row["receiver_tx_count_24h"] = count_24h
        row["receiver_volume_24h"] = volume_24h

        if not self.is_scoring_enabled:
            row["run_id"] = self.run_id
            row["raw_margin"] = 0.0
            row["score"] = 0.0
            row["is_alert"] = False
            yield json.dumps(row)
            return

        row = self._encode(row)
        vector = self.numpy.array(
            [[float(row[name]) for name in self.feature_names]], dtype=self.numpy.float32
        )
        matrix = self.xgboost.DMatrix(vector, feature_names=self.feature_names)

        margin = float(self.booster.predict(matrix, output_margin=True)[0])
        sigmoid = 1.0 / (1.0 + self.numpy.exp(-margin))
        calibrated = float(self.calibrator.predict(self.numpy.array([sigmoid]))[0])

        row["run_id"] = self.run_id
        row["raw_margin"] = round(margin, 6)
        row["score"] = round(calibrated, 6)
        row["is_alert"] = calibrated >= self.threshold

        if row["is_alert"]:
            contributions = self.booster.predict(matrix, pred_contribs=True)[0]
            ranked = sorted(
                range(len(self.feature_names)),
                key=lambda index: abs(contributions[index]),
                reverse=True,
            )[:TOP_CONTRIBUTORS]
            row["top_contributors"] = [
                {
                    "feature": self.feature_names[index],
                    "value": float(row[self.feature_names[index]]),
                    "contribution": round(float(contributions[index]), 6),
                }
                for index in ranked
            ]
        yield json.dumps(row)


def to_gold_row(record: str) -> Row:
    """Convert a scored record into a typed row for the Iceberg table.

    Parameters
    ----------
    record : str
        Scored record as JSON.

    Returns
    -------
    Row
        Positional row matching `GOLD_COLUMNS`.
    """
    scored = json.loads(record)
    scored["event_date"] = str(scored.get("event_time", ""))[:10]
    scored["top_contributors"] = json.dumps(scored.get("top_contributors", []))
    values = []
    for name, _sql, _field, cast in GOLD_COLUMNS:
        raw = scored.get(name)
        values.append(None if raw is None else cast(raw))
    return Row(*values)


def gold_table_ddl(database: str) -> str:
    """Build the Iceberg table definition for the scored output.

    Parameters
    ----------
    database : str
        Glue database holding the table.

    Returns
    -------
    str
        `CREATE TABLE IF NOT EXISTS` statement.
    """
    columns = ",\n  ".join(f"`{name}` {sql}" for name, sql, _field, _cast in GOLD_COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS `{GOLD_CATALOG}`.`{database}`.`{GOLD_TABLE}` (\n"
        f"  {columns}\n"
        ") PARTITIONED BY (`event_date`) WITH ('format-version'='2')"
    )


def build_json_gold_sink(properties: dict[str, str]) -> FileSink:
    """Build the JSON gold sink used while the Iceberg path is unavailable.

    Iceberg's Flink catalog constructs a Hadoop `Configuration`, so it needs
    Hadoop classes shaded into the pipeline JAR; until that is in place
    `gold.mode` defaults here.

    Parameters
    ----------
    properties : dict[str, str]
        Runtime property map; reads `bucket` and `run.id`.

    Returns
    -------
    FileSink
        Row-formatted sink under `gold/run_id=<id>/`.
    """
    run_id = properties.get("run.id", "manual")
    return (
        FileSink.for_row_format(
            f"s3://{properties['bucket']}/gold/run_id={run_id}/",
            Encoder.simple_string_encoder(),
        )
        .with_rolling_policy(RollingPolicy.default_rolling_policy(part_size=64 * 1024 * 1024))
        .build()
    )


def attach_gold_sink(
    environment: StreamExecutionEnvironment,
    scored: DataStream,
    properties: dict[str, str],
) -> None:
    """Write the scored stream into an Iceberg table in the Glue catalog.

    Iceberg has no PyFlink DataStream sink, so the stream crosses into the
    Table API. `attach_as_datastream` folds the insert into this job's graph,
    so the caller's `execute` runs it alongside the Kafka sinks; calling
    `StatementSet.execute` instead would submit a second job.

    Parameters
    ----------
    environment : StreamExecutionEnvironment
        Environment the pipeline is attached to.
    scored : DataStream
        Scored records as JSON strings.
    properties : dict[str, str]
        Runtime property map.

    Returns
    -------
    None
    """
    database = properties.get("glue.database", GOLD_DATABASE_DEFAULT)
    warehouse = f"s3://{properties['bucket']}/gold"

    table_environment = StreamTableEnvironment.create(environment)
    table_environment.execute_sql(
        f"CREATE CATALOG IF NOT EXISTS `{GOLD_CATALOG}` WITH ("
        "'type'='iceberg',"
        "'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',"
        f"'warehouse'='{warehouse}',"
        "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO')"
    )
    table_environment.execute_sql(gold_table_ddl(database))

    rows = scored.map(to_gold_row, output_type=GOLD_ROW_TYPE).name("gold-row").uid("gold-row")
    table_environment.create_temporary_view(
        "gold_rows", table_environment.from_data_stream(rows)
    )
    statements = table_environment.create_statement_set()
    statements.add_insert_sql(
        f"INSERT INTO `{GOLD_CATALOG}`.`{database}`.`{GOLD_TABLE}` SELECT * FROM gold_rows"
    )
    statements.attach_as_datastream()


def hourly_paths(bucket: str, start: str, end: str) -> list[str]:
    """List the hourly files covering a half-open window.

    Parameters
    ----------
    bucket : str
        Bucket holding `bronze/hourly/`.
    start : str
        First instant to include, as `YYYY-MM-DDTHH:MM`.
    end : str
        First instant to exclude, same format.

    Returns
    -------
    list[str]
        One `s3://` path per hour, in order.
    """
    cursor = datetime.strptime(start, "%Y-%m-%dT%H:%M").replace(minute=0)
    final = datetime.strptime(end, "%Y-%m-%dT%H:%M")
    paths = []
    while cursor < final:
        paths.append(f"s3://{bucket}/bronze/hourly/{cursor:%Y-%m-%d-%H}.csv")
        cursor += timedelta(hours=1)
    return paths


def csv_line_to_json(line: str) -> str:
    """Convert one hourly-file line into the message shape the job expects.

    Parameters
    ----------
    line : str
        One line from an hourly file.

    Returns
    -------
    str
        Row as JSON, or an empty string for a header or malformed line.
    """
    row = features.row_from_csv_line(line)
    return "" if row is None else json.dumps(row)


def build_file_source(properties: dict[str, str]) -> FileSource:
    """Build a bounded source over the hourly files for a window.

    Parameters
    ----------
    properties : dict[str, str]
        Runtime property map; reads `bucket`, `source.from` and `source.to`.

    Monitors continuously rather than reading a static set: a bounded source
    ends the job once input is exhausted, and a finished job cannot be
    snapshotted, which is the only reason these phases run.

    Returns
    -------
    FileSource
        Continuously monitoring source over the listed files.

    Raises
    ------
    KeyError
        If `source.from` or `source.to` is absent.
    """
    paths = hourly_paths(
        properties["bucket"], properties["source.from"], properties["source.to"]
    )
    paths.reverse()
    return (
        FileSource.for_record_stream_format(StreamFormat.text_line_format(), *paths)
        .monitor_continuously(Duration.of_seconds(60))
        .build()
    )


def build_watermarks() -> WatermarkStrategy:
    """Build the event-time strategy for the transaction source.

    Returns
    -------
    WatermarkStrategy
        Bounded out-of-orderness with an idleness timeout.
    """
    return (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(60))
        .with_idleness(Duration.of_seconds(60))
        .with_timestamp_assigner(EventTimeAssigner())
    )


def build_kafka_sink(bootstrap: str, client_properties: dict[str, str], topic: str) -> KafkaSink:
    """Build an IAM-authenticated sink writing strings to one topic.

    Parameters
    ----------
    bootstrap : str
        Comma-separated broker list.
    client_properties : dict[str, str]
        Properties from `kafka_iam_properties`.
    topic : str
        Destination topic.

    Returns
    -------
    KafkaSink
        Configured sink.
    """
    builder = (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
    )
    for name, setting in client_properties.items():
        builder = builder.set_property(name, setting)
    return builder.build()


def build_pipeline(environment: StreamExecutionEnvironment, properties: dict[str, str]) -> None:
    """Wire source, feature stages, scoring and the three sinks.

    Parameters
    ----------
    environment : StreamExecutionEnvironment
        Execution environment to attach the pipeline to.
    properties : dict[str, str]
        Runtime property map from `load_properties`.

    Returns
    -------
    None
    """
    bootstrap = properties["bootstrap.servers"]
    client_properties = kafka_iam_properties(bootstrap)
    run_id = properties.get("run.id", "manual")

    if properties.get("source.mode", "kafka") == "files":
        raw = (
            environment.from_source(
                build_file_source(properties), build_watermarks(), "hourly-files"
            )
            .uid("file-source")
            .map(csv_line_to_json, output_type=Types.STRING())
            .name("csv-to-json")
            .uid("csv-to-json")
            .filter(lambda record: record != "")
            .name("csv-rows")
            .uid("csv-rows")
        )
    else:
        source = (
            KafkaSource.builder()
            .set_topics(properties["source.topic"])
            .set_group_id(f"bq1-scorer-{run_id}")
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .set_properties(client_properties)
            .build()
        )
        raw = environment.from_source(
            source, build_watermarks(), "transactions"
        ).uid("kafka-source")

    parsed = (
        raw.process(ParseTransaction(), output_type=Types.STRING())
        .name("parse")
        .uid("parse")
    )

    is_scoring_enabled = properties.get("score.enabled", "true").strip().lower() != "false"

    scored = (
        parsed.filter(lambda record: not is_rejected(record))
        .name("accepted")
        .uid("accepted")
        .key_by(lambda record: json.loads(record)["from_account"], key_type=Types.STRING())
        .process(SenderFeatures(), output_type=Types.STRING())
        .name("sender-features")
        .uid("sender-features")
        .key_by(lambda record: json.loads(record)["to_account"], key_type=Types.STRING())
        .process(
            ReceiverFeaturesAndScore(run_id, is_scoring_enabled), output_type=Types.STRING()
        )
        .name("receiver-features-and-score")
        .uid("receiver-features-and-score")
    )

    if properties.get("kafka.sinks", "true").strip().lower() != "false":
        scored.filter(lambda record: json.loads(record)["is_alert"]).name("alerting").uid(
            "alerting"
        ).sink_to(
            build_kafka_sink(bootstrap, client_properties, properties["alerts.topic"])
        ).name("alerts").uid("alerts")

        parsed.filter(is_rejected).name("rejecting").uid("rejecting").sink_to(
            build_kafka_sink(bootstrap, client_properties, properties["rejected.topic"])
        ).name("rejected").uid("rejected")

    gold_from = properties.get("gold.from", "")
    eligible = (
        scored.filter(
            lambda record, cutoff=gold_from: json.loads(record).get("event_time", "") >= cutoff
        )
        .name("gold-eligible")
        .uid("gold-eligible")
    )

    if properties.get("gold.mode", "files") == "iceberg":
        attach_gold_sink(environment, eligible, properties)
    else:
        eligible.sink_to(build_json_gold_sink(properties)).name("gold").uid("gold")


def main() -> None:
    """Build and execute the scoring pipeline.

    Returns
    -------
    None
    """
    environment = StreamExecutionEnvironment.get_execution_environment()
    environment.set_parallelism(1)
    build_pipeline(environment, load_properties())
    environment.execute("aml-bq1-scorer")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    main()

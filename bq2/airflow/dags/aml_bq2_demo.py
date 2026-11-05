"""
Airflow DAG: aml_bq2_demo  (one run per simulated day, 1-11 Sep 2022)

Catchup backfills all 11 days in order; each run checks only its own day ({{ ds }}), read from
the pre-cut daily files in bq2/out/stream/. Airflow is the agent that runs the monitor: every
step is code, with no model call. The AI chat lives in the Helical dashboard instead.

    run_checks -> gate -> accept_day | quarantine -> report_slice -> send_escalation -> upload_results
        -> sync_snapshot

Every task can be rerun: it rewrites its own day's files, and the escalation log keeps one entry per day.
"""
from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator


def run_checks(ds, **_):
    """All four checks plus the gate for the day; writes out/monitor/checks/<day>.json."""
    from monitoring.run import run_checks as run_all
    return run_all(ds)["path"]


def choose_path(ti, **_):
    return "accept_day" if ti.xcom_pull(task_ids="run_checks") == "build_marts" else "quarantine"


def accept_day(ds, **_):
    """One row of the monitor's daily charts: out/monitor/daily/<day>.json."""
    from monitoring.daily import daily_row
    from monitoring.run import CHECKS_DIR, REPORTS_DIR
    row = daily_row(json.loads((CHECKS_DIR / f"{ds}.json").read_text()))
    out = REPORTS_DIR.parent / "daily"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{ds}.json").write_text(json.dumps(row, indent=2))


def quarantine(ds, **_):
    """Keep the day's raw rows aside in out/monitor/quarantine/slice=<day>/."""
    from monitoring.replay import STREAM_DIR
    from monitoring.run import REPORTS_DIR
    dst = REPORTS_DIR.parent / "quarantine" / f"slice={ds}"
    shutil.rmtree(dst, ignore_errors=True)
    shutil.copytree(STREAM_DIR / f"slice={ds}", dst)


def report_slice(ds, **_):
    """The day's check report (out/monitor/reports/<day>.json), filled in from a fixed template."""
    from monitoring.run import report_slice as write
    return write(ds, escalate=False)


def send_escalation(ti, **_):
    """Record the report in out/escalations.log if the day was quarantined (shown on the dashboard)."""
    from monitoring.escalations import send_escalation as record
    return record(ti.xcom_pull(task_ids="report_slice"))


def upload_results(ds, **_):
    """Copy the day's results to s3://<bucket>/monitor/, the source of truth for the dashboard."""
    import os

    from monitoring.escalations import ESCALATIONS_LOG
    from monitoring.run import REPORTS_DIR
    from pipeline.aws import S3
    s3, monitor = S3(os.environ.get("AML_BUCKET", "aml-bq2")), REPORTS_DIR.parent
    for folder in ("checks", "reports", "daily"):
        path = monitor / folder / f"{ds}.json"
        if path.exists():
            s3.upload(path, f"monitor/{folder}/{ds}.json")
    for path in sorted((monitor / "quarantine" / f"slice={ds}").glob("*")):
        s3.upload(path, f"monitor/quarantine/slice={ds}/{path.name}")
    if ESCALATIONS_LOG.exists():
        s3.upload(ESCALATIONS_LOG, "monitor/escalations.log")


def sync_snapshot(ds, **_):
    """Rebuild the Helical snapshot from S3 with the slices up to this day, so the monitor page grows daily."""
    import os

    from pipeline.aws import S3
    from pipeline.snapshot import SNAPSHOT_PATH, build_snapshot
    return build_snapshot(S3(os.environ.get("AML_BUCKET", "aml-bq2")), SNAPSHOT_PATH, through=ds)


with DAG(
    dag_id="aml_bq2_demo",
    start_date=datetime(2022, 9, 1),
    end_date=datetime(2022, 9, 11),
    schedule="@daily",
    catchup=True,                       # backfill all 11 days before the demo
    max_active_runs=1,                  # days run in order
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    is_paused_upon_creation=True,       # the shared stack switches new DAGs on; ours write to S3
    tags=["aml", "bq2", "monitor"],
) as dag:
    checks = PythonOperator(task_id="run_checks", python_callable=run_checks)
    gate = BranchPythonOperator(task_id="gate", python_callable=choose_path)
    accept = PythonOperator(task_id="accept_day", python_callable=accept_day)
    hold = PythonOperator(task_id="quarantine", python_callable=quarantine)
    report = PythonOperator(task_id="report_slice", python_callable=report_slice,
                            trigger_rule="none_failed_min_one_success")
    escalation = PythonOperator(task_id="send_escalation", python_callable=send_escalation)
    upload = PythonOperator(task_id="upload_results", python_callable=upload_results)
    sync = PythonOperator(task_id="sync_snapshot", python_callable=sync_snapshot)

    checks >> gate >> [accept, hold] >> report >> escalation >> upload >> sync

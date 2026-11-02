"""
Airflow DAG: aml_bq2  (runs locally in Docker; see README)

Batch part (steps 1–7 of the plan) runs once per replay, then the monitoring part runs
for every replayed daily slice. Keep this file thin: the logic lives in lambdas/,
sql/ and monitoring/ so each owner can test their piece on its own.

Airflow is the agent that runs the monitor: every step, including the check report, is code
on a schedule, with no model call. The AI chat lives in the Helical dashboard instead.

Needs: apache-airflow 2.x, apache-airflow-providers-amazon, plus the repo folders
mounted into the containers. Connection "aws_default" should use your AWS profile.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

BUCKET = os.environ.get("AML_BUCKET", "aml-bq2-YOUR_TEAM_NAME")
DB = os.environ.get("ATHENA_DATABASE", "aml_bq2")
ATHENA_OUT = os.environ.get("ATHENA_OUTPUT", f"s3://{BUCKET}/athena-results/")
SQL_DIR = Path(os.environ.get("AML_SQL_DIR", "/opt/airflow/repo/sql"))


def sql(name: str) -> str:
    return (SQL_DIR / name).read_text().replace("aml-bq2-YOUR_TEAM_NAME", BUCKET)


# ---- monitoring task bodies (fill in; each pulls its slice from S3/Athena) ----
def replay_slice(**ctx):
    """Return the next slice id (pushed to XCom). Local mode: slices are pre-cut into out/stream/
    by `python -m monitoring.replay build`; the cursor in out/stream/_cursor.json moves one slice per run."""
    from airflow.exceptions import AirflowSkipException
    from monitoring.replay import next_slice
    sid = next_slice()
    if sid is None:
        raise AirflowSkipException("end of replay")
    return sid


def run_checks(check: str, **ctx):
    """Push one check's result dict to XCom. monitoring.run.run_checks runs all four on the slice
    (curate, score with the live model, check) and caches nothing between tasks, so each task reruns it."""
    from monitoring.run import run_checks as run_all
    sid = ctx["ti"].xcom_pull(task_ids="replay_slice")
    return next(r for r in run_all(sid, write=check == "volume")["results"] if r["check"] == check)


def choose_path(**ctx):
    from monitoring.checks import gate
    results = [ctx["ti"].xcom_pull(task_ids=t) for t in
               ("check_recon", "check_quality", "check_drift", "check_volume")]
    return "build_marts" if gate(results) == "pass" else "quarantine"


def report_slice(**ctx):
    """Check report for the slice (out/monitor/reports/<slice>.json), filled in from a fixed template;
    reads the checks file check_volume wrote. The escalation is left to send_escalation."""
    from monitoring.run import report_slice as write
    return write(ctx["ti"].xcom_pull(task_ids="replay_slice"), escalate=False)


def send_escalation(**ctx):
    """Log the check report to out/escalations.log when the slice was quarantined (shown on the dashboard)."""
    from monitoring.escalations import send_escalation as log_escalation
    report = ctx["ti"].xcom_pull(task_ids="report_slice")
    return log_escalation(report) if report else False


default_args = {"retries": 1, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="aml_bq2",
    start_date=datetime(2026, 10, 1),
    schedule=None,                      # trigger by hand; switch to "*/1 * * * *" for the live demo
    catchup=False,
    default_args=default_args,
    tags=["aml", "bq2"],
) as dag:

    # ------------------------- batch: steps 2–7 -------------------------
    parse_patterns = LambdaInvokeFunctionOperator(
        task_id="parse_patterns", function_name="parse_patterns",
        payload=f'{{"bucket": "{BUCKET}", "key": "raw/patterns/HI_Small_Patterns.txt"}}')
    fetch_fx = LambdaInvokeFunctionOperator(task_id="fetch_fx", function_name="fetch_fx", payload="{}")
    curated = AthenaOperator(task_id="curated_transactions", query=sql("02_curated_transactions.sql"),
                             database=DB, output_location=ATHENA_OUT)
    channel_lift = AthenaOperator(task_id="mart_channel_lift", query=sql("marts/channel_lift.sql"),
                                  database=DB, output_location=ATHENA_OUT)
    # add one AthenaOperator per mart file in sql/marts/ here
    model = EmptyOperator(task_id="xgboost_model")  # replace with a PythonOperator calling the notebook code
    publish = LambdaInvokeFunctionOperator(task_id="publish", function_name="publish", payload="{}")

    [parse_patterns, fetch_fx] >> curated >> channel_lift >> model >> publish

    # ------------------- monitoring: one replayed slice -------------------
    replay = PythonOperator(task_id="replay_slice", python_callable=replay_slice)
    load = EmptyOperator(task_id="load_slice")      # Athena INSERT INTO for the slice
    checks = [PythonOperator(task_id=t, python_callable=run_checks, op_kwargs={"check": c})
              for t, c in [("check_recon", "recon"), ("check_quality", "quality"),
                           ("check_drift", "drift"), ("check_volume", "volume")]]
    gate_task = BranchPythonOperator(task_id="gate", python_callable=choose_path)
    build_marts = EmptyOperator(task_id="build_marts")
    quarantine = EmptyOperator(task_id="quarantine")  # move slice to quarantine/
    report = PythonOperator(task_id="report_slice", python_callable=report_slice,
                            trigger_rule="none_failed_min_one_success")
    escalation = PythonOperator(task_id="send_escalation", python_callable=send_escalation)

    publish >> replay >> load >> checks >> gate_task >> [build_marts, quarantine]
    [build_marts, quarantine] >> report >> escalation

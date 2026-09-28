"""
Airflow DAG: aml_bq2  (runs locally in Docker; see README)

Batch part (steps 1–7 of the plan) runs once per replay, then the monitoring part runs
for every replayed hourly slice. Keep this file thin: the logic lives in lambdas/,
sql/, monitoring/ and agent/ so each owner can test their piece on its own.

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
    """Copy the next simulated hour of HI-Small into raw/stream/slice=<ts>/.
    The slice id is the DAG run's logical date mapped onto 1–10 Sep 2022."""
    raise NotImplementedError


def run_checks(check: str, **ctx):
    """Load the slice + baseline, call monitoring.checks.check_<check>, push the result dict to XCom."""
    raise NotImplementedError


def choose_path(**ctx):
    from monitoring.checks import gate
    results = [ctx["ti"].xcom_pull(task_ids=t) for t in
               ("check_recon", "check_quality", "check_drift", "score_and_anomaly")]
    return "build_marts" if gate(results) == "pass" else "quarantine"


def agent_triage(**ctx):
    """Call agent.triage_agent.triage(...) and write marts/agent/<slice>.json."""
    raise NotImplementedError


def send_alert(**ctx):
    """Email the team when the agent's severity is 'high'."""
    raise NotImplementedError


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
        payload=f'{{"bucket": "{BUCKET}", "key": "raw/patterns/HI-Small_Patterns.txt"}}')
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
                           ("check_drift", "drift"), ("score_and_anomaly", "anomaly")]]
    gate_task = BranchPythonOperator(task_id="gate", python_callable=choose_path)
    build_marts = EmptyOperator(task_id="build_marts")
    quarantine = EmptyOperator(task_id="quarantine")  # move slice to quarantine/
    triage = PythonOperator(task_id="agent_triage", python_callable=agent_triage,
                            trigger_rule="none_failed_min_one_success")
    alert = PythonOperator(task_id="send_alert", python_callable=send_alert)

    publish >> replay >> load >> checks >> gate_task >> [build_marts, quarantine]
    [build_marts, quarantine] >> triage >> alert

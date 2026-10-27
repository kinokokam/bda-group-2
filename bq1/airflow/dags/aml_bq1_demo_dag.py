"""Airflow DAG: the BQ1 real-time scoring demo.

Runs only the replay. `pipeline.sh setup` builds the account state once, and
`pipeline.sh predemo` brings the cluster back and leaves Flink running and
restored, so this DAG does nothing but push the window through and stop
cleanly.

Every task shells out to `pipeline.sh`, so the demo has one implementation and
`pipeline.sh demo` is a working fallback if Airflow is unavailable.

Requires the repository mounted at `/opt/airflow/repo` and AWS credentials at
`/home/airflow/.aws`, both provided by `docker-compose.yml`.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = "/opt/airflow/repo" if os.path.isdir("/opt/airflow/repo") else os.getcwd()
SCRIPT = "bq1/scripts/pipeline.sh"
DRAIN_SECONDS = 180

default_args = {
    "owner": "g1t2",
    "retries": 0,
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="aml_bq1_demo",
    description="Score the 10 Sep 11:00-13:00 window from the pre-demo snapshot",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["bq1", "demo", "flink"],
) as dag:
    run_demo = BashOperator(
        task_id="replay_demo_window",
        bash_command=f"cd {REPO} && bash {SCRIPT} demo",
    )

    drain = BashOperator(
        task_id="drain",
        bash_command=f"sleep {DRAIN_SECONDS}",
    )

    stop = BashOperator(
        task_id="stop_gracefully",
        bash_command=f"cd {REPO} && bash {SCRIPT} stop",
    )

    run_demo >> drain >> stop

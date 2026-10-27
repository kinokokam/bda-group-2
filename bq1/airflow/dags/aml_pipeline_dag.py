"""
Apache Airflow DAG: End-to-End AML Transaction Laundering Detection Pipeline.

Medallion Storage Organization:
- Raw Ingestion:    s3://<BUCKET_NAME>/bronze/
- Spark ETL Output: s3://<BUCKET_NAME>/processed/
- Evaluation:       s3://<BUCKET_NAME>/evaluation/
- Model Weights:    s3://<BUCKET_NAME>/model/

Dependency Flow:
  task_ingest_data >> task_submit_spark_etl >> task_train_xgboost

Cloud Execution:
- Ingestion: Kaggle API -> AWS S3 Bronze (s3://<BUCKET_NAME>/bronze/)
- Distributed Spark ETL: AWS EMR Cluster (EMR_CLUSTER_ID) -> AWS S3 Processed Parquet
- XGBoost Training & Evaluation: AWS EMR Cluster -> AWS S3 Model & Evaluation
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Ensure repo root and data directories are properly resolved (both local & inside Airflow container)
if os.path.exists("/opt/airflow/data"):
    REPO_ROOT = "/opt/airflow"
    DATA_DIR = "/opt/airflow/data"
else:
    REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    DATA_DIR = os.path.join(REPO_ROOT, "data")

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Automatically load credentials from .env if present
def _load_env_file():
    candidates = [
        "/opt/airflow/.env",
        "/opt/airflow/repo/.env",
        os.path.join(REPO_ROOT, ".env"),
        os.path.join(os.getcwd(), ".env")
    ]
    for env_file in candidates:
        if os.path.exists(env_file):
            try:
                with open(env_file) as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            k, v = line.split("=", 1)
                            k, v = k.strip(), v.strip("\"' ")
                            os.environ[k] = v
                break
            except Exception:
                pass

_load_env_file()

logger = logging.getLogger("aml_pipeline_dag")

# =============================================================================
# Environment Variables & Configuration
# =============================================================================
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "your-aml-bucket")
EMR_CLUSTER_ID = os.environ.get("EMR_CLUSTER_ID", "")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")


# =============================================================================
# Task Callables
# =============================================================================

def run_ingest_task(**context):
    """Executes Step 1: Data ingestion & Bronze staging to AWS S3."""
    logger.info("Starting Task: task_ingest_data (Mode: AWS S3 Bronze)...")
    from ingestion.ingest_aml_data import run_ingestion_pipeline

    use_synthetic = os.environ.get("USE_SYNTHETIC", "false").lower() == "true"

    run_ingestion_pipeline(
        raw_dir=os.path.join(DATA_DIR, "raw"),
        s3_bucket=S3_BUCKET_NAME,
        s3_prefix="bronze",
        use_synthetic=use_synthetic,
        clean_local=False,
        num_txs=15000,
        num_accounts=600
    )
    logger.info("task_ingest_data completed successfully.")


def run_spark_etl_task(**context):
    """
    Executes Step 2 & 3: Distributed Spark ETL.
    Submits Spark ETL job to active AWS EMR cluster (EMR_CLUSTER_ID).
    """
    _load_env_file()
    cluster_id = os.environ.get("EMR_CLUSTER_ID", "")
    s3_bucket = os.environ.get("S3_BUCKET_NAME", S3_BUCKET_NAME)
    aws_region = os.environ.get("AWS_DEFAULT_REGION", AWS_REGION)

    logger.info(f"Starting Task: task_submit_spark_etl (Mode: AWS EMR Cluster, Cluster: {cluster_id})...")

    if not cluster_id:
        raise ValueError("EMR_CLUSTER_ID is required to run distributed Spark ETL on AWS EMR.")

    import boto3
    emr_client = boto3.client("emr", region_name=aws_region)

    # PySpark ETL script location in S3
    script_s3_uri = f"s3://{s3_bucket}/scripts/etl_pipeline.py"
    s3_client = boto3.client("s3", region_name=aws_region)
    local_script = os.path.join(REPO_ROOT, "spark/etl_pipeline.py")

    logger.info(f"Staging ETL script to S3: {script_s3_uri}...")
    s3_client.upload_file(local_script, s3_bucket, "scripts/etl_pipeline.py")

    step_args = [
        "spark-submit",
        "--deploy-mode", "cluster",
        script_s3_uri,
        "--s3-bucket", s3_bucket
    ]

    logger.info(f"Submitting Spark Step to EMR Cluster {cluster_id}...")
    response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "AML_Spark_Feature_Engineering",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": step_args
                }
            }
        ]
    )
    step_id = response["StepIds"][0]
    logger.info(f"Submitted EMR Step ID: {step_id}. Waiting for completion...")

    # Poll step status
    while True:
        desc = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = desc["Step"]["Status"]["State"]
        logger.info(f"EMR Step {step_id} status: {state}")
        if state in ["COMPLETED"]:
            logger.info("EMR Spark Step successfully finished.")
            break
        elif state in ["CANCELLED", "FAILED", "INTERRUPTED"]:
            reason = desc["Step"]["Status"].get("FailureDetails", {}).get("Reason", "Unknown")
            raise RuntimeError(f"EMR Step {step_id} failed with state: {state}. Reason: {reason}")
        time.sleep(30)


def run_xgboost_task(**context):
    """
    Executes Step 4, 5 & 6: Distributed XGBoost model training and evaluation upload on AWS EMR.
    Submits training job to active EMR cluster (EMR_CLUSTER_ID) using command-runner.jar.
    """
    logger.info("Starting Task: task_train_xgboost (Mode: AWS EMR Cluster)...")
    _load_env_file()
    cluster_id = os.environ.get("EMR_CLUSTER_ID", "")
    s3_bucket = os.environ.get("S3_BUCKET_NAME", S3_BUCKET_NAME)
    aws_region = os.environ.get("AWS_DEFAULT_REGION", AWS_REGION)

    if not cluster_id:
        raise ValueError("EMR_CLUSTER_ID is required to train XGBoost on the AWS EMR cluster.")

    import boto3
    emr_client = boto3.client("emr", region_name=aws_region)
    s3_client = boto3.client("s3", region_name=aws_region)

    # 1. Stage train_xgboost.py script to S3
    script_s3_uri = f"s3://{s3_bucket}/scripts/train_xgboost.py"
    local_script = os.path.join(REPO_ROOT, "models/train_xgboost.py")

    logger.info(f"Staging XGBoost training script to S3: {script_s3_uri}...")
    s3_client.upload_file(local_script, s3_bucket, "scripts/train_xgboost.py")

    # 2. Stage run_train.sh wrapper script to S3
    sh_content = f"""#!/bin/bash
set -ex
echo "=== Staging training script ==="
aws s3 cp {script_s3_uri} /tmp/train_xgboost.py

echo "=== Installing dependencies ==="
python3 -m pip install --no-cache-dir --user xgboost scikit-learn matplotlib pyarrow boto3

echo "=== Running XGBoost Pipeline on AWS EMR ==="
USER_SITE=$(python3 -m site --user-site 2>/dev/null || echo "/home/hadoop/.local/lib/python3.9/site-packages")
export PYTHONPATH="$USER_SITE:/home/hadoop/.local/lib/python3.9/site-packages:/home/hadoop/.local/lib/python3.10/site-packages:$PYTHONPATH"
python3 /tmp/train_xgboost.py --s3-bucket {s3_bucket}
"""
    s3_client.put_object(
        Bucket=s3_bucket,
        Key="scripts/run_train.sh",
        Body=sh_content.encode("utf-8")
    )

    step_cmd = f"aws s3 cp s3://{s3_bucket}/scripts/run_train.sh /tmp/run_train.sh && bash /tmp/run_train.sh"
    step_args = ["bash", "-c", step_cmd]

    logger.info(f"Submitting XGBoost Training Step to EMR Cluster {cluster_id}...")
    response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "AML_XGBoost_Model_Training",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": step_args
                }
            }
        ]
    )
    step_id = response["StepIds"][0]
    logger.info(f"Submitted EMR XGBoost Training Step ID: {step_id}. Waiting for completion...")

    # 3. Poll step status until completion
    while True:
        desc = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = desc["Step"]["Status"]["State"]
        logger.info(f"EMR Training Step {step_id} status: {state}")
        if state in ["COMPLETED"]:
            logger.info("EMR XGBoost Training Step successfully finished on AWS.")
            break
        elif state in ["CANCELLED", "FAILED", "INTERRUPTED"]:
            reason = desc["Step"]["Status"].get("FailureDetails", {}).get("Reason", "Unknown")
            log_file = desc["Step"]["Status"].get("FailureDetails", {}).get("LogFile", "")
            raise RuntimeError(f"EMR Training Step {step_id} failed with state: {state}. Reason: {reason}. Log: {log_file}")
        time.sleep(30)

    logger.info("task_train_xgboost completed successfully on AWS EMR.")


# =============================================================================
# DAG Definition
# =============================================================================

default_args = {
    "owner": "aml_data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="aml_end_to_end_pipeline",
    default_args=default_args,
    description="End-to-end Medallion AML Pipeline: Ingestion -> Spark ETL -> XGBoost Training & Evaluation",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aml", "medallion", "xgboost", "spark", "s3"],
) as dag:

    # 1. Ingest Data (Kaggle / Synthetic -> Bronze S3)
    task_ingest_data = PythonOperator(
        task_id="task_ingest_data",
        python_callable=run_ingest_task,
        provide_context=True,
    )

    # 2. Submit Spark ETL (AWS EMR -> Processed S3)
    task_submit_spark_etl = PythonOperator(
        task_id="task_submit_spark_etl",
        python_callable=run_spark_etl_task,
        provide_context=True,
    )

    # 3. Train XGBoost Model & Save Evaluation/Weights (Processed -> Evaluation & Model S3)
    task_train_xgboost = PythonOperator(
        task_id="task_train_xgboost",
        python_callable=run_xgboost_task,
        provide_context=True,
    )

    # Dependency Flow: Ingestion -> Spark ETL -> XGBoost Training
    task_ingest_data >> task_submit_spark_etl >> task_train_xgboost

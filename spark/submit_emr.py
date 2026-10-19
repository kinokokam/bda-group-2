"""
Standalone AWS EMR Step Submitter for AML Spark ETL Pipeline.

Uploads spark/etl_pipeline.py to s3://<S3_BUCKET_NAME>/scripts/etl_pipeline.py
and submits a spark-submit step to an existing AWS EMR cluster without requiring Terraform.
Monitors the step status until COMPLETED or FAILED.
"""

import os
import sys
import time
import argparse
import logging
import boto3

# Automatically load credentials from .env if present
def _load_env_file():
    for base in [os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), os.getcwd()]:
        env_file = os.path.join(base, ".env")
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip("\"' ")
                        if k not in os.environ:
                            os.environ[k] = v
            break

_load_env_file()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("submit_emr")


def submit_spark_job_to_emr(
    cluster_id: str,
    s3_bucket: str,
    region: str = "us-east-1",
    script_local_path: str = "./spark/etl_pipeline.py"
):
    """Submits PySpark ETL script to EMR and monitors progress."""
    if not cluster_id or not cluster_id.startswith("j-"):
        raise ValueError(f"Invalid EMR Cluster ID: '{cluster_id}'. Cluster IDs must start with 'j-'.")

    s3_client = boto3.client("s3", region_name=region)
    emr_client = boto3.client("emr", region_name=region)

    # 1. Upload ETL script to S3 scripts prefix
    s3_script_key = "scripts/etl_pipeline.py"
    s3_script_uri = f"s3://{s3_bucket}/{s3_script_key}"
    logger.info(f"Uploading {script_local_path} to {s3_script_uri}...")
    s3_client.upload_file(script_local_path, s3_bucket, s3_script_key)

    # 2. Define Spark Step
    step_args = [
        "spark-submit",
        "--deploy-mode", "cluster",
        s3_script_uri,
        "--s3-bucket", s3_bucket
    ]

    logger.info(f"Submitting Spark step to EMR cluster {cluster_id}...")
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
    logger.info(f"Successfully submitted step ID: {step_id}")
    logger.info("Polling EMR step status every 20 seconds...")

    # 3. Poll Step Status
    while True:
        desc = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
        status = desc["Step"]["Status"]
        state = status["State"]
        logger.info(f"EMR Step [{step_id}] Status: {state}")

        if state == "COMPLETED":
            logger.info("Spark ETL job on EMR finished successfully!")
            logger.info(f"Processed Parquet datasets are ready at: s3://{s3_bucket}/processed/")
            return True
        elif state in ["CANCELLED", "FAILED", "INTERRUPTED"]:
            reason = status.get("FailureDetails", {}).get("Reason", "Unknown")
            message = status.get("FailureDetails", {}).get("Message", "")
            logger.error(f"EMR Step failed! State: {state}, Reason: {reason}, Message: {message}")
            return False

        time.sleep(20)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit AML PySpark ETL to AWS EMR")
    parser.add_argument("--cluster-id", type=str, default=os.environ.get("EMR_CLUSTER_ID", ""), help="EMR Cluster ID (e.g. j-XXXXXXXXXXXX)")
    parser.add_argument("--s3-bucket", type=str, default=os.environ.get("S3_BUCKET_NAME", ""), help="Target S3 Bucket")
    parser.add_argument("--region", type=str, default=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"), help="AWS Region")

    args = parser.parse_args()

    if not args.cluster_id:
        logger.error("EMR_CLUSTER_ID is required. Set it in .env or pass --cluster-id j-XXXXXXXXXXXX")
        sys.exit(1)
    if not args.s3_bucket:
        logger.error("S3_BUCKET_NAME is required. Set it in .env or pass --s3-bucket your-bucket")
        sys.exit(1)

    success = submit_spark_job_to_emr(
        cluster_id=args.cluster_id,
        s3_bucket=args.s3_bucket,
        region=args.region
    )
    if not success:
        sys.exit(1)

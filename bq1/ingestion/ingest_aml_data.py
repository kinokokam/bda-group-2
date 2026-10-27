"""
Unified AML Data Ingestion Orchestrator.

Combines kaggle_download.py and s3_uploader.py into a single, robust entrypoint.
Workflow:
1. Downloads IBM AML dataset from Kaggle or triggers synthetic generator fallback.
2. Uploads raw CSV files (HI-Small_Trans.csv, HI-Small_accounts.csv) to s3://<BUCKET_NAME>/bronze/.
3. Verifies S3 upload and cleans up temporary local unzipped files if requested.
"""

import os
import sys
import logging
import argparse

# Ensure parent directory is in sys.path for standalone script execution
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from ingestion.kaggle_download import get_aml_data, generate_synthetic_hi_small
    from ingestion.s3_uploader import upload_bronze_dataset
except ImportError:
    from kaggle_download import get_aml_data, generate_synthetic_hi_small
    from s3_uploader import upload_bronze_dataset

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
logger = logging.getLogger("ingest_aml_data")


def run_ingestion_pipeline(
    raw_dir: str = "./data/raw",
    s3_bucket: str = None,
    s3_prefix: str = "bronze",
    use_synthetic: bool = False,
    clean_local: bool = False,
    num_txs: int = 15000,
    num_accounts: int = 600
):
    """Executes the complete ingestion lifecycle."""
    logger.info("=== STEP 1: Starting AML Data Ingestion ===")
    s3_bucket = s3_bucket or os.environ.get("S3_BUCKET_NAME", "")
    if not s3_bucket:
        raise ValueError("S3_BUCKET_NAME is required for AML data ingestion to Bronze layer.")

    logger.info(f"Target Mode: AWS (s3://{s3_bucket}/{s3_prefix}/)")

    # 1. Acquire Data (Strict Kaggle Ingestion)
    if use_synthetic:
        logger.info("Explicit --use-synthetic flag provided: generating synthetic AMLSim dataset for offline testing...")
        generate_synthetic_hi_small(output_dir=raw_dir, num_transactions=num_txs, num_accounts=num_accounts)
    else:
        logger.info("Acquiring data via Kaggle API (ealtman2019/ibm-transactions-for-anti-money-laundering-aml)...")
        try:
            get_aml_data(output_dir=raw_dir, force_synthetic=False)
        except Exception as e:
            logger.error(f"Data ingestion failed: {e}")
            logger.error("No synthetic fallback allowed. Pipeline aborted.")
            sys.exit(1)

    # 2. Stage to Bronze Layer (S3)
    logger.info("Staging raw CSV files to Bronze layer in AWS S3...")
    success = upload_bronze_dataset(
        raw_dir=raw_dir,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        clean_local=clean_local
    )

    if success:
        logger.info("=== STEP 1: Ingestion Successfully Completed ===")
    else:
        logger.error("=== STEP 1: Ingestion Failed ===")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified AML Data Ingestion Orchestrator")
    parser.add_argument("--raw-dir", type=str, default="./data/raw", help="Local staging directory for raw downloads")
    parser.add_argument("--s3-bucket", type=str, default=os.environ.get("S3_BUCKET_NAME", ""), help="S3 bucket name")
    parser.add_argument("--s3-prefix", type=str, default="bronze", help="Bronze prefix in S3 (default: bronze)")
    parser.add_argument("--use-synthetic", action="store_true", help="Force synthetic data generator")
    parser.add_argument("--clean-local", action="store_true", help="Remove raw staging files after successful upload")
    parser.add_argument("--num-txs", type=int, default=15000, help="Number of synthetic transactions if using synthetic")
    parser.add_argument("--num-accounts", type=int, default=600, help="Number of synthetic accounts if using synthetic")

    args = parser.parse_args()

    run_ingestion_pipeline(
        raw_dir=args.raw_dir,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        use_synthetic=args.use_synthetic,
        clean_local=args.clean_local,
        num_txs=args.num_txs,
        num_accounts=args.num_accounts
    )

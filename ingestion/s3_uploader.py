"""
AWS S3 Medallion Bronze Layer Uploader.

Uploads raw AML datasets (HI-Small_Trans.csv, HI-Small_accounts.csv)
to s3://<BUCKET_NAME>/bronze/ with:
- Multipart upload support (boto3 TransferConfig)
- Exponential backoff retries for network resilience
- S3 size verification against local file size
"""

import os
import sys
import time
import argparse
import logging
import shutil

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("s3_uploader")

TARGET_FILES = ["HI-Small_Trans.csv", "HI-Small_accounts.csv"]


def upload_file_to_s3(
    local_path: str,
    s3_bucket: str,
    s3_key: str,
    max_retries: int = 4,
    backoff_factor: float = 2.0
) -> bool:
    """
    Uploads a local file to S3 using multipart upload with exponential backoff retry.
    Verifies object existence and file size matching in S3 before returning True.
    """
    try:
        import boto3
        from boto3.s3.transfer import TransferConfig
        from botocore.exceptions import ClientError
    except ImportError:
        logger.error("boto3 is not installed. Run 'pip install boto3'")
        return False

    if not os.path.exists(local_path):
        logger.error(f"Local file not found: {local_path}")
        return False

    local_size = os.path.getsize(local_path)
    logger.info(f"Preparing upload of {local_path} ({local_size / (1024*1024):.2f} MB) to s3://{s3_bucket}/{s3_key}")

    # Configure multipart upload for high throughput and reliability
    config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,   # 8 MB
        max_concurrency=10,
        multipart_chunksize=8 * 1024 * 1024,  # 8 MB
        use_threads=True
    )

    s3_client = boto3.client("s3")

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"[Attempt {attempt}/{max_retries}] Uploading to s3://{s3_bucket}/{s3_key}...")
            s3_client.upload_file(
                Filename=local_path,
                Bucket=s3_bucket,
                Key=s3_key,
                Config=config
            )

            # Verification: Head Object to check existence and size match
            logger.info(f"Verifying uploaded object s3://{s3_bucket}/{s3_key}...")
            head_resp = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
            remote_size = head_resp.get("ContentLength", 0)

            if remote_size == local_size:
                logger.info(f"Verification SUCCESS: s3://{s3_bucket}/{s3_key} size ({remote_size} bytes) matches local file.")
                return True
            else:
                logger.warning(f"Verification MISMATCH: local={local_size} vs s3={remote_size}. Retrying...")

        except Exception as e:
            wait_time = backoff_factor ** attempt
            logger.warning(f"Upload failed on attempt {attempt}: {e}. Retrying in {wait_time:.1f}s...")
            time.sleep(wait_time)

    logger.error(f"Failed to upload {local_path} to s3://{s3_bucket}/{s3_key} after {max_retries} attempts.")
    return False


def upload_bronze_dataset(
    raw_dir: str = "./data/raw",
    s3_bucket: str = None,
    s3_prefix: str = "bronze",
    clean_local: bool = False
):
    """
    Finds HI-Small_Trans.csv and HI-Small_accounts.csv in raw_dir and uploads
    to s3://<s3_bucket>/bronze/.
    """
    s3_bucket = s3_bucket or os.environ.get("S3_BUCKET_NAME", "")
    if not s3_bucket:
        raise ValueError("S3_BUCKET_NAME is required to upload raw data to Bronze layer.")

    success_count = 0
    uploaded_files = []

    for filename in TARGET_FILES:
        local_path = os.path.join(raw_dir, filename)
        if not os.path.exists(local_path):
            logger.warning(f"File {local_path} not found in {raw_dir}.")
            continue

        s3_key = f"{s3_prefix}/{filename}"
        if upload_file_to_s3(local_path, s3_bucket, s3_key):
            success_count += 1
            uploaded_files.append(local_path)

    logger.info(f"Staged {success_count}/{len(TARGET_FILES)} raw files to bronze layer in s3://{s3_bucket}/{s3_prefix}/.")

    if clean_local and success_count == len(TARGET_FILES):
        for f in uploaded_files:
            try:
                os.remove(f)
                logger.info(f"Cleaned up temporary staging file: {f}")
            except OSError as e:
                logger.warning(f"Could not remove {f}: {e}")

    return success_count == len(TARGET_FILES)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload raw AML data to Bronze layer")
    parser.add_argument("--raw-dir", type=str, default="./data/raw", help="Path to local raw data directory")
    parser.add_argument("--s3-bucket", type=str, default=os.environ.get("S3_BUCKET_NAME", ""), help="Target S3 bucket name")
    parser.add_argument("--s3-prefix", type=str, default="bronze", help="Bronze prefix in S3 (default: bronze)")
    parser.add_argument("--clean-local", action="store_true", help="Remove raw staging files after successful upload")

    args = parser.parse_args()
    upload_bronze_dataset(
        raw_dir=args.raw_dir,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        clean_local=args.clean_local
    )

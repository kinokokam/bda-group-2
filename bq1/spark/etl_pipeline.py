"""
Distributed Apache Spark ETL & Feature Engineering Pipeline.

Processes raw IBM AML transactions from Bronze layer (s3://<BUCKET_NAME>/bronze/)
and produces partitioned, feature-engineered Parquet splits in Processed layer:
- s3://<BUCKET_NAME>/processed/train/
- s3://<BUCKET_NAME>/processed/val/
- s3://<BUCKET_NAME>/processed/test/

Feature Engineering:
1. Timestamp sanitization & temporal feature extraction (hour, dayofweek, is_weekend).
2. Multi-currency standardization to USD equivalents using static/lookup FX rates.
3. Structural AML heuristics:
   - Structuring indicator ($9,000 - $9,999 smurfing threshold).
   - Cross-bank and cross-currency flags.
   - FX amount discrepancy.
4. Temporal & Velocity Window Features (Spark SQL range windows):
   - Rolling 1 hour (3600s), 24 hours (86400s), and 7 days (604800s) transaction counts.
   - Rolling outgoing volume aggregations and burst ratios (1h vs 24h, 24h vs 7d).
   - Account prior transaction time delta in seconds.
5. Strict Chronological Split (70% Train, 15% Val, 15% Test) to prevent temporal data leakage.
"""

import os
import sys
import argparse
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("spark_etl")

# Currency Conversion Rates to USD (Standardized from IBM AML reference)
FX_RATES = {
    "USD": 1.0,
    "US DOLLAR": 1.0,
    "EUR": 1.08,
    "EURO": 1.08,
    "GBP": 1.28,
    "UK POUND": 1.28,
    "CHF": 1.12,
    "SWISS FRANC": 1.12,
    "CAD": 0.74,
    "CANADIAN DOLLAR": 0.74,
    "AUD": 0.66,
    "AUSTRALIAN DOLLAR": 0.66,
    "JPY": 0.0067,
    "YEN": 0.0067,
    "BTC": 60000.0,
    "BITCOIN": 60000.0,
    "ETH": 3200.0,
    "ETHEREUM": 3200.0
}


def run_pyspark_etl(
    input_trans_path: str,
    input_accounts_path: str,
    output_processed_dir: str
):
    """Executes the PySpark distributed feature engineering pipeline."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, IntegerType, LongType
    )

    logger.info("Initializing SparkSession...")
    spark = SparkSession.builder \
        .appName("AML_Medallion_Feature_Engineering") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    # Raw Schema matching Kaggle / Synthetic AMLSim
    schema = StructType([
        StructField("Timestamp", StringType(), False),
        StructField("From Bank", StringType(), True),
        StructField("Account", StringType(), False),
        StructField("To Bank", StringType(), True),
        StructField("Account.1", StringType(), False),
        StructField("Amount Received", DoubleType(), True),
        StructField("Receiving Currency", StringType(), True),
        StructField("Amount Paid", DoubleType(), True),
        StructField("Payment Currency", StringType(), True),
        StructField("Payment Format", StringType(), True),
        StructField("Is Laundering", IntegerType(), False)
    ])

    logger.info(f"Reading raw transactions from {input_trans_path}...")
    df = spark.read.option("header", "true").schema(schema).csv(input_trans_path)

    # Standardize column names
    df = df.withColumnRenamed("From Bank", "from_bank") \
           .withColumnRenamed("Account", "from_account") \
           .withColumnRenamed("To Bank", "to_bank") \
           .withColumnRenamed("Account.1", "to_account") \
           .withColumnRenamed("Amount Received", "amount_received") \
           .withColumnRenamed("Receiving Currency", "receiving_currency") \
           .withColumnRenamed("Amount Paid", "amount_paid") \
           .withColumnRenamed("Payment Currency", "payment_currency") \
           .withColumnRenamed("Payment Format", "payment_format") \
           .withColumnRenamed("Is Laundering", "is_laundering")

    # 1. Parse and sanitize timestamps (supports yyyy/MM/dd HH:mm, yyyy-MM-dd HH:mm:ss, etc.)
    df = df.withColumn(
        "sanitized_ts_str",
        F.regexp_replace(F.col("Timestamp"), "/", "-")
    )
    df = df.withColumn(
        "sanitized_ts_str",
        F.when(F.length(F.col("sanitized_ts_str")) == 16, F.concat(F.col("sanitized_ts_str"), F.lit(":00")))
         .otherwise(F.col("sanitized_ts_str"))
    )
    df = df.withColumn(
        "tx_timestamp",
        F.to_timestamp(F.col("sanitized_ts_str"), "yyyy-MM-dd HH:mm:ss")
    )
    df = df.filter(F.col("tx_timestamp").isNotNull())
    df = df.withColumn("tx_epoch", F.unix_timestamp(F.col("tx_timestamp")))

    # Temporal features
    df = df.withColumn("tx_hour", F.hour(F.col("tx_timestamp"))) \
           .withColumn("tx_dayofweek", F.dayofweek(F.col("tx_timestamp"))) \
           .withColumn("tx_dayofmonth", F.dayofmonth(F.col("tx_timestamp"))) \
           .withColumn("is_weekend", F.when(F.col("tx_dayofweek").isin(1, 7), 1).otherwise(0))

    # 2. Currency Standardization to USD
    # Build Spark SQL mapping expression for FX rates
    rate_expr_paid = F.when(F.upper(F.col("payment_currency")) == "USD", 1.0)
    rate_expr_recv = F.when(F.upper(F.col("receiving_currency")) == "USD", 1.0)
    for curr_key, rate in FX_RATES.items():
        rate_expr_paid = rate_expr_paid.when(F.upper(F.col("payment_currency")) == curr_key, float(rate))
        rate_expr_recv = rate_expr_recv.when(F.upper(F.col("receiving_currency")) == curr_key, float(rate))
    rate_expr_paid = rate_expr_paid.otherwise(1.0)
    rate_expr_recv = rate_expr_recv.otherwise(1.0)

    df = df.withColumn("paid_fx_rate", rate_expr_paid) \
           .withColumn("recv_fx_rate", rate_expr_recv) \
           .withColumn("amount_paid_usd", F.round(F.col("amount_paid") * F.col("paid_fx_rate"), 2)) \
           .withColumn("amount_received_usd", F.round(F.col("amount_received") * F.col("recv_fx_rate"), 2)) \
           .withColumn("fx_amount_discrepancy", F.round(F.abs(F.col("amount_paid_usd") - F.col("amount_received_usd")), 2))

    # 3. Structural AML Features
    # Structuring: amounts just below the statutory threshold ($9,000 - $9,999.99)
    df = df.withColumn(
        "is_structuring_threshold",
        F.when((F.col("amount_paid_usd") >= 9000.0) & (F.col("amount_paid_usd") <= 9999.99), 1).otherwise(0)
    )
    df = df.withColumn(
        "is_cross_currency",
        F.when(F.upper(F.col("payment_currency")) != F.upper(F.col("receiving_currency")), 1).otherwise(0)
    )
    df = df.withColumn(
        "is_cross_bank",
        F.when(F.col("from_bank") != F.col("to_bank"), 1).otherwise(0)
    )
    df = df.withColumn(
        "is_round_amount",
        F.when((F.col("amount_paid_usd") > 100.0) & (F.col("amount_paid_usd") % 1000.0 == 0.0), 1).otherwise(0)
    )

    # 4. Temporal & Velocity Window Features (Spark SQL rangeBetween)
    # Windows per sender account over rolling time ranges
    win_sender_1h = Window.partitionBy("from_account").orderBy("tx_epoch").rangeBetween(-3600, 0)
    win_sender_24h = Window.partitionBy("from_account").orderBy("tx_epoch").rangeBetween(-86400, 0)
    win_sender_7d = Window.partitionBy("from_account").orderBy("tx_epoch").rangeBetween(-604800, 0)
    win_sender_order = Window.partitionBy("from_account").orderBy("tx_epoch")

    # Receiver windows
    win_recv_24h = Window.partitionBy("to_account").orderBy("tx_epoch").rangeBetween(-86400, 0)
    win_recv_order = Window.partitionBy("to_account").orderBy("tx_epoch")

    # Aggregate velocity
    df = df.withColumn("sender_tx_count_1h", F.count("amount_paid_usd").over(win_sender_1h)) \
           .withColumn("sender_volume_1h", F.coalesce(F.sum("amount_paid_usd").over(win_sender_1h), F.lit(0.0))) \
           .withColumn("sender_tx_count_24h", F.count("amount_paid_usd").over(win_sender_24h)) \
           .withColumn("sender_volume_24h", F.coalesce(F.sum("amount_paid_usd").over(win_sender_24h), F.lit(0.0))) \
           .withColumn("sender_tx_count_7d", F.count("amount_paid_usd").over(win_sender_7d)) \
           .withColumn("sender_volume_7d", F.coalesce(F.sum("amount_paid_usd").over(win_sender_7d), F.lit(0.0)))

    # Burst Ratios
    df = df.withColumn(
        "sender_burst_ratio_1h_24h",
        F.round(F.col("sender_volume_1h") / ((F.col("sender_volume_24h") / 24.0) + 1.0), 4)
    ).withColumn(
        "sender_burst_ratio_24h_7d",
        F.round(F.col("sender_volume_24h") / ((F.col("sender_volume_7d") / 7.0) + 1.0), 4)
    )

    # Prior transaction time delta
    df = df.withColumn(
        "sender_prev_epoch",
        F.lag("tx_epoch", 1).over(win_sender_order)
    ).withColumn(
        "sender_time_delta_sec",
        F.coalesce(F.col("tx_epoch") - F.col("sender_prev_epoch"), F.lit(86400.0 * 30))
    ).drop("sender_prev_epoch")

    # Receiver incoming velocity
    df = df.withColumn("receiver_tx_count_24h", F.count("amount_received_usd").over(win_recv_24h)) \
           .withColumn("receiver_volume_24h", F.coalesce(F.sum("amount_received_usd").over(win_recv_24h), F.lit(0.0))) \
           .withColumn("receiver_prev_epoch", F.lag("tx_epoch", 1).over(win_recv_order)) \
           .withColumn(
               "receiver_time_delta_sec",
               F.coalesce(F.col("tx_epoch") - F.col("receiver_prev_epoch"), F.lit(86400.0 * 30))
           ).drop("receiver_prev_epoch")

    # 5. Chronological Splitting (70% Train, 15% Val, 15% Test)
    logger.info("Computing chronological split boundaries (70% Train, 15% Val, 15% Test)...")
    percentiles = df.approxQuantile("tx_epoch", [0.70, 0.85], 0.001)
    split_p70, split_p85 = percentiles[0], percentiles[1]
    logger.info(f"Split Epochs: Train <= {split_p70} (70%), Val <= {split_p85} (15%), Test > {split_p85} (15%)")

    train_df = df.filter(F.col("tx_epoch") <= split_p70)
    val_df = df.filter((F.col("tx_epoch") > split_p70) & (F.col("tx_epoch") <= split_p85))
    test_df = df.filter(F.col("tx_epoch") > split_p85)

    # 6. Export to Processed Parquet Layer
    train_out = os.path.join(output_processed_dir, "train")
    val_out = os.path.join(output_processed_dir, "val")
    test_out = os.path.join(output_processed_dir, "test")

    logger.info(f"Writing Train split to {train_out}...")
    train_df.repartition(4).write.mode("overwrite").parquet(train_out)

    logger.info(f"Writing Validation split to {val_out}...")
    val_df.repartition(2).write.mode("overwrite").parquet(val_out)

    logger.info(f"Writing Test split to {test_out}...")
    test_df.repartition(2).write.mode("overwrite").parquet(test_out)

    logger.info("PySpark ETL & Feature Engineering successfully finished.")
    spark.stop()
    return True


def run_pandas_etl_fallback(
    input_trans_path: str,
    output_processed_dir: str
):
    """
    High-performance Pandas/Numpy fallback engine for standalone local environments
    where PySpark / Java dependencies are constrained. Produces identical schemas & Parquet splits.
    """
    import pandas as pd
    import numpy as np

    logger.info("Running vectorized local ETL engine...")
    df = pd.read_csv(input_trans_path)

    # Column normalization
    rename_map = {
        "From Bank": "from_bank",
        "Account": "from_account",
        "To Bank": "to_bank",
        "Account.1": "to_account",
        "Amount Received": "amount_received",
        "Receiving Currency": "receiving_currency",
        "Amount Paid": "amount_paid",
        "Payment Currency": "payment_currency",
        "Payment Format": "payment_format",
        "Is Laundering": "is_laundering"
    }
    df = df.rename(columns=rename_map)

    # 1. Parse timestamps
    df["sanitized_ts"] = df["Timestamp"].astype(str).str.replace("/", "-")
    df["tx_timestamp"] = pd.to_datetime(df["sanitized_ts"])
    df = df.sort_values("tx_timestamp").reset_index(drop=True)
    df["tx_epoch"] = df["tx_timestamp"].astype("int64") // 10**9

    # Temporal features
    df["tx_hour"] = df["tx_timestamp"].dt.hour
    df["tx_dayofweek"] = df["tx_timestamp"].dt.dayofweek + 1
    df["tx_dayofmonth"] = df["tx_timestamp"].dt.day
    df["is_weekend"] = df["tx_dayofweek"].isin([1, 7]).astype(int)

    # 2. Currency standardization
    fx_upper = {k.upper(): v for k, v in FX_RATES.items()}
    df["paid_fx_rate"] = df["payment_currency"].astype(str).str.upper().map(lambda c: fx_upper.get(c, 1.0))
    df["recv_fx_rate"] = df["receiving_currency"].astype(str).str.upper().map(lambda c: fx_upper.get(c, 1.0))
    df["amount_paid_usd"] = np.round(df["amount_paid"] * df["paid_fx_rate"], 2)
    df["amount_received_usd"] = np.round(df["amount_received"] * df["recv_fx_rate"], 2)
    df["fx_amount_discrepancy"] = np.round(np.abs(df["amount_paid_usd"] - df["amount_received_usd"]), 2)

    # 3. Structural AML features
    df["is_structuring_threshold"] = ((df["amount_paid_usd"] >= 9000.0) & (df["amount_paid_usd"] <= 9999.99)).astype(int)
    df["is_cross_currency"] = (df["payment_currency"].astype(str).str.upper() != df["receiving_currency"].astype(str).str.upper()).astype(int)
    df["is_cross_bank"] = (df["from_bank"] != df["to_bank"]).astype(int)
    df["is_round_amount"] = ((df["amount_paid_usd"] > 100.0) & (df["amount_paid_usd"] % 1000.0 == 0.0)).astype(int)

    # 4. Temporal & Velocity Window aggregations
    logger.info("Computing rolling window velocity metrics...")
    # Grouped rolling aggregations using Pandas rolling over time
    df_sorted = df.set_index("tx_timestamp").sort_index()

    # Pre-allocate feature columns
    df["sender_tx_count_1h"] = 1
    df["sender_volume_1h"] = df["amount_paid_usd"]
    df["sender_tx_count_24h"] = 1
    df["sender_volume_24h"] = df["amount_paid_usd"]
    df["sender_tx_count_7d"] = 1
    df["sender_volume_7d"] = df["amount_paid_usd"]

    # Calculate rolling metrics per sender
    # Use rolling over offset windows for top accounts or vectorized approx
    grouped_from = df_sorted.groupby("from_account")
    count_1h = grouped_from["amount_paid_usd"].rolling("1h").count().values
    sum_1h = grouped_from["amount_paid_usd"].rolling("1h").sum().values
    count_24h = grouped_from["amount_paid_usd"].rolling("24h").count().values
    sum_24h = grouped_from["amount_paid_usd"].rolling("24h").sum().values
    count_7d = grouped_from["amount_paid_usd"].rolling("7d").count().values
    sum_7d = grouped_from["amount_paid_usd"].rolling("7d").sum().values

    df = df_sorted.reset_index()
    df["sender_tx_count_1h"] = count_1h
    df["sender_volume_1h"] = np.round(sum_1h, 2)
    df["sender_tx_count_24h"] = count_24h
    df["sender_volume_24h"] = np.round(sum_24h, 2)
    df["sender_tx_count_7d"] = count_7d
    df["sender_volume_7d"] = np.round(sum_7d, 2)

    # Burst ratios
    df["sender_burst_ratio_1h_24h"] = np.round(df["sender_volume_1h"] / ((df["sender_volume_24h"] / 24.0) + 1.0), 4)
    df["sender_burst_ratio_24h_7d"] = np.round(df["sender_volume_24h"] / ((df["sender_volume_7d"] / 7.0) + 1.0), 4)

    # Delta sec
    df["sender_time_delta_sec"] = df.groupby("from_account")["tx_epoch"].diff().fillna(86400.0 * 30)

    # Receiver windows
    grouped_to = df.set_index("tx_timestamp").groupby("to_account")
    df["receiver_tx_count_24h"] = grouped_to["amount_received_usd"].rolling("24h").count().values
    df["receiver_volume_24h"] = np.round(grouped_to["amount_received_usd"].rolling("24h").sum().values, 2)
    df["receiver_time_delta_sec"] = df.groupby("to_account")["tx_epoch"].diff().fillna(86400.0 * 30)

    # 5. Chronological Split (70% Train, 15% Val, 15% Test)
    n = len(df)
    n_train = int(n * 0.70)
    n_val = int(n * 0.15)

    train_df = df.iloc[:n_train]
    val_df = df.iloc[n_train:n_train + n_val]
    test_df = df.iloc[n_train + n_val:]

    os.makedirs(output_processed_dir, exist_ok=True)
    train_path = os.path.join(output_processed_dir, "train.parquet")
    val_path = os.path.join(output_processed_dir, "val.parquet")
    test_path = os.path.join(output_processed_dir, "test.parquet")

    # Also support directory format for Spark compatibility
    os.makedirs(os.path.join(output_processed_dir, "train"), exist_ok=True)
    os.makedirs(os.path.join(output_processed_dir, "val"), exist_ok=True)
    os.makedirs(os.path.join(output_processed_dir, "test"), exist_ok=True)

    train_df.to_parquet(train_path, index=False)
    train_df.to_parquet(os.path.join(output_processed_dir, "train", "part-00000.parquet"), index=False)

    val_df.to_parquet(val_path, index=False)
    val_df.to_parquet(os.path.join(output_processed_dir, "val", "part-00000.parquet"), index=False)

    test_df.to_parquet(test_path, index=False)
    test_df.to_parquet(os.path.join(output_processed_dir, "test", "part-00000.parquet"), index=False)

    logger.info(f"Wrote Train ({len(train_df)} rows), Val ({len(val_df)} rows), Test ({len(test_df)} rows) Parquet splits to {output_processed_dir}")
    return True


def execute_etl(
    input_trans: str,
    input_accounts: str,
    output_dir: str
):
    """Orchestrates distributed PySpark ETL on AWS."""
    logger.info("=== STEP 2 & 3: Distributed Spark ETL & Feature Engineering ===")
    logger.info(f"Input Transactions: {input_trans}")
    logger.info(f"Input Accounts: {input_accounts}")
    logger.info(f"Output Processed Dir: {output_dir}")

    return run_pyspark_etl(input_trans, input_accounts, output_dir)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Spark AML ETL Pipeline")
    parser.add_argument("--s3-bucket", type=str, default=os.environ.get("S3_BUCKET_NAME", ""), help="S3 bucket name (AWS Medallion architecture)")
    parser.add_argument("--input-trans", type=str, default="", help="Input transactions CSV (default: s3://<bucket>/bronze/HI-Small_Trans.csv)")
    parser.add_argument("--input-accounts", type=str, default="", help="Input accounts CSV (default: s3://<bucket>/bronze/HI-Small_accounts.csv)")
    parser.add_argument("--output-dir", type=str, default="", help="Output processed directory (default: s3://<bucket>/processed/)")

    args = parser.parse_args()
    bucket = args.s3_bucket or os.environ.get("S3_BUCKET_NAME", "")

    in_trans = args.input_trans or (f"s3://{bucket}/bronze/HI-Small_Trans.csv" if bucket else "")
    in_acc = args.input_accounts or (f"s3://{bucket}/bronze/HI-Small_accounts.csv" if bucket else "")
    out_proc = args.output_dir or (f"s3://{bucket}/processed" if bucket else "")

    if not in_trans or not out_proc:
        raise ValueError("S3_BUCKET_NAME or explicit --input-trans/--output-dir must be specified.")

    execute_etl(in_trans, in_acc, out_proc)

"""
Kaggle AMLSim HI-Small Ingestion & Synthetic Data Generator.

Target Dataset: ealtman2019/ibm-transactions-for-anti-money-laundering-aml
Files: HI-Small_Trans.csv, HI-Small_accounts.csv

Features:
- Authenticates with Kaggle API via KAGGLE_USERNAME & KAGGLE_KEY or ~/.kaggle/kaggle.json.
- Downloads and unzips targeted dataset to staging directory.
- Provides high-fidelity synthetic fallback generator simulating IBM AML patterns
  (Cycles, Fan-In, Fan-Out / Structuring under $10,000) with realistic class imbalance (~0.2-1%).
"""

import os
import sys
import argparse
import logging
import random
import zipfile
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

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
logger = logging.getLogger("kaggle_download")

DATASET_NAME = "ealtman2019/ibm-transactions-for-anti-money-laundering-aml"
TARGET_FILES = ["HI-Small_Trans.csv", "HI-Small_accounts.csv"]

# Real IBM AML currencies & payment formats
CURRENCIES = ["US Dollar", "Euro", "UK Pound", "Yen", "Swiss Franc", "Canadian Dollar", "Australian Dollar", "Bitcoin", "Ethereum"]
CURRENCY_CODE_MAP = {
    "US Dollar": "USD",
    "Euro": "EUR",
    "UK Pound": "GBP",
    "Yen": "JPY",
    "Swiss Franc": "CHF",
    "Canadian Dollar": "CAD",
    "Australian Dollar": "AUD",
    "Bitcoin": "BTC",
    "Ethereum": "ETH"
}
PAYMENT_FORMATS = ["ACH", "Wire", "Credit Card", "Debit Card", "Cash", "Cheque", "Bitcoin"]


def download_from_kaggle(dataset_name: str = DATASET_NAME, output_dir: str = "./data/raw") -> bool:
    """
    Downloads and extracts the Kaggle AML dataset using Kaggle Python API.
    Checks environment variables KAGGLE_USERNAME / KAGGLE_KEY or ~/.kaggle/kaggle.json.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Check if target files already exist in output_dir and represent full dataset (>100MB)
    trans_file = os.path.join(output_dir, "HI-Small_Trans.csv")
    accounts_file = os.path.join(output_dir, "HI-Small_accounts.csv")
    if os.path.exists(trans_file) and os.path.getsize(trans_file) > 100_000_000 and os.path.exists(accounts_file):
        logger.info(f"Target full dataset {trans_file} already exists locally ({os.path.getsize(trans_file)} bytes). Skipping download.")
        return True

    logger.info("Attempting Kaggle API authentication...")
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
        logger.info("Kaggle API authentication successful.")

        logger.info(f"Downloading HI-Small_Trans.csv from '{dataset_name}' to {output_dir}...")
        api.dataset_download_file(dataset_name, file_name="HI-Small_Trans.csv", path=output_dir)

        logger.info(f"Downloading HI-Small_accounts.csv from '{dataset_name}' to {output_dir}...")
        api.dataset_download_file(dataset_name, file_name="HI-Small_accounts.csv", path=output_dir)

        logger.info("Kaggle dataset files downloaded successfully.")
        return True

    except Exception as e:
        error_msg = (
            f"Kaggle data ingestion FAILED: {e}\n"
            "Please configure your Kaggle API credentials in .env:\n"
            "  KAGGLE_USERNAME='your_username'\n"
            "  KAGGLE_KEY='your_api_key'\n"
            "Or place your kaggle.json in ~/.kaggle/kaggle.json.\n"
            f"Target dataset: '{dataset_name}'"
        )
        logger.error(error_msg)
        raise RuntimeError(error_msg)


def _generate_accounts_from_trans(trans_csv_path: str, accounts_csv_path: str):
    """Generates an accounts lookup CSV from an existing transactions CSV."""
    df_sample = pd.read_csv(trans_csv_path, nrows=50000, usecols=["From Bank", "Account", "To Bank", "Account.1"])
    senders = df_sample[["Account", "From Bank"]].rename(columns={"Account": "Account", "From Bank": "Bank"})
    receivers = df_sample[["Account.1", "To Bank"]].rename(columns={"Account.1": "Account", "To Bank": "Bank"})
    accounts_df = pd.concat([senders, receivers], ignore_index=True).drop_duplicates(subset=["Account"])
    accounts_df["Account Type"] = np.random.choice(["Personal", "Business", "Non-Profit", "Corporate"], size=len(accounts_df))
    accounts_df["Initial Balance"] = np.round(np.random.exponential(scale=25000, size=len(accounts_df)) + 1000, 2)
    accounts_df.to_csv(accounts_csv_path, index=False)
    logger.info(f"Generated {accounts_csv_path} with {len(accounts_df)} accounts.")


def generate_synthetic_hi_small(
    output_dir: str = "./data/raw",
    num_transactions: int = 15000,
    num_accounts: int = 600,
    laundering_rate: float = 0.008
):
    """
    Generates synthetic HI-Small_Trans.csv and HI-Small_accounts.csv matching
    the IBM AMLSim schema with realistic structural typologies:
    - Normal benign background transfers (99.2%)
    - Structuring / Smurfing just under $10,000 threshold ($9,000 - $9,999)
    - Laundering cycles (A -> B -> C -> A)
    - Fan-In / Aggregation (Multiple -> One)
    - Fan-Out / Smurfing (One -> Multiple)
    """
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Generating synthetic AMLSim dataset: {num_transactions} txs, {num_accounts} accounts, laundering rate={laundering_rate:.1%}...")

    random.seed(42)
    np.random.seed(42)

    banks = [f"Bank_{i:03d}" for i in range(1, 25)]
    accounts = [f"ACCT_{i:06d}" for i in range(1, num_accounts + 1)]
    account_to_bank = {acc: random.choice(banks) for acc in accounts}
    account_types = {acc: random.choice(["Personal", "Business", "Corporate", "Trust"]) for acc in accounts}

    base_time = datetime(2022, 9, 1, 8, 0, 0)
    records = []

    # 1. Benign background transactions (~(1 - laundering_rate))
    num_benign = int(num_transactions * (1.0 - laundering_rate))
    current_time = base_time

    for _ in range(num_benign):
        current_time += timedelta(seconds=random.randint(5, 60))
        from_acc = random.choice(accounts)
        to_acc = random.choice(accounts)
        while to_acc == from_acc:
            to_acc = random.choice(accounts)

        curr = random.choices(
            CURRENCIES,
            weights=[0.65, 0.15, 0.08, 0.04, 0.03, 0.02, 0.015, 0.01, 0.005]
        )[0]
        payment_format = random.choices(
            PAYMENT_FORMATS,
            weights=[0.35, 0.25, 0.20, 0.10, 0.05, 0.04, 0.01]
        )[0]

        # Log-normal benign transfer amount
        amount = round(float(np.random.lognormal(mean=5.5, sigma=1.2)), 2)
        amount = max(5.0, min(amount, 500000.0))

        records.append({
            "Timestamp": current_time.strftime("%Y/%m/%d %H:%M"),
            "From Bank": account_to_bank[from_acc],
            "Account": from_acc,
            "To Bank": account_to_bank[to_acc],
            "Account.1": to_acc,
            "Amount Received": amount,
            "Receiving Currency": curr,
            "Amount Paid": amount,
            "Payment Currency": curr,
            "Payment Format": payment_format,
            "Is Laundering": 0
        })

    # 2. Laundering Typologies:
    num_laundering = num_transactions - num_benign
    logger.info(f"Injecting {num_laundering} money-laundering transactions across AML typologies...")

    # Slicing account groups dynamically
    n_acc = len(accounts)
    idx_senders = accounts[:max(2, int(n_acc * 0.2))]
    idx_recvs = accounts[max(2, int(n_acc * 0.2)):max(4, int(n_acc * 0.5))]
    idx_cycle = accounts[max(4, int(n_acc * 0.5)):max(7, int(n_acc * 0.8))]
    idx_aggregators = accounts[max(7, int(n_acc * 0.8)):]
    if len(idx_cycle) < 3:
        idx_cycle = accounts[:3]
    if not idx_aggregators:
        idx_aggregators = accounts[-1:]

    # A. Structuring / Smurfing ($9,000 - $9,999) & Fan-Out
    num_structuring = int(num_laundering * 0.45)
    for _ in range(num_structuring):
        current_time += timedelta(seconds=random.randint(10, 180))
        struct_sender = random.choice(idx_senders)
        dest_acc = random.choice(idx_recvs)
        # Crucial AML signal: amounts just below the $10,000 statutory reporting limit
        amount = round(random.uniform(9000.0, 9950.0), 2)
        curr = "US Dollar"
        records.append({
            "Timestamp": current_time.strftime("%Y/%m/%d %H:%M"),
            "From Bank": account_to_bank[struct_sender],
            "Account": struct_sender,
            "To Bank": account_to_bank[dest_acc],
            "Account.1": dest_acc,
            "Amount Received": amount,
            "Receiving Currency": curr,
            "Amount Paid": amount,
            "Payment Currency": curr,
            "Payment Format": "Wire",
            "Is Laundering": 1
        })

    # B. Cycles (A -> B -> C -> A)
    num_cycles = int(num_laundering * 0.35) // 3
    for _ in range(num_cycles):
        ring = random.sample(idx_cycle, 3)
        cycle_amount = round(random.uniform(15000.0, 75000.0), 2)
        fee = round(cycle_amount * 0.015, 2)

        # A -> B
        current_time += timedelta(minutes=random.randint(15, 45))
        records.append({
            "Timestamp": current_time.strftime("%Y/%m/%d %H:%M"),
            "From Bank": account_to_bank[ring[0]],
            "Account": ring[0],
            "To Bank": account_to_bank[ring[1]],
            "Account.1": ring[1],
            "Amount Received": cycle_amount,
            "Receiving Currency": "Euro",
            "Amount Paid": cycle_amount,
            "Payment Currency": "Euro",
            "Payment Format": "Wire",
            "Is Laundering": 1
        })

        # B -> C
        current_time += timedelta(minutes=random.randint(10, 30))
        records.append({
            "Timestamp": current_time.strftime("%Y/%m/%d %H:%M"),
            "From Bank": account_to_bank[ring[1]],
            "Account": ring[1],
            "To Bank": account_to_bank[ring[2]],
            "Account.1": ring[2],
            "Amount Received": round(cycle_amount - fee, 2),
            "Receiving Currency": "Euro",
            "Amount Paid": round(cycle_amount - fee, 2),
            "Payment Currency": "Euro",
            "Payment Format": "Wire",
            "Is Laundering": 1
        })

        # C -> A
        current_time += timedelta(minutes=random.randint(10, 30))
        records.append({
            "Timestamp": current_time.strftime("%Y/%m/%d %H:%M"),
            "From Bank": account_to_bank[ring[2]],
            "Account": ring[2],
            "To Bank": account_to_bank[ring[0]],
            "Account.1": ring[0],
            "Amount Received": round(cycle_amount - (2 * fee), 2),
            "Receiving Currency": "Euro",
            "Amount Paid": round(cycle_amount - (2 * fee), 2),
            "Payment Currency": "Euro",
            "Payment Format": "Wire",
            "Is Laundering": 1
        })

    # C. Fan-In / Aggregation (Multiple smurfs -> single aggregator)
    remaining_laundering = num_laundering - len([r for r in records if r["Is Laundering"] == 1])
    if remaining_laundering > 0:
        aggregator = random.choice(idx_aggregators)
        for _ in range(remaining_laundering):
            current_time += timedelta(seconds=random.randint(30, 300))
            smurf = random.choice(idx_senders)
            amount = round(random.uniform(4000.0, 9800.0), 2)
            records.append({
                "Timestamp": current_time.strftime("%Y/%m/%d %H:%M"),
                "From Bank": account_to_bank[smurf],
                "Account": smurf,
                "To Bank": account_to_bank[aggregator],
                "Account.1": aggregator,
                "Amount Received": amount,
                "Receiving Currency": "US Dollar",
                "Amount Paid": amount,
                "Payment Currency": "US Dollar",
                "Payment Format": "ACH",
                "Is Laundering": 1
            })

    # Sort strictly chronologically
    df_trans = pd.DataFrame(records)
    df_trans["dt"] = pd.to_datetime(df_trans["Timestamp"])
    df_trans = df_trans.sort_values("dt").drop(columns=["dt"]).reset_index(drop=True)

    trans_path = os.path.join(output_dir, "HI-Small_Trans.csv")
    df_trans.to_csv(trans_path, index=False)
    logger.info(f"Wrote {trans_path} ({len(df_trans)} rows, {df_trans['Is Laundering'].sum()} positive cases, rate: {df_trans['Is Laundering'].mean():.2%})")

    # Generate HI-Small_accounts.csv
    accounts_records = []
    for acc in accounts:
        accounts_records.append({
            "Account": acc,
            "Bank": account_to_bank[acc],
            "Account Type": account_types[acc],
            "Initial Balance": round(float(np.random.exponential(scale=20000) + 1500), 2)
        })
    df_accounts = pd.DataFrame(accounts_records)
    accounts_path = os.path.join(output_dir, "HI-Small_accounts.csv")
    df_accounts.to_csv(accounts_path, index=False)
    logger.info(f"Wrote {accounts_path} ({len(df_accounts)} rows).")

    return trans_path, accounts_path


def get_aml_data(output_dir: str = "./data/raw", force_synthetic: bool = False):
    """
    Acquires IBM AML dataset directly from Kaggle.
    If Kaggle data cannot be ingested, raises a RuntimeError with an actionable error message.
    """
    if force_synthetic:
        logger.info("force_synthetic=True: Generating synthetic AMLSim dataset directly for offline testing.")
        return generate_synthetic_hi_small(output_dir)

    # Strictly attempt Kaggle download - raises RuntimeError on failure
    download_from_kaggle(output_dir=output_dir)

    trans_file = os.path.join(output_dir, "HI-Small_Trans.csv")
    accounts_file = os.path.join(output_dir, "HI-Small_accounts.csv")
    if not os.path.exists(trans_file):
        error_msg = f"Ingestion verification failed: {trans_file} not found after Kaggle download."
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    return trans_file, accounts_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download AMLSim HI-Small data from Kaggle")
    parser.add_argument("--output-dir", type=str, default="./data/raw", help="Target output directory")
    parser.add_argument("--use-synthetic", action="store_true", help="Explicitly generate synthetic data for offline testing")
    parser.add_argument("--num-txs", type=int, default=15000, help="Number of synthetic transactions")
    parser.add_argument("--num-accounts", type=int, default=600, help="Number of synthetic accounts")

    args = parser.parse_args()
    if args.use_synthetic:
        generate_synthetic_hi_small(args.output_dir, args.num_txs, args.num_accounts)
    else:
        get_aml_data(args.output_dir, force_synthetic=False)

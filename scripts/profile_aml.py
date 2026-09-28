"""
profile_aml.py  --  Step 1 of the AML big-data project.

Profiles and transforms the IBM AML HI-Small dataset locally (pandas only, no
extra installs) so that every "to confirm" number in the approach guide can be
filled in, and writes a cleaned Parquet/CSV that mirrors what the AWS Glue job
will later produce.

Usage
-----
    python profile_aml.py --data-dir aml_data --out-dir aml_out [--sample 0]

Inputs expected in --data-dir (from kagglehub download, dataset
"ealtman2019/ibm-transactions-for-anti-money-laundering-aml"):
    HI-Small_Trans.csv      ~5.08M rows, 11 columns
    HI-Small_Patterns.txt   laundering attempts grouped by pattern type

Outputs written to --out-dir:
    profile_summary.json          headline numbers (row count, laundering rate, ...)
    profile_*.csv                 breakdown tables (by payment format, currency, hour, pattern, ...)
    hi_small_clean.parquet|.csv   cleaned + feature-engineered transactions
    account_features.csv          per-account aggregates (fan-in/out degree, volumes)
    laundering_patterns.csv       parsed Patterns file: one row per transaction with its pattern label

Everything here is deliberately plain pandas so it runs on any laptop. The
same logic is what we port to PySpark in the Glue job.
"""
import argparse
import json
import os
import re
import sys
import time

import numpy as np
import pandas as pd

RAW_COLS = [
    "Timestamp", "From Bank", "Account", "To Bank", "Account.1",
    "Amount Received", "Receiving Currency", "Amount Paid",
    "Payment Currency", "Payment Format", "Is Laundering",
]
CLEAN_COLS = {
    "Timestamp": "ts",
    "From Bank": "from_bank",
    "Account": "from_account",
    "To Bank": "to_bank",
    "Account.1": "to_account",
    "Amount Received": "amount_received",
    "Receiving Currency": "receiving_currency",
    "Amount Paid": "amount_paid",
    "Payment Currency": "payment_currency",
    "Payment Format": "payment_format",
    "Is Laundering": "is_laundering",
}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# --------------------------------------------------------------------------- #
# 1. Load
# --------------------------------------------------------------------------- #
def load_transactions(path, sample_rows=0):
    log(f"Reading {path}")
    dtypes = {
        "From Bank": "int64", "To Bank": "int64",
        "Account": "string", "Account.1": "string",
        "Amount Received": "float64", "Amount Paid": "float64",
        "Receiving Currency": "category", "Payment Currency": "category",
        "Payment Format": "category", "Is Laundering": "int8",
    }
    nrows = sample_rows if sample_rows and sample_rows > 0 else None
    df = pd.read_csv(path, dtype=dtypes, nrows=nrows)
    missing = [c for c in RAW_COLS if c not in df.columns]
    if missing:
        sys.exit(f"Unexpected schema, missing columns: {missing}")
    df = df.rename(columns=CLEAN_COLS)
    # int8 is fine for storage but overflows in 100 * sum(); widen for arithmetic
    df["is_laundering"] = df["is_laundering"].astype("int64")
    # Timestamps are 'YYYY/MM/DD HH:MM' strings
    df["ts"] = pd.to_datetime(df["ts"], format="%Y/%m/%d %H:%M", errors="coerce")
    log(f"Loaded {len(df):,} rows x {df.shape[1]} cols, "
        f"{df.memory_usage(deep=True).sum() / 1e6:,.0f} MB in memory")
    return df


# --------------------------------------------------------------------------- #
# 2. Parse the Patterns file
# --------------------------------------------------------------------------- #
def parse_patterns(path):
    """
    HI-Small_Patterns.txt looks like:

        BEGIN LAUNDERING ATTEMPT - FAN-OUT
        2022/09/01 00:20,0112,8000EBD30,01120,8000EBD30,...,1
        ...
        END LAUNDERING ATTEMPT

    Returns one row per transaction with the pattern label attached.
    """
    if not os.path.exists(path):
        log(f"Patterns file not found at {path}; skipping pattern labels")
        return None
    log(f"Parsing {path}")
    rows, attempt_id, pattern, pattern_detail = [], -1, None, None
    begin_re = re.compile(r"^BEGIN LAUNDERING ATTEMPT\s*-\s*(.+?)\s*$", re.I)
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            m = begin_re.match(line)
            if m:
                attempt_id += 1
                # e.g. "FAN-OUT:  Max 16-degree Fan-Out" -> type "FAN-OUT", detail kept separately
                raw = m.group(1)
                pattern = raw.split(":")[0].strip().upper()
                pattern_detail = raw.strip()
                continue
            if line.upper().startswith("END LAUNDERING ATTEMPT"):
                pattern = None
                continue
            if pattern is None:
                continue
            parts = line.split(",")
            if len(parts) < 11:
                continue
            rows.append({
                "attempt_id": attempt_id,
                "pattern": pattern,
                "pattern_detail": pattern_detail,
                "ts": parts[0],
                "from_bank": int(parts[1]),
                "from_account": parts[2],
                "to_bank": int(parts[3]),
                "to_account": parts[4],
                "amount_received": float(parts[5]),
                "receiving_currency": parts[6],
                "amount_paid": float(parts[7]),
                "payment_currency": parts[8],
                "payment_format": parts[9],
                "is_laundering": int(parts[10]),
            })
    pat = pd.DataFrame(rows)
    if pat.empty:
        log("Patterns file parsed but no rows found")
        return None
    pat["ts"] = pd.to_datetime(pat["ts"], format="%Y/%m/%d %H:%M", errors="coerce")
    log(f"Patterns: {attempt_id + 1:,} attempts, {len(pat):,} transactions, "
        f"{pat['pattern'].nunique()} pattern types")
    return pat


# --------------------------------------------------------------------------- #
# 3. Profile
# --------------------------------------------------------------------------- #
def profile(df, pat, out_dir):
    s = {}
    s["rows"] = int(len(df))
    s["laundering_rows"] = int(df["is_laundering"].sum())
    s["laundering_rate_pct"] = round(100 * s["laundering_rows"] / s["rows"], 4)
    s["one_in_n"] = round(s["rows"] / max(s["laundering_rows"], 1))
    s["ts_min"] = str(df["ts"].min())
    s["ts_max"] = str(df["ts"].max())
    s["days_covered"] = int((df["ts"].max() - df["ts"].min()).days) + 1
    s["null_timestamps"] = int(df["ts"].isna().sum())
    s["duplicate_rows"] = int(df.duplicated().sum())
    s["unique_from_banks"] = int(df["from_bank"].nunique())
    s["unique_to_banks"] = int(df["to_bank"].nunique())
    s["unique_banks_total"] = int(pd.concat([df["from_bank"], df["to_bank"]]).nunique())
    s["unique_from_accounts"] = int(df["from_account"].nunique())
    s["unique_to_accounts"] = int(df["to_account"].nunique())
    s["unique_accounts_total"] = int(pd.concat([df["from_account"], df["to_account"]]).nunique())
    s["currencies"] = sorted(df["payment_currency"].astype(str).unique().tolist())
    s["payment_formats"] = sorted(df["payment_format"].astype(str).unique().tolist())
    s["cross_currency_pct"] = round(100 * (df["payment_currency"].astype(str)
                                           != df["receiving_currency"].astype(str)).mean(), 3)
    s["cross_bank_pct"] = round(100 * (df["from_bank"] != df["to_bank"]).mean(), 3)
    s["self_transfer_pct"] = round(100 * (df["from_account"] == df["to_account"]).mean(), 3)
    s["amount_paid_zero_or_neg"] = int((df["amount_paid"] <= 0).sum())
    s["amount_paid_quantiles"] = {q: float(df["amount_paid"].quantile(q))
                                 for q in (0.01, 0.25, 0.5, 0.75, 0.99, 0.999)}
    if pat is not None:
        s["pattern_attempts"] = int(pat["attempt_id"].nunique())
        s["pattern_transactions"] = int(len(pat))
        s["pattern_types"] = pat["pattern"].value_counts().to_dict()
        # How many labelled laundering rows are covered by the patterns file?
        key = ["ts", "from_account", "to_account", "amount_paid"]
        merged = df[df["is_laundering"] == 1][key].merge(
            pat[key].drop_duplicates(), on=key, how="left", indicator=True)
        s["laundering_rows_with_pattern_pct"] = round(
            100 * (merged["_merge"] == "both").mean(), 2)

    with open(os.path.join(out_dir, "profile_summary.json"), "w") as fh:
        json.dump(s, fh, indent=2, default=str)
    log("Summary: " + json.dumps({k: s[k] for k in
        ("rows", "laundering_rows", "laundering_rate_pct", "one_in_n", "ts_min", "ts_max",
         "unique_banks_total", "unique_accounts_total", "cross_currency_pct", "cross_bank_pct")}))

    # Breakdown tables --------------------------------------------------------
    def rate_table(col):
        g = df.groupby(col, observed=True).agg(
            txns=("is_laundering", "size"),
            laundering=("is_laundering", "sum"),
            amount_paid_sum=("amount_paid", "sum"),
        )
        g["laundering_rate_pct"] = 100 * g["laundering"] / g["txns"]
        g["share_of_txns_pct"] = 100 * g["txns"] / g["txns"].sum()
        g["share_of_laundering_pct"] = 100 * g["laundering"] / max(g["laundering"].sum(), 1)
        g["lift"] = g["share_of_laundering_pct"] / g["share_of_txns_pct"].replace(0, np.nan)
        return g.sort_values("laundering_rate_pct", ascending=False)

    rate_table("payment_format").to_csv(os.path.join(out_dir, "profile_by_payment_format.csv"))
    rate_table("payment_currency").to_csv(os.path.join(out_dir, "profile_by_currency.csv"))

    df["_hour"] = df["ts"].dt.hour
    df["_date"] = df["ts"].dt.date
    df["_cross_currency"] = (df["payment_currency"].astype(str) != df["receiving_currency"].astype(str))
    df["_cross_bank"] = df["from_bank"] != df["to_bank"]
    rate_table("_hour").sort_index().to_csv(os.path.join(out_dir, "profile_by_hour.csv"))
    rate_table("_date").sort_index().to_csv(os.path.join(out_dir, "profile_by_date.csv"))
    rate_table("_cross_currency").to_csv(os.path.join(out_dir, "profile_by_cross_currency.csv"))
    rate_table("_cross_bank").to_csv(os.path.join(out_dir, "profile_by_cross_bank.csv"))

    # Currency corridor (payment -> receiving)
    corr = df.groupby(["payment_currency", "receiving_currency"], observed=True).agg(
        txns=("is_laundering", "size"), laundering=("is_laundering", "sum"),
        amount_paid_sum=("amount_paid", "sum"))
    corr["laundering_rate_pct"] = 100 * corr["laundering"] / corr["txns"]
    corr.sort_values("laundering", ascending=False).to_csv(
        os.path.join(out_dir, "profile_currency_corridors.csv"))

    # Amount bins
    bins = [0, 100, 1_000, 10_000, 50_000, 100_000, 1e6, 1e12]
    df["_amt_bin"] = pd.cut(df["amount_paid"], bins=bins)
    rate_table("_amt_bin").sort_index().to_csv(os.path.join(out_dir, "profile_by_amount_bin.csv"))

    if pat is not None:
        pp = pat.groupby("pattern").agg(
            attempts=("attempt_id", "nunique"), txns=("attempt_id", "size"),
            amount_paid_sum=("amount_paid", "sum"),
            median_txns_per_attempt=("attempt_id", lambda x: x.value_counts().median()),
        ).sort_values("txns", ascending=False)
        pp.to_csv(os.path.join(out_dir, "profile_by_pattern.csv"))
        pat.groupby(["pattern", "payment_format"]).size().unstack(fill_value=0).to_csv(
            os.path.join(out_dir, "profile_pattern_x_payment_format.csv"))

    df.drop(columns=[c for c in df.columns if c.startswith("_")], inplace=True)
    return s


# --------------------------------------------------------------------------- #
# 4. Transform (mirrors the future Glue job)
# --------------------------------------------------------------------------- #
def transform(df, pat):
    log("Cleaning")
    before = len(df)
    df = df.drop_duplicates()
    df = df[df["ts"].notna() & (df["amount_paid"] > 0)]
    log(f"Dropped {before - len(df):,} duplicate / invalid rows")

    # --- time features -------------------------------------------------------
    df["txn_date"] = df["ts"].dt.date
    df["hour_of_day"] = df["ts"].dt.hour.astype("int8")
    df["day_of_week"] = df["ts"].dt.dayofweek.astype("int8")
    df["is_weekend"] = (df["day_of_week"] >= 5).astype("int8")
    df["is_night"] = ((df["hour_of_day"] < 6) | (df["hour_of_day"] >= 22)).astype("int8")

    # --- amount features -----------------------------------------------------
    df["log_amount_paid"] = np.log1p(df["amount_paid"])
    df["is_round_amount"] = (np.isclose(df["amount_paid"] % 1000, 0) & (df["amount_paid"] >= 1000)).astype("int8")
    # Structuring signal: just under a common reporting threshold (USD 10,000 CTR)
    df["just_under_10k"] = ((df["amount_paid"] >= 9_000) & (df["amount_paid"] < 10_000)).astype("int8")
    df["fx_ratio"] = df["amount_received"] / df["amount_paid"]

    # --- relationship features ----------------------------------------------
    df["is_cross_currency"] = (df["payment_currency"].astype(str)
                               != df["receiving_currency"].astype(str)).astype("int8")
    df["is_cross_bank"] = (df["from_bank"] != df["to_bank"]).astype("int8")
    df["is_self_transfer"] = (df["from_account"] == df["to_account"]).astype("int8")

    # --- account-level aggregates (the PaySim sender_freq / receiver_freq idea,
    #     generalised into graph-degree features) -----------------------------
    log("Building account-level features")
    out_stats = df.groupby("from_account").agg(
        out_txn_count=("amount_paid", "size"),
        out_amount_sum=("amount_paid", "sum"),
        out_unique_counterparties=("to_account", "nunique"),
        out_unique_banks=("to_bank", "nunique"),
        out_formats=("payment_format", "nunique"),
    )
    in_stats = df.groupby("to_account").agg(
        in_txn_count=("amount_received", "size"),
        in_amount_sum=("amount_received", "sum"),
        in_unique_counterparties=("from_account", "nunique"),
        in_unique_banks=("from_bank", "nunique"),
    )
    acct = out_stats.join(in_stats, how="outer").fillna(0)
    acct["fan_out_degree"] = acct["out_unique_counterparties"]
    acct["fan_in_degree"] = acct["in_unique_counterparties"]
    acct["total_degree"] = acct["fan_out_degree"] + acct["fan_in_degree"]
    acct["pass_through_ratio"] = acct["out_amount_sum"] / (acct["in_amount_sum"] + 1)
    acct["is_laundering_any"] = 0
    launder_accts = set(df.loc[df["is_laundering"] == 1, "from_account"]) | \
                    set(df.loc[df["is_laundering"] == 1, "to_account"])
    acct.loc[acct.index.isin(launder_accts), "is_laundering_any"] = 1
    acct.index.name = "account"

    # Same-day velocity per sender (how many txns did this sender make that day)
    vel = df.groupby(["from_account", "txn_date"]).size().rename("sender_txns_same_day")
    df = df.join(vel, on=["from_account", "txn_date"])
    velr = df.groupby(["to_account", "txn_date"]).size().rename("receiver_txns_same_day")
    df = df.join(velr, on=["to_account", "txn_date"])

    # Attach sender / receiver degree features to each transaction
    df = df.join(acct[["fan_out_degree", "out_txn_count", "pass_through_ratio"]]
                 .rename(columns=lambda c: f"sender_{c}"), on="from_account")
    df = df.join(acct[["fan_in_degree", "in_txn_count"]]
                 .rename(columns=lambda c: f"receiver_{c}"), on="to_account")

    # --- pattern label (training-time only; never a model input) --------------
    if pat is not None:
        key = ["ts", "from_account", "to_account", "amount_paid"]
        lab = pat[key + ["pattern", "attempt_id"]].drop_duplicates(subset=key)
        df = df.merge(lab, on=key, how="left")
        df["pattern"] = df["pattern"].fillna("NONE")
    else:
        df["pattern"] = "NONE"
        df["attempt_id"] = np.nan

    return df, acct


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="aml_data")
    ap.add_argument("--out-dir", default="aml_out")
    ap.add_argument("--sample", type=int, default=0,
                    help="read only the first N rows (quick test); 0 = all")
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    df = load_transactions(os.path.join(args.data_dir, "HI-Small_Trans.csv"), args.sample)
    pat = parse_patterns(os.path.join(args.data_dir, "HI-Small_Patterns.txt"))
    profile(df, pat, args.out_dir)
    clean, acct = transform(df, pat)

    acct.to_csv(os.path.join(args.out_dir, "account_features.csv"))
    if pat is not None:
        pat.to_csv(os.path.join(args.out_dir, "laundering_patterns.csv"), index=False)
    try:
        clean.to_parquet(os.path.join(args.out_dir, "hi_small_clean.parquet"), index=False)
        log("Wrote hi_small_clean.parquet")
    except Exception as e:  # pyarrow missing
        log(f"Parquet write failed ({e}); writing CSV instead")
        clean.to_csv(os.path.join(args.out_dir, "hi_small_clean.csv"), index=False)
    log(f"Done. Clean rows: {len(clean):,}, columns: {clean.shape[1]}, "
        f"accounts: {len(acct):,}")


if __name__ == "__main__":
    main()

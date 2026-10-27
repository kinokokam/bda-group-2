"""
Features for the live score (model A without look-ahead), shared by training and the live monitor.

Same 22 inputs as Experiment A in notebooks/aml_xgboost.ipynb, with one change: the same-day
counts only count transfers up to the transfer's own minute ("so far today"), so a slice can be
scored with nothing but its own day's earlier slices. The notebook's whole-day counts see later
transfers that a live system could not have.

Account features (degrees, counts, banks, pass-through) come from a history window and are
mapped onto rows by account id; accounts with no history get NaN.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from monitoring.checks import KNOWN_CURRENCIES, KNOWN_FORMATS

FORMATS = sorted(KNOWN_FORMATS)          # fixed categories: training and live slices must agree
CURRENCIES = sorted(KNOWN_CURRENCIES)

SENDER_ACCT = ["sender_fan_out_degree", "sender_out_txn_count", "sender_unique_banks", "sender_pass_through_ratio"]
RECEIVER_ACCT = ["receiver_fan_in_degree", "receiver_in_txn_count", "receiver_unique_banks"]

LIVE_FEATURES = [
    "hour_of_day", "day_of_week", "is_weekend", "is_night",
    "amount", "log_amount", "is_round_amount", "just_under_10k",
    "is_cross_bank", "is_cross_currency", "is_self_transfer",
    "payment_format", "payment_currency",
    "sender_txns_so_far_today", "receiver_txns_so_far_today",
    *SENDER_ACCT, *RECEIVER_ACCT,
]


def transaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """Per-transfer features. df: curated rows with ts, ids, amount_paid, amount_paid_usd, currencies, format."""
    hour = df["ts"].dt.hour
    usd = df["amount_paid_usd"].astype("float32")
    return pd.DataFrame({
        "hour_of_day": hour.astype("int8"),
        "day_of_week": df["ts"].dt.dayofweek.astype("int8"),
        "is_weekend": (df["ts"].dt.dayofweek >= 5).astype("int8"),
        "is_night": ((hour >= 22) | (hour < 6)).astype("int8"),
        "amount": usd,
        "log_amount": np.log1p(usd).astype("float32"),
        "is_round_amount": (np.isclose(df["amount_paid"] % 1000, 0) & (df["amount_paid"] >= 1000)).astype("int8"),
        "just_under_10k": ((usd >= 9_000) & (usd < 10_000)).astype("int8"),
        "is_cross_bank": (df["from_bank"] != df["to_bank"]).astype("int8"),
        "is_cross_currency": (df["payment_currency"] != df["receiving_currency"]).astype("int8"),
        "is_self_transfer": (df["from_account"] == df["to_account"]).astype("int8"),
        "payment_format": pd.Categorical(df["payment_format"], categories=FORMATS),
        "payment_currency": pd.Categorical(df["payment_currency"], categories=CURRENCIES),
    }, index=df.index)


def so_far_today(df: pd.DataFrame) -> pd.DataFrame:
    """Transfers by the same sender (receiver) on the same calendar day at or before this transfer's minute.
    df must hold every earlier transfer of the day, not just the slice being scored."""
    day = df["ts"].dt.normalize()
    return pd.DataFrame({
        "sender_txns_so_far_today": df.groupby([df["from_account"], day])["ts"].rank(method="max"),
        "receiver_txns_so_far_today": df.groupby([df["to_account"], day])["ts"].rank(method="max"),
    }, index=df.index).astype("int32")


def fit_account_features(hist: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """One-hop sender and receiver tables, indexed by account id, from the history rows."""
    h = hist[["from_account", "to_account", "from_bank", "to_bank", "amount_paid_usd"]]
    snd = h.groupby("from_account").agg(sender_out_txn_count=("to_account", "size"), out_usd=("amount_paid_usd", "sum"))
    snd["sender_fan_out_degree"] = h.drop_duplicates(["from_account", "to_account"]).groupby("from_account").size()
    snd["sender_unique_banks"] = h.drop_duplicates(["from_account", "to_bank"]).groupby("from_account").size()
    rcv = h.groupby("to_account").agg(receiver_in_txn_count=("from_account", "size"), in_usd=("amount_paid_usd", "sum"))
    rcv["receiver_fan_in_degree"] = h.drop_duplicates(["to_account", "from_account"]).groupby("to_account").size()
    rcv["receiver_unique_banks"] = h.drop_duplicates(["to_account", "from_bank"]).groupby("to_account").size()
    snd["sender_pass_through_ratio"] = snd["out_usd"].astype("float64") / (
        rcv["in_usd"].astype("float64").reindex(snd.index).fillna(0) + 1)
    return snd[SENDER_ACCT].astype("float32"), rcv[RECEIVER_ACCT].astype("float32")


def account_features(df: pd.DataFrame, snd: pd.DataFrame, rcv: pd.DataFrame) -> pd.DataFrame:
    s = snd.reindex(df["from_account"].values)
    r = rcv.reindex(df["to_account"].values)
    return pd.concat([s.set_axis(df.index), r.set_axis(df.index)], axis=1)


def live_features(df: pd.DataFrame, snd: pd.DataFrame, rcv: pd.DataFrame) -> pd.DataFrame:
    """The model input, columns in LIVE_FEATURES order."""
    out = pd.concat([transaction_features(df), so_far_today(df), account_features(df, snd, rcv)], axis=1)
    return out[LIVE_FEATURES]

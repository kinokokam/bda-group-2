"""
Local-mode curation of a raw slice: the pandas twin of sql/02_curated_transactions.sql.

Drops exact duplicates, then inner-joins the currency lookup and the FX table on the transfer's
date, exactly like the Athena CTAS. A row whose date or currency has no FX rate is therefore
dropped, which is what the recon check is there to catch.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
FX_RATES = Path(os.environ.get("AML_FX_RATES", "~/aml_data/aml_out/fx_rates.csv")).expanduser()
CURRENCY_LOOKUP = REPO / "reference" / "currency_lookup.csv"


def load_fx(path: Path = FX_RATES) -> pd.DataFrame:
    """fx_rates.csv (rate_date 'YYYY/MM/DD', iso_code, units_per_usd), the table behind amount_paid_usd."""
    return pd.read_csv(path, usecols=["rate_date", "iso_code", "units_per_usd"])


def load_currency_lookup(path: Path = CURRENCY_LOOKUP) -> pd.DataFrame:
    return pd.read_csv(path, usecols=["currency_name", "iso_code"])


def curate_slice(raw: pd.DataFrame, fx: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    """Raw slice -> curated rows with amount_paid_usd. Rows without an FX rate are dropped (inner join)."""
    t = raw.drop_duplicates().copy()
    t["rate_date"] = t["ts"].dt.strftime("%Y/%m/%d")
    t = t.merge(lookup, left_on="payment_currency", right_on="currency_name", how="inner")
    t = t.merge(fx, on=["iso_code", "rate_date"], how="inner")
    t["amount_paid_usd"] = t["amount_paid"] / t["units_per_usd"]
    return (t.drop(columns=["rate_date", "currency_name", "iso_code", "units_per_usd"])
             .sort_values("ts", kind="stable").reset_index(drop=True))

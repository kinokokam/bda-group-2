"""
build_fx_from_data.py  --  FX rates implied by the HI-Small data itself.

The IBM simulator converts currencies at its own fixed rates (e.g. 105.4 Yen per USD),
not at real 2022 market rates (139.3 Yen per USD on 1 Sep 2022). Converting the data with
ECB rates would give the same transfer two different USD values (paid side vs received
side). So we recover the simulator's rates from the cross-currency rows that have USD on
one side, and use those for amount_paid_usd.

Output: one row per (calendar day, currency) for 1-18 Sep 2022, laid out exactly as the
aml_bq2.fx_rates table expects:
    rate_date (YYYY/MM/DD), iso_code, units_per_usd, source

Usage
-----
    python scripts/build_fx_from_data.py --trans ~/aml_data/HI-Small_Trans.csv \
        --out ~/aml_data/aml_out/fx_rates.csv
    aws s3 cp ~/aml_data/aml_out/fx_rates.csv s3://<bucket>/reference/fx/fx_rates.csv --profile aml-bq2

implied_rates() is importable so the notebook uses the very same rates in local mode.
"""
import argparse
import datetime as dt
import os
import sys

import numpy as np
import pandas as pd

USD = "US Dollar"
SOURCE = "dataset_implied"
MAX_CV = 0.10          # warn if a currency's implied rate is noisier than this
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_trans(path):
    cols = {"Amount Received": "amount_received", "Receiving Currency": "receiving_currency",
            "Amount Paid": "amount_paid", "Payment Currency": "payment_currency"}
    df = pd.read_csv(path, usecols=list(cols), engine="pyarrow")
    return df.rename(columns=cols)


def implied_rates(df):
    """Units of each currency per 1 USD, from rows with USD on exactly one side.

    df needs amount_paid, payment_currency, amount_received, receiving_currency.
    Returns a DataFrame indexed by currency name: units_per_usd, n_rows, cv.
    """
    pay, rec = df["payment_currency"].astype(str), df["receiving_currency"].astype(str)
    # X paid, USD received -> X per USD = paid / received; and the reverse direction
    a = df[(rec == USD) & (pay != USD)]
    b = df[(pay == USD) & (rec != USD)]
    obs = pd.concat([
        pd.DataFrame({"currency": a["payment_currency"].astype(str),
                      "rate": a["amount_paid"] / a["amount_received"]}),
        pd.DataFrame({"currency": b["receiving_currency"].astype(str),
                      "rate": b["amount_received"] / b["amount_paid"]}),
    ])
    obs = obs[np.isfinite(obs["rate"]) & (obs["rate"] > 0)]
    g = obs.groupby("currency")["rate"].agg(units_per_usd="median", n_rows="size", std="std")
    g["cv"] = (g["std"] / g["units_per_usd"]).fillna(0)
    g.loc[USD] = [1.0, 0, 0.0, 0.0]
    return g.drop(columns="std").sort_index()


def consistency(df, rates):
    """Median |paid_usd - received_usd| / received_usd over all cross-currency rows."""
    r = rates["units_per_usd"]
    x = df[df["payment_currency"].astype(str) != df["receiving_currency"].astype(str)]
    paid = x["amount_paid"] / x["payment_currency"].astype(str).map(r)
    recv = x["amount_received"] / x["receiving_currency"].astype(str).map(r)
    return float(((paid - recv).abs() / recv).median()), len(x)


def build_table(rates, lookup, start, end):
    iso = lookup.set_index("currency_name")["iso_code"]
    missing = sorted(set(iso.index) - set(rates.index))
    if missing:
        sys.exit(f"No USD-paired rows to derive a rate for: {missing}")
    days = pd.date_range(start, end, freq="D")
    rows = [(d.strftime("%Y/%m/%d"), iso[c], float(rates.at[c, "units_per_usd"]), SOURCE)
            for d in days for c in iso.index]
    return pd.DataFrame(rows, columns=["rate_date", "iso_code", "units_per_usd", "source"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trans", default=os.path.expanduser("~/aml_data/HI-Small_Trans.csv"))
    ap.add_argument("--lookup", default=os.path.join(REPO, "reference", "currency_lookup.csv"))
    ap.add_argument("--out", default=os.path.expanduser("~/aml_data/aml_out/fx_rates.csv"))
    ap.add_argument("--start", default="2022-09-01")
    ap.add_argument("--end", default="2022-09-18")
    a = ap.parse_args()

    df = load_trans(a.trans)
    rates = implied_rates(df)
    lookup = pd.read_csv(a.lookup)

    print(rates.assign(iso=rates.index.map(lookup.set_index("currency_name")["iso_code"]))
          .to_string(float_format=lambda v: f"{v:.6g}"))
    noisy = rates.index[rates["cv"] > MAX_CV].tolist()
    if noisy:
        print(f"WARNING: implied rate varies more than {MAX_CV:.0%} for {noisy}")
    med_gap, n = consistency(df, rates)
    print(f"Paid vs received USD gap on {n:,} cross-currency rows: median {med_gap:.4%}")

    table = build_table(rates, lookup, a.start, a.end)
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    table.to_csv(a.out, index=False, float_format="%.10g")
    print(f"Wrote {len(table)} rows ({table['rate_date'].nunique()} days x "
          f"{table['iso_code'].nunique()} currencies) to {a.out}")


if __name__ == "__main__":
    main()

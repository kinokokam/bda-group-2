"""
Lambda: fetch_fx  (pipeline step 3)

Builds reference/fx/fx_rates.csv with one row per (calendar date, currency):
    rate_date (YYYY/MM/DD), iso_code, units_per_usd

Source: Frankfurter API (ECB reference rates, no key), one time-series call.
The ECB publishes no weekend rates, so weekends are filled with the previous business day.
Without that fill, 3, 4 and 10 Sep drop out of the Athena join and 27% of laundering disappears.

Currencies the ECB does not publish:
  - SAR: pegged at 3.75 per USD, filled here.
  - RUB and BTC: put them in reference/fx_manual/fx_manual.csv (same three columns,
    one row per date) from the Bank of Russia and CoinGecko. For BTC, units_per_usd = 1 / BTC price in USD.

Runtime: Python 3.12, 128 MB, 30 s. Standard library + boto3 only.
"""
import csv
import datetime as dt
import io
import json
import os
import urllib.request

import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["AML_BUCKET"]
START = os.environ.get("FX_START", "2022-09-01")
END = os.environ.get("FX_END", "2022-09-18")
OUT_KEY = "reference/fx/fx_rates.csv"
MANUAL_KEY = "reference/fx_manual/fx_manual.csv"
ECB_CODES = ["EUR", "CNY", "JPY", "INR", "ILS", "CAD", "AUD", "CHF", "GBP", "BRL", "MXN"]


def fetch_ecb():
    # Start a few days early so a weekend at the very start still has a previous rate.
    start = (dt.date.fromisoformat(START) - dt.timedelta(days=5)).isoformat()
    url = (f"https://api.frankfurter.dev/v1/{start}..{END}"
           f"?base=USD&symbols={','.join(ECB_CODES)}")
    with urllib.request.urlopen(url, timeout=20) as r:
        return json.load(r)["rates"]            # {"2022-09-01": {"EUR": 0.9996, ...}, ...}


def calendar_days():
    d, end = dt.date.fromisoformat(START), dt.date.fromisoformat(END)
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


def lambda_handler(event, context):
    published = fetch_ecb()
    rows, last = [], None
    for day in calendar_days():
        iso = day.isoformat()
        # take today's rates, else the most recent published day before today
        for back in range(0, 7):
            k = (day - dt.timedelta(days=back)).isoformat()
            if k in published:
                last = published[k]
                break
        if last is None:
            raise RuntimeError(f"No ECB rate on or before {iso}")
        rd = day.strftime("%Y/%m/%d")
        rows.append([rd, "USD", 1.0])
        rows.append([rd, "SAR", 3.75])
        rows += [[rd, code, rate] for code, rate in last.items()]

    try:
        manual = s3.get_object(Bucket=BUCKET, Key=MANUAL_KEY)["Body"].read().decode()
        rows += [r for r in csv.reader(io.StringIO(manual))][1:]
    except s3.exceptions.NoSuchKey:
        pass    # RUB and BTC rows missing: the recon check will flag the dropped rows

    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["rate_date", "iso_code", "units_per_usd"])
    w.writerows(rows)
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=buf.getvalue().encode(), ContentType="text/csv")
    return {"rows": len(rows), "days": len(list(calendar_days())), "out": f"s3://{BUCKET}/{OUT_KEY}"}

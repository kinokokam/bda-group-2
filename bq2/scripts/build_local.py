"""
build_local.py - local stand-in for the team's curated_transactions table.

Put this file next to HI-Small_Trans.csv and HI-Small_Patterns.txt, then run:
    pip install duckdb
    python build_local.py

It creates aml_local.duckdb containing:
    patterns              one row per laundering transfer listed in Patterns.txt
    curated_transactions  same column names as the shared contract in the plan

If fx_rates.csv (from build_fx_from_data.py) is in this folder, amounts are
converted to USD and the bands use USD. Without it, amount_paid_usd is NULL and
the bands use the payer's own currency. Band labels and edges match sql/02_curated_transactions.sql exactly.
"""
import csv
import os
import re
import duckdb

TRANS_FILE = "HI-Small_Trans.csv"
PATTERNS_FILE = "HI-Small_Patterns.txt"
# Made by build_fx_from_data.py; used automatically when it sits in this folder.
FX_FILE = "fx_rates.csv" if os.path.exists("fx_rates.csv") else None
LOOKUP_FILE = "currency_lookup.csv"
DB_FILE = "aml_local.duckdb"


def parse_patterns(src, dst):
    """Turn BEGIN LAUNDERING ATTEMPT ... END blocks into one CSV row per transfer."""
    begin = re.compile(r"BEGIN LAUNDERING ATTEMPT\s*-\s*([A-Z\-]+)", re.I)
    attempt, pattern, rows = 0, None, []
    with open(src, newline="") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            m = begin.match(line)
            if m:
                attempt += 1
                pattern = m.group(1).upper()
                continue
            if line.upper().startswith("END LAUNDERING ATTEMPT"):
                pattern = None
                continue
            if pattern:
                p = next(csv.reader([line]))
                # ts, from_bank, from_account, to_bank, to_account, amt_recv, recv_cur, amt_paid, pay_cur, format, label
                rows.append([p[0], p[1], p[2], p[3], p[4], p[7], pattern, attempt])
    with open(dst, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "from_bank", "from_account", "to_bank", "to_account",
                    "amount_paid", "pattern", "attempt_id"])
        w.writerows(rows)
    print(f"patterns: {attempt} schemes, {len(rows)} transfers")


parse_patterns(PATTERNS_FILE, "patterns.csv")

con = duckdb.connect(DB_FILE)

# Explicit column names: the raw header repeats "Account", and bank IDs must stay text.
con.execute(f"""
CREATE OR REPLACE TABLE raw_trans AS
SELECT * FROM read_csv('{TRANS_FILE}', header = true, columns = {{
  'ts': 'VARCHAR', 'from_bank': 'VARCHAR', 'from_account': 'VARCHAR',
  'to_bank': 'VARCHAR', 'to_account': 'VARCHAR',
  'amount_received': 'DOUBLE', 'receiving_currency': 'VARCHAR',
  'amount_paid': 'DOUBLE', 'payment_currency': 'VARCHAR',
  'payment_format': 'VARCHAR', 'is_laundering': 'INTEGER'}})
""")

con.execute("""
CREATE OR REPLACE TABLE patterns AS
SELECT * FROM read_csv('patterns.csv', header = true, columns = {
  'ts': 'VARCHAR', 'from_bank': 'VARCHAR', 'from_account': 'VARCHAR',
  'to_bank': 'VARCHAR', 'to_account': 'VARCHAR', 'amount_paid': 'DOUBLE',
  'pattern': 'VARCHAR', 'attempt_id': 'INTEGER'})
""")

con.execute(f"CREATE OR REPLACE TABLE currency_lookup AS SELECT * FROM read_csv_auto('{LOOKUP_FILE}')")

if FX_FILE:
    con.execute(f"CREATE OR REPLACE TABLE fx_rates AS SELECT * FROM read_csv_auto('{FX_FILE}', all_varchar = true)")
    usd = "t.amount_paid / CAST(fx.units_per_usd AS DOUBLE)"
    fx_join = "LEFT JOIN fx_rates fx ON fx.iso_code = cl.iso_code AND fx.rate_date = substr(t.ts, 1, 10)"
else:
    usd = "CAST(NULL AS DOUBLE)"
    fx_join = ""

band_on = "amount_paid_usd" if FX_FILE else "amount_paid"

con.execute(f"""
CREATE OR REPLACE TABLE curated_transactions AS
WITH t AS (SELECT DISTINCT * FROM raw_trans),
     p AS (  -- one label per transfer even if a row appears in two schemes
       SELECT ts, from_account, to_account, amount_paid,
              min(pattern) AS pattern, min(attempt_id) AS attempt_id
       FROM patterns GROUP BY ALL),
     base AS (
       SELECT
         strptime(t.ts, '%Y/%m/%d %H:%M')                    AS ts,
         hour(strptime(t.ts, '%Y/%m/%d %H:%M'))              AS txn_hour,
         strftime(strptime(t.ts, '%Y/%m/%d %H:%M'), '%Y-%m-%d') AS txn_date,
         t.from_bank, t.from_account, t.to_bank, t.to_account,
         t.payment_format, t.payment_currency, t.receiving_currency,
         t.amount_paid,
         {usd}                                                AS amount_paid_usd,
         CASE WHEN t.from_bank <> t.to_bank THEN 1 ELSE 0 END AS is_cross_bank,
         CASE WHEN t.payment_currency <> t.receiving_currency THEN 1 ELSE 0 END AS is_cross_currency,
         t.is_laundering,
         COALESCE(p.pattern, CASE WHEN t.is_laundering = 1 THEN 'UNSTRUCTURED' ELSE 'NONE' END) AS pattern,
         p.attempt_id, cl.country, cl.basel_score
       FROM t
       LEFT JOIN currency_lookup cl ON cl.currency_name = t.payment_currency
       {fx_join}
       LEFT JOIN p ON p.ts = t.ts AND p.from_account = t.from_account
                  AND p.to_account = t.to_account AND p.amount_paid = t.amount_paid)
SELECT *,
  CASE WHEN {band_on} <     100 THEN '0-100'
       WHEN {band_on} <    1000 THEN '100-1k'
       WHEN {band_on} <   10000 THEN '1k-10k'
       WHEN {band_on} <   50000 THEN '10k-50k'
       WHEN {band_on} <  100000 THEN '50k-100k'
       WHEN {band_on} < 1000000 THEN '100k-1M'
       ELSE '1M+' END                                           AS amount_band_usd,
  CASE WHEN txn_date > '2022-09-10' THEN 1 ELSE 0 END         AS in_tail
FROM base
""")

# Sanity checks against the plan
print(con.sql("""
SELECT count(*) AS rows, sum(is_laundering) AS laundering,
       sum(CASE WHEN in_tail = 0 THEN is_laundering END) AS laundering_1_to_10_sep,
       count(*) FILTER (WHERE country IS NULL AND payment_currency <> 'Bitcoin') AS unmatched_currency_rows
FROM curated_transactions
"""))
print(con.sql("""
SELECT payment_format,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1)                   AS pct_txns,
       round(100.0 * sum(is_laundering) / sum(sum(is_laundering)) OVER (), 1) AS pct_laundering_all,
       round(100.0 * sum(is_laundering) FILTER (WHERE in_tail = 0)
             / sum(sum(is_laundering) FILTER (WHERE in_tail = 0)) OVER (), 1) AS pct_laundering_1_to_10
FROM curated_transactions GROUP BY payment_format ORDER BY pct_laundering_all DESC
"""))
print(f"USD conversion: {'ON (' + FX_FILE + ')' if FX_FILE else 'OFF - run build_fx_from_data.py first'}")
print(con.sql("""
SELECT count(*) FILTER (WHERE amount_paid_usd IS NULL) AS rows_without_usd,
       round(sum(amount_paid_usd) / 1e9, 2) AS total_usd_billions
FROM curated_transactions"""))
print(con.sql("SELECT pattern, count(*) AS txns FROM curated_transactions WHERE is_laundering = 1 GROUP BY 1 ORDER BY 2 DESC"))

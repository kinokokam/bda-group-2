-- Step 4a · Athena tables over the raw and reference files.
-- Replace aml-bq2-YOUR_TEAM_NAME with the real bucket name. Run each statement separately.
-- Hand-written DDL instead of a Glue crawler: same result, no crawler cost.

CREATE DATABASE IF NOT EXISTS aml_bq2;

-- Raw transactions. The CSV header has "Account" twice, so we name columns ourselves
-- and skip the header row. Bank IDs stay text because they carry leading zeros.
CREATE EXTERNAL TABLE IF NOT EXISTS aml_bq2.raw_trans (
  ts                 string,
  from_bank          string,
  from_account       string,
  to_bank            string,
  to_account         string,
  amount_received    double,
  receiving_currency string,
  amount_paid        double,
  payment_currency   string,
  payment_format     string,
  is_laundering      int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://aml-bq2-YOUR_TEAM_NAME/raw/trans/'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- One row per laundering transfer with its scheme, written by lambdas/parse_patterns.
CREATE EXTERNAL TABLE IF NOT EXISTS aml_bq2.patterns (
  attempt_id         int,
  pattern            string,
  pattern_detail     string,
  ts                 string,
  from_bank          string,
  from_account       string,
  to_bank            string,
  to_account         string,
  amount_received    double,
  receiving_currency string,
  amount_paid        double,
  payment_currency   string,
  payment_format     string,
  is_laundering      int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://aml-bq2-YOUR_TEAM_NAME/curated/patterns/'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- Daily FX, written by lambdas/fetch_fx. rate_date is 'YYYY/MM/DD' to match substr(ts, 1, 10).
-- units_per_usd: how many units of the currency buy 1 USD (USD = 1.0).
CREATE EXTERNAL TABLE IF NOT EXISTS aml_bq2.fx_rates (
  rate_date      string,
  iso_code       string,
  units_per_usd  double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://aml-bq2-YOUR_TEAM_NAME/reference/fx/'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- 15-row lookup from reference/currency_lookup.csv (upload it to reference/currency/).
CREATE EXTERNAL TABLE IF NOT EXISTS aml_bq2.currency_lookup (
  currency_name  string,
  iso_code       string,
  country        string,
  basel_score    double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://aml-bq2-YOUR_TEAM_NAME/reference/currency/'
TBLPROPERTIES ('skip.header.line.count' = '1');

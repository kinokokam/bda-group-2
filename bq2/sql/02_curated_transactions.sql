-- Step 4b · clean, convert to USD, attach scheme labels, write Snappy Parquet by date.
-- Drop the table (and empty curated/transactions/) before re-running.

CREATE TABLE aml_bq2.curated_transactions
WITH (
  format = 'PARQUET',
  write_compression = 'SNAPPY',
  external_location = 's3://aml-bq2-YOUR_TEAM_NAME/curated/transactions/',
  partitioned_by = ARRAY['txn_date']
) AS
WITH t AS (
  SELECT DISTINCT * FROM aml_bq2.raw_trans          -- removes the 9 exact duplicates
),
p AS (
  -- one label per transfer, in case the same transfer appears in two attempts
  SELECT ts, from_account, to_account, amount_paid,
         min(pattern) AS pattern, min(attempt_id) AS attempt_id
  FROM aml_bq2.patterns
  GROUP BY ts, from_account, to_account, amount_paid
)
SELECT
  date_parse(t.ts, '%Y/%m/%d %H:%i')                          AS ts,
  hour(date_parse(t.ts, '%Y/%m/%d %H:%i'))                    AS txn_hour,
  t.from_bank, t.from_account, t.to_bank, t.to_account,
  t.payment_format, t.payment_currency, t.receiving_currency,
  t.amount_paid,
  t.amount_paid / fx.units_per_usd                            AS amount_paid_usd,
  CASE
    WHEN t.amount_paid / fx.units_per_usd <   100 THEN '0-100'
    WHEN t.amount_paid / fx.units_per_usd <  1000 THEN '100-1k'
    WHEN t.amount_paid / fx.units_per_usd < 10000 THEN '1k-10k'
    WHEN t.amount_paid / fx.units_per_usd < 50000 THEN '10k-50k'
    WHEN t.amount_paid / fx.units_per_usd < 100000 THEN '50k-100k'
    WHEN t.amount_paid / fx.units_per_usd < 1000000 THEN '100k-1M'
    ELSE '1M+'
  END                                                         AS amount_band_usd,
  t.is_laundering,
  IF(t.from_bank <> t.to_bank, 1, 0)                          AS is_cross_bank,
  IF(t.payment_currency <> t.receiving_currency, 1, 0)        AS is_cross_currency,
  COALESCE(p.pattern, IF(t.is_laundering = 1, 'UNSTRUCTURED', 'NONE')) AS pattern,
  p.attempt_id,
  cl.country,
  cl.basel_score,
  IF(substr(t.ts, 1, 10) > '2022/09/10', 1, 0)                AS in_tail,
  date_format(date_parse(t.ts, '%Y/%m/%d %H:%i'), '%Y-%m-%d') AS txn_date   -- partition column last
FROM t
JOIN aml_bq2.currency_lookup cl ON cl.currency_name = t.payment_currency
JOIN aml_bq2.fx_rates fx        ON fx.iso_code = cl.iso_code
                               AND fx.rate_date = substr(t.ts, 1, 10)
LEFT JOIN p                     ON p.ts = t.ts
                               AND p.from_account = t.from_account
                               AND p.to_account = t.to_account
                               AND p.amount_paid = t.amount_paid;

-- Recon check straight after: both numbers must match (5,078,336 after de-duplication).
-- If the second is lower, the FX table is missing dates (weekends!) or currencies.
-- SELECT count(*) FROM (SELECT DISTINCT * FROM aml_bq2.raw_trans);
-- SELECT count(*) FROM aml_bq2.curated_transactions;

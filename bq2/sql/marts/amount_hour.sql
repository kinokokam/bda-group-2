-- Mart · laundering by USD amount band × hour of day (heat map, up to 7 bands × 24 hours).
-- lift = this cell's laundering rate ÷ the overall laundering rate.
-- band_order sorts the bands smallest first on the chart axis.

CREATE TABLE aml_bq2.mart_amount_hour
WITH (
  format = 'PARQUET',
  external_location = 's3://aml-bq2-YOUR_TEAM_NAME/marts/amount_hour/'
) AS
WITH cells AS (
  SELECT
    amount_band_usd,
    txn_hour,
    count(*)                                              AS txns,
    sum(is_laundering)                                    AS laundering_txns,
    coalesce(sum(amount_paid_usd) FILTER (WHERE is_laundering = 1), 0) AS laundering_usd
  FROM aml_bq2.curated_transactions
  WHERE txn_date <= '2022-09-10'
  GROUP BY amount_band_usd, txn_hour
)
SELECT
  amount_band_usd,
  CASE amount_band_usd
    WHEN '0-100'    THEN 1 WHEN '100-1k'  THEN 2 WHEN '1k-10k' THEN 3
    WHEN '10k-50k'  THEN 4 WHEN '50k-100k' THEN 5 WHEN '100k-1M' THEN 6
    ELSE 7 END                                            AS band_order,
  txn_hour,
  txns,
  laundering_txns,
  laundering_usd,
  100.0 * laundering_txns / txns                          AS laundering_rate_pct,
  (CAST(laundering_txns AS DOUBLE) / txns)
    / (CAST(sum(laundering_txns) OVER () AS DOUBLE) / sum(txns) OVER ()) AS lift
FROM cells;

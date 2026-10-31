-- Mart · laundering lift by payment currency, by count and USD, with each currency's country risk.
-- Joins currency_lookup directly (not the copy baked into curated_transactions), so filling in
-- basel_score only needs the lookup re-uploaded, not the curated table rebuilt.
-- Expect Saudi Riyal ≈ 4.1× the average laundering rate.

CREATE TABLE aml_bq2.mart_currency_risk
WITH (
  format = 'PARQUET',
  external_location = 's3://aml-bq2-YOUR_TEAM_NAME/marts/currency_risk/'
) AS
WITH c AS (
  SELECT
    payment_currency,
    count(*)                                              AS txns,
    sum(is_laundering)                                    AS laundering_txns,
    sum(amount_paid_usd)                                  AS total_usd,
    coalesce(sum(amount_paid_usd) FILTER (WHERE is_laundering = 1), 0) AS laundering_usd
  FROM aml_bq2.curated_transactions
  WHERE txn_date <= '2022-09-10'
  GROUP BY payment_currency
)
SELECT
  c.payment_currency,
  cl.iso_code,
  cl.country,
  cl.basel_score,
  c.txns,
  c.laundering_txns,
  c.total_usd,
  c.laundering_usd,
  100.0 * c.laundering_txns / c.txns                     AS laundering_rate_pct,
  (CAST(c.laundering_txns AS DOUBLE) / sum(c.laundering_txns) OVER ())
    / (CAST(c.txns AS DOUBLE) / sum(c.txns) OVER ())      AS lift_count,
  (c.laundering_usd / sum(c.laundering_usd) OVER ())
    / (c.total_usd / sum(c.total_usd) OVER ())            AS lift_usd
FROM c
LEFT JOIN aml_bq2.currency_lookup cl ON cl.currency_name = c.payment_currency;

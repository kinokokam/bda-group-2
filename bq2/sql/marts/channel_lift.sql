-- Mart · lift by payment format (share of laundering ÷ share of transfers), by count and USD.
-- Expect ACH ≈ 7.2× on count (1–10 Sep; 7.3× over all dates). laundering_usd is a raw sum and a
-- few giant transfers dominate it, so the chart should lead with counts and the median. Copy this file as the template for the other marts:
-- currency_risk, bank_concentration, corridors, amount_hour, typology_profile.

CREATE TABLE aml_bq2.mart_channel_lift
WITH (
  format = 'PARQUET',
  external_location = 's3://aml-bq2-YOUR_TEAM_NAME/marts/channel_lift/'
) AS
SELECT
  payment_format,
  count(*)                                              AS txns,
  sum(is_laundering)                                    AS laundering_txns,
  sum(amount_paid_usd)                                  AS total_usd,
  coalesce(sum(amount_paid_usd) FILTER (WHERE is_laundering = 1), 0) AS laundering_usd,
  approx_percentile(amount_paid_usd, 0.5) FILTER (WHERE is_laundering = 1) AS median_laundering_usd,
  (sum(is_laundering) * 1.0 / sum(sum(is_laundering)) OVER ())
    / (count(*) * 1.0 / sum(count(*)) OVER ())          AS lift_count
FROM aml_bq2.curated_transactions
WHERE txn_date <= '2022-09-10'
GROUP BY payment_format;

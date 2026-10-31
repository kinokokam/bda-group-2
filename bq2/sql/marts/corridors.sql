-- Mart · the top 20 bank-to-bank routes and the top 20 currency pairs by laundering USD.
-- corridor_type is 'bank_route' (from_bank → to_bank) or 'currency_pair' (paid → received).
-- Expect routes to be thin (top 10 routes ≈ 2.9% of laundering) and every laundering
-- currency pair to be same-currency (no cross-currency transfer is laundering).

CREATE TABLE aml_bq2.mart_corridors
WITH (
  format = 'PARQUET',
  external_location = 's3://aml-bq2-YOUR_TEAM_NAME/marts/corridors/'
) AS
WITH routes AS (
  SELECT
    'bank_route'                                          AS corridor_type,
    from_bank                                             AS from_key,
    to_bank                                               AS to_key,
    count(*)                                              AS txns,
    sum(is_laundering)                                    AS laundering_txns,
    coalesce(sum(amount_paid_usd) FILTER (WHERE is_laundering = 1), 0) AS laundering_usd
  FROM aml_bq2.curated_transactions
  WHERE txn_date <= '2022-09-10'
  GROUP BY from_bank, to_bank
),
pairs AS (
  SELECT
    'currency_pair'                                       AS corridor_type,
    payment_currency                                      AS from_key,
    receiving_currency                                    AS to_key,
    count(*)                                              AS txns,
    sum(is_laundering)                                    AS laundering_txns,
    coalesce(sum(amount_paid_usd) FILTER (WHERE is_laundering = 1), 0) AS laundering_usd
  FROM aml_bq2.curated_transactions
  WHERE txn_date <= '2022-09-10'
  GROUP BY payment_currency, receiving_currency
),
ranked AS (
  SELECT
    u.*,
    row_number() OVER (PARTITION BY corridor_type
                       ORDER BY laundering_usd DESC, laundering_txns DESC, from_key, to_key) AS corridor_rank,
    100.0 * laundering_txns / sum(laundering_txns) OVER (PARTITION BY corridor_type) AS pct_of_laundering_txns,
    100.0 * laundering_usd  / sum(laundering_usd)  OVER (PARTITION BY corridor_type) AS pct_of_laundering_usd,
    100.0 * laundering_txns / txns                        AS laundering_rate_pct
  FROM (SELECT * FROM routes UNION ALL SELECT * FROM pairs) u
)
SELECT * FROM ranked
WHERE corridor_rank <= 20 AND laundering_txns > 0;

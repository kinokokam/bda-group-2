-- Mart · how few sending banks hold most of the laundering (cumulative curve).
-- One row per sending bank that sent at least one laundering transfer, ranked by laundering count.
-- pct_of_banks is out of ALL sending banks, so the curve's x-axis is "% of banks".
-- Expect about 89 of 30,470 sending banks (0.3%) to reach 50% of laundering.

CREATE TABLE aml_bq2.mart_bank_concentration
WITH (
  format = 'PARQUET',
  external_location = 's3://aml-bq2-YOUR_TEAM_NAME/marts/bank_concentration/'
) AS
WITH b AS (
  SELECT
    from_bank,
    count(*)                                              AS txns,
    sum(is_laundering)                                    AS laundering_txns,
    coalesce(sum(amount_paid_usd) FILTER (WHERE is_laundering = 1), 0) AS laundering_usd
  FROM aml_bq2.curated_transactions
  WHERE txn_date <= '2022-09-10'
  GROUP BY from_bank
),
r AS (
  SELECT
    b.*,
    row_number() OVER (ORDER BY laundering_txns DESC, laundering_usd DESC, from_bank) AS bank_rank,
    count(*)             OVER ()                          AS sending_banks,
    sum(laundering_txns) OVER ()                          AS all_laundering
  FROM b
)
SELECT
  from_bank,
  bank_rank,
  txns,
  laundering_txns,
  laundering_usd,
  sending_banks,
  100.0 * bank_rank / sending_banks                       AS pct_of_banks,
  100.0 * sum(laundering_txns) OVER (ORDER BY bank_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        / all_laundering                                  AS cum_pct_laundering
FROM r
WHERE laundering_txns > 0;

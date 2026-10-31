-- Mart · one row per laundering scheme (the eight scheme cards).
-- A laundering attempt is one run of a scheme. Per attempt: hops (transfers), distinct accounts,
-- hours from first to last hop, USD moved. Then summarised per scheme.
-- Unstructured laundering has no attempts, so it is not a card here.
-- Note: the 1–10 Sep filter cuts off hops of attempts that run into the tail, as everywhere else.
-- Measured on 1–10 Sep (31 Oct): gather-scatter median ≈ 75 h across 5 accounts; bipartite ≈ 24 h;
-- stack has the most accounts (median 15.5). usd_per_hop is a mean and outlier-driven; use the median.

CREATE TABLE aml_bq2.mart_typology_profile
WITH (
  format = 'PARQUET',
  external_location = 's3://aml-bq2-YOUR_TEAM_NAME/marts/typology_profile/'
) AS
WITH s AS (
  SELECT *
  FROM aml_bq2.curated_transactions
  WHERE txn_date <= '2022-09-10'
    AND pattern NOT IN ('NONE', 'UNSTRUCTURED')
),
acct AS (
  SELECT attempt_id, count(DISTINCT account) AS accounts
  FROM (SELECT attempt_id, from_account AS account FROM s
        UNION ALL
        SELECT attempt_id, to_account FROM s) a
  GROUP BY attempt_id
),
att AS (
  SELECT
    s.attempt_id,
    min(s.pattern)                                        AS pattern,
    count(*)                                              AS hops,
    date_diff('minute', min(s.ts), max(s.ts)) / 60.0      AS hours_first_to_last,
    sum(s.amount_paid_usd)                                AS usd_moved,
    count(DISTINCT s.from_bank)                           AS sending_banks
  FROM s
  GROUP BY s.attempt_id
),
mix AS (
  SELECT
    pattern,
    count(*)                                              AS transfers,
    100.0 * count(*) FILTER (WHERE payment_format = 'ACH')         / count(*) AS pct_ach,
    100.0 * count(*) FILTER (WHERE payment_format = 'Cheque')      / count(*) AS pct_cheque,
    100.0 * count(*) FILTER (WHERE payment_format = 'Credit Card') / count(*) AS pct_credit_card,
    100.0 * count(*) FILTER (WHERE payment_format = 'Cash')        / count(*) AS pct_cash,
    100.0 * count(*) FILTER (WHERE payment_format = 'Bitcoin')     / count(*) AS pct_bitcoin,
    100.0 * sum(is_cross_bank) / count(*)                 AS pct_cross_bank,
    sum(amount_paid_usd) / count(*)                       AS usd_per_hop,
    approx_percentile(amount_paid_usd, 0.5)               AS median_usd_per_hop
  FROM s
  GROUP BY pattern
)
SELECT
  att.pattern,
  count(*)                                                AS attempts,
  mix.transfers,
  approx_percentile(att.hops, 0.5)                        AS median_hops,
  max(att.hops)                                           AS max_hops,
  approx_percentile(acct.accounts, 0.5)                   AS median_accounts,
  approx_percentile(att.hours_first_to_last, 0.5)         AS median_hours,
  max(att.hours_first_to_last)                            AS max_hours,
  approx_percentile(att.usd_moved, 0.5)                   AS median_usd_per_attempt,
  sum(att.usd_moved)                                      AS total_usd,
  mix.usd_per_hop,
  mix.median_usd_per_hop,
  approx_percentile(att.sending_banks, 0.5)               AS median_sending_banks,
  mix.pct_ach, mix.pct_cheque, mix.pct_credit_card, mix.pct_cash, mix.pct_bitcoin,
  mix.pct_cross_bank
FROM att
JOIN acct ON acct.attempt_id = att.attempt_id
JOIN mix  ON mix.pattern = att.pattern
GROUP BY att.pattern, mix.transfers, mix.usd_per_hop, mix.median_usd_per_hop, mix.pct_ach, mix.pct_cheque,
         mix.pct_credit_card, mix.pct_cash, mix.pct_bitcoin, mix.pct_cross_bank;

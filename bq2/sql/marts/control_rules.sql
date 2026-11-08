-- Mart · control impact: what each review rule would cost (transfers reviewed) and catch (laundering).
-- One row per rule and period. period 'all' is 1-10 Sep; 'held_out' is 8-10 Sep only, a check that the
-- rule still holds on days it was not chosen on. Top 95 banks use the same ranking as
-- mart_bank_concentration (laundering count, then laundering USD, over 1-10 Sep).
-- Expect ACH + USD 10k-50k on 'all': about 1.1% of transfers reviewed, 29% of laundering caught, 26.7x lift.
-- Analysis table for the deck and the AI chat; it is not one of the batch DAG's six marts.

CREATE TABLE aml_bq2.mart_control_rules
WITH (
  format = 'PARQUET',
  external_location = 's3://aml-bq2-YOUR_TEAM_NAME/marts/control_rules/'
) AS
WITH t AS (
  SELECT from_bank, payment_format, amount_band_usd, is_laundering, amount_paid_usd, txn_date
  FROM aml_bq2.curated_transactions
  WHERE txn_date <= '2022-09-10'
),
top_banks AS (
  SELECT from_bank
  FROM (
    SELECT from_bank,
           row_number() OVER (ORDER BY sum(is_laundering) DESC,
                                       coalesce(sum(amount_paid_usd) FILTER (WHERE is_laundering = 1), 0) DESC,
                                       from_bank) AS bank_rank
    FROM t
    GROUP BY from_bank
  )
  WHERE bank_rank <= 95
),
flags AS (
  SELECT
    CASE WHEN txn_date >= '2022-09-08' THEN 'held_out' ELSE 'other' END AS slice,
    is_laundering,
    payment_format = 'ACH'                                   AS is_ach,
    amount_band_usd = '10k-50k'                              AS is_10k_50k,
    from_bank IN (SELECT from_bank FROM top_banks)           AS is_top_bank
  FROM t
),
periods AS (
  SELECT 'all' AS period, f.* FROM flags f
  UNION ALL
  SELECT 'held_out' AS period, f.* FROM flags f WHERE slice = 'held_out'
),
rules AS (
  SELECT period, is_laundering, 1 AS rule_order, 'ACH only' AS rule_name FROM periods WHERE is_ach
  UNION ALL
  SELECT period, is_laundering, 2, 'Top 95 sending banks' FROM periods WHERE is_top_bank
  UNION ALL
  SELECT period, is_laundering, 3, 'USD 10k-50k, any channel' FROM periods WHERE is_10k_50k
  UNION ALL
  SELECT period, is_laundering, 4, 'ACH + USD 10k-50k' FROM periods WHERE is_ach AND is_10k_50k
  UNION ALL
  SELECT period, is_laundering, 5, 'ACH + USD 10k-50k + top 95 banks' FROM periods WHERE is_ach AND is_10k_50k AND is_top_bank
),
totals AS (
  SELECT period, count(*) AS all_txns, sum(is_laundering) AS all_laundering FROM periods GROUP BY period
)
SELECT
  r.rule_order,
  r.rule_name,
  r.period,
  count(*)                                                   AS txns_reviewed,
  100.0 * count(*) / tot.all_txns                            AS pct_txns_reviewed,
  sum(r.is_laundering)                                       AS laundering_caught,
  100.0 * sum(r.is_laundering) / tot.all_laundering          AS pct_laundering_caught,
  100.0 * sum(r.is_laundering) / count(*)                    AS hit_rate_pct,
  (1.0 * sum(r.is_laundering) / count(*)) / (1.0 * tot.all_laundering / tot.all_txns) AS lift
FROM rules r
JOIN totals tot ON tot.period = r.period
GROUP BY r.rule_order, r.rule_name, r.period, tot.all_txns, tot.all_laundering
ORDER BY r.period, r.rule_order;

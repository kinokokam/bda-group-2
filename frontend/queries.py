"""Every SQL statement the dashboard runs against `aml_bq1_catalog.scored`.

Columns are those of `GOLD_COLUMNS` in `bq1/flink/main.py`. Values a user can
influence are always `?` placeholders bound by `athena.query`; `{width}` in
the activity queries is filled only from a fixed choice in `data.py`.

The analyst queries never select `label`: a reviewer works without knowing the
outcome, so only the `EVALUATION_*` queries read it, for the model performance
page.

`backfill` is the run `pipeline.sh setup` scored from 8 Sep to 10 Sep 11:00.
It survives teardown, and ends where the demo window begins, so pairing it with
a demo run gives each account a continuous history.
"""

BACKFILL_RUN = "backfill"

# Priority tiers on the calibrated score, an estimated probability of
# laundering. Alerts start at the 0.075 threshold and cluster just above it,
# so fixed cut-offs at 0.10 and 0.20 split them into a short High list, a
# Medium band and the long Low tail.
HIGH_PRIORITY = 0.20
MEDIUM_PRIORITY = 0.10

# The rates the scoring job multiplied by, copied from `bq1/flink/features.py`
# (a test keeps the two equal). Any other currency got 1.0, so its
# `amount_paid_usd` is really the amount in its own currency: a 27.6 billion
# ruble transfer reads as 27.6 billion "USD". Only these sum as dollars.
PIPELINE_FX_RATES = {
    "USD": 1.0,
    "US DOLLAR": 1.0,
    "EUR": 1.08,
    "EURO": 1.08,
    "GBP": 1.28,
    "UK POUND": 1.28,
    "CHF": 1.12,
    "SWISS FRANC": 1.12,
    "CAD": 0.74,
    "CANADIAN DOLLAR": 0.74,
    "AUD": 0.66,
    "AUSTRALIAN DOLLAR": 0.66,
    "JPY": 0.0067,
}

USD_RATED = "upper(payment_currency) IN (" + ", ".join(f"'{name}'" for name in PIPELINE_FX_RATES) + ")"

PRIORITY_CASE = f"""CASE WHEN score >= {HIGH_PRIORITY} THEN 'High'
            WHEN score >= {MEDIUM_PRIORITY} THEN 'Medium'
            ELSE 'Low' END"""

RUNS = """
SELECT run_id,
       count(*) AS scored,
       count_if(is_alert) AS alerts,
       min(event_time) AS first_event,
       max(event_time) AS last_event
FROM scored
GROUP BY run_id
ORDER BY run_id DESC
"""

OVERVIEW = f"""
SELECT count(*) AS scored,
       count_if(is_alert) AS alerts,
       count_if(is_alert AND score >= {HIGH_PRIORITY}) AS high,
       count_if(is_alert AND score >= {MEDIUM_PRIORITY} AND score < {HIGH_PRIORITY}) AS medium,
       coalesce(sum(CASE WHEN is_alert AND {USD_RATED} THEN amount_paid_usd END), 0) AS alert_value_usd,
       count_if(is_alert AND NOT {USD_RATED}) AS alerts_unrated,
       count(DISTINCT CASE WHEN is_alert THEN from_account END) AS accounts_alerted,
       max(event_time) AS last_event
FROM scored
WHERE run_id = ?
"""

ACTIVITY = """
SELECT substr(event_time, 1, {width}) AS period,
       count(*) AS scored,
       count_if(is_alert) AS alerts
FROM scored
WHERE run_id = ?
GROUP BY 1
ORDER BY 1
"""

ALERTS_BY_FORMAT = """
SELECT payment_format,
       count(*) AS alerts
FROM scored
WHERE run_id = ? AND is_alert
GROUP BY payment_format
ORDER BY alerts DESC
"""

ALERT_QUEUE = """
SELECT txn_id, event_time, score,
       from_bank, from_account, to_bank, to_account,
       amount_paid_usd, amount_received_usd, payment_currency, payment_format,
       is_cross_bank, is_cross_currency, is_structuring_threshold,
       sender_tx_count_24h, sender_volume_24h, receiver_tx_count_24h,
       top_contributors
FROM scored
WHERE run_id = ? AND is_alert
ORDER BY score DESC, event_time
LIMIT 500
"""

ACCOUNT_HISTORY = f"""
SELECT run_id, event_time,
       CASE WHEN from_account = ? THEN 'Sent' ELSE 'Received' END AS direction,
       from_account, to_account, amount_paid_usd, payment_currency, payment_format,
       score, is_alert
FROM scored
WHERE run_id IN ('{BACKFILL_RUN}', ?)
  AND (from_account = ? OR to_account = ?)
ORDER BY event_time DESC
LIMIT 200
"""

WATCH_LIST = f"""
SELECT from_account,
       arbitrary(from_bank) AS from_bank,
       count_if(is_alert) AS alerts,
       count_if(is_alert AND run_id = ?) AS alerts_this_run,
       max(score) AS max_score,
       count(*) AS transfers,
       count(DISTINCT to_account) AS counterparties,
       max(event_time) AS last_seen
FROM scored
WHERE run_id IN ('{BACKFILL_RUN}', ?)
GROUP BY from_account
HAVING count_if(is_alert) >= 2
ORDER BY alerts DESC, max_score DESC
LIMIT 200
"""

EVALUATION_TOTALS = """
SELECT count(*) AS scored,
       count_if(is_alert) AS alerts,
       coalesce(sum(label), 0) AS laundering,
       count_if(is_alert AND label = 1) AS caught
FROM scored
WHERE run_id = ?
"""

EVALUATION_BY_PRIORITY = f"""
SELECT {PRIORITY_CASE} AS priority,
       count(*) AS alerts,
       count_if(label = 1) AS caught
FROM scored
WHERE run_id = ? AND is_alert
GROUP BY 1
"""

EVALUATION_ACTIVITY = """
SELECT substr(event_time, 1, {width}) AS period,
       count_if(is_alert) AS alerts,
       count_if(is_alert AND label = 1) AS caught,
       count_if(NOT is_alert AND label = 1) AS missed
FROM scored
WHERE run_id = ?
GROUP BY 1
ORDER BY 1
"""

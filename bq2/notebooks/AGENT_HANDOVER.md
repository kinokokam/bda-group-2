# Agent handover: `notebooks/aml_xgboost.ipynb`

**Owner:** Qiaoye · **Repo:** `aml-bq2-pipeline` · **Business question (BQ2):** where does money laundering concentrate, and how does each laundering scheme move money?

## 1. Your job in one paragraph

Build one Jupyter notebook that trains an XGBoost model on the IBM AML HI-Small data and runs it twice. Experiment A uses the current ~21 per-transaction and one-hop account features. Experiment B adds four new network features that follow money across more than one hop. Compare the two runs, mainly on **detection rate per laundering scheme**. That comparison is the BQ2 finding: it shows which schemes only become visible once you look at the network. Write the result tables to `out/model/` in the formats listed in section 7. Do not change anything outside `notebooks/`.

## 2. Facts you can rely on (measured on the real file)

| Item | Value |
|---|---|
| Rows | 5,078,345 raw; 5,078,336 after dropping 9 exact duplicates |
| Laundering | 5,177 rows (1 in 981) |
| Accounts | 515,080 |
| Date range | 1–10 Sep 2022, plus a thin 11–18 Sep tail that is 57–73% laundering. **Always drop the tail.** |
| Train window | 1–7 Sep: 3,731,669 rows, 3,027 laundering |
| Test window | 8–10 Sep: about 1.35 M rows, 1,495 laundering |
| Scheme labels | 8 typologies from `HI-Small_Patterns.txt` cover 62% of laundering rows. The rest are labelled `UNSTRUCTURED`. |
| Baseline to beat | PR-AUC 0.31, best minority-class F1 0.34, 62.7% of laundering caught in the top 0.5% of scores (6,728 alerts) |

The baseline came from scikit-learn `HistGradientBoostingClassifier`, not XGBoost. Re-measure it as Experiment A, and report it as "baseline" if it differs.

Detection at the top 0.5% by scheme (baseline):

| Scheme | Caught |
|---|---|
| unstructured | 35.6% |
| bipartite | 58.8% |
| stack | 67.7% |
| cycle | 69.6% |
| random | 80.4% |
| gather-scatter | 82.9% |
| fan-in | 87.9% |
| fan-out | 88.3% |
| scatter-gather | 94.1% |

**Hypothesis to test:** the one-hop features already catch shapes that are visible from a single account (fan-in, fan-out, scatter-gather). Multi-hop features should mainly lift **cycle, stack, bipartite and random**.

## 3. Inputs (support both)

1. **Local, use first:** `~/aml_data/aml_out/hi_small_clean.csv`, the output of `scripts/profile_aml.py`. Read `from_bank`, `to_bank`, `from_account` and `to_account` as **strings** (bank ids have leading zeros). `pattern` is empty for unpatterned rows: fill it with `UNSTRUCTURED` where `is_laundering == 1` and with `NONE` otherwise.
2. **AWS:** `aws s3 sync s3://<bucket>/curated/ ./curated/`, then `pd.read_parquet("curated/transactions")`. This table has `amount_paid_usd` and `pattern`, but **not** `amount_received` or the time and account features. So skip `fx_ratio`.

**Do not reuse the precomputed account columns** in `hi_small_clean.csv` (`sender_fan_out_degree`, `sender_pass_through_ratio`, `receiver_fan_in_degree` and so on). They were computed over all 10 days, so they leak test-period behaviour into training. Recompute every feature inside the notebook from these base columns only:

`ts, from_bank, from_account, to_bank, to_account, amount_paid (or amount_paid_usd), payment_format, payment_currency, receiving_currency, is_laundering, pattern, attempt_id`

Add `txn_id = row number after sorting by ts`. No id exists today.

## 4. Rules that must not be broken

- **Never use these as features:** `is_laundering`, `pattern`, `attempt_id`, `txn_date`, `in_tail`. Add an `assert` that checks the feature list.
- **Split by time, never at random.** For early stopping, train on 1–6 Sep and validate on 7 Sep. Touch 8–10 Sep only for the final evaluation.
- **No leakage across the split.**
  - Account-level features (degrees, pass-through, two-hop): fit on the train window (1–7 Sep) only, then map them onto both train and test rows. Accounts unseen in training get `NaN`, which XGBoost handles.
  - Time-local features (same-day counts, forwarding, returns): compute them **separately inside each window**, so no lookup crosses from 7 Sep into 8 Sep.
- **Same settings for A and B:** same split, hyper-parameters and seed (`random_state=42`). The only difference is the feature list.
- **No secrets** in the notebook. AWS access goes through the named profile `aml-bq2` only.
- **No data in git.** The CSV and Parquet files are git-ignored; keep it that way.
- **Clear outputs before committing:** `jupyter nbconvert --clear-output --inplace notebooks/aml_xgboost.ipynb`.
- **Do not `git push`.** Commit locally and leave pushing to Qiaoye.

## 5. Notebook sections (build in this order)

1. **Setup.** Imports (`pandas`, `numpy`, `pyarrow`, `xgboost`, `shap`, `scikit-learn`), constants (`DATA_PATH`, `OUT_DIR = "out/model"`, `SEED = 42`, `ALERT_SHARE = 0.005`) and a config cell that switches between the local and S3 inputs.
2. **Load and check.** Load, drop duplicates, drop the tail (`ts < 2022-09-11`), then assert the row and laundering counts from section 2. If they don't match, stop and report.
3. **Split.** Split into `train` (1–7 Sep) and `test` (8–10 Sep). Inside `train`, split again into `fit` (1–6 Sep) and `valid` (7 Sep).
4. **Base features (Experiment A, ~21 features).**
   - Time: `hour_of_day`, `day_of_week`, `is_weekend`, `is_night` (22:00–05:59).
   - Amount: `amount` (USD when available), `log_amount`, `is_round_amount` (a multiple of 1,000 and at least 1,000), `just_under_10k` (9,000–9,999).
   - Flags: `is_cross_bank`, `is_cross_currency`, `is_self_transfer`.
   - Categoricals: `payment_format`, `payment_currency` (use pandas `category` dtype with `enable_categorical=True`).
   - Same-day velocity (per window): `sender_txns_same_day`, `receiver_txns_same_day`.
   - One-hop account features (train-fitted): `sender_fan_out_degree`, `sender_out_txn_count`, `sender_unique_banks`, `sender_pass_through_ratio`, `receiver_fan_in_degree`, `receiver_in_txn_count`, `receiver_unique_banks`.
5. **Network features (Experiment B = A + these).** See section 6.
6. **Train.** One function, `train_eval(feature_list, name)`, returns the model and test scores. Settings: `XGBClassifier(n_estimators=2000, learning_rate=0.05, max_depth=6, min_child_weight=5, subsample=0.8, colsample_bytree=0.8, scale_pos_weight=neg/pos on fit, eval_metric="aucpr", tree_method="hist", early_stopping_rounds=100, random_state=42)`. Do not run a large tuning grid; at most 3–4 settings, chosen on `valid`.
7. **Evaluate both experiments on test.**
   - PR-AUC.
   - Best minority-class F1, with its precision and recall.
   - Recall at the top 0.5% and the top 1% of test rows. Use a count-based cut-off of `ceil(0.005 * len(test))` rows.
   - Detection rate per `pattern` at the top 0.5%.
8. **Explain (Experiment B, plus A for comparison).**
   - XGBoost gain importance.
   - SHAP `TreeExplainer` on a sample of the top 1,000 test alerts plus 5,000 random test rows, giving mean |SHAP| per feature.
9. **Exposure.** Sum the B test scores by `payment_format`, `payment_currency` and `from_bank`. Compare each group's share of total score with its share of transactions (the risk lift), and show the actual laundering count beside it.
10. **Monitoring hand-off.** Save `model.json` (`model.save_model`) and `score_threshold.json`, which holds the score at the top 0.5% cut-off on the **train** days. `monitoring/checks.py::check_anomaly` reads this threshold.
11. **Write outputs** (section 7). The last cell holds the upload command commented out: `# aws s3 cp out/model/ s3://<bucket>/marts/model/ --recursive --profile aml-bq2`.

## 6. The new network features (Experiment B)

| Feature | Definition | Scope | Schemes it targets |
|---|---|---|---|
| `fwd_within_24h` | 1 if the receiver sends any transfer in (ts, ts + 24 h] | per window | stack, random, cycle (relay chains) |
| `mins_to_forward` | minutes until the receiver's next outgoing transfer; NaN if none in the window | per window | same as above |
| `returns_to_sender_72h` | 1 if the receiver sends money back to the sender (B → A) within (ts, ts + 72 h] | per window | cycle, bipartite |
| `sender_two_hop_out` | sum of `fan_out_degree` over the sender's distinct out-neighbours, approximating 2-hop reach | train-fitted | gather-scatter, bipartite |
| `receiver_two_hop_in` | sum of `fan_in_degree` over the receiver's distinct in-neighbours | train-fitted | gather-scatter, fan-in chains |
| *(stretch)* `in_3cycle_72h` | 1 if A → B → C → A all happen within 72 h | per window | cycle |

Implementation hints. Use these; avoid graph libraries and row loops.

**Exclude self-transfers (A → A) from both lookups.** They are 15% of train rows and most of the 1 Sep midnight batch. If you keep them, `returns_to_sender_72h` matches the row against itself and comes out about 73% "true". Give self-transfer rows `NaN` for these features.

```python
# w = one window (train or test), sorted by ts, with txn_id
nw = w[w["from_account"] != w["to_account"]]            # no self-transfers

# fwd_within_24h / mins_to_forward: next outgoing transfer of the receiver
out = nw[["from_account", "ts"]].rename(columns={"from_account": "acct", "ts": "next_out_ts"}).sort_values("next_out_ts")
left = nw[["txn_id", "to_account", "ts"]].rename(columns={"to_account": "acct"}).sort_values("ts")
m = pd.merge_asof(left, out, left_on="ts", right_on="next_out_ts", by="acct", direction="forward")
m["mins_to_forward"] = (m["next_out_ts"] - m["ts"]).dt.total_seconds() / 60
m["fwd_within_24h"] = (m["mins_to_forward"] <= 24 * 60).astype("int8")

# returns_to_sender_72h: the reversed pair B -> A after ts
back = nw[["to_account", "from_account", "ts"]].rename(
    columns={"to_account": "from_account", "from_account": "to_account", "ts": "back_ts"}).sort_values("back_ts")
r = pd.merge_asof(nw[["txn_id", "from_account", "to_account", "ts"]].sort_values("ts"), back,
                  left_on="ts", right_on="back_ts", by=["from_account", "to_account"], direction="forward")
r["returns_to_sender_72h"] = ((r["back_ts"] - r["ts"]) <= pd.Timedelta("72h")).astype("int8")
# merge m and r back onto w by txn_id; self-transfer rows stay NaN

# sender_two_hop_out: groupby over unique edges in the train window
edges = train[["from_account", "to_account"]].drop_duplicates()
fan_out = edges.groupby("from_account").size()
edges["nbr_fan_out"] = edges["to_account"].map(fan_out).fillna(0)
two_hop_out = edges.groupby("from_account")["nbr_fan_out"].sum()
```

**Early signal (measured 12 Oct on the train window, 1–7 Sep, self-transfers excluded).** This is the share of rows where each feature is 1:

| Rows | `fwd_within_24h` | `returns_to_sender_72h` |
|---|---|---|
| Normal | 35.9% | 0.05% |
| Laundering, all | 35.7% | 4.9% |
| Cycle | 70.9% | 14.6% |
| Random | 79.8% | 6.4% |
| Stack | 52.9% | 5.1% |
| Gather-scatter | 40.4% | 9.8% |
| Bipartite | 36.4% | 4.0% |
| Unstructured | 24.6% | 2.3% |

`returns_to_sender_72h` is about 100× more common in laundering than in normal traffic. `fwd_within_24h` doesn't separate laundering overall, but it does separate cycle, random and stack, which the trees can pick up by combining features. Neither proves the model will improve; Experiment B is the test.

**Stretch: `in_3cycle_72h`.** Build it only after A and B are done, and time-box it to about an hour. Leave out hub accounts with more than 1,000 transfers in the window; otherwise the three-way join explodes. Give hub rows `NaN`.

**Caveat to state in the results.** `fwd_within_24h`, `mins_to_forward`, `returns_to_sender_72h` and `in_3cycle_72h` look forward in time. A score that uses them is available only 24–72 hours after the transfer. That's acceptable for BQ2, which explains past behaviour; for real-time alerting it would be a delayed score. Write this in a markdown cell next to the results.

## 7. Outputs (`out/model/`, CSV with a header row)

| File | Columns |
|---|---|
| `metrics.csv` | `experiment, pr_auc, best_f1, precision_at_best_f1, recall_at_best_f1, recall_top_0_5pct, recall_top_1pct, n_alerts_0_5pct, best_iteration` |
| `detection_by_scheme.csv` | `experiment, pattern, laundering_rows, caught_top_0_5pct, detection_rate_pct` |
| `feature_importance.csv` | `experiment, feature, gain, rank` |
| `shap_summary.csv` | `experiment, feature, mean_abs_shap, rank` |
| `exposure_by_channel.csv`, `exposure_by_currency.csv`, `exposure_by_bank.csv` | `key, txns, share_of_txns_pct, sum_score, share_of_score_pct, risk_lift, actual_laundering` (bank file: top 100 banks by `sum_score`) |
| `model.json` | Experiment B model |
| `score_threshold.json` | `{"alert_share": 0.005, "threshold": <float>, "computed_on": "train 1-7 Sep"}` |

## 8. Done means

- [ ] Restart & Run All finishes without errors in under about 20 minutes and under about 8 GB of RAM on a laptop. Use `float32` and `category` dtypes.
- [ ] The row and laundering counts in section 2 are asserted and pass.
- [ ] The feature-list assert proves no label, pattern or date column is an input.
- [ ] Experiment A is within about ±0.05 of the baseline (PR-AUC 0.31, F1 0.34). If it isn't, explain why in a markdown cell.
- [ ] `metrics.csv` and `detection_by_scheme.csv` hold rows for both A and B.
- [ ] A markdown cell answers in 3–5 plain sentences: which schemes improved with network features, by how many points, and which are still hard to catch.
- [ ] All files from section 7 exist in `out/model/`.
- [ ] Outputs are cleared, the commit is local only, and there is no push.

## 9. Out of scope

SageMaker or any paid AWS compute; graph neural networks or `networkx` over the full graph; editing `sql/`, `lambdas/`, `monitoring/`, `agent/` or `airflow/`; changing column names in `curated_transactions`; dashboards.

## 10. Report back to Qiaoye with

1. The A vs B metrics table.
2. The A vs B detection rate per scheme, with the change in points.
3. The top 10 features of B by mean |SHAP|.
4. Anything you measured that contradicts a number in this file.

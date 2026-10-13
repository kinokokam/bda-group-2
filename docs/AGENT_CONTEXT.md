# Agent context: Laundering Exposure Plan (AML BQ2)

This file condenses `Laundering_Exposure_Plan.html`, the team's plan page with four tabs: Overview, End-to-end pipeline, Live monitoring + AI and Work split. Treat it as the source of truth for the project. Sections 9 and 10 list decisions made **after** the HTML was written. Where they disagree with the HTML, they win.

You are working for **Qiaoye** (SMU Big Data group project, team of three). Qiaoye owns the XGBoost notebook, the Airflow DAG, the live monitoring checks and the Claude triage agent.

---

## 1. The project in five lines

- **Business question (BQ2):** where does money laundering concentrate, and how does each laundering scheme move money?
- **Data:** IBM AML synthetic transactions, **HI-Small only**, from Kaggle (`ealtman2019/ibm-transactions-for-anti-money-laundering-aml`). It is fully synthetic, so it contains no personal data.
- **Deliverable:** an AWS big-data pipeline from ingestion to visualisation, plus slides. Markers grade business value, a working technical solution and an "X-factor".
- **Budget:** US$250 on a shared course AWS account. The plan's estimate is about US$3.
- **Banned by the brief:** Redshift, RDS, QuickSight and anything from AWS Marketplace. Avoid expensive services in general.

## 2. Problem statement (Overview tab, a)

1. Banks process millions of transfers and only about 1 in 1,000 is laundering, so checking everything equally wastes effort.
2. Limits are set the same for every channel, currency and partner bank, without evidence of where the risk sits.
3. Launderers reuse a few schemes (fan-out, cycle, stack and others) spread across many accounts and banks. You can't see them one transfer at a time.
4. Amounts arrive in 15 currencies, so nobody can compare money at risk until everything is in one currency.
5. The goal is a cheap, repeatable pipeline that turns 5 M raw transfers into a map of where laundering concentrates and which schemes are hardest to catch.

**End goal (three questions the output answers):**

- Which channels, currencies, banks and amount bands carry more than their fair share of laundering, measured in USD?
- How does each of the eight schemes move money (hops, accounts, hours, USD per hop)?
- Which schemes are hardest to catch?

**Who benefits:**

| Who | Their problem today | What our output gives them |
|---|---|---|
| Bank risk-policy team (sets limits per channel) | Same checks everywhere; no data on where risk sits | A ranked list of channels and amount bands. Tightening ACH covers 87% of laundering while touching 12% of transfers. |
| Data analysts (Transactions), who review alerts | Tools catch some schemes well and others barely | Detection rate per scheme, so they can add targeted rules for bipartite, stack and cycle |
| Correspondent-banking team (vets partner banks) | Due diligence is spread over thousands of banks | The short list of 89 sending banks behind half of all laundering |
| Regulators and industry groups | Typology guidance is written from case stories | Measured profiles of each scheme in USD and hours |

## 3. Measured facts (HI-Small, from `profile_aml.py` and a first model)

Quote these exactly, and don't invent new numbers. If you measure something different, say so.

| Fact | Value |
|---|---|
| Transfers | 5,078,345 raw (1–10 Sep 2022 plus a tail); 5,078,336 after dropping 9 exact duplicates |
| Laundering | 5,177 rows, 1 in 981 (0.10%) |
| Accounts / sending banks | 515,080 / 30,470 |
| Channel | ACH is 11.8% of transfers but 86.6% of laundering (about 7× its share). Wire and Reinvestment have none. |
| Banks | 89 of 30,470 sending banks (0.3%) hold 50% of laundering. The top 10 bank-to-bank routes hold only 2.9%. |
| Currency | 98% of laundering crosses banks, but none of the 72,170 cross-currency transfers is laundering. Saudi Riyal launders at 4.1× the average rate. |
| Amounts | 72% of laundering is 1,000–50,000 in the payer's currency; the 10k–50k band carries 2.5× its share. |
| Schemes | Patterns file: 370 attempts, 3,209 transfers, 8 typologies, covering 62% of laundering rows. The rest are labelled `UNSTRUCTURED`. Gather-scatter runs about 6 days across 15 accounts; bipartite finishes within a day. |
| Tail | 11–18 Sep is 1,108 rows, 57–73% laundering, all ACH (simulator leftovers). **Always exclude it** (`txn_date <= '2022-09-10'`). |
| Weekends | 3, 4 and 10 Sep hold 12% of transfers but 27% of laundering (1,240 of 4,522). The ECB publishes no weekend FX, so a plain FX join drops 623,137 rows. |
| Month start | On 1 Sep, 43% of transfers are Reinvestment and 344,208 land at midnight. Format PSI is 3.9 against a normal weekday. This is expected drift. |
| Baseline model (superseded, leaky) | Train 1–7 Sep (3,731,669 rows, 3,027 laundering), test 8–10 Sep (1,345,559 rows, 1,495 laundering). PR-AUC 0.31, F1 0.34. The top 0.5% of scores (6,728 alerts) catches 62.7% of laundering. **These numbers used account features computed over all 10 days, which see the test period** (reproduced on 13 Oct: PR-AUC 0.36, 64.3%). Don't quote them as achievable; use the Experiment A/B rows below. |
| Detection by scheme, leaky baseline (top 0.5%) | unstructured 35.6, bipartite 58.8, stack 67.7, cycle 69.6, random 80.4, gather-scatter 82.9, fan-in 87.9, fan-out 88.3, scatter-gather 94.1 (%) |
| XGBoost Experiment A vs B (13 Oct, honest features) | Test 8–10 Sep. **A** (22 one-hop features): PR-AUC 0.170, best F1 0.256, 45.4% caught in the top 0.5%. **B** (A + 5 network features): PR-AUC 0.236, best F1 0.301, 47.9%. Source: `notebooks/aml_xgboost.ipynb`, `out/model/metrics.csv`. |
| Detection by scheme, A → B (top 0.5%) | bipartite 51.5 → 60.3 (+8.8), gather-scatter 57.5 → 65.7 (+8.3), stack 58.4 → 66.5 (+8.1), cycle 46.7 → 54.3 (+7.6), random 57.1 → 62.5 (+5.4), fan-out 66.0 → 69.1 (+3.2), scatter-gather 60.0 → 61.8 (+1.8), unstructured 24.1 → 22.3 (−1.7), fan-in 73.6 → 70.3 (−3.3) (%). Each scheme has 56–181 test rows, so changes under about 3 points are noise. |
| Model exposure (B scores, test) | ACH is 13.2% of test transfers but 90.6% of the summed risk score, and holds 1,299 of 1,495 test laundering rows. |
| FX rates | The IBM simulator converts at its own **fixed** rates (e.g. 105.4 Yen, 0.8534 Euro, 77.804 Ruble, 0.0000842 Bitcoin per USD), not market rates (ECB on 1 Sep 2022: 139.28 Yen, 0.9996 Euro). With these rates, paid and received USD agree on the 72,170 cross-currency rows (median gap 0.0002%). |

## 4. The batch pipeline (End-to-end tab)

Seven steps, one S3 bucket, everything pay-per-use and nothing to switch off.

| # | Step | Tool | Reads → writes | Cost |
|---|---|---|---|---|
| 1 | Ingest | Laptop + `kagglehub` + AWS CLI | Kaggle → `raw/trans/`, `raw/patterns/` | $0 |
| 2 | Parse schemes | Lambda `parse_patterns` | `raw/patterns/` → `curated/patterns/` (one row per transfer with `attempt_id`, typology) | free tier |
| 3 | FX rates | `scripts/build_fx_from_data.py` (the simulator's own fixed rates, recovered from the data; see §10.7). Lambda `fetch_fx` (Frankfurter/ECB) stays as the real-market alternative and writes to `reference/fx_ecb/`. | → `reference/fx/fx_rates.csv` (all 15 currencies × 18 days). `currency_lookup.csv` → `reference/currency/` | $0 |
| 4 | Clean and enrich | Athena DDL + CTAS | `raw/` + `reference/` + `curated/patterns/` → `curated/transactions/` (Snappy Parquet partitioned by `txn_date`). A second CTAS builds `curated/accounts/`. | ≈ $0.01 |
| 5 | Analyse | Athena CTAS | `curated/` → `marts/` (the seven tables in section 6) | cents |
| 6 | Model drivers | Jupyter + XGBoost on a laptop | `aws s3 sync` of `curated/` → notebook → `marts/model/` | $0 |
| 7 | Publish | Lambda `publish` + S3 static site | `marts/` → `site/data/*.json`; `site/index.html` draws Plotly.js charts | ≈ $0 |

**Orchestration:** Apache Airflow in Docker on a laptop. MWAA was rejected at about $0.49/hr, roughly $350 a month.

**Dropped on purpose, and why:**

- **Glue ETL:** $0.44 per DPU-hour; Athena CTAS does the same for about a cent.
- **Glue crawler:** replaced by hand-written DDL.
- **EMR:** idle cost.
- **Managed Grafana:** needs IAM Identity Center and costs $9 per editor.
- **QuickSight, Redshift and RDS:** banned by the brief.
- **Fallback if public S3 websites are blocked:** `aws s3 sync s3://…/site ./site` and open `index.html` locally.

**Cost table:**

| Item | Cost |
|---|---|
| S3 | < $0.20 |
| Lambda | $0 |
| Athena | < $1 |
| XGBoost and Airflow | $0 |
| Claude agent | about $1–2 (about 240 calls for a full replay) |
| **Total** | **≈ US$3** |

Set an AWS Budgets alarm at $20 on day one.

### S3 layout (`s3://aml-bq2/`, region `ap-southeast-1`)

```
raw/trans/HI-Small_Trans.csv
raw/patterns/HI_Small_Patterns.txt      # underscore in S3; CRLF line endings
reference/fx/fx_rates.csv               # dataset-implied rates (rate_date, iso_code, units_per_usd, source)
reference/fx_backup/fx_rates_ecb.csv    # Owen's original ECB file, kept for reference
reference/currency/currency_lookup.csv
curated/patterns/patterns.csv
curated/transactions/txn_date=2022-09-01/…parquet
curated/accounts/…parquet
marts/{channel_lift,currency_risk,bank_concentration,corridors,amount_hour,typology_profile}/
marts/model/…
site/index.html   site/data/*.json
athena-results/   # lifecycle: expire after 7 days
```

Each Athena table points at its own folder.

## 5. Shared contract: `curated_transactions` columns

**Do not rename these without telling the team.**

| Columns | Meaning |
|---|---|
| `ts, txn_hour, txn_date` | Time. `txn_date` is the partition, as `'YYYY-MM-DD'`. |
| `from_bank, from_account, to_bank, to_account` | **Text**: bank ids have leading zeros |
| `payment_format, payment_currency, receiving_currency` | Channel and currencies |
| `amount_paid, amount_paid_usd` | Original amount and USD amount |
| `is_cross_bank, is_cross_currency, amount_band_usd` | Flags for the lift tables |
| `is_laundering, pattern, attempt_id` | Label. `pattern` is a typology, `UNSTRUCTURED` or `NONE`. |
| `country, basel_score, in_tail` | Risk join; `in_tail` = after 10 Sep |

**Gotchas:**

- `fx_rates.rate_date` is stored as `'YYYY/MM/DD'` text to match the raw timestamp prefix.
- The curated table has **no** `amount_received`.
- The pattern join key is `(ts, from_account, to_account, amount_paid)`.
- `fx_rates` and `patterns` CSV column order must match the DDL in `sql/01_tables.sql` exactly. Athena maps by position, and a mismatch gives empty or shifted joins without any error.

## 6. The seven result tables (marts)

| Mart | Question | Chart |
|---|---|---|
| `channel_lift` | Which payment formats carry more than their share of laundering, by count and USD? | Paired bars |
| `currency_risk` | Which currencies over-carry laundering, and does that match the Basel score? | Lift bars with the Basel score |
| `bank_concentration` | How few banks hold most of the laundering? | Cumulative curve |
| `corridors` | Which bank routes and currency pairs carry the most laundering USD? | Top-20 table |
| `amount_hour` | Which USD amount bands and hours are riskiest? | Heat map |
| `typology_profile` | Per scheme: count, hops, accounts, hours first-to-last, USD per hop, channel mix | Eight cards |
| `model/*` | Drivers, missed schemes, risk-weighted exposure | Driver bars, detection bars, exposure by channel |

`lift = (laundering share) ÷ (transfer share)`. Every mart filters `txn_date <= '2022-09-10'`.

## 7. Step 6: the XGBoost notebook (`notebooks/aml_xgboost.ipynb`)

- **Inputs:** `curated_transactions` (plus account features), or the local `profile_aml.py` output while AWS is being built.
- **Features:** about 21:
  - payment format, currency, USD amount and its log;
  - hour, weekday, night and weekend flags;
  - round-amount and just-under-10k flags;
  - cross-bank, cross-currency and self-transfer flags;
  - sender fan-out degree, receiver fan-in degree, same-day sender and receiver counts, pass-through ratio.
- **Never inputs:** `is_laundering`, `pattern`, `attempt_id`. The pattern is used only afterwards, to group results.
- **Split by time:** train 1–7 Sep, test 8–10 Sep. Never split at random.
- **Settings:** `scale_pos_weight = neg/pos` (≈ 1,232), shallow trees, `eval_metric='aucpr'`, `tree_method='hist'`. Tune on PR-AUC.
- **Outputs to `marts/model/`:** feature importance, SHAP summary, detection rate per scheme at a 0.5% alert budget, and summed risk score per channel, currency and bank.
- **Baseline:** the old F1 0.34 / PR-AUC 0.31 is leaky (§3, §10.10). The honest reference is Experiment A: PR-AUC 0.170. The paper reports F1 0.63 with graph features.
- **Environment:** runs in Qiaoye's own Jupyter on a laptop, not SageMaker. Kernel: `aml-bq2 (.venv)`. Run All takes 10.4 min with the kernel at about 2.5 GB. If macOS swap is near full, restart the Mac first; on 13 Oct a leaking `audioanalyticsd` (9.4 GB) made runs take hours.
- **Status (13 Oct): built and run** on branch `qiaoye/p1-xgboost-notebook`. See §10.9 for where it departs from this section.

**How the model relates to BQ2:** the model is a means, not the answer. BQ2 is about concentration and schemes. The model contributes detection per scheme ("which schemes are missed") and risk-weighted exposure (scores summed by channel, currency and bank). It does not produce a per-transaction alert queue for investigators.

## 8. Live monitoring + AI layer (tab 3; Qiaoye owns it end to end)

**Purpose:** turn a one-off report into a live feed that is checked and explained. The pipeline replays HI-Small one simulated hour at a time. Four checks gate each slice before it can reach the marts or the dashboard, and a Claude agent writes a short note on what changed.

**DAG for each slice:** `replay_slice → load_slice` (Athena INSERT) `→ [check_recon, check_quality, check_drift, score_and_anomaly]` (in parallel) `→ gate` (an Airflow branch in plain code) `→ build_marts | quarantine` (`quarantine/` prefix) `→ agent_triage → send_alert` (email if severity is high).

| Check | Compares | Fails when |
|---|---|---|
| Recon | Row count, total `amount_paid` and laundering count, raw slice vs curated slice | Any gap beyond the known duplicates |
| Quality | Nulls in key columns; only the 15 currencies and 7 formats; amount > 0; timestamp inside the slice; USD amount present | Any rule broken |
| Drift | PSI of format mix, currency mix, USD amount bands and model-score bands against the same hour on normal weekdays (2–9 Sep) | PSI > 0.25 (0.1–0.25 is a warning) |
| Anomaly | XGBoost scores; each bank's and channel's share of the top 0.5%; volume against the same hour's baseline | More than 3 standard deviations |

**Three real cases to demo:**

1. **Weekend FX gap.** Recon fails, and the agent names the weekend as the cause.
2. **1 Sep Reinvestment batch.** Drift flags it, and the agent rates it low as an expected month-start batch.
3. **11 Sep tail.** Drift PSI is 8.8 and anomaly shows 58.6% laundering across 396 transfers, so the slice is quarantined. The agent says "data issue".

**Agent design:**

- **Stack:** LangChain + Claude, via Bedrock if the account allows it, otherwise the Anthropic API.
- **Reads:** check results (numbers only).
- **Tools:** `run_saved_query` (five fixed, read-only Athena queries), `get_baseline`, `get_top_alerts`; at most 3 tool calls per slice.
- **Output:** structured JSON with `severity`, `verdict` (expected / investigate / data issue), a 3-sentence `summary`, the `evidence` numbers and a `suggested_action`.
- **Guardrails:**
  - no free-form SQL and no writes;
  - a token cap per call;
  - every number in the note must appear in the inputs (flag any that don't as `unverified_numbers`);
  - **the gate is decided by code, never by the agent.**
- **On screen:** the monitor page `site/monitor.html` shows each slice's check badges and the agent note, labelled "AI-generated, review before acting".

**X-factors claimed:**

| X-factor | Evidence on the slide |
|---|---|
| Automation (one Airflow DAG) | Live demo |
| Near-real-time + AI with a job | The quarantined 11 Sep slice and its agent note |
| Cost and speed | The same lift query on raw CSV vs Parquet; Athena "data scanned" for each (≈ 476 MB vs a few MB) |
| External data that changes the answer | Rankings before and after USD conversion, plus Basel scores |

## 9. Work split and handoffs (Work split tab)

| Person | Lane | Est. cost |
|---|---|---|
| **Owen** | Data platform and **everything in S3**: bucket, IAM, budget alarm, uploads, the `parse_patterns` and `fetch_fx` Lambdas, Athena DDL, curated CTAS, S3 static site, CSV-vs-Parquet test | ≈ $0.50 |
| **Neeharika** | BQ2 analysis and dashboard: `currency_lookup.csv`, the seven marts (DuckDB locally first, then Athena), `publish` Lambda, `site/index.html`, one-paragraph finding per chart | ≈ $0.50 |
| **Qiaoye** | XGBoost notebook, Airflow DAG, replay, the four checks, the Claude agent, `site/monitor.html`, the demo recording. **Not assigned S3 setup work.** | ≈ $2 |

| ID | From → to | Handed over | Blocks |
|---|---|---|---|
| B0 | Neeharika → Owen | `currency_lookup.csv` | The CTAS join to USD and risk |
| B1 | Owen → Neeharika, Qiaoye | `curated_transactions` and `curated_accounts` in Athena | Athena marts; the notebook on real curated data |
| B2 | Qiaoye → Neeharika | `marts/model/` | The model charts on the dashboard |
| B3 | Owen, Neeharika → Qiaoye | Final Lambda, CTAS and mart SQL | The final Airflow DAG |

Phase 4 (slides and rehearsal) is shared by all three; each person presents their own lane.

## 10. Updates since the HTML (these win over the HTML)

1. **Network features for the model (Experiment B).** One-hop features miss multi-hop schemes. Add `fwd_within_24h`, `mins_to_forward`, `returns_to_sender_72h`, `sender_two_hop_out` and `receiver_two_hop_in` (stretch: `in_3cycle_72h`), and compare A vs B on detection per scheme. Full spec: `notebooks/AGENT_HANDOVER.md`.
   - Measured on 1–7 Sep: `returns_to_sender_72h` is true for 0.05% of normal rows vs 4.9% of laundering rows, and 14.6% of cycle rows.
2. **Exclude self-transfers from the multi-hop lookups.** They are 15% of rows; with them included, `returns_to_sender_72h` matches the row against itself.
3. **Leakage fix.** The account features saved in `hi_small_clean.csv` (`sender_fan_out_degree` and the rest) were computed over all 10 days. Recompute them inside the notebook. Fitting them on "1–7 Sep only" turned out to leak too (§10.9), so the notebook fits them **out-of-day**. The `curated_accounts` CTAS has the same problem if it aggregates every date; the model doesn't need it.
4. **Baseline wording.** F1 0.34, PR-AUC 0.31 and 62.7% came from scikit-learn `HistGradientBoostingClassifier`. Call them "baseline" until the XGBoost run replaces them.
5. **Open conflict: the dashboard.** The HTML says a Plotly static site on S3 ($0). The newer slide diagram shows **Streamlit on Amazon EC2** as the exposure dashboard. Ask Qiaoye which is final before building either, and keep the cost table in sync (a t3.small costs about $0.02/hr, so stop it when not demoing).
6. **Slide wording to reuse.** The BQ2 subtitle is "We analysed 5 million bank transfers to find the channels, banks and currencies where laundering clusters, and the patterns each scheme leaves behind, so investigators know where to look first." For the model, say "The risk score uses each account's direct network (fan-in, fan-out, counterparties, pass-through). Adding features that follow money across more than one hop lifts detection of bipartite, gather-scatter, stack and cycle schemes by 8–9 points, while fan-in and fan-out, already visible from one account, barely change."
7. **FX: use the simulator's own rates, not ECB (decided by Qiaoye, 13 Oct).** Real ECB rates gave each cross-currency transfer two different USD values (Yen about 25% off, Ruble about 30%) and had no Bitcoin. `scripts/build_fx_from_data.py` takes the median paid ÷ received on rows with USD on one side and writes `reference/fx/fx_rates.csv`. The notebook's local mode computes the same rates, so both modes give the same `amount_usd`.
8. **`curated_transactions` is built (13 Oct).** It was built in Athena with `scripts/run_athena_sql.py`. Recon passes: 5,078,336 rows, 5,177 laundering, 3,209 with a scheme, 1,968 `UNSTRUCTURED`, 0 NULL `amount_paid_usd`. Fixes needed on the way (all on branch `qiaoye/fix-curated-pipeline`):
   - `fx_rates.csv` had `iso_code` first, so the join matched nothing.
   - `patterns.csv` in S3 had 13 columns against a 14-column table; it was regenerated with the handler's own `parse()`.
   - `currency_lookup.csv` had never been uploaded.
   - Bitcoin was missing from the FX file.
   - The DAG used `HI-Small_Patterns.txt`, but the S3 object is `HI_Small_Patterns.txt`.
   - `fetch_fx` now writes to `reference/fx_ecb/`, so a DAG run can't overwrite the table.

   The IAM user `aml-cli` got the inline policy `AthenaCuratedBuild` (Athena primary workgroup, Glue `aml_bq2`, S3 bucket `aml-bq2`). It still has no Lambda permissions.
9. **Notebook departs from the spec in three ways (approved by Qiaoye, 13 Oct):**
   - **Out-of-day account features.** Each train day's one-hop and two-hop features are fitted on the other six train days; test rows use all of 1–7 Sep. Fitting on 1–7 Sep for every row let train rows see their own transfer. That run scored 0.42 PR-AUC on 7 Sep but 0.105 on test.
   - **Cut-off-aware relay features.** `fwd_within_24h` and `returns_to_sender_72h` are NaN, not 0, when their horizon runs past the window end without a match. "After ts" is strict: same-minute matches don't count.
   - **`learning_rate=0.1` instead of 0.05.** At 0.05, A ran to the 2,000-round cap and took 33 min.

   The spec-version outputs are kept locally in `out/model_spec_run/`.
10. **Why the old baseline is out of reach.** A 13 Oct diagnostic rebuilt the account features four ways (10% of normal rows, 300 rounds):
    - all 10 days (leaky): PR-AUC 0.360, 64.3% in the top 0.5%;
    - fitted on 1–7 Sep: 0.143;
    - out-of-day: 0.192;
    - none: 0.151.

    The 0.31 / 62.7% baseline only appears when the features see the test days.
11. **Alert threshold.** `out/model/score_threshold.json` has threshold 0.272898 (the top 0.5% of train-day scores). On the test days, 9,672 rows (0.72%) score at or above it, so `check_anomaly` will flag about 44% more than the 0.5% budget. Consider recomputing it out-of-sample before P4.

## 11. Repo and working rules

**Repo:** the git clone `bda-group-2`, with a `.venv`. Its `origin` is `https://github.com/kinokokam/aml-bq2-pipeline.git` (a personal GitHub repo, and there are no GitHub credentials on the laptop, so `git fetch` fails). Work only there. AWS region is `ap-southeast-1`; Claude is `global.anthropic.claude-haiku-4-5-20251001-v1:0` on Bedrock. Layout:

```
docs/              plan page (Laundering_Exposure_Plan.html), this file
scripts/           download_aml.py, profile_aml.py
reference/         currency_lookup.csv (the only CSV allowed in git)
sql/               01_tables.sql, 02_curated_transactions.sql, marts/*.sql
lambdas/           parse_patterns/, fetch_fx/, publish/   (handler.py each)
notebooks/         aml_xgboost.ipynb, AGENT_HANDOVER.md
airflow/dags/      aml_bq2_dag.py
monitoring/        checks.py   (recon, quality, drift PSI, anomaly, gate)
agent/             triage_agent.py  (LangChain + Claude, structured output, number guardrail)
site/              index.html (Neeharika), monitor.html (Qiaoye)
```

**Local data:** `~/aml_data/` holds `HI-Small_Trans.csv` and `HI-Small_Patterns.txt`; `~/aml_data/aml_out/` holds the `profile_aml.py` outputs.

**Rules:**

- **No secrets** in code, notebooks, commits or logs. Use `.env` (git-ignored) with `.env.example` placeholders, and the AWS named profile `aml-bq2`. If a key is ever exposed, tell Qiaoye to rotate it.
- **No data in git:** no CSV outside `reference/`, no Parquet. Clear notebook outputs before committing.
- **Git:** start each piece of work on a new branch from the latest `main`. Commit locally. Claude may push feature branches for Qiaoye's MRs, but **only** to remotes under `https://gitlab.com/shopback` (ShopBack policy) and never to `main`/`master`. The current `origin` is personal GitHub, so nothing is pushed; Qiaoye pushes.
- **Never use these as model inputs:** `is_laundering`, `pattern`, `attempt_id`. Always drop the 11–18 Sep tail. Split by time.
- **Cost:** don't create paid AWS resources (EC2, Glue jobs, SageMaker, MWAA) without asking. Never touch Marketplace, Redshift, RDS or QuickSight.
- **Stay in your lane:** change only Qiaoye's areas (`notebooks/`, `airflow/`, `monitoring/`, `agent/`, `site/monitor.html`) unless asked. Owen's and Neeharika's files are theirs. Exception: on 13 Oct Qiaoye asked for Owen's curated pipeline to be fixed (§10.8).
- **Permissions on this laptop:** reading `.env` and `.env.example` is blocked for Claude; Qiaoye edits those by hand.
- **Writing style for anything Qiaoye presents:** plain, complete sentences; concise; no jargon without a one-line explanation; never call the notebook "PaySim" or an "earlier project".
- **Numbers:** quote measured numbers from section 3 or from your own runs, and say which. Never estimate a figure and present it as measured.

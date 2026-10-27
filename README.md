# IS459 BDA — G1T2

Anti-money-laundering on the IBM synthetic transactions dataset, HI-Small only
(5,078,345 transfers, 1 in 981 laundering), from
[Kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml).

| | Question | Approach |
| --- | --- | --- |
| **BQ1** | How can financial institutions predict and prioritise potentially suspicious transactions for AML investigation? | Score each transaction as it arrives so the riskiest are reviewed first. |
| **BQ2** | Where does laundering concentrate, and how does each scheme move money? | Map the whole portfolio to show where the risk sits and how each scheme works. |

## Folder layout

| Folder | What lives here |
| --- | --- |
| `bq1/` | Batch training (Spark ETL, XGBoost) and real-time scoring (replay, Flink) |
| `bq2/` | Exposure analysis: Athena marts, dashboard, monitoring, triage agent |
| `infra/` | Infrastructure as code for the AWS footprint (empty) |
| `frontend/` | Analyst-facing UI (alert queue, watch-list) |

## Setup

Dependencies come from one `pyproject.toml` via [uv](https://docs.astral.sh/uv/),
with a dependency group per business question.

```bash
uv sync --group bq1          # or --group bq2, or --all-groups
cp .env.example .env         # AWS and Kaggle credentials, never commit it
```

Airflow is deliberately absent from the lock file; it runs from the official
image in `docker-compose.yml` at the root, which serves both DAGs.

The dataset is already in `s3://aml-bq1/bronze/`, so ingestion only needs
re-running if the bucket is rebuilt:

```bash
uv run python bq1/ingestion/ingest_aml_data.py
```

## Running it

Assumes `aws configure --profile bda` has been done once and `.env` is filled in.

**Step 1 is already done. A demo is steps 2, 3 and 4 only:**

```bash
./bq1/scripts/pipeline.sh predemo     # ~30 min, well before presenting
./bq1/scripts/pipeline.sh demo        # 3 min, when ready
./bq1/scripts/pipeline.sh teardown    # ~5 min, afterwards
```

`demo` will not work without `predemo` first — that is what pushes the new
broker addresses into the replay Lambda and creates this run's topics.

To rehearse again, leave the cluster up and run:

```bash
./bq1/scripts/pipeline.sh redemo       # wipe last run, then predemo + demo
```

With MSK already running `predemo` only restarts the job, so a retry is about
five minutes rather than thirty. Leaving the cluster up between rehearsals
costs 11.6 ¢/hour against 29 minutes to rebuild it, so only tear down when
finished for the day.

The MSK cluster and the Glue endpoint are the only resources billed by the
hour, so only they are destroyed between sessions; the account state the model
needs survives inside the Flink application, which is free while stopped.

### 1. Setup — ALREADY DONE, do not run again

This ran on 27 October 2026. Both snapshots exist and survive teardown:

| snapshot | status | holds |
| --- | --- | --- |
| `warmup-7d` | `READY` | account history, 1–8 September |
| `pre-demo` | `READY` | account history up to 10 September 11:00 |

**Skip to step 2.** Rerunning this is a seven-hour mistake and it needs no part
of a demo. Only run it again if the feature code changes, since the snapshot's
account state would no longer match the code reading it.

```bash
# ./bq1/scripts/pipeline.sh setup    # already done — leave it alone
```

What it did, for the record: builds and uploads both packages, provisions the
infrastructure, reads
1–8 September from S3 to build every account's rolling history, snapshots it,
then reads on to 10 September 11:00 so the Iceberg table has real scored data.
It leaves the `pre-demo` snapshot behind.

Measured, not estimated: warm-up is 3.73M records over about five and a half
hours, backfill about 1.2M over eighty minutes. Throughput falls from roughly
280 records a second to 100 as keyed state fills, which is why the warm-up
dominates. Warm-up skips the model entirely — it only needs account history —
and writes nothing to the gold table.

Neither phase touches MSK: both read S3 directly and run with the Kafka sinks
switched out of the job graph, so the cluster can be destroyed throughout.
`provision` still creates it, since it is one Terraform apply.

Only rerun this if the feature code changes, since the snapshot's account
state would no longer match.

### 2. Pre-demo — each session, about 30 minutes before presenting

```bash
./bq1/scripts/pipeline.sh predemo
```

Rebuilds the MSK cluster, points the application at its new brokers, creates
this run's topics, and starts Flink restored from `pre-demo`. When it returns,
Flink is running with warm account history and waiting for transactions. The
cluster takes 25–30 minutes to come up, so start this well before you present.

### 3. The demo

Trigger the `aml_bq1_demo` DAG in Airflow at `http://localhost:8080`, or:

```bash
./bq1/scripts/pipeline.sh demo
```

Replays 10 September 11:00–13:00 — 16,098 transactions, 66 of them laundering
— at 40× speed, so roughly three minutes. Scored rows land in the Iceberg
table and alerts go to the Kafka alerts topic.

Measured on 27 October: 16,098 sent, 16,098 consumed, 16,098 scored, no loss at
any hop; 126 alerts, 35 of the 66 laundering transactions caught. Precision
27.8%, recall 53.0%, lift 68× against a 0.41% base rate.

Iceberg commits at each checkpoint, so rows are queryable in Athena while the
replay is still running and a forced stop costs only the uncommitted tail. The
replay call itself is synchronous for the full three minutes — `replay()` sets
`--cli-read-timeout 900` because the CLI's 60-second default times out and
retries, which publishes the window twice.

### 4. Tear down

```bash
./bq1/scripts/pipeline.sh teardown
```

Teardown also wipes this run's scored rows from the gold table, reading the run
id `predemo` recorded. It deletes only that run, so the 1.24M backfill rows
stay as history and rehearsals never pile up. Run it after every demo and the
next one starts from a clean table.

| | Cost while it exists | Teardown |
| --- | --- | --- |
| MSK cluster, 2 × `kafka.t3.small` | 11.6 ¢/hour | destroyed |
| Glue interface VPC endpoint | about 2 ¢/hour | destroyed |
| Flink application and its snapshots | nothing while stopped | **kept** |
| Replay Lambda | per invocation only | **kept** |
| VPC, subnets, security groups, IAM roles, log groups, MSK configuration | nothing | kept |
| Glue database, Athena workgroup | nothing | kept |
| S3 — `bronze/`, `model/`, `artifacts/`, `gold/` | about 0.1 ¢/day | kept |

A running Flink application also costs about 27.6 ¢/hour: Managed Flink bills
two KPUs at 13.8 ¢ each, one running the job and one for the application's
coordinator. Stop it when a phase finishes rather than leaving it idle — a job
left running after it drained cost about $4.50 overnight on 26 October.

A stopped application is free. Both `KPU-Hour` and `RunningApplicationStorage`
are billed against allocated KPUs, and a stopped application allocates none;
`DurableApplicationBackups` bills at $0.0000 per day for our 42 MB of
snapshots. Rates and that evidence are from this account's own bill, not a
list price.

The Glue endpoint is not optional while the gold sink is Iceberg: Flink runs in
private subnets with no NAT, so the catalog has no other route to the Glue API.

Back to zero cost, and the Iceberg table stays queryable in Athena because it
is Glue metadata over S3 files and both survive.

**Never run a bare `terraform destroy` in `infra/`.** Snapshots belong to the
Flink application, so destroying it destroys them, and Managed Flink offers no
export — a lost `warmup-7d` means replaying 1–8 September again, five and a
half hours. The `-target` flags in `teardown` are what keep the application out
of scope.

### Settings and individual steps

Settings live in [`bq1/scripts/config.sh`](bq1/scripts/config.sh).

| Variable | Meaning |
| --- | --- |
| `WARMUP_START`, `BACKFILL_START` | Phase boundaries; the 7-day mark is where scores become trustworthy |
| `START`, `END` | The demo window |
| `GOLD_FROM` | Records before this are scored but kept out of the gold table |
| `SPEED` | Simulated seconds per real second. `40` sends two hours in three minutes |
| `CHUNK_HOURS` | Replay chunk size for the MSK fallback path |

`setup` is a wrapper; `build`, `provision`, `deploy`, `warmup`, `backfill` and
`run` can each be run alone. `smoke` pushes five minutes of data through for a
quick check. `wipe <run_id>` deletes one run's rows from the Iceberg table with
Athena SQL — never delete gold files from S3 directly, as the table tracks them
in metadata and queries then fail on missing files. `warmup-kafka` and
`backfill-kafka` do the bulk phases through MSK instead of reading S3.

## BQ1 — predict and prioritise

**Pipeline A, batch training.** Airflow pulls the Kaggle CSVs into S3 `bronze/`,
a Spark job writes feature-engineered train/val/test Parquet to `processed/`,
then XGBoost trains with isotonic calibration and saves weights to `model/` and
metrics to `evaluation/`. Full artifact contract in
[`bq1/README_PIPELINE.md`](bq1/README_PIPELINE.md).

**Pipeline B, real-time scoring.** A Lambda replays the pre-sorted hourly files
in `bronze/hourly/` into MSK as if the transactions were arriving live.
Managed Flink reads the topic, derives each record's own features, keeps seven
days of per-account history in keyed state to compute rolling 1h/24h/7d counts,
volumes and burst ratios, then scores with the Pipeline A model and attributes
the score to its top contributing features. Alerts go back to MSK on `alerts`,
unparseable records to `rejected`, and every eligible scored row lands in an
Iceberg table in the Glue catalog over `gold/`, for Athena.

No account join: every feature comes from the transaction itself or from the
history Flink is already holding, which is what lets it score on arrival.

The warm-up and backfill phases read `bronze/hourly/` from S3 directly rather
than through MSK, since nothing about them needs a queue — they exist only to
build the account state the demo restores from.

Pipeline A has to finish before Pipeline B can run — Flink loads the trained
model at startup.

## BQ2 — where laundering concentrates

Kaggle → S3 `raw/` → Lambda (parse patterns, fetch FX) → Athena CTAS → S3
`curated/` (Parquet by date) → Athena marts → XGBoost in Jupyter → Lambda
publish → S3 `site/` (Plotly dashboard). One Airflow DAG runs the lot, with
live checks and a Claude triage agent on every replayed slice. The full plan
is in [`bq2/docs/Laundering_Exposure_Plan.html`](bq2/docs/Laundering_Exposure_Plan.html).

| Folder | What lives here | Owner |
| --- | --- | --- |
| `bq2/docs/` | Plan page (open in a browser) | all |
| `bq2/scripts/` | `download_aml.py` (Kaggle → `~/aml_data`), `profile_aml.py` (local profiling + cleaned output) | all |
| `bq2/reference/` | Small lookup CSVs committed to git (`currency_lookup.csv`) | Neeharika |
| `bq2/sql/` | Athena DDL, the curated CTAS, and one file per mart in `marts/` | Owen (DDL, CTAS), Neeharika (marts) |
| `bq2/lambdas/` | `parse_patterns`, `fetch_fx`, `publish` handlers | Owen (first two), Neeharika (publish) |
| `bq2/notebooks/` | `aml_xgboost.ipynb` | Qiaoye |
| `bq2/airflow/dags/` | `aml_bq2_dag.py`, the one DAG that runs everything | Qiaoye |
| `bq2/monitoring/` | Recon, quality, drift (PSI) and anomaly checks | Qiaoye |
| `bq2/agent/` | LangChain + Claude triage agent | Qiaoye |
| `bq2/site/` | `index.html` dashboard and `monitor.html` live monitor | Neeharika, Qiaoye |

## Getting started

```bash
# 1. Python deps for local work
python -m pip install -r requirements.txt

# 2. Get the data (asks for your Kaggle API key the first time)
python3 bq2/scripts/download_aml.py         # → ~/aml_data/

# 3. Profile and create the cleaned local copy everyone codes against
python3 bq2/scripts/profile_aml.py --data-dir ~/aml_data --out-dir ~/aml_data/aml_out

# 4. AWS: use your own named profile, never keys in code
cp .env.example .env                        # fill in, never commit .env
aws configure --profile aml-bq2
```

Airflow runs locally with the official Docker Compose file
(see the [Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)).
Mount `bq2/airflow/dags/`, `bq2/monitoring/` and `bq2/agent/` into the
containers and add `apache-airflow-providers-amazon` to the image.

## Rules

- **No data in git.** The 476 MB CSV is over GitHub's 100 MB file limit and belongs in S3. `.gitignore` already blocks `*.csv` outside `bq2/reference/`, and blocks Parquet and the data folders.
- **No secrets in git.** AWS keys, Kaggle keys and the Anthropic key go in `.env` (ignored) or your AWS profile. If one is ever committed, rotate it straight away.
- **Column names are a contract.** `curated_transactions` columns are listed in the plan page; change them only after telling the team.
- **Keep the 11–18 Sep tail out** of every mart and model run (`txn_date <= '2022-09-10'`).

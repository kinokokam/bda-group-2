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

BQ1 is the only part with a deployed pipeline. Its commands, costs and settings
live with the code, in
[`bq1/README_PIPELINE.md`](bq1/README_PIPELINE.md#6-pipeline-b-real-time-scoring).

A demo is three commands from the repository root:

```bash
./bq1/scripts/pipeline.sh predemo     # ~30 min, well before presenting
./bq1/scripts/pipeline.sh demo        # 3 min, when ready
./bq1/scripts/pipeline.sh teardown    # ~5 min, afterwards
```

`setup` has already been run and must not be repeated; the snapshots it built
survive teardown. BQ2 scripts run from `bq2/`, which is what puts `monitoring/`
and `agent/` on the import path.

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

Dependencies are managed with [uv](https://docs.astral.sh/uv/); `pyproject.toml`
is the single source of truth and `uv.lock` pins the resolved versions.

```bash
# 1. Python deps for local work — pick the groups you need
uv sync --group bq1                         # replay, Flink packaging, dashboard
uv sync --group bq2                         # Spark ETL, training, marts, agent
uv sync --group airflow                     # only to run Airflow outside Docker

# 2. Get the data (asks for your Kaggle API key the first time)
uv run bq2/scripts/download_aml.py          # → ~/aml_data/

# 3. Profile and create the cleaned local copy everyone codes against
uv run bq2/scripts/profile_aml.py --data-dir ~/aml_data --out-dir ~/aml_data/aml_out

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

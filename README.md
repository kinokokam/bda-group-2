# AML BQ2 pipeline

Big Data final project, Business Question 2: **where does money laundering concentrate, and how does each laundering scheme move money?**

Dataset: IBM AML synthetic transactions, HI-Small only (5,078,345 transfers, 1 in 981 laundering) from
[Kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml).
The full plan (problem, pipeline diagram, live monitoring, work split) is in [`docs/Laundering_Exposure_Plan.html`](docs/Laundering_Exposure_Plan.html).

## Pipeline in one line

Kaggle → S3 `raw/` → Lambda (parse patterns, fetch FX) → Athena CTAS → S3 `curated/` (Parquet by date) → Athena marts → XGBoost in Jupyter → Lambda publish → S3 `site/` (Plotly dashboard), all run by one Airflow DAG, with live checks and a Claude triage agent on every replayed slice.

## Repo layout

| Folder | What lives here | Owner |
| --- | --- | --- |
| `docs/` | Plan page (open in a browser) | all |
| `scripts/` | `download_aml.py` (Kaggle → `~/aml_data`), `profile_aml.py` (local profiling + cleaned output) | all |
| `reference/` | Small lookup CSVs committed to git (`currency_lookup.csv`) | Neeharika |
| `sql/` | Athena DDL, the curated CTAS, and one file per mart in `sql/marts/` | Owen (DDL, CTAS), Neeharika (marts) |
| `lambdas/` | `parse_patterns`, `fetch_fx`, `publish` handlers | Owen (first two), Neeharika (publish) |
| `notebooks/` | `aml_xgboost.ipynb` | Qiaoye |
| `airflow/dags/` | `aml_bq2_dag.py`, the one DAG that runs everything | Qiaoye |
| `monitoring/` | Recon, quality, drift (PSI) and anomaly checks | Qiaoye |
| `agent/` | LangChain + Claude triage agent | Qiaoye |
| `site/` | `index.html` dashboard and `monitor.html` live monitor | Neeharika, Qiaoye |

## Getting started

```bash
# 1. Python deps for local work
python3 -m pip install pandas numpy pyarrow xgboost shap scikit-learn boto3

# 2. Get the data (asks for your Kaggle API key the first time)
python3 scripts/download_aml.py            # → ~/aml_data/

# 3. Profile and create the cleaned local copy everyone codes against
python3 scripts/profile_aml.py --data-dir ~/aml_data --out-dir ~/aml_data/aml_out

# 4. AWS: use your own named profile, never keys in code
cp .env.example .env                        # fill in, never commit .env
aws configure --profile aml-bq2
```

Airflow runs locally with the official Docker Compose file
(see the [Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)).
Mount `airflow/dags/`, `monitoring/` and `agent/` into the containers and add
`apache-airflow-providers-amazon` to the image.

## Rules

- **No data in git.** The 476 MB CSV is over GitHub's 100 MB file limit and belongs in S3. `.gitignore` already blocks `*.csv` outside `reference/`, and blocks Parquet and the data folders.
- **No secrets in git.** AWS keys, Kaggle keys and the Anthropic key go in `.env` (ignored) or your AWS profile. If one is ever committed, rotate it straight away.
- **Column names are a contract.** `curated_transactions` columns are listed in the plan page; change them only after telling the team.
- **Keep the 11–18 Sep tail out** of every mart and model run (`txn_date <= '2022-09-10'`).

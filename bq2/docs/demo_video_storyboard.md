# BQ2 demo video: storyboard (Gate 1, awaiting approval)

**Requirements:** the video shows, with annotations, (1) how to set up the BQ2 pipeline end to end and (2) how to run it end to end.
**Audience:** course assessors. **Platform:** YouTube, unlisted, 1920x1080 MP4.
**Sound:** none (no voice, no music). Annotations only: burned-in captions, chapter cards, callout boxes.
**Branding:** no band, no logo. **Close:** one recap card, no call to action.
**Recording:** demo-video skill harness in a Linux Docker container (Xvfb + Chromium + ffmpeg), driving Airflow (:8080) and Helical (:443) on this Mac. No AWS console and no credentials in the recorder: AWS evidence is real CLI output (`aws s3 ls`, `aws lambda`) and Airflow task logs, shown in a terminal panel.
**State filmed:** setup steps are shown as their commands plus the finished result (they were run before and must not be rerun on camera: they cost money or rewrite S3). Run steps show finished outputs: `aml_bq2_batch` run `rehearsal3_20261108_1635` (all green), `aml_bq2_demo` 1-11 Sep (3 Nov, all green), Helical as saved.
**Secrets:** never on screen. `.env.example` only, never `.env`; no `aws configure list`, no account ID.

## Part 1: Set up (one time)

| # | Screen | Action | Proof on screen | Caption | Duration |
|---|---|---|---|---|---|
| 0 | Chapter card | - | - | "BQ2: a daily monitor for AML transfer data. Part 1: set up." | 4s |
| S1 | `architecture_v2.drawio`, rendered | Pan raw -> curated -> marts -> monitor -> Helical | The diagram | "Setup 1 of 11: the architecture. One S3 bucket, Athena, one Lambda, Airflow, Helical." | 30s |
| S2 | Repo tree | Expand `bq2/`: `airflow/dags`, `lambdas`, `sql`, `pipeline`, `monitoring`, `scripts` | Where each part lives | "Setup 2: the codebase. BQ2 sits beside BQ1 in one repo." | 25s |
| S3 | Terminal panel | `uv sync` | Install completes | "Setup 3: clone the repo and install with uv." | 20s |
| S4 | `.env.example`, then terminal `aws s3 ls s3://aml-bq2/` | Highlight the keys; list the bucket | Placeholders; raw/, reference/, curated/, marts/, monitor/ | "Setup 4: config and AWS. Keys stay in .env and the aml-bq2 profile, never in git." | 30s |
| S5 | `scripts/download_aml.py`, then `aws s3 ls .../raw/ --recursive` | Show the script; list raw | HI-Small_Trans.csv, HI_Small_Patterns.txt | "Setup 5: download IBM AML HI-Small from Kaggle and upload it to raw/." | 30s |
| S6 | `sql/01_tables.sql`, then `run_athena_sql.py` usage | Scroll the DDL | External tables on raw/ and curated/ | "Setup 6: create the Athena tables over S3." | 30s |
| S7 | `lambdas/parse_patterns/handler.py`, then `aws lambda get-function-configuration` | Show the handler; show the deployed function | python3.12, 60 s timeout | "Setup 7: deploy the parse_patterns Lambda." | 30s |
| S8 | Terminal: `ls bq2/out/` and the replay/train usage lines | Show the monitor inputs | stream/ (daily slices), live_model/, fx_rates.csv | "Setup 8: monitor inputs: cut the 11 daily slices, train the live model, copy the FX table." | 35s |
| S9a | `docker-compose.yml` volumes | Highlight the `./bq2/...` mounts | `./bq2/airflow/dags:/opt/airflow/dags/bq2` and the code mounts | "Setup 9: add BQ2 to the team's Airflow by mounting its DAGs and code." | 30s |
| S9b | Terminal `docker compose up -d`, then the Airflow DAG list | Show both DAGs | `aml_bq2_batch`, `aml_bq2_demo` | "One command starts Airflow 2.8; both BQ2 DAGs appear." | 20s |
| S10 | `helical/docker-compose.override.yml`, then the Helical data source | Highlight the snapshot mount and the read-only DuckDB URL | `/snapshot:ro`, `access_mode=READ_ONLY` | "Setup 10: Helical reads a DuckDB snapshot synced from S3, read-only." | 30s |
| S11 | `helical/instantbi-bedrock/apply.py` header | Scroll to the model and region | Claude Haiku 4.5, us-east-1, profile mount | "Setup 11: the AI chat runs on Amazon Bedrock. No key is stored." | 25s |

## Part 2: Run (end to end)

| # | Screen | Action | Proof on screen | Caption | Duration |
|---|---|---|---|---|---|
| 1 | Chapter card | - | - | "Part 2: run the pipeline." | 3s |
| R1 | `aml_bq2_batch.py` task chain, then the DAG's Graph view (green) | Show `patterns >> curated >> marts`; hover the tasks | 8 green tasks | "Run 1: trigger the batch DAG once: Lambda, then Athena builds the curated table and six marts." | 40s |
| R2 | The `curated_transactions` log line, then `aws s3 ls .../marts/` | Show the audit line | Audit passed; 6 mart folders | "Every rebuild is audited before it replaces the live table." | 25s |
| R3 | XGBoost notebook results, rendered read-only | Scroll to the A vs B table | PR-AUC 0.170 vs 0.236 | "Run 2: XGBoost, trained once. Relay features lift PR-AUC from 0.170 to 0.236." | 30s |
| R4 | `aml_bq2_demo.py` task chain, then the Grid view | Show the chain; 11 green runs | 1-11 Sep | "Run 3: the monitor DAG runs once per day: four checks, then a gate." | 35s |
| R5 | Graph view, 1 Sep, then 5 Sep | Compare the gate branch | 1 Sep -> quarantine; 5 Sep -> accept_day | "1 Sep fails and is quarantined. 5 Sep passes." | 40s |
| R6 | 1 Sep check report and the escalation log | Scroll | Facts only, no verdict | "Code writes the check report. Every quarantined day escalates." | 25s |
| R7 | Helical Monitor dashboard | Pan the KPI cards and the check table | 11 slices, 8 passed, 3 quarantined | "Run 4: results in Helical, refreshed by the last task, sync_snapshot." | 35s |
| R8 | Helical Analysis dashboard | Pan A1-A7 | ACH 85% of laundering, 7.2x lift | "One chart per mart, each with its finding." | 35s |
| R9 | Helical Instant chat | Ask one question | ACH, 3,828 of 599,685 | "Ask the data in plain English (Claude on Bedrock)." | 30s |
| 2 | Recap card | - | - | "Set up once: bucket, tables, Lambda, Airflow, Helical. Run: batch DAG, monitor DAG, dashboards and chat." | 8s |

Total: about 11 min.

## Claim-to-frame audit
- "Keys stay out of git": S4 shows `.env.example` placeholders only.
- "Mounting adds BQ2 to Airflow": S9a mount lines, then S9b both DAGs listed.
- "Read-only snapshot": S10 URL.
- "Audited rebuild": R2 log line.
- "Three days quarantined": R4, R5, R7.
- "Code writes the report": R5 `report_slice` task, R6 report.
- "Refreshed by sync_snapshot": the task visible in R5.

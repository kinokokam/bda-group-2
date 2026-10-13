# Agent handover: Qiaoye's lane, status as of 13 Oct 2026

**Who you work for:** Qiaoye. Qiaoye's lane is the XGBoost notebook, the Airflow DAG, the four live checks, the Claude triage agent and the live monitor page.

**Read these first, in this order:**

1. This file: what is done, where it lives, and what comes next.
2. `docs/AGENT_CONTEXT.md` is the whole project: the question, measured facts, pipeline, column contract and rules. Its §10.7–10.11 record the 13 Oct decisions.
3. `notebooks/AGENT_HANDOVER.md` is the original notebook spec. The notebook now departs from it in three approved ways (`AGENT_CONTEXT.md` §10.9).

**First thing to run:** the `project-progress` skill (`python .claude/skills/project-progress/scripts/check_progress.py` from the repo root). It reports section 0, P1–P9 and the repo rules deterministically. It exists only on the branches that contain this file.

---

## 0. Where the work is (git)

The repo is the clone `bda-group-2`. `origin` is `https://github.com/kinokokam/aml-bq2-pipeline.git`. That is a personal GitHub repo, so ShopBack policy doesn't allow pushing to it, and the laptop has no GitHub credentials (`git fetch` fails). **Everything below is committed locally and unpushed.** All three branches start from `main` (`5529e97 Initial project skeleton`).

| Branch | Commits | Contents |
|---|---|---|
| `qiaoye/section0-setup` | `7bf021e`, `8606855`, plus this handover update | `agent/triage_agent.py` (Bedrock Converse + dotenv), `requirements.txt`, README install step, `.env.example`, the handover docs, the `.claude/skills/project-progress` skill |
| `qiaoye/fix-curated-pipeline` | `26f56ea`, `999814f` | `scripts/build_fx_from_data.py`, `scripts/run_athena_sql.py`, `lambdas/fetch_fx/handler.py`, `sql/01_tables.sql`, the DAG patterns key, `.env.example` (`ap-southeast-1`, bucket `aml-bq2`, Haiku 4.5) |
| `qiaoye/p1-xgboost-notebook` | `63198de` | `notebooks/aml_xgboost.ipynb`, with outputs cleared |

**Merge order:** section0 → fix-curated → p1. Both of the first two branches change `.env.example`; keep the `fix-curated-pipeline` version.

**Stash:** `stash@{0}` holds older uncommitted `.env.example` edits from `section0-setup`. Later commits superseded them; ask Qiaoye before dropping the stash.

**Not in git:** `out/` is not in `.gitignore`. The CSVs are covered by `*.csv`, but `model.json` and `score_threshold.json` are not. Don't stage `out/`. Adding `out/` to `.gitignore` is Qiaoye's call.

## 1. Done

| # | Item | Where | State |
|---|---|---|---|
| D1 | HI-Small data on the laptop | `~/aml_data/HI-Small_Trans.csv`, `HI-Small_Patterns.txt` | Complete. `hi_small_clean.csv` was **never written**. The notebook reads the raw files instead. |
| D2 | Section 0 setup (12 Oct list) | `qiaoye/section0-setup` | Complete. The `.venv` has `requirements.txt` plus `nbconvert`, `ipykernel` and `nbformat`, which are not yet listed in requirements. `brew install libomp` was needed for XGBoost. Jupyter kernel `aml-bq2 (.venv)` is registered. |
| D3 | AWS access | profile `aml-bq2` | `aws sts` returns account `741448922451`, `user/aml-cli`. Bedrock `converse` (Haiku 4.5, `ap-southeast-1`) replied OK. `python agent/triage_agent.py` printed a valid JSON note for 2022-09-11 (severity `high`, empty `unverified_numbers`). `aml-cli` has S3, Bedrock and the inline policy `AthenaCuratedBuild`; it has no Lambda or IAM rights. |
| D4 | Curated pipeline (Owen's lane, fixed at Qiaoye's request) | `qiaoye/fix-curated-pipeline`; S3 `aml-bq2` | `aml_bq2.curated_transactions` is built (Parquet in `curated/transactions/`). Recon passes: 5,078,336 rows, 5,177 laundering, 3,209 with a scheme, 0 NULL USD. Details: `AGENT_CONTEXT.md` §10.8. |
| D5 | FX rates | `reference/fx/fx_rates.csv` | The simulator's own fixed rates for all 15 currencies × 18 days (`AGENT_CONTEXT.md` §10.7). Owen's ECB file is backed up at `reference/fx_backup/fx_rates_ecb.csv`. |
| D6 | **P1: XGBoost notebook** | `notebooks/aml_xgboost.ipynb` | Run All takes 10.4 min. All asserts pass, and all 9 files in `out/model/` were written on 13 Oct. Results: B lifts PR-AUC from 0.170 to 0.236; bipartite, gather-scatter, stack, cycle and random gain 5–9 points at the top 0.5% (`AGENT_CONTEXT.md` §3). |
| D7 | **P2: monitoring hand-off** | `out/model/model.json`, `out/model/score_threshold.json` | The threshold is 0.272898, but it lets 0.72% of test rows through instead of 0.5% (`AGENT_CONTEXT.md` §10.11). |
| D8 | Spec-version results kept for the write-up | `out/model_spec_run/` | The first run, with account features fitted on 1–7 Sep: A PR-AUC 0.105, B 0.096. |
| D9 | Note for Owen | in the 13 Oct chat | Drafted, but **not sent**. It lists the five curated-pipeline fixes. Qiaoye sends it. |
| D10 | Monitoring checks, agent v1, DAG skeleton | `monitoring/checks.py`, `agent/triage_agent.py`, `airflow/dags/aml_bq2_dag.py` | Unchanged from 12 Oct, apart from the DAG's patterns key. |

## 2. Blockers

| ID | State |
|---|---|
| B1 `curated_transactions` | **Resolved 13 Oct** (D4). The notebook's `AML_MODE=aws` reads it after `aws s3 sync s3://aml-bq2/curated/ ./curated/ --profile aml-bq2`. This mode has not been run yet. |
| B1b `curated_accounts` | Not needed by the model. If Owen builds it, it must not aggregate test dates. |
| B2 `marts/model/` for Neeharika | **The files are ready locally** in `out/model/` but not uploaded. Upload with `aws s3 cp out/model/ s3://aml-bq2/marts/model/ --recursive --profile aml-bq2` once Qiaoye says so. |
| B3 Final Lambda / mart SQL | Still waiting on Owen and Neeharika. |
| B4 Bucket and region | **Resolved:** `aml-bq2`, `ap-southeast-1`. Qiaoye should set `AML_BUCKET=aml-bq2` in `.env`. |
| B6 Dashboard: Plotly on S3 or Streamlit on EC2 | Open team decision. |

## 3. Next tasks (P3–P9, unchanged in scope)

| # | Task | Notes for 13 Oct onward |
|---|---|---|
| P3 | `replay_slice` in local mode: one file per simulated hour under `out/stream/slice=YYYY-MM-DDTHH/`; the 11 Sep tail only as the quarantine demo slice. | There is no `hi_small_clean.csv`. Cut slices from the raw CSV, or from the curated Parquet, which has `amount_paid_usd` and `pattern`. |
| P4 | `run_checks` plus `tests/test_demo_cases.py` (the three demo cases). | `check_anomaly` needs a `score` column. Scoring a slice needs the notebook's feature code, so factor it into a module first. Recompute the threshold out-of-sample (§10.11). |
| P5 | Agent with no tools, a `FakeLLM` for tests, and token `usage` logged in each note. | Bedrock works (D3). |
| P6 | Airflow in Docker, local mode. | |
| P7 | `out/site/data/monitor/<slice>.json` plus `site/monitor.html`. | The final look waits on B6. |
| P8 | `send_alert` writes to `out/alerts.log`. | |
| P9 | Slide outline. | Use the A/B numbers in `AGENT_CONTEXT.md` §3, the slide sentence in §10.6, and the baseline-leakage story in §10.10. |

**The three demo cases** are unchanged: the weekend FX gap (recon fails, Saturday), the 1 Sep month-start batch (drift, rated expected) and the 11 Sep tail (396 rows, 58.6% laundering, quarantined).

## 4. Open decisions (ask Qiaoye)

1. Dashboard (B6).
2. Whether to build the stretch feature `in_3cycle_72h`.
3. Whether to add `out/` to `.gitignore`, and `nbconvert` / `ipykernel` to `requirements.txt`.
4. When to upload `out/model/` to S3 for Neeharika (B2), and whether to send the note to Owen.
5. Where to push: a ShopBack GitLab remote is needed before any MR.

## 5. Known small issues

- The progress script reports `3cycle(stretch)=y` because the notebook's markdown mentions the feature it didn't build. It needs a code-only check.
- Run the progress script from inside the repo. Copied elsewhere, it can't find the repo root.
- `notebooks/README.md` still quotes the old leaky baseline.
- `raw/patterns/HI_Small_Patterns.txt` in S3 has CRLF line endings. `parse()` strips them, but any other reader should too.
- If runs crawl, check `sysctl vm.swapusage`. On 13 Oct, a leaking macOS `audioanalyticsd` (9.4 GB) filled swap, and a 10-minute notebook took hours. Restart the Mac and keep only VS Code open.

## 6. Rules (the full list is in `docs/AGENT_CONTEXT.md` §11)

- **Credentials:** never open, copy, print or commit credential files. Don't read the AWS key CSV in `~/Downloads/Telegram Desktop/`. AWS access goes only through `--profile aml-bq2`. Claude can't read `.env` or `.env.example` on this laptop; Qiaoye edits them.
- **Git:** branch from the latest `main` for each new piece of work. Commit locally. Push feature branches only to `gitlab.com/shopback` remotes, never to `main`/`master`.
- **No data in git,** and clear notebook outputs before committing.
- **No paid AWS resources** (EC2, Glue, SageMaker, MWAA) without asking. Nothing from Marketplace, Redshift, RDS or QuickSight.
- **Numbers:** keep `is_laundering`, `pattern` and `attempt_id` out of model inputs. Drop the 11–18 Sep tail. Split by time. Quote only measured numbers and say where each came from.

## 7. When you stop, report back with

1. Which of P3–P9 are done, with file paths.
2. Anything that contradicts a number in `docs/AGENT_CONTEXT.md`.
3. Anything you changed outside Qiaoye's folders, and why.

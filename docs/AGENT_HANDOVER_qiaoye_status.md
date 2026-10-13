# Agent handover: Qiaoye's lane, status as of 12 Oct 2026 (updated after Bedrock setup)

**Who you work for:** Qiaoye. Qiaoye's lane is the XGBoost notebook, the Airflow DAG, the four live checks, the Claude triage agent and the live monitor page. Qiaoye does **no S3 setup**; that is Owen's.

**Read these first, in this order:**

0. Section 0 below: apply the 12 Oct changes before anything else.
1. `docs/AGENT_CONTEXT.md` is the whole project: the question, measured facts, pipeline, column contract and rules.
2. `notebooks/AGENT_HANDOVER.md` is the detailed spec for the notebook work in P1.
3. `docs/Laundering_Exposure_Plan.html` is the team plan page and the original source.

**Where the code is:** the git clone **`bda-group-2`** (GitHub `kinokokam/bda-group-2`), with its own `.venv`. Work only there. The older copy `~/aml_data/aml-bq2-pipeline 2/` is not a git repo; it is only the source for the files in Step 0. Ignore it afterwards. **Commit locally, never push**; Qiaoye pushes.

---

## 0. Apply these changes first (in `bda-group-2`, in this order)

These are fixes made on 12 Oct in the older copy. Re-apply them here and check each one before starting section 4.

**0.1 Activate the venv.** All commands run from the repo root with `.venv` active. Use `python`, not `python3`.

**0.2 Update `agent/triage_agent.py`.** There are two edits; leave everything else as it is.

(a) Directly after the line `from pydantic import BaseModel, Field`, add:

```python

try:  # read settings from .env in the repo root, if python-dotenv is installed
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
```

(b) In `get_llm()`, replace the whole `if provider == "bedrock":` block (the one using `ChatBedrock` and `model_kwargs`) with:

```python
    if provider == "bedrock":
        # Converse API: works with inference profiles such as global.anthropic.claude-haiku-4-5-...
        import boto3
        from langchain_aws import ChatBedrockConverse
        session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"),
                                region_name=os.environ.get("AWS_REGION", "ap-southeast-1"))
        return ChatBedrockConverse(client=session.client("bedrock-runtime"),
                                   model=os.environ["BEDROCK_MODEL_ID"], provider="anthropic",
                                   max_tokens=400, temperature=0)
```

Also change the docstring install line to `pip install langchain-core pydantic python-dotenv  plus ONE of:`.

Check: `python -m py_compile agent/triage_agent.py` prints nothing.

**0.3 Update `.env.example`.** It holds placeholders only, never real keys. Set these two lines:

```
AWS_REGION=ap-southeast-1
BEDROCK_MODEL_ID=global.anthropic.claude-haiku-4-5-20251001-v1:0
```

**0.4 Create `.env`** only if it doesn't exist: `cp -n .env.example .env`. It must keep `AWS_PROFILE=aml-bq2`, `AWS_REGION=ap-southeast-1`, `AGENT_PROVIDER=bedrock` and the model ID above. Check: `git check-ignore .env` must print `.env`. If it prints nothing, add `.env` to `.gitignore` before doing anything else. **Never read keys into `.env`, code or logs; AWS access comes only from the profile.**

**0.5 Add `requirements.txt`** (new file) so every teammate installs the same libraries:

```
pandas
numpy
pyarrow
scikit-learn
xgboost
shap
boto3
langchain-core
langchain-aws
pydantic
python-dotenv
```

Then run `python -m pip install -r requirements.txt`. Update the README "Getting started" step 1 to use `python -m pip install -r requirements.txt`.

**0.6 Check AWS and Claude access.** Run these in order and stop if any fails:

| Command | Expected |
|---|---|
| `aws sts get-caller-identity --profile aml-bq2` | Account `741448922451`, user `aml-cli` |
| `aws bedrock-runtime converse --model-id global.anthropic.claude-haiku-4-5-20251001-v1:0 --messages '[{"role":"user","content":[{"text":"Reply with OK"}]}]' --region ap-southeast-1 --profile aml-bq2 --no-cli-pager` | A reply with text `OK` |
| `python agent/triage_agent.py` | A JSON note for slice 2022-09-11 with `severity`, `verdict`, `summary`, `evidence`, `suggested_action`, and an empty `unverified_numbers` |

If any of these fail:

- **`AccessDeniedException` on `converse`:** the `aml-cli` user is missing its `InvokeClaudeHaiku` policy. Report this to Qiaoye; do not change IAM yourself.
- **`could not be found` on the profile:** Qiaoye has to run `aws configure --profile aml-bq2`. Never handle the keys yourself.
- **The agent script fails but `converse` works:** report the full error.

**0.7 Commit locally** with the message `Agent: Bedrock Converse + dotenv, Singapore region, requirements.txt, handover docs`. Run `git status` first and confirm `.env` is **not** staged. Do not push.

---

## 1. Done

| # | Item | Where | State |
|---|---|---|---|
| D1 | HI-Small data on the laptop | `~/aml_data/HI-Small_Trans.csv` (+ `.gz`), `HI-Small_Patterns.txt` | Complete |
| D2 | Profiling and cleaned local copy | `~/aml_data/aml_out/`: `hi_small_clean.csv` (32 cols), `account_features.csv`, `laundering_patterns.csv`, `profile_*.csv` | Complete. **Its account columns leak test days**, so recompute them (see P1). |
| D3 | Baseline model numbers | in `AGENT_CONTEXT.md` §3 | Measured with sklearn `HistGradientBoostingClassifier`, not XGBoost: PR-AUC 0.31, F1 0.34, 62.7% caught in the top 0.5% |
| D4 | Network-feature signal check | `notebooks/AGENT_HANDOVER.md` §6 | Measured on 1–7 Sep: `returns_to_sender_72h` is true for 0.05% of normal rows vs 4.9% of laundering rows (14.6% for cycle) |
| D5 | Monitoring checks | `monitoring/checks.py` | `check_recon`, `check_quality`, `check_drift` (PSI), `check_anomaly` and `gate` are written and tested on real data. Measured PSI: 3.9 for 1 Sep, 8.8 for 11 Sep. |
| D6 | Claude triage agent, first version | `agent/triage_agent.py` | Structured output (severity, verdict, summary, evidence, action) and the number guardrail are written. **Not yet run against a real model**, because no credentials are set. **Tools not built yet** (see P5). |
| D7 | DAG skeleton | `airflow/dags/aml_bq2_dag.py` | Task graph done for the batch and monitoring parts. Bodies are stubs: `replay_slice`, `run_checks`, `agent_triage` and `send_alert` raise `NotImplementedError`; `load_slice`, `quarantine`, `build_marts` and `xgboost_model` are `EmptyOperator`s. Only one mart (`channel_lift`) is wired. |
| D8 | Notebook spec | `notebooks/AGENT_HANDOVER.md` | Complete. **The notebook itself does not exist yet.** |
| D9 | Repo skeleton | 18 files: SQL, the 3 Lambda handlers, README, `.gitignore`, `.env.example` | Complete as a first version. Owen and Neeharika own the SQL and Lambdas. |
| D10 | Presentation assets | `~/aml_data/Claude outputs/` | Pipeline diagram, result-table image, BQ2 slide wording |
| D11 | **Claude on Bedrock connected (12 Oct)** | `.env`, `agent/triage_agent.py` | Region `ap-southeast-1` (Singapore). Model `global.anthropic.claude-haiku-4-5-20251001-v1:0`. Laptop profile `aml-bq2` = IAM user `aml-cli`, with inline policy `InvokeClaudeHaiku` (Haiku 4.5 only). The terminal `converse` test returned OK. `get_llm()` now uses `ChatBedrockConverse` and loads `.env` with python-dotenv. Next: confirm `python3 agent/triage_agent.py` prints a JSON note. |
| D12 | Budgets | AWS Budgets | Budget `aml-bq2`: $10/month, $1.43 actual, $4.02 forecast (12 Oct). Daily and project-total budgets proposed. |

## 2. Waiting on others (blocked)

| ID | From | What we need | What it blocks for Qiaoye | Workaround: keep building with this |
|---|---|---|---|---|
| B1 | Owen | `curated_transactions` in Athena / S3 (`curated/transactions/`) | Running the notebook on real curated data; `load_slice` and replay from S3 | Use `~/aml_data/aml_out/hi_small_clean.csv`. Keep one config switch, `AML_MODE=local|aws`, so only the data source changes later. |
| B1b | Owen | `curated_accounts` | Nothing. **Tell Owen the model does not need it**: the notebook computes account features from 1–7 Sep only, to avoid leakage. If Owen still builds it, it must not aggregate test dates. | n/a |
| B3 | Owen, Neeharika | Final Lambda code, curated CTAS and the six other mart SQL files | The final DAG (one `AthenaOperator` per mart) | Wire the DAG with the current skeleton files and the mart names from the plan; swap in the final SQL when it arrives. |
| B4 | Owen | Bucket name and region (should be `ap-southeast-1`, the same as Bedrock) | `AML_BUCKET` in `.env`; any S3 or Athena call | Build everything against local files. **Bedrock part resolved (D11).** |
| B6 | Team (decision) | Dashboard: **Plotly static site on S3** (plan) or **Streamlit on EC2** (newer slide diagram)? | The format of the monitor page | Write the monitor data as JSON files first (P7); both options can read them. |

**Check before assuming B1 or B4 is still blocked.** If `aws sts get-caller-identity --profile aml-bq2` works, run `aws s3 ls s3://<bucket>/curated/ --profile aml-bq2` to see whether the curated data has landed. Report what you find; don't guess.

## 3. Others waiting on Qiaoye

| ID | To | What | Status |
|---|---|---|---|
| **B2** | Neeharika | `marts/model/` result files (drivers, detection rate per scheme, exposure by channel, currency and bank) | **Not started. This is the top priority** because Neeharika's model charts wait on it. Hand over the local CSVs as soon as P1 produces them; S3 can come later. |

## 4. Build now, in parallel (no dependency on anyone)

Work roughly in this order. P1 and P2 come first because they unblock Neeharika and feed the anomaly check.

| # | Task | Done when | Depends on |
|---|---|---|---|
| **P1** | Build `notebooks/aml_xgboost.ipynb`: Experiment A (about 21 baseline features) and Experiment B (A plus network features), exactly as in `notebooks/AGENT_HANDOVER.md` | It runs top to bottom on the local CSV; `out/model/` holds the files in that spec's §7; a markdown cell says which schemes improved | nothing |
| **P2** | Save `out/model/model.json` and `out/model/score_threshold.json` (the top 0.5% cut-off on the train days) | `check_anomaly` can load both | P1 |
| P3 | Implement `replay_slice` in local mode: cut `hi_small_clean.csv` into one file per simulated hour under `out/stream/slice=YYYY-MM-DDTHH/`, using the DAG run's logical date mapped onto 1–10 Sep. Include the 11 Sep tail **only** as the quarantine demo slice. | Running it on 3 logical dates gives 3 correct slice files | nothing |
| P4 | Implement `run_checks`. It loads the slice and a baseline (same hour on the normal days 2, 5, 6, 7, 8 and 9 Sep: weekdays only, not 1 Sep), calls `monitoring.checks`, and pushes the result dicts to XCom. Add `tests/test_demo_cases.py` with the three demo cases below. | The three tests pass | P2 (anomaly only), P3 |
| P5 | **Keep the agent simple: no tools.** Code works out the few context facts itself (baseline laundering rate, normal volume for that hour, day of month, which category moved) and passes them into `triage()` alongside the check results, so Claude makes one call per slice. Add a `FakeLLM` for tests. Log each reply's `usage` (input and output tokens) into the note JSON, for the cost slide. | `triage()` returns valid `Triage` JSON for the three demo cases with `FakeLLM` and with real Claude; the guardrail fills `unverified_numbers` when a fake reply invents a number; each note carries its token usage | P4 |
| P6 | Run Airflow locally with Docker (official `docker-compose.yaml`, adding `apache-airflow-providers-amazon`). Mount `airflow/dags`, `monitoring` and `agent`. In local mode, skip AWS tasks with a branch or `EmptyOperator`. | The DAG parses without errors; one manual run goes replay → checks → gate → triage on a local slice | P3, P4, P5 |
| P7 | Write monitor output as `out/site/data/monitor/<slice>.json` (check badges and the agent note, labelled "AI-generated note, review before acting"), plus a simple `site/monitor.html` that reads the JSON and refreshes every 60 s | Opening `monitor.html` locally shows the 11 Sep slice as quarantined with its note | P5. The final look waits on B6. |
| P8 | `send_alert`: for now, write a high-severity line to `out/alerts.log`. Real email can come later (SMTP or SNS needs Owen and credentials). | A high-severity slice creates a log line | P5 |
| P9 | Draft the slides for Qiaoye's lane: the model result (A vs B detection per scheme), the three monitoring cases and the agent note | A short markdown outline with real numbers from P1 and P4 | P1, P4 |

**The three demo cases (use them as tests).**

1. **Weekend FX gap.** Build a fake "curated" slice for 3 Sep with the rows dropped as if FX were missing. `check_recon` must **fail**, and `missing_by_weekday` must show Saturday.
2. **1 Sep month-start batch.** The midnight slice of 1 Sep makes `check_drift` fail on `psi_payment_format`. The agent (with `FakeLLM`) can rate it `low` / `expected`.
3. **11 Sep tail.** The 11 Sep slice (396 rows, 58.6% laundering) fails drift (format PSI about 8.8) and anomaly. The gate returns `fail`, so the slice goes to quarantine.

## 5. Open decisions (ask Qiaoye; don't decide alone)

1. **Dashboard:** Plotly static site or Streamlit on EC2 (B6). EC2 costs money while it runs.
2. ~~Agent provider~~: resolved, Bedrock (D11).
3. **Whether to attempt** the stretch feature `in_3cycle_72h` after Experiment B.

## 6. Rules (the full list is in `docs/AGENT_CONTEXT.md` §11)

- **Credentials:**
  - Never open, copy, print or commit credential files.
  - In particular there is an AWS access-key CSV in `~/Downloads/Telegram Desktop/`. Do not read it.
  - AWS access goes only through `--profile aml-bq2`. Agent keys live only in `.env`.
- **No data in git** (no CSV outside `reference/`, no Parquet), and clear notebook outputs before Qiaoye uploads anything.
- **No pushing.** No paid AWS resources (EC2, Glue, SageMaker, MWAA) without asking. Nothing from Marketplace, Redshift, RDS or QuickSight.
- **Stay in Qiaoye's folders:** `notebooks/`, `airflow/`, `monitoring/`, `agent/`, `site/monitor.html`, `tests/`. Owen's SQL and Lambdas and Neeharika's marts and `index.html` are theirs; suggest changes, don't make them.
- **Numbers:** keep `is_laundering`, `pattern` and `attempt_id` out of model inputs; drop the 11–18 Sep tail from the model and marts; split by time. Quote only measured numbers, and say where each came from.

## 7. When you stop, report back with

1. Which of P1–P9 are done, with file paths.
2. The A vs B metrics and the detection-per-scheme table, if P1 ran.
3. What you found when checking B1 and B4.
4. Anything that contradicts a number in `docs/AGENT_CONTEXT.md`.

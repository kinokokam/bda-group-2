# Agent handover: Qiaoye's lane, status as of 2 Nov 2026

**Who you work for:** Qiaoye. Qiaoye's lane is the XGBoost notebook, the Airflow DAG, the four checks, the check report and the monitor page. As of 23 Oct the dashboard is Helical Insight, which also hosts the monitor page and the AI chat.

**Read these first, in this order:**

1. This file: what is done, what was decided, and what comes next.
2. `CONTEXT.md`: the project's vocabulary (scheme, live score, slice, gate, quarantine, check report, escalation, snapshot).
3. `docs/AGENT_CONTEXT.md`: the whole project. §8 is the current monitor design; §10.12–10.18 record the 13 Oct–2 Nov decisions.
4. `notebooks/AGENT_HANDOVER.md`: the original notebook spec.

**First thing to run:** `python .claude/skills/project-progress/scripts/check_progress.py` from the repo root. It may still describe the hourly build; trust this file over it.

---

## 0. Where the work is (git)

- **Updated 1 Nov (new laptop).** All the earlier `qiaoye/*` work is merged into GitHub `main`. Ryan's 27 Oct reorganisation moved BQ2 code and docs under `bq2/`.
- **P3–P5 (2 Nov)** are on branch `qiaoye/p3-p5-daily`, committed, waiting for Qiaoye to push and open the PR.
- **Git rule (1 Nov):** the agent commits on `qiaoye/*` branches only. Qiaoye pushes and opens PRs; the agent never pushes, and nothing goes straight to `main`.
- `bq2/out/` is git-ignored: replay slices, the live model, checks, check reports and the escalation log. The old laptop's `out/` was lost; S3 is now the source of truth (`AGENT_CONTEXT.md` §10.17).

## 1. Done

| # | Item | Where | State |
|---|---|---|---|
| P1 | XGBoost notebook, Experiment A vs B | `notebooks/aml_xgboost.ipynb`, `out/model/` | Done. A PR-AUC 0.170, B 0.236 (test 8–10 Sep). Result tables uploaded to `s3://aml-bq2/marts/model/` on 19 Oct (CSVs only). |
| P2 | Monitoring hand-off | `bq2/out/live_model/`, `s3://aml-bq2/monitor/live_model/` | Done, replaced by the live model: `scripts/train_live_model.py`, threshold 0.953098 (top 0.5% of 7 Sep). Retrained 1 Nov on XGBoost 3.4.1. |
| P3 | Replay, daily | `monitoring/replay.py`, `tests/test_replay.py` | Done 2 Nov: 11 daily slices (1–10 Sep plus 11 Sep). |
| P4 | Curate, score, four checks, gate | `monitoring/curate.py`, `features.py`, `run.py`, `checks.py`, `tests/test_checks.py`, `tests/test_demo_cases.py` | Done 2 Nov: pass or fail only, normal days of the same kind, volume check (hot-bank rule dropped). 1, 2 and 11 Sep fail; 3–10 Sep pass. |
| P5 | Check report | `monitoring/report.py`, `tests/test_report.py` | Done 2 Nov: filled in by code from a fixed template, facts only, no model. Claude agent removed. |
| P8 | Escalations | `monitoring/escalations.py`, `tests/test_escalations.py` | Done 2 Nov: every quarantined slice's report goes to `out/escalations.log`. |
| — | Glossary and agent-skills setup | `CONTEXT.md`, `CLAUDE.md`, `docs/agents/` | Done. |

Tests: 84 pass (`.venv/bin/python -m pytest -q tests`, about 2.5 minutes). The demo-case tests need the replay slices and the live model in `out/`.

Outputs of the 2 Nov run are in `bq2/out/monitor/` and on `s3://aml-bq2/monitor/` (`checks/`, `reports/`, `escalations.log`, `quarantine/slice=<day>/` for 1, 2 and 11 Sep).

## 2. Decisions of 1–2 Nov (details in `AGENT_CONTEXT.md` §10.18)

1. Daily slices, pass or fail only, each day compared with the other normal days of its kind, never itself.
2. The anomaly check became a volume check; the per-day hot-bank rule was dropped (it fired on 10 of 11 days). Account-level alert concentration is a later idea.
3. No model in the monitor. Code writes the check report from a fixed template; Airflow runs it (`report_slice`).
4. The report states facts only: no verdict, severity or suggested action. The risk team decides what a failed day means.
5. Every quarantined slice escalates. Escalations show on the dashboard; Slack is an open decision.
6. The AI is the Helical chat on Bedrock (P7b).

## 3. Next tasks

| # | Task | Notes |
|---|---|---|
| P6 | Airflow locally in Docker | Monitor DAG `aml_bq2_demo` (the BQ2 demo: Grid view, then 1 Sep vs 5 Sep in Graph view) done 4 Nov in the team's shared stack: `docker compose up -d` from the repo root, UI at http://localhost:8080 (admin / admin by default). All 11 days match the 2 Nov reports (`AGENT_CONTEXT.md` §10.19); results on `s3://aml-bq2/monitor/`. Monitor setup before a fresh run: the daily slices and live model in `bq2/out/`, plus `aws s3 cp s3://aml-bq2/reference/fx/fx_rates.csv bq2/out/fx_rates.csv`. Keep the laptop awake during runs. Batch DAG built and tested with fakes, **not run yet**: Qiaoye tells Owen and Neeharika/cnvltdfx first. |
| P7 | Helical: snapshot + monitor page | `scripts/sync_snapshot.py`: S3 → DuckDB file (format readable by Helical's `duckdb_jdbc-0.9.2`), plus monitor checks, reports and escalations tables. Then the monitor page and the analysis pages in Helical. |
| P7b | Helical AI chat on Bedrock | Patch in `AGENT_CONTEXT.md` §10.15. Copy of the patch goes in `bda-group-2/helical/`. |
| — | Architecture diagram | Redrawn 2 Nov as `bq2/docs/architecture_v2.drawio`; check it renders and fix layout. Changes from the original: FX from the data (not the Frankfurter Lambda), Basel score as a fixed column, Helical in Docker instead of Streamlit on EC2, daily two-DAG Airflow, no Claude agent in the monitor (check report + escalations), Claude in the Helical chat, S3 `monitor/`. Qiaoye asked on 2 Nov to do it later. |
| — | Demo video (option, decided later) | Qiaoye, 5 Nov: instead of only showing finished runs, film a replay (unpause `aml_bq2_demo`, watch the 11 days run, Helical refreshing from the snapshot) and play the video as the "live demo". A video removes the on-stage risk (laptop sleep broke the 2 Sep upload once). Needs P7 (snapshot sync, Helical page) first. Supersedes "pre-run, not live" (AGENT_CONTEXT §10.16) only if chosen. |
| P9 | Slides | Use `AGENT_CONTEXT.md` §3 numbers and §10.6, §10.10, §10.12, §10.16. |
| — | Run the notebook once in `AML_MODE=aws` | So "XGBoost reads the clean table from S3" is literally true. |

## 4. Open questions for Qiaoye

1. When to tell Neeharika and the team that Helical replaces the planned dashboard.
2. ~~Who builds the five remaining Athena marts~~ Resolved 1 Nov: cnvltdfx wrote them (PRs #1 and #2), and all six were run in Athena. Tell cnvltdfx they're built, and that `bank_concentration`'s header numbers need updating (§10.17).
3. Whether to send Owen the note about the five curated-pipeline fixes (drafted 19 Oct; Qiaoye sends it).
4. Where the code can be pushed (waiting on IT).

## 5. Known issues

- macOS swap was nearly full during the 19 Oct runs; check `sysctl vm.swapusage` if runs crawl.
- `notebooks/README.md` still quotes the old leaky baseline.
- `uv.lock` was rewritten by a newer uv (lock revision 2 → 5); teammates on an old uv may need to upgrade.

## 6. Rules

- **Credentials:** never open, copy or print credential files or `.env`. AWS access only through `--profile aml-bq2`. Qiaoye creates the separate AWS folder for the Helical container.
- **Git:** the agent commits on `qiaoye/*` branches and never pushes; Qiaoye pushes and opens PRs. Never straight to `main`/`master`.
- **No data in git.** Clear notebook outputs before committing.
- **No paid AWS resources** (EC2, Glue, SageMaker, MWAA) without asking.
- **Numbers:** quote only measured numbers and say where each came from.

# Agent handover: Qiaoye's lane, status as of 26 Oct 2026 (git, P2 and marts updated 1 Nov)

**Who you work for:** Qiaoye. Qiaoye's lane is the XGBoost notebook, the Airflow DAG, the four checks, the Claude triage agent and the monitor page. As of 23 Oct the dashboard is Helical Insight, which also hosts the monitor page and the AI chat.

**Read these first, in this order:**

1. This file: what is done, what was decided, and what comes next.
2. `CONTEXT.md`: the project's vocabulary (scheme, live score, slice, gate, quarantine, triage note, snapshot).
3. `docs/AGENT_CONTEXT.md`: the whole project. §10.12–10.16 record the 13–26 Oct decisions.
4. `notebooks/AGENT_HANDOVER.md`: the original notebook spec.

**First thing to run:** `python .claude/skills/project-progress/scripts/check_progress.py` from the repo root. It still reports P3–P5 as done for the hourly build; see §2 for the changes the 26 Oct decisions require.

---

## 0. Where the work is (git)

- **Updated 1 Nov (new laptop).** All the earlier `qiaoye/*` work is merged into GitHub `main`. Ryan's 27 Oct reorganisation moved BQ2 code and docs under `bq2/`.
- **Git rule (1 Nov):** the agent commits on `qiaoye/*` branches only. Qiaoye pushes and opens PRs; the agent never pushes, and nothing goes straight to `main`.
- `bq2/out/` is git-ignored: replay slices, the live model, checks, notes and the alert log. The old laptop's `out/` was lost; S3 is now the source of truth (`AGENT_CONTEXT.md` §10.17).

## 1. Done

| # | Item | Where | State |
|---|---|---|---|
| P1 | XGBoost notebook, Experiment A vs B | `notebooks/aml_xgboost.ipynb`, `out/model/` | Done. A PR-AUC 0.170, B 0.236 (test 8–10 Sep). Result tables uploaded to `s3://aml-bq2/marts/model/` on 19 Oct (CSVs only). |
| P2 | Monitoring hand-off | `bq2/out/live_model/`, `s3://aml-bq2/monitor/live_model/` | Done, replaced by the live model: `scripts/train_live_model.py`, threshold 0.953098 (top 0.5% of 7 Sep). Retrained 1 Nov on XGBoost 3.4.1. |
| P3 | Replay | `monitoring/replay.py`, `tests/test_replay.py` | Built **hourly** (241 slices). Needs the switch to daily (§2). |
| P4 | Curate, score, four checks, gate | `monitoring/curate.py`, `features.py`, `run.py`, `checks.py`, `tests/test_demo_cases.py` | Built. Needs pass/fail only and daily baselines (§2). |
| P5 | Triage agent | `agent/triage_agent.py`, `tests/test_agent.py` | Built: `FakeLLM`, token usage, verdict definitions, `BEDROCK_REGION`. Needs "no agent when every check passes" (§2). |
| P8 | Alert log | `monitoring/alerts.py`, `tests/test_alerts.py` | Done. `out/alerts.log` has the real high-severity notes. |
| — | Glossary and agent-skills setup | `CONTEXT.md`, `CLAUDE.md`, `docs/agents/` | Done. |

Tests: 22 pass (`.venv/bin/python -m pytest -q tests`). The demo-case tests need the replay slices and the live model in `out/`.

Real agent notes (19–21 Oct, Haiku 4.5 via `us-east-1`): month-start batch = expected, 11 Sep tail = data issue. About 1,700–2,000 tokens each. Notes in `out/monitor/notes/`; the first run, before the verdict definitions, is kept in `out/alerts.before_verdict_rules.log`.

## 2. Decided but not yet built (24–26 Oct)

1. **Daily slices.** One slice per day, 1–10 Sep plus 11 Sep (11 slices). Change `slice_id`, `slice_bounds` and the tests in `replay.py`; in `run.py`, compare each day with normal days of the same type (weekday: 2, 5–9 Sep; weekend: 3–4 Sep), and drop the "so far today" scoring loop over earlier hourly slices.
2. **Pass or fail only.** Remove the drift warning level (`PSI_WARN`, `_status`): PSI above 0.25 fails, anything else passes.
3. **Agent only on failure.** In `triage_slice`, if every check passes, write an "All checks passed" note with no model call and no alert.
4. **Only natural failures.** No injected weekend-FX incident in the demo. Drop `test_weekend_fx_gap_fails_recon_on_saturday` or keep it as a unit test of recon only, not a demo case.
5. **Pre-run.** Run every slice through Airflow before the presentation; show the finished runs and the Helical monitor page.

Expected after the switch: 2 failing slices (1 Sep, month-start batch, and 11 Sep, tail), so 2 agent notes. Measure this; the hourly run failed all 24 hours of 1 Sep plus the tail and passed every hour of 2–10 Sep.

## 3. Next tasks

| # | Task | Notes |
|---|---|---|
| — | Build §2 | First. Then re-run the 11 slices and the tests. |
| P6 | Airflow locally in Docker | Split into two DAGs: batch (once) and monitor (one slice per run). Today `publish >> replay_slice` would rerun the batch every slice. Compute the four checks once instead of four times. `load_slice`, `build_marts` and `quarantine` are still `EmptyOperator`. |
| P7 | Helical: snapshot + monitor page | `scripts/sync_snapshot.py`: S3 → DuckDB file (format readable by Helical's `duckdb_jdbc-0.9.2`), plus monitor checks and notes tables. Then the monitor page and the analysis pages in Helical. |
| P7b | Helical AI chat on Bedrock | Patch in `AGENT_CONTEXT.md` §10.15. Copy of the patch goes in `bda-group-2/helical/`. |
| P9 | Slides | Use `AGENT_CONTEXT.md` §3 numbers and §10.6, §10.10, §10.12, §10.16. |
| — | Run the notebook once in `AML_MODE=aws` | So "XGBoost reads the clean table from S3" is literally true. |

## 4. Open questions for Qiaoye

1. When to tell Neeharika and the team that Helical replaces the planned dashboard.
2. ~~Who builds the five remaining Athena marts~~ Resolved 1 Nov: cnvltdfx wrote them (PRs #1 and #2), and all six were run in Athena. Tell cnvltdfx they're built, and that `bank_concentration`'s header numbers need updating (§10.17).
3. Whether to send Owen the note about the five curated-pipeline fixes (drafted 19 Oct; Qiaoye sends it).
4. Where the code can be pushed (waiting on IT).

## 5. Known issues

- `out/monitor/notes/2022-09-11.json` lists `-10` as an unverified number; it was written before the guardrail fix. Re-running that slice clears it.
- macOS swap was nearly full during the 19 Oct runs; check `sysctl vm.swapusage` if runs crawl.
- `notebooks/README.md` still quotes the old leaky baseline.

## 6. Rules

- **Credentials:** never open, copy or print credential files or `.env`. AWS access only through `--profile aml-bq2`. Qiaoye creates the separate AWS folder for the Helical container.
- **Git:** the agent commits on `qiaoye/*` branches and never pushes; Qiaoye pushes and opens PRs. Never straight to `main`/`master`.
- **No data in git.** Clear notebook outputs before committing.
- **No paid AWS resources** (EC2, Glue, SageMaker, MWAA) without asking.
- **Numbers:** quote only measured numbers and say where each came from.

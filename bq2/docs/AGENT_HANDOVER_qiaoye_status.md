# Agent handover: Qiaoye's lane, status as of 5 Nov 2026

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
- **P3–P5 (2 Nov)** merged as PR #7. **P6** pushed by Qiaoye on `qiaoye/p6-airflow`. **P7** is on `qiaoye/p7-helical` (branched from P6), committed, not pushed.
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
| P7 | Helical: snapshot + monitor page + analysis page | **Snapshot done 5 Nov** (`pipeline/snapshot.py`, `sync_snapshot` task, read through Helical's swapped 1.5.6 driver; §10.20). Data source and metadata done in Helical; reports in progress, see §3a. Qiaoye tells Neeharika that Qiaoye builds the analysis page. |
| P7b | Helical AI chat on Bedrock | Patch in `AGENT_CONTEXT.md` §10.15. Copy of the patch goes in `bda-group-2/helical/`. |
| — | Architecture diagram | Redrawn 2 Nov as `bq2/docs/architecture_v2.drawio`; check it renders and fix layout. Changes from the original: FX from the data (not the Frankfurter Lambda), Basel score as a fixed column, Helical in Docker instead of Streamlit on EC2, daily two-DAG Airflow, no Claude agent in the monitor (check report + escalations), Claude in the Helical chat, S3 `monitor/`. Qiaoye asked on 2 Nov to do it later. |
| — | Demo video (option, decided later) | Qiaoye, 5 Nov: instead of only showing finished runs, film a replay (unpause `aml_bq2_demo`, watch the 11 days run, Helical refreshing from the snapshot) and play the video as the "live demo". A video removes the on-stage risk (laptop sleep broke the 2 Sep upload once). Needs P7 (snapshot sync, Helical page) first. Supersedes "pre-run, not live" (AGENT_CONTEXT §10.16) only if chosen. |
| P9 | Slides | Use `AGENT_CONTEXT.md` §3 numbers and §10.6, §10.10, §10.12, §10.16. |
| — | Run the notebook once in `AML_MODE=aws` | So "XGBoost reads the clean table from S3" is literally true. |

## 3a. P7 Helical build (as of 5 Nov)

Qiaoye builds the Helical pages by hand in the browser; the agent guides from screenshots. Claude in Chrome never reached the ShopBack Claude Code session, so the agent cannot click for Qiaoye. Reports can't be generated either: Helical 7.0 stores them across many tables in its own Postgres, and writing those directly risks corrupting it.

- **Helical:** `https://localhost/hi-ee`, `hiadmin` / `hiadmin`, installed in `~/helical/helicalinsight-7.0.0-docker/` (AGENT_CONTEXT §10.20).
- **Done:** data source `aml_bq2_snapshot` (DuckDB, URL `jdbc:duckdb:/snapshot/aml.duckdb;access_mode=READ_ONLY`); metadata `aml_bq2` with all 20 snapshot tables, no joins.

**Reports (trimmed plan, agreed 5 Nov):**

| # | Page | Report | Table | Visual | Status |
|---|---|---|---|---|---|
| 01 | Monitor | Check results by day | `monitor_slices` | Table (slice, weekday, quarantined, recon, quality, drift, volume), 20 rows per page | Done, saved unsorted |
| 02 | Monitor | Transfers per day | `monitor_daily` | Bar: `day` on Columns, `transfers` on Rows | In progress |
| 04 | Monitor | Payment format mix | `monitor_format_mix` | Stacked bar: `day`, `pct`, split by `payment_format` | To do |
| 05 | Monitor | Check report | `monitor_reports` | Table: slice, failed_checks, summary | To do |
| 06 | Monitor | Escalations | `monitor_escalations` | Table: slice, weekday, failed_checks, summary | To do |
| A1–A6 | Analysis | One chart per mart | `mart_channel_lift`, `mart_currency_risk`, `mart_bank_concentration`, `mart_corridors`, `mart_amount_hour`, `mart_typology_profile` | Chart plus a one-sentence finding (Q54) | To do |
| A7 | Analysis | Detection by scheme | `model_detection_by_scheme` | Bar | To do |

- **Dropped:** 03 Alerts per day (the check report already lists the top alerts, and it keeps BQ2 apart from BQ1's alert story); feature importance unless a slide needs it.
- **Then:** two dashboards (monitor page, analysis page) in the Dashboard Designer. The layout and looks are done there; the report editor always looks plain.

**Helical 7.0 tips (learned 5 Nov):**

- Adding tables to metadata: tick them, then drag or right-click the table *name* (not the tick box) into the middle panel.
- Charts need a dimension on **Columns** and a measure on **Rows** ("at least 1 dimension and 1 measure is required"). Dragging a number adds `sum_<column>`, sometimes twice (`sum_<column>_1`); remove the duplicate.
- Pick the visual in the right-hand Visualization panel; grids are under its Table section. Set a grid's page size to 20 to show all 11 slices.
- Sorting: the ÷ icon on a pill does not open a sort menu. Still to try: right-click the pill, or the ··· next to Visualization.

## 3b. P7 Helical: where the builder and designer agents stopped (late 5 Nov)

Claude in Chrome did reach this laptop's Claude Code session on 5 Nov (`switch_browser`, the Chrome window named "kinokokam"), so agents can click in Helical. It dropped mid-design; see "Open problem" below. Helical folder: `aml_bq2`.

**Builder: done.** All 12 reports (01, 02, 04, 05, 06, A1–A7) and two dashboards, `BQ2_Monitor.efwdd` and `BQ2_Analysis.efwdd`, are saved. Backups "BQ2 Monitor (pre-design)" and "BQ2 Analysis (pre-design)" are the plain versions. Every chart was checked against `bq2/out/snapshot/aml.duckdb`. A4 is a table (bank routes only: every laundering transfer stays in one currency). A6 charts median hours only. Findings in the A1–A7 titles, all measured: ACH 3,828 of 4,522 laundering transfers (85%) at 7.2x lift; Saudi Riyal 0.34%, 3.8x lift; top 95 of 30,528 sending banks send half; top 20 bank routes carry 4.6%; USD 10k–50k at 11:00 peaks at 0.54% (6.0x); RANDOM 14.6 h vs GATHER-SCATTER 75.3 h; relay features lift BIPARTITE 51.5% to 60.3% and STACK 58.4% to 66.5%, while FAN-IN falls 73.6% to 70.3%.

**Designer: partly done** (target: the green Donezo-style scheme from Qiaoye's reference image, not its name or content).
- Monitor dashboard: Dashboard Settings → ··· → Advance → CSS and JS (Enable on), plus an HTML widget as a dark-green header card; Escalations panel made taller. Copies of the exact CSS, JS and header HTML are in `helical/dashboard-theme/`.
- Report 02: green gradient on the bars. Report 04: one green or grey colour per payment format.
- **Not done:** the Analysis dashboard (paste the same CSS and JS, add its own header card, save); A1–A7 title size and colours (shorten or shrink the long titles on A3, A6, A7; green gradient for single-measure bars; per-category colours where there is a split); A2's clipped y-axis; `sum_` headers on 05 and A4. An unsaved edit on A1 was lost.
- Limits found: charts are drawn on canvas, so CSS can't recolour them; custom colour palettes don't apply (use Color → Gradient or Field Value); no rounded bars or hatched fills.

**Open problem (5 Nov, late).** Qiaoye saw the styled Monitor dashboard disappear. The Helical database shows `BQ2_Monitor` last saved at 15:23 UTC (23:23 SGT), earlier than the header-card file the designer made at 23:42, so the final save may not have happened. The Chrome window also dropped, and the browser now connected shows an error page for `https://localhost/hi-ee` although Helical answers 200 from the shell. Check first which browser and which dashboard file Qiaoye is looking at; if the styling is really gone, re-apply it from `helical/dashboard-theme/`.

**Bedrock chat (P7b): patch prepared, not applied.** `helical/instantbi-bedrock/` (`apply.py`, a `BedrockFactory`, a token-usage factory, a compose add-on): Claude Haiku 4.5 in `us-east-1`, signing in through the `aml-bq2` profile folder only (no key stored), Python 3.11 container. A real Bedrock call with that profile worked. Apply it only after the blind review, because it restarts the Instant BI container.

**Next:** finish the design, run the blind review of both dashboards, then apply the Bedrock patch and test asking the chat for a dashboard.

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

## Bedrock chat (P7b): applied, 6 Nov

- **Applied** with `python helical/instantbi-bedrock/apply.py`, then `docker compose up -d --force-recreate --no-deps instantbi` in `~/helical/helicalinsight-7.0.0-docker/`. Only the Instant BI container was recreated (`hiee`, nginx and Postgres untouched). The container is now `python:3.11-slim`, provider `bedrock`, model `global.anthropic.claude-haiku-4-5-20251001-v1:0`, region `us-east-1`, signing in through the read-only mount of `~/.aws-aml-bq2-home/.aws` with `AWS_PROFILE=aml-bq2`. No key is stored in any config. Each edited Helical file has a `.pre-bedrock` backup next to it.
- **Two bugs fixed in the patch:** (1) the unpinned requirements now pull a `langchain-community` that no longer has `ChatOllama`, so the whole app crashed at import; `apply.py` now imports it lazily in `OllamaFactory.py`. (2) `BedrockTokenUsageFactory` now overrides `from_ai_message` so `model_name` is filled in (it was `None`). The compose add-on also carries a valid healthcheck: Helical's own one has an unterminated quote and always showed `unhealthy`.
- **Measured:** a Converse call with `--profile aml-bq2` from the shell works (13 in / 4 out tokens). Inside the container, `/interactive` (called through Flask's test client with a hand-built model of `mart_channel_lift` and `monitor_daily`, because Helical has no Instant BI model yet) returned sensible DuckDB SQL: "Which payment format has the highest laundering rate?" gave `SUM(laundering_txns)/SUM(txns)` grouped by `payment_format`, ordered descending (4,399 in / 381 out tokens, 6.0 s LLM time); "Build a dashboard of transfers per day" gave `SELECT day, transfers FROM monitor_daily` (4,204 / 246, 5.4 s). Token usage is recorded with the model name.
- **Not yet tested:** the Helical UI path. Query execution, charts and the usage audit row need a real Instant BI model over the `aml_bq2` connection in Helical, which does not exist yet (the test stubbed the model fetch, so the execution step failed as expected). Next: create the model in the UI, open the chat and ask the two questions above.
- Rerun `apply.py` after any Helical upgrade; it is idempotent.

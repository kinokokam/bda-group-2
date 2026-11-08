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

## Instant BI verification, 6 Nov

- **Result: completed (second attempt, 6 Nov ~01:20-01:40 SGT).** The first attempt (browser tab contention) asked 0 questions; this one used the existing MCP tab and worked.
- **Model created and saved:** "AML BQ2 analysis" in folder `aml_bq2` (file `AML_BQ2_analysis.model`; stored in Helical's file repository, so `cube_metadata` still shows 0 rows). One cube over 6 tables (`mart_channel_lift`, `mart_amount_hour`, `mart_currency_risk`, `mart_bank_concentration`, `model_detection_by_scheme`, `monitor_daily`): 10 dimensions, 36 measures, domain "AML BQ2", topic "Laundering analysis". Fields are named like "Transfers (Channel lift)". Connect it in Instant via the cube icon in the chat box, then File Browser, `aml_bq2`, the model.
- **How the model was built (Helical UI tips):** table names cannot be dragged, but **column** rows drag into Fields with a real mouse drag (text drops into Dimensions, numbers into Measures). The JSON tab is an editable Monaco editor; the small save icon at its top right ("Save JSON") syncs edits back to the Fields panel. So bulk renames, `aiContext.instructions` and the domain/topic `components` list were done by editing the JSON. The file browser's folders open only with a dblclick event (dispatch it from JS). Save with the toolbar Save icon; click the `aml_bq2` folder row first so it saves there, not in the root. Navigating between hash routes while a page has unsaved state hangs the tab; reload `https://localhost/hi-ee/` and use the top-left menu instead.
- **Bedrock questions this run: 11** (project total 2 + 11 = 13 of 20): 2 are the automatic "Suggested" questions that Instant fires each time a model is connected (`/analyst`, about 8.4k in / 0.2-0.3k out tokens each), 1 failed (see below), 8 real questions. Each real question uses roughly 26-36k input and 0.5-0.9k output tokens (about 3-4 Haiku calls; a rough guess of 3-4 US cents per question, not measured).

| # | Question | Result vs DuckDB (`bq2/out/snapshot/aml.duckdb`) | Tokens in/out |
|---|---|---|---|
| 1 | Which payment format has the highest laundering rate? (first model) | **Incorrect.** Rate was laundering transfers divided by `total_usd` (USD, not transfers), giving Credit Card first. Truth: ACH 0.64% (3,828 of 599,685), then Bitcoin 0.038%. | 27,968 / 738 (log only) |
| 2 | Build a dashboard of transfers per day | Correct: bar chart of 8 days, all 8 values match `monitor_daily` (207,382 ... 654,463). | 25,701 / 543 |
| 1b | Same as 1, after renaming `txns` to "Transfers" and adding `aiContext.instructions` (rate = SUM(laundering_txns)/SUM(txns) of the same table) | **Correct**: ACH 3,828 / 599,685 = 0.64%, Bitcoin 0.04, Cheque 0.02, Credit Card 0.02, Cash 0.02, Reinvestment 0, Wire 0. | 34,359 / 684 |
| 3 | Which payment currency has the highest laundering rate? | Correct: Saudi Riyal 302/88,934 = 0.34%, Euro 0.10, US Dollar 0.088; top 10 match. | 35,811 / 820 |
| 4 | Show the number of laundering transfers by hour of the day | Correct: scatter of 24 hours (peak 12:00 = 288, 16:00 = 276, low 22:00 = 122). | 32,835 / 610 |
| 5 | Which 5 sending banks have the most laundering transfers? | Correct: 070 (633), 012 (64), 0119 (51), 020 (47), 001 (44); bar chart. | 32,802 / 641 |
| 6 | Compare the share of laundering transfers caught in the top 0.5 percent by pattern, for experiment A | First try failed: Bedrock `ServiceUnavailableException` after 4 retries (transient, no audit row). Retry (the one allowed): **correct**, all 9 patterns match (FAN-IN 73.6%, UNSTRUCTURED 24.1%). | 34,153 / 888 |
| 7 | Which day had the most alerts, and how many transfers were there that day? | Correct: 2022-09-06, 2,863 alerts, 482,089 transfers. | 35,077 / 622 |

- **Charts:** the chat picks a visual itself (table for lists, bar for per-day and top-N, scatter for hours); every value read matched DuckDB. Generated SQL is not shown in the UI and not logged; correctness was judged from the result columns and values.
- **Lesson:** the model picked the wrong denominator when two measures looked alike (`total_usd` vs `txns`). Plain names and `aiContext.instructions` on measures fixed it; keep them when rebuilding the model.
- **Usage audit (`hi_llm_usage_audit` in Postgres `hiee`):** rows are now written with the model name for `/interactive` and (from the second suggestion call on) `/analyst`. Bug found and fixed: the audit POST used the browser's base URL (`https://localhost/hi-ee`), which the container cannot reach (ConnectionError, so question 1's usage was lost). `apply.py` step 6c now prefers the configured in-network URL (`http://hiee:8080/hi-ee`); only the Instant BI container was recreated for it. Remaining small gaps: the very first `/analyst` row has `model_name` empty; `input_cost`, `output_cost`, `total_cost` are empty (no price configured).
- **Still to do (Qiaoye):** (1) decide if Instant BI goes in the demo; the model is a single cube with no joins, so questions that span tables will mix columns. (2) A token-based cost cap or a price in the audit table if you want costs on screen. (3) Rerun `apply.py` after any Helical upgrade (it now also carries the audit fix). (4) Instant fires 1 Bedrock call each time a model is connected (suggested questions), so avoid reconnecting needlessly. (5) An untitled blank report tab was opened by mistake once (no report was saved or changed; BQ2_Monitor and BQ2_Analysis were not touched).

## Demo video, 8 Nov

- **Made:** `~/Desktop/bda submission/BQ2_demo_v1.mp4` (7:27, 1920x1080, silent, burned-in captions) plus `BQ2_demo_v1_chapters.txt` for the YouTube description. Two parts: set up (11 steps) and run (4 steps: batch DAG, model, monitor DAG, Helical + chat), in BQ1's step order. Storyboard: `bq2/docs/demo_video_storyboard.md`; toolkit and re-make instructions: `bq2/demo_video/README.md`.
- **Filmed, not staged:** every frame is the real Airflow UI, Helical, Instant chat or a page rendered from the real repo files and captured CLI output. No secrets on screen (`.env.example` placeholders only; QA scan in `bq2/demo_video/post/QA_8nov.md`).
- **Batch DAG fixed on the way:** redeployed the `parse_patterns` Lambda (the 7 Oct copy wrote 13 columns, shifting `aml_bq2.patterns`) and let the audit tolerate `approx_percentile` drift (`pipeline/rebuild.py`); `aml_bq2_batch` ran green (`rehearsal3_20261108_1635`). The marts' medians moved by <1%; the Helical snapshot was not re-synced.
- **Bedrock:** 19 calls used of the 30 Qiaoye allowed (16 before + 1 model connect + 2 questions in the film). The Instant chat "AML BQ2 analysis" model is now connected in tab "Untitled 1"; reconnecting costs one call.
- **Docker Desktop memory raised to 8 GB** (the Airflow scheduler took 2.8 GB of the old 3.8 GB and starved everything). Airflow scheduler/webserver and Helical were restarted during the work; all healthy afterwards.

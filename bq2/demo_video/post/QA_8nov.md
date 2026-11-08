# QA of the BQ2 demo-video material (pre-recording)

Date: 8 Nov 2026. Scope: read-only check of `bq2/docs/demo_video_storyboard.md`, `recorder/beats.py`, `genstage.py` + `stage/*.html`, `captures/*.txt`, the two smoke takes and their frames. Nothing was modified. Secret values are never printed here.

Inputs checked against the repo at `/Users/qiaoye/Documents/GitHub/bda-group-2` (branch `qiaoye/p8-demo-video`): `docker-compose.yml`, `bq2/airflow/dags/*.py`, `bq2/lambdas/parse_patterns/handler.py`, `bq2/sql/01_tables.sql`, `bq2/pipeline/rebuild.py`, `bq2/monitoring/{escalations,replay,run}.py`, `bq2/out/monitor/reports/*.json`, `bq2/out/escalations.log`, `bq2/out/live_model/`, `helical/docker-compose.override.yml`, `helical/instantbi-bedrock/apply.py`, `.gitignore`, `bq2/docs/AGENT_CONTEXT.md`.

## Ranked findings

### High (fix before the real take)

**H1. Every Airflow graph beat stalls ~60-90 s on camera, and the caption bar never appears on Airflow pages.**
Evidence: `recordings/run-smoke.json` beat R1_GRAPH: v_nav 13.63 -> v_ready 107.31 (94 s for a beat whose hold is 8 s; the whole take is 110 s for 21 s of planned content). Frames cut from `run-smoke.mp4`: 30 s = blank white page, 70 s = Airflow chrome with a spinner, 109.5 s = graph loaded, **no caption bar and no badge** (2.2 s after `__cap` was called; stage-page frames show the bar fine). `recordings/run-smoke-frames/02_R1_GRAPH.png` likewise has no bar.
Root cause of the stall (verified): Airflow 2.8.1's grid bundle (`airflow/www/static/dist/grid.*.js` inside `bda-group-2-airflow-webserver-1`) passes `onlyRenderVisibleElements: true` to React Flow, so `.react-flow__node` only counts nodes inside the viewport. The graph pane shows 6 of 8 nodes (mart_typology_profile is half off-screen in the frame), so `NODES = "...length>=8"` (`beats.py` line 11) can never be true; `wait_for` runs the full 60 s `ready_timeout` (`record.py` line 51) while ffmpeg keeps recording. The same `NODES` guard is used by R1_GRAPH, R5_SEP1 and R5_SEP5 (`beats.py` lines 54, 65, 67), so expect three such stalls. The missing caption is not yet explained (dbg2-dbg4.py show it is being investigated; `dbg4.mp4` is 48 bytes, so that run failed). The slow first load (blank for >30 s) is a separate issue: the Mac was busy, and `nav()` caps its readyState loop at 15 s then falls through.
Fix:
1. `beats.py` line 11: `NODES = "document.body.innerText.includes('mart_typology_profile')"` for the batch DAG and a second guard `"document.body.innerText.includes('sync_snapshot')"` for the demo DAG graph beats (both texts are in the grid pane, which always renders), or `document.querySelectorAll('.react-flow__node').length>=1`. Add `"ready_timeout": 20` to the three Airflow graph beats.
2. Pre-warm Airflow off camera: in `record.py` after `login(p)` (line 33) navigate once to each Airflow URL used by the take before ffmpeg starts, so the JS bundle and grid API are cached.
3. Verify the caption on an Airflow page with `dbg4.py` (its ffmpeg call fails; drop ffmpeg from it) before recording: print `getComputedStyle(b).opacity` and `b.getBoundingClientRect()` after `__cap(...)`. If the bar exists but is off-screen, check whether Airflow's `html` element has a transform/zoom; if `__cap` is undefined, the re-injection in `record.py` lines 54-56 is being skipped by the `if (window.__cap) return;` guard while the `addScriptToEvaluateOnNewDocument` copy ran in a different world.
4. Post: whatever happens, cut each beat from `v_ready - 0.5` to `v_end` using the sidecar marks, so nav stalls never reach the final cut.

**H2. S1_ARCH: the bottom of the architecture diagram is hidden under the caption bar.**
Evidence: `setup-smoke-frames/01_S1_ARCH.png` and the frame at 15.5 s: the Airflow / Jupyter+XGBoost / Helical Insight boxes are cut at their lower edge, the "Risk & AML teams" actor and the "Docker" label are half covered, and the bottom of the dashed "orchestrates" box is gone. The 74 px bar covers y >= 1006, and `scale(0.87)` (`beats.py` line 21) leaves the diagram ending at about y 1060.
Fix: `beats.py` line 21: `transform='scale(0.80)'` (keeps the top margin, ends around y 975). Alternatively keep 0.87 and add `document.documentElement.scrollTop=60` after the transform. Re-cut one frame to confirm.

**H3. Part 2 card promises "chat" but no chat beat is recorded (R9 missing, as known).**
`genstage.py` line 98: card_part2 subtitle "Batch DAG -> model -> daily monitor DAG -> dashboards and chat." `card_recap` (line 99-101) does not mention chat, so it is fine. S11 ("AI chat: apply.py points ...") is a setup step and is fine on its own.
Fix: either record R9 or change line 98 to "Batch DAG -> model -> daily monitor DAG -> dashboards." and update the storyboard recap row ("dashboards and chat") to match.

### Medium

**M1. R6_ESC caption overclaims.** `beats.py` line 71: "Every quarantined day is escalated to the risk team." `bq2/monitoring/escalations.py` lines 2-6 and 20-32: `send_escalation` only writes one JSON line per quarantined slice to `out/escalations.log` ("In a real bank this would email or page the risk team; here the dashboard shows it"). Assessors reading the code will see no team is contacted.
Fix: "Every quarantined day is written to the escalation log, which the Monitor dashboard shows." (95 chars).

**M2. S5_RAW: unit and capitalisation.** `beats.py` line 32: "...and upload it to raw/: 454 MB of transfers plus the laundering pattern file." The screen (`captures/s3_raw.txt` line 2) shows `453.6 MiB`, which is 475.6 MB. The caption also starts with "..." then lowercase "and" (fails the capital-first rule).
Fix: line 31 "Ingest: download IBM AML HI-Small from Kaggle." and line 32 "Upload it to raw/: 454 MiB of transfers plus the 320 KiB laundering pattern file." (or "about 450 MiB").

**M3. R1_GRAPH framing: earlier failed rehearsals and a clipped graph are on screen.** `02_R1_GRAPH.png`: the grid pane's first two run columns are red/orange (failed and upstream_failed from earlier rehearsals) beside the selected green run, while the caption says "The run: all 8 tasks green." The graph pane shows 6 nodes; the last two marts are below the fold and the caption bar will cover more. R5_SEP1/R5_SEP5 use the same layout with 8 tasks.
Fix (pick one): add an act that collapses the grid pane (the `<<` toggle at the top-left of the pane; probe its selector with `dbg2.py`, likely `button[aria-label*="grid" i]`) so the graph gets the full width and React Flow re-fits; or add `("js", "document.body.style.zoom=0.8;1")` after ready; or at minimum reword to "This run: all 8 tasks green (the two left columns are earlier rehearsals)." Also consider a 1-run grid: the runs dropdown writes `num_runs=` to the URL, but 5 is the smallest value, so it does not hide the rehearsals.

**M4. S4 deviates from the approved storyboard and its claim-to-frame audit.** Storyboard S4 and the audit line say `.env.example` placeholders are shown ("Keys stay out of git"); `beats.py` lines 26-30 show `docker-compose.yml` lines 3-9/16-19, the DAG's `AML_BUCKET` line and the bucket listing instead. `.env.example` exists at the repo root and `.env` is ignored (`.gitignore` line 2), so the caption "secrets live in .env (git-ignored)" is true; it is just not what the storyboard promised. I did not open `.env.example`.
Fix: either add `code("s4_env_example", ".env.example", [(1, 30)])` to `genstage.py` and a beat before S4_ENV (after confirming the file holds placeholders only), or amend the storyboard's S4 row and audit bullet to "compose `env_file: .env` lines".

**M5. r4_demo_code callout text does not match its anchor.** `beats.py` line 62: callout on row 112 (`checks >> gate >> [accept, hold] >> ...`) says "One day at a time". The fact lives on line 97 of `aml_bq2_demo.py` (`max_active_runs=1, # days run in order`).
Fix: `("callout", row(97), "One day at a time", 2600)` with the scroll target `row(91)`, or keep row 112 and say "Seven tasks, one line".

**M6. r6_report highlight skips the summary line.** `genstage.py` line 131: `hl={5, 6, 7, 8, 10}`; line 9 of the report is the `summary` (the facts), line 10 is just `"biggest_shifts": [`.
Fix: `hl={5, 6, 7, 8, 9}`. Note the summary ends "The day is quarantined."; if "Facts only, no verdict" (`beats.py` line 69) is meant strictly, say "Facts only: which checks failed and by how much."

**M7. Storyboard rows only partly covered (all beats map to a row; these rows lose a sub-item).**
- S6: "then `run_athena_sql.py` usage" is not shown (only the DDL).
- S10: "the Helical data source ... `access_mode=READ_ONLY`" is not shown; only the `:ro` mount. The caption now claims only `:ro`, so it is consistent, but the storyboard audit bullet "Read-only snapshot: S10 URL" no longer holds.
- S11: "profile mount" is not shown (fine; caption does not claim it).
- R3: the storyboard wants the rendered notebook A-vs-B table; the beat shows `metrics.csv` in a terminal (`stage/r3_metrics.html`). Numbers match, see below.
- R4: "four checks, then a gate" appears only in R5_SEP5's caption; acceptable.
Fix: add the missing sub-shots or trim the storyboard rows and the audit list so Gate 1 and the film agree.

### Low

**L1. Secrets/PII scan: clean.** Grep of `stage/*.html`, `stage/arch_url.txt` and `captures/*.txt` for `AKIA...`, 12-digit numbers, e-mail addresses, password/secret/token/Fernet/api key/aws_access/aws_secret/private-key headers/Bearer/ARNs and 40+ char base64 runs found nothing (the only hits are the Pygments CSS comment "Literal.Number.Integer.Long"). Note on the premise in the brief: the compose page does **not** show the Fernet default. `stage/s4_compose_env.html` contains lines 3-9 and 16-19 with a "lines 10-15 not shown" gap, so `docker-compose.yml` line 11 (Postgres URL with default credentials) and line 12 (default Fernet key) are already excluded; keep them excluded. `s9_mounts` shows lines 20-33, which also leaves out the `.env` and `~/.aws` mounts (lines 35, 37). The report JSON shows bank ids such as `0214322`: synthetic IBM AML data, not PII. `lambda.txt` carries no account id or ARN. The bucket name `aml-bq2` and the profile name appear, as the storyboard allows.

**L2. Capitalisation.** Fails: S5_RAW (see M2) and the S7_LAMBDA callout "14 columns, incl. pattern_detail" (first letter is lowercase "c"; fix: "HEADER: 14 columns, incl. pattern_detail"). Borderline: R5_SEP1 "1 Sep: ..." and R5_SEP5 "5 Sep: ..." start with a digit; their first letter "S" is a capital, so they pass the rule as written. If the rule means first character, use "On 1 Sep drift and volume fail, so the gate sends the day to quarantine." / "On 5 Sep all four checks pass, so the day is accepted." All other captions, badges, callouts and card titles start with a capital.

**L3. Caption lengths: all pass.** Longest: S1_ARCH 134, R2_LOG 124, R4_CODE 113, R1_CODE 112, S4_ENV 109, S2_TREE 107, S7_LAMBDA 106, R4_GRID 104, S9_MOUNTS 104 chars; everything else <= 102. The smoke frames show 2-line captions fully inside the 74 px bar with a "Setup 1 of 11" badge. No typos found.

**L4. Repo text visible on screen contradicts the AWS listing.** `handler.py` lines 4 and 13 (shown in S7_LAMBDA) say `HI-Small_Patterns.txt`; S5_RAW and `aml_bq2_batch.py` line 32 use `HI_Small_Patterns.txt`. Also the deployed `Handler` is `lambda_function.lambda_handler` (`captures/lambda.txt` line 5) while the repo file is `handler.py`. Optional: fix the two docstring lines in the repo before recording (a one-line commit), or ignore.

**L5. `01_tables.sql` shows `LOCATION 's3://aml-bq2-YOUR_TEAM_NAME/...'`** (lines 23, 45, 59) while every other shot says `aml-bq2`. The file is a template (line 2 says so and `rebuild.py` substitutes the bucket), so this is correct, but a short callout on row 2 ("Template: the DAG fills in the bucket") would pre-empt the question.

**L6. r6_escalations lines are cut at the right edge.** Lines are 417, 276 and 453 chars; the window fits about 195 at 15 px mono, and `.row{white-space:pre}` prevents wrapping. The visible part still shows `escalated_at`, `slice`, `weekday`, `failed_checks` and the start of `summary`. Fix: give this page `white-space:pre-wrap` or render it as a `term()` block pretty-printed with `json.dumps(indent=2)`.

**L7. s3_uv header is synthesised.** `genstage.py` line 105 prepends "Resolved and installed the project's Python packages:" to a capture that holds only the last six package lines. Fix: capture uv's real tail (`Resolved N packages in ...` / `Installed N packages in ...`) or label the command `uv sync   # last lines`.

**L8. s2_tree label vs content.** The prompt says `tree bq2 -L 2` but the listing (built from `git ls-files`) shows `sql/marts/` as a separate level and caps each folder at 7 entries plus "... N more". Cosmetic: use `tree bq2   # source files, abridged`.

**L9. r2_marts stray row.** `captures/s3_marts.txt` line 8 `2026-10-07 14:25:26  0` is the zero-byte `marts/` folder marker. Strip it from the capture.

**L10. Unused stage pages.** `stage/r3_notebook_note.html` (renders `bq2/notebooks/README.md`, which says "Put aml_xgboost.ipynb here" and "Baseline to beat: F1 0.34, PR-AUC 0.31", numbers that would contradict R3) and `stage/s1_arch.html` are generated but not referenced by any beat. Keep them unused or delete them from `genstage.py`.

**L11. Storyboard self-inconsistency.** Row S1's caption says "Setup 1 of 9" but there are 11 setup rows; beats correctly say "of 11". Fix the storyboard text.

## Coverage: storyboard rows vs beats

| Storyboard row | Beat(s) | Status |
|---|---|---|
| 0 chapter card | OPEN (`card_open`) | covered |
| S1 architecture | S1_ARCH (diagrams.net viewer) | covered; clipped (H2) |
| S2 repo tree | S2_TREE | covered |
| S3 uv sync | S3_UV | covered (L7) |
| S4 .env.example + `aws s3 ls` | S4_ENV, S4_BUCKET, S4_S3 | covered with substitution (M4) |
| S5 download script + raw listing | S5_DL, S5_RAW | covered (M2) |
| S6 DDL + run_athena_sql usage | S6_DDL | partial (M7) |
| S7 handler + get-function-configuration | S7_LAMBDA, S7_DEPLOYED | covered |
| S8 monitor inputs | S8_INPUTS | covered |
| S9a mounts | S9_MOUNTS | covered |
| S9b compose up + DAG list | S9_UP, S9_DAGS | covered |
| S10 override + data source URL | S10_HELICAL | partial (M7) |
| S11 apply.py | S11_BEDROCK | covered |
| 1 chapter card | PART2 (`card_part2`) | covered (H3) |
| R1 task chain + graph | R1_CODE, R1_GRAPH | covered (H1, M3) |
| R2 audit log line + marts listing | R2_LOG, R2_MARTS | covered |
| R3 notebook A vs B | R3_MODEL (`r3_metrics`) | covered with substitution (M7) |
| R4 demo chain + grid | R4_CODE, R4_GRID | covered (M5) |
| R5 1 Sep vs 5 Sep graphs | R5_SEP1, R5_SEP5 | covered (H1, M3) |
| R6 report + escalation log | R6_REPORT, R6_ESC | covered (M1, M6, L6) |
| R7 Monitor dashboard | R7_MONITOR | covered |
| R8 Analysis dashboard | R8_ANALYSIS | covered |
| R9 Instant chat | none | **missing (known)** |
| 2 recap card | RECAP (`card_recap`) | covered |

Every beat in `beats.py` maps to a storyboard row; no orphan beats. Badge numbering is consistent: Setup 1-11 of 11, Run 1-4 of 4 (Run 4 now holds only the two dashboards).

## Caption facts verified

| Claim (beat) | Source | Result |
|---|---|---|
| "454 MB" (S5_RAW) | `captures/s3_raw.txt`: 453.6 MiB | unit wrong, see M2 |
| "320 KiB pattern file" (not claimed, available) | `s3_raw.txt` line 1 | ok |
| "PR-AUC from 0.170 to 0.236" (R3_MODEL) | `captures/metrics.txt`: A 0.16956, B 0.23618 | ok |
| "Relay features (B)" (R3_MODEL) | `bq2/docs/AGENT_CONTEXT.md` line 262 | ok |
| "11 runs, all green ... 1 to 11 Sep" (R4_GRID) | 11 files in `bq2/out/monitor/reports/`, 11 `slice=` dirs in `bq2/out/stream/`, DAG start/end 1-11 Sep 2022 | ok (green state itself must be checked live) |
| "quarantine task ran on 1, 2 and 11 Sep" (R4_GRID) | reports: quarantined true for 2022-09-01, -02, -11; `escalations.log` has exactly those three | ok |
| "1 Sep: drift and volume fail" (R5_SEP1) | `reports/2022-09-01.json` failed_checks ["drift","volume"] | ok |
| "5 Sep: all four checks pass" (R5_SEP5) | `reports/2022-09-05.json` failed_checks []; `monitoring/run.py` line 96 "All four checks" | ok |
| "11 days checked, 8 passed, 3 quarantined" (R7) | reports above | ok |
| "Python 3.12, 60 s timeout" (S7_DEPLOYED) | `captures/lambda.txt` Runtime python3.12, Timeout 60; `handler.py` line 15 | ok |
| "14 columns, incl. pattern_detail" (S7 callout) | `handler.py` lines 27-29: 14 names; `01_tables.sql` line 27 | ok |
| "all 8 tasks" (R1_GRAPH) / "six marts" (R1_CODE, R2_MARTS) | `aml_bq2_batch.py` lines 22, 52-56: 1 + 1 + 6 | ok |
| "model results in S3" (R2_MARTS) | `s3_marts.txt`: 6 mart prefixes + `model/` | ok |
| "rebuilt in staging and audited ... backed up ... published" (R2_LOG) | `pipeline/rebuild.py` lines 2-10: write -> audit -> back up -> publish | ok |
| "AML_BUCKET (default aml-bq2)" (S4_BUCKET) | `aml_bq2_batch.py` line 21 | ok |
| "raw, reference, curated, marts, monitor" (S4_S3) | `s3_root.txt` also lists athena-results/, backup/, curated_backup/, site/ | ok (caption says "one prefix per layer", not "only") |
| "Airflow 2.8" (S9_UP) | `docker-compose.yml` line 5 image 2.8.1 | ok |
| "Airflow finds the DAGs itself" (S9_MOUNTS) | `docker-compose.yml` line 21 comment: scans recursively | ok |
| "11 daily slices, live model, FX table" (S8) | `out_ls.txt`, `out_stream.txt`; `monitoring/replay.py` line 11 `build`; `scripts/train_live_model.py` exists; threshold 0.953098 = `out/live_model/score_threshold.json` 0.9530981779 | ok |
| "mounted read-only (:ro)" (S10) | `helical/docker-compose.override.yml` line 8 | ok |
| "Claude Haiku 4.5 on Amazon Bedrock. No key is stored." (S11) | `apply.py` line 15 model id, line 93 us-east-1, line 85 comment "no key; AWS profile chain" | ok |
| "secrets live in .env (git-ignored)" (S4_ENV) | `.gitignore` line 2 | ok |
| "checks, gate, accept or quarantine, report, escalation, upload, snapshot" (R4_CODE) | `aml_bq2_demo.py` lines 102-112 | ok |
| "escalated to the risk team" (R6_ESC) | `monitoring/escalations.py` | overclaim, see M1 |

## Code-shot line ranges (genstage.py)

| Page (line) | Range / hl | Shows what the caption claims? |
|---|---|---|
| s4_compose_env (106) | 3-9, 16-19 / hl 6, 7, 19 | yes: `env_file: - .env` (6-7) and `AML_FX_RATES` (19); callout row 7 lands on `- .env`. Lines 10-15 (DB URL, Fernet) correctly elided. |
| s4_dag_bucket (107) | 1-24 / hl 21 | yes: `BUCKET = os.environ.get("AML_BUCKET", "aml-bq2")` |
| s5_download (109) | 1-40 / none | yes: docstring, dataset id, both file names; scroll to end stops at line 40 (`main()` mid-body), acceptable |
| s6_ddl (111) | 1-60 / hl 28-46 | yes: the `patterns` table; callout row 29 (`attempt_id`) under comment line 26 "One row per laundering transfer". Placeholder bucket name visible (L5) |
| s7_lambda (112) | 1-75 / hl 27-29 | yes: `HEADER` with 14 names; callout row 27 |
| s9_mounts (119) | 20-33 / hl 23, 28-32 | yes: `./bq2/airflow/dags`, monitoring, pipeline, sql, reference, out; bq1 mounts left unhighlighted so the "BQ2's DAGs join BQ1's" callout on row 23 reads right |
| s10_override (122) | 1-30 / hl 8 | yes (file is only 8 lines): the `:/snapshot:ro` mount; callout row 8 |
| s11_bedrock (123) | 1-22, 88-96 / hl 15, 92, 93 | yes: MODEL constant, `model:` and `region: us-east-1`; callout row 15 |
| r1_batch_code (126) | 44-58 / hl 58 | yes: `patterns >> curated >> marts`; callout row 58 |
| r3_notebook_note (128) | 1-40 | unused page; would contradict R3 if shown (L10) |
| r4_demo_code (130) | 91-112 / hl 112 | yes for the chain; callout text mismatch (M5) |
| r6_report (131) | 1-40 / hl 5, 6, 7, 8, 10 | `failed_checks` block highlighted; line 10 should be 9 (M6); callout row 7 (`"volume"`) |
| r6_escalations (132) | 1-3 | yes, but lines are cut at ~195 chars (L6) |

## Smoke frames

- `setup-smoke-frames/00_OPEN.png`, `run-smoke-frames/00_PART2.png`: cards render cleanly, no caption bar (correct, cards carry no cap).
- `01_S1_ARCH.png`: diagram bottom row hidden under the bar (H2).
- `02_S2_TREE.png` and the 25.2 s frame: terminal window ends above the bar (window inset-bottom 92 px vs bar 74 px); the scroll reaches `notebooks/aml_xgboost.ipynb`; nothing clipped. Two-line caption fits.
- `01_R1_CODE.png`: lines 44-58 fully visible, highlight on 58 clear; two-line caption fits.
- `02_R1_GRAPH.png` and the 30 s / 70 s / 109.5 s cuts: see H1 (blank, spinner, then loaded without caption) and M3 (rehearsal failures in the grid pane, graph clipped at the bottom).
- Caption bar: 21 px text, badge left; nothing overlaps it on stage pages because `.win` stops 92 px from the bottom. Only live pages (diagrams.net, Airflow, Helical) can run under it; Helical was not in the smoke takes, so check R7/R8 frames after the first full take (the dashboards scroll to the end, so the last KPI row may sit under the bar).

## Not checked

- Helical beats (R7, R8) were not in the smoke takes; no frames to judge.
- `.env.example` contents (not opened; only its existence and `.gitignore` were checked).
- The live Airflow state (all green, run id `rehearsal3_20261108_1635`) was taken from the smoke frame, not re-queried.

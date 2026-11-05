# Blind review: Helical dashboards and chat, 6 Nov 2026

Reviewer: independent agent, read-only. Branch `qiaoye/p7-helical`. Dashboards opened in the viewer only; nothing in Helical was edited or saved. Numbers checked with Python `duckdb` (read-only) on a copy of `bq2/out/snapshot/aml.duckdb`.

## Verdict

Demo-ready after a short fix list. The look is close to the reference (green band, cream page, white rounded cards, KPI cards over the band edge, pill status cells). All numbers on both dashboards match the snapshot. Weak spots: the Analysis page has several default-coloured charts (A3 blue line, A5 pink/blue heat map, A7 blue/pink), lowercase axis titles and legends, and clipped x labels on A2. Instant BI gave one wrong answer out of three (amount band and hour). One stray file, `BQ2_Monitor_01.efwdd`, should go.

## Scores (0 to 10 against the reference)

| Item | Monitor | Analysis |
|---|---|---|
| Header band (green, greeting, large title, white pill) | 9 | 9 |
| Cream background | 9 | 9 |
| Card style (rounded, soft shadow) | 9 | 9 |
| KPI cards straddling the band | 8 | 8 |
| Fonts (Plus Jakarta Sans) | 8 | 6 (chart sub-titles and axis text use a different font and size) |
| Colour consistency | 8 | 5 (A3, A5, A7 off palette) |
| Spacing, overlap, clipping | 7 | 6 |
| Capitalisation | 7 | 5 |
| Overall | 8 | 6.5 |

## Ranked defects

Severity: H = visible in a demo, M = noticeable, L = polish.

1. H, Analysis A7 (Detection by scheme): bars are default light blue and pink, not the green palette. Axis title `sum_detection_rate_pct`, strip label `experiment` (repeated 9 times) and legend title `experiment` are lowercase. Fix: green and amber for A and B, rename the measure and dimension aliases with a capital (for example "Detection rate (%)", "Experiment").
2. H, Analysis A5 (Amount x hour): heat map uses default blue-to-pink ramp. Hour axis is cut off at 16 and needs a horizontal scroll bar. Legend title `sum_laundering_...` truncated and lowercase, axis title `amount_band` and `hour` lowercase. Fix: green ramp, widen or shrink the cells, rename aliases.
3. H, Analysis A3 (Bank concentration): line is the default blue, not green. Chart sub-title has no "A3" prefix (A1, A2, A4 to A6 do). Fix: set the line to #14574A, add the prefix or drop the prefixes everywhere.
4. M, Analysis A2 (Currency risk): x labels are clipped at the bottom of the card and overlap the "Powered by Helical Insight" footer; only every third label shows. Fix: make the card taller or rotate the labels. A1 also shows only some labels (Cash, Credit Card, Wire; Bitcoin and Cheque and Reinvestment are not labelled).
5. M, Analysis A1, A2, A6 and A3 to A7: the in-chart sub-title (the long finding sentence) is in a larger, different font and centred, which clashes with the 17px left-aligned card title. A2 and A4 titles wrap onto two lines. Fix: match the font, left-align, shorten.
6. M, Monitor, Check report: the last row (2022-09-11) is cut off at "The date is..." by the pagination footer; the card needs more height or the summary column needs a scroll. The table also shows inner scrolling.
7. M, Monitor, Escalations: card is cut at the bottom of the page; the third row (2022-09-11) is partly hidden behind an inner scroll bar and the pager is not visible.
8. M, column headers `Failed_checks` (Check report, Escalations) and chart measure names such as `sum_*` keep underscores. Capitalised, but not clean. Fix: alias to "Failed checks".
9. M, Monitor, Payment format mix: six greens and dark greens are hard to tell apart (Wire, ACH and Cash look alike); the y axis runs to 120 for a 100 percent stack; the x axis shows every second date. Fix: cap the axis at 100, add an amber for one format, label every date.
10. L, Monitor, Transfers per day: a gradient legend (0 to 700,000) with small marker icons sits next to the bars; a legend is not needed for a single series. Chart shows only 3 to 10 Sep (8 days) because `monitor_daily` holds only passed days; the three quarantined days are not shown. Say so in the card title or note.
11. L, KPI pills: `of 30,528`, `vs average` start with a lowercase letter, which breaks the capital-first rule (pills `1–11 Sep`, `3–10 Sep` start with a digit, which is fine). Fix: "Of 30,528 banks", "Vs average".
12. L, Analysis KPI card "Banks sending half" and "ACH lift" icon is the same arrow glyph as the corner button; the Monitor and Analysis KPI icons are text glyphs, not icons like the reference.
13. L, `aml_bq2` (metadata) and `BQ2_Monitor_01.efwdd` file names start with lowercase or are unclear; the metadata name is shown in the file browser.
14. L, A7 headline "relay features (B) lift BIPARTITE and STACK" is incomplete: B also lifts GATHER-SCATTER by 8.3 points (BIPARTITE 8.8, STACK 8.1, CYCLE 7.6). Fix: say "most schemes except FAN-IN and UNSTRUCTURED" or list the top three.

## Data correctness

Everything shown matches the snapshot. No mismatches.

| Shown | Source | Result |
|---|---|---|
| Monitor table, 11 days, all pass or fail cells and quarantined flags | `monitor_slices` | match |
| Monitor KPI 11, 8, 3, 4 | counts from `monitor_slices` (4 check columns) | match |
| Transfers per day, 8 bars (207,382 to 654,463) | `monitor_daily` | match |
| Payment format mix | `monitor_format_mix` (sums 99.9 to 100.1 from rounding) | match |
| Check report and Escalations text, 3 escalations | `monitor_reports`, `monitor_escalations` | match, text identical |
| ACH 85 percent, 3,828 of 4,522, 7.2x | `mart_channel_lift` (3,828 / 4,522 = 84.7 percent, lift 7.17) | match |
| Saudi Riyal 0.34 percent, 3.8x, "no other above 1.2x" | `mart_currency_risk` (Euro 1.16 is next) | match |
| 95 of 30,528 banks send half | `mart_bank_concentration` (rank 95 = 50.18 percent) | match |
| Corridors: top 20 routes 4.6 percent; every laundering transfer in one currency | `mart_corridors` (sum 4.56 percent; currency pairs all same-currency in the top 10, 15 pairs total 4,522) | match for the top 10 pairs; full "every" claim not checked row by row |
| Corridor table 24, 19, 17, 14 and shares 0.53, 0.42, 0.38, 0.31 | `mart_corridors` | match |
| A5: 10k-50k band peaks at 0.54 percent at 11:00, 6.0x | `mart_amount_hour` (0.5369, 6.03) | match |
| A6 median hours: random 14.6, gather-scatter 75.3 | `mart_typology_profile` | match |
| A7 detection rates A and B for 9 schemes | `model_detection_by_scheme` | match (bar heights read from the chart) |

KPI figures without a source in the data: none found. The "3 to 10 Sep passed" and "1, 2, 11 Sep quarantined" pill texts are hard-coded in the header HTML but agree with `monitor_slices`. The Analysis KPI texts are also hard-coded HTML, so they will not update if the snapshot changes (risk if the data is rebuilt).

## Completeness

All 12 intended reports are present and render with data.
- Monitor: Check results by day, Transfers per day, Payment format mix, Check report, Escalations (5 of 5). Note the Monitor page has no A-series and no extra reports.
- Analysis: A1 Channel lift, A2 Currency risk, A3 Bank concentration, A4 Laundering corridors, A5 Amount x hour, A6 Scheme profile, A7 Detection by scheme (7 of 7). A6 uses `mart_typology_profile` (scheme profile); the plan lists "one chart per mart" and this is present.

## Stray files (folder aml_bq2)

Reports: 01 Check results by day, 02 Transfers per day, 04 Payment format mix, 05 Check report, 06 Escalations, A1 to A7 (12 reports).
Other: AML BQ2 analysis (model), aml_bq2 (metadata), BQ2 Analysis, BQ2 Analysis (pre-design), BQ2 Monitor, BQ2 Monitor (pre-design), and `BQ2_Monitor_01.efwdd` (also titled "BQ2 Monitor", modified 6 Nov 00:39).
- Flag: `BQ2_Monitor_01.efwdd` is a duplicate or leftover of the Monitor dashboard; two items named "BQ2 Monitor" is confusing in a demo. Not deleted.
- The two "(pre-design)" copies are intended backups; consider hiding or removing them before the demo.
- There is no report 03 (dropped by design).

## Repo patch review (`helical/instantbi-bedrock/`, `helical/dashboard-theme/`)

I could not run a text search over the folders (the harness blocked a `grep` for credential words), so I read every file in full instead.
- Secrets: none found. No keys or tokens in any file. Credentials come only from the mounted `aml-bq2` profile folder (`docker-compose.instantbi.yml` lines 11 to 16), mounted read-only. The compose default path `../../.aws-aml-bq2-home/.aws` is a personal path; it is overridable with `AML_AWS_DIR`.
- Fresh clone concerns:
  1. `docker-compose.override.yml` line 8 defaults to `../../Documents/GitHub/bda-group-2/bq2/out/snapshot`, which assumes the repo lives in `~/Documents/GitHub`; `bq2/out/` is git-ignored, so a fresh clone has no snapshot until `pipeline/snapshot.py` runs. It is documented in a comment; fine.
  2. `apply.py` hard-codes `~/helical/helicalinsight-7.0.0-docker` as the default (argument overrides). It does string replacement on Helical files; if Helical changes the text, the edit silently does nothing (it prints "ok" for unchanged files, which is indistinguishable from "already applied"). Suggest a warning when a pattern is not found (lines 50 to 58, 66 to 74, 112 to 120, 128 to 131).
  3. `apply.py` line 81: the registry append assumes the file ends with a nested mapping; fine today.
  4. `docker-compose.instantbi.yml` line 2 mentions `apply.sh` but the file is `apply.py` (stale comment).
  5. Requirements are unpinned (`langchain-aws`, `boto3`, line 104); a future release can break the app the same way `langchain-community` did. Suggest pinning versions.
  6. `apply.py` line 15 hard-codes the model id `global.anthropic.claude-haiku-4-5-20251001-v1:0` and region `us-east-1`; ok but note it.
- `BedrockFactory.py`: no bug. `BedrockTokenUsageFactory.py`: reasonable fallback if `usage_metadata` is missing.
- `dashboard-theme/`: `bq2_theme.css` and `bq2_theme_analysis.css` are both 43 lines (near-identical copies); `bq2_theme.css` line 1 loads Google Fonts over the network, so the page falls back to Inter or Arial when offline (a demo risk without internet). Line 41 to 42 shift every card by 190px with `margin-top` and rely on `:has()` (needs a current Chrome). The header HTML hard-codes the KPI numbers.
- Pagination controls and the `pointer-events:none` on the hero make the "Check report" and "Detection by scheme" pill buttons non-clickable decorations; a presenter might try to click them.

## Chat (Instant BI): 3 questions asked (project total now 16 of the cap of 20)

| # | Question | Result vs DuckDB |
|---|---|---|
| 1 | Which payment format has the highest laundering rate? | Correct. ACH 3,828 of 599,685 (0.638 percent), then Bitcoin 0.038, Cash 0.022, Cheque 0.017, Credit Card 0.016, Wire and Reinvestment 0. |
| 2 | Which amount band and hour of day has the highest laundering rate? | **Incorrect.** The table listed 50k-100k at 12:00 (0.052 percent) first. The true top row in `mart_amount_hour` is 10k-50k at 11:00 (0.537 percent, then 12:00 at 0.530, 16:00 at 0.523). The chat appears to have sorted by the band label (text, descending) before the rate. |
| 3 | How many transfers were there on each day? | Correct. Bar chart of 3 to 10 Sep, all 8 values match `monitor_daily`. The chart is default blue, off the dashboard palette. |

Notes: the narrative text above each answer is generic filler and the results show unrounded numbers (0.6383351259411191). A demo should avoid questions that need a sort over two columns, or add an `aiContext.instructions` line ("rank by the rate, not the band"). The "Leave site?" prompt blocked a normal reload of the Instant tab (unsaved chat), so I used a second tab for the dashboards. I did not reconnect or edit the model.

## Must do before the demo

1. Delete or rename `BQ2_Monitor_01.efwdd` (confirm it is not the live Monitor first).
2. Recolour A3, A5, A7 and rename lowercase axis titles, legends and strip labels (defects 1 to 3).
3. Fix A2 and A1 axis label clipping and the Check report and Escalations card heights (defects 4, 6, 7).
4. Warm up the chat once and avoid the amount-band-and-hour question, or add a ranking instruction to the model.
5. Check the demo laptop for internet (the font loads from Google Fonts) and use a current Chrome.
6. Keep the header KPI HTML in sync if the snapshot is rebuilt (values are typed in).
7. I left a second Chrome tab open in the MCP tab group (Analysis viewer); close it if it is in the way.

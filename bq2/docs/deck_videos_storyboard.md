# BQ2 deck videos: storyboard (approved 8 Nov)

Three short silent videos for the presentation's Demo section; they replace deck slides 27-29.
1920x1080, burned-in captions, no voice, no music. Same recorder as the pipeline video (`bq2/demo_video/`).

| Video | Beats | Covers |
|---|---|---|
| 1. Batch analysis (~60 s) | Title card; `aml_bq2_batch` graph, all 8 tasks green; curated audit log; Helical Analysis page, one stop per chart row (A1-A2, A3-A4, A5-A6, A7) | Slides 27, 28 |
| 2. Daily monitor (~60 s) | Title card; `aml_bq2_demo` grid, 11 green runs; 1 Sep quarantined; 5 Sep accepted; Helical Monitor page: KPIs, check table, check report, escalations | Daily-monitor slide |
| 3. AI chat (~75 s) | Title card; Instant BI; connect "AML BQ2 analysis"; Q1 highest-rate payment format (ACH 0.64%); Q2 ACH + USD 10k-50k share (29.2% caught, 1.1% reviewed) | Slide 29 |

Every number in a caption is checked against `bq2/out/snapshot/aml.duckdb` or Athena. Bedrock: 3 calls for video 3 (connect + 2 questions).

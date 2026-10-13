---
name: project-progress
description: Check where Qiaoye's lane of the AML BQ2 project stands (section 0 setup fixes, tasks P1–P9, repo rules) by running a deterministic script. Use at the start of a session, before reporting status, before committing, or whenever asked "what's done / what's left / progress / status".
---

# Project progress check

Run the script from the repo root. Don't work out progress by hand:

```bash
python .claude/skills/project-progress/scripts/check_progress.py
```

Options:
- `--json` gives machine-readable output, for a report or for comparing runs.
- `--aws` adds the `aml-bq2` profile check (`aws sts get-caller-identity`). Use it only when asked about AWS, or when checking blockers B1/B4.

The script uses only the standard library and is read-only. It inspects files, git metadata and Python ASTs. It never opens `.env` or credential files, and it never calls Bedrock.

## How to use the result

1. Quote the statuses as the script printed them. Don't upgrade a `PARTIAL` to `DONE` from your own reading.
2. Statuses:
   - `DONE`: the artifact exists and passes the structural check.
   - `PARTIAL`: some pieces exist.
   - `TODO`: nothing found.
   - `WARN`: a project rule is at risk, such as data or secrets tracked in git, being on `main`, or a non-ShopBack push remote.
   - `SKIP`: not checked.
3. Raise every `WARN` before doing anything else.
4. The script checks structure only. It doesn't run tests, the notebook, or the Airflow DAG parse. Say so when you report P1, P4 or P6, and run those separately when asked.
5. If a task was finished in a way the script doesn't detect (for example, a different file name), update `scripts/check_progress.py` so the result stays deterministic. Don't override the result in your report.

## What it checks

| Section | Items |
|---|---|
| Setup (section 0) | `.venv`; `agent/triage_agent.py` Converse + dotenv edits; `.env.example` lines; `.env` exists and is git-ignored; `requirements.txt` + README; the setup commit |
| Tasks | P1 notebook cells, cleared outputs, and the 9 `out/model/` files with both A and B rows; P2 model and threshold JSON; P3–P8 DAG stubs replaced (`NotImplementedError` via AST), tests, slices, `monitor.html`, `alerts.log`; P9 `*slide*.md` |
| Rules | No CSV or Parquet tracked outside `reference/`; no secrets tracked; working tree; branch and unpushed commits; push remote is ShopBack GitLab |

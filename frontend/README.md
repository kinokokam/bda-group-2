# BQ1 analyst dashboard

Streamlit over the Pipeline B gold table, `aml_bq1_catalog.scored`, read through
Athena, and, on EC2 during a demo, the live Kafka alerts topic. It never writes.
It is built for the analyst reviewing alerts, so the monitoring pages never show
whether a transfer was actually laundering; a reviewer works without that.

| Page | Shows |
| --- | --- |
| Overview (landing) | Alerts to review, high priority, accounts involved, transactions screened; the five to review first; alerts by priority, over time and by payment format; repeat accounts. Refreshes every 15 s on a live feed |
| Alert queue | Alerts ranked by risk, filterable by priority; select one for the factors behind it and both accounts' history |
| Watch-list | Senders with two or more alerts, and a lookup for any account |
| Model performance | The only page using known outcomes: caught, precision, recall, lift, hit rate per priority tier, and the model's held-out test results. For model owners |

Priority tiers come from the calibrated risk score, an estimated likelihood of
laundering: High at 20% or more, Medium at 10–20%, Low below that (alerts start
at the 7.5% threshold). On the 31 Oct feed, 10 of the 13 High alerts were
laundering, against 16 of the 87 Low.

Amounts are shown in each transfer's own currency. The scoring job converts
only US Dollar, Euro, UK Pound, Swiss Franc, Canadian Dollar and Australian
Dollar; every other currency is stored at a rate of 1.0, so its "USD" figure
is really the amount in that currency. The dashboard undoes the job's rate for
display and adds no amounts up, since a total would mix currencies. (It showed
a "value under alert" total of the converted currencies only until 8 Nov.)

## Investigator brief (AI)

On the alert queue, **Explain this alert** asks Claude Haiku 4.5 on Amazon
Bedrock (`ai.py`) for a short brief: why the alert scored high, what the two
accounts' histories show, and what to check next. It never says whether the
transfer is laundering.

- **What is sent:** only what the page shows: the alert, its five factors (with
  notes on what each number means) and each account's 25 most recent transfers,
  oldest first, each placed against the alert. Counts (24 hours before, after,
  by day) are computed in code and given to the model, which got them wrong when
  it counted itself. The outcome label is never sent; a test checks.
- **Accuracy:** checked against the data on 8 Nov. After those fixes about half
  the briefs were exact and the rest had one small slip each (a date range or a
  repeat count off by one). The page says to check it before acting.
- **Model and region:** `BEDROCK_MODEL` (default
  `global.anthropic.claude-haiku-4-5-20251001-v1:0`) and `BEDROCK_REGION`
  (default `ap-southeast-1`). Haiku 4.5 is the model this account has a
  Marketplace agreement for. The SDK stays below 2.0, whose API version Bedrock
  accepts only for Claude 4.7 and later.
- **Credentials:** the same as Athena: `AWS_PROFILE` on a laptop, the instance
  role on EC2 (`infra/dashboard.tf` grants `bedrock:InvokeModel` on that model only).
- **Cost:** about 2,000 tokens in and 400 out, under half a cent per brief.
  It runs only on a click and is cached per alert for an hour.
- **Turn it off:** `BQ1_AI=off`.
- **Check with the team:** Claude on Bedrock is billed through AWS Marketplace,
  which the BQ2 notes list as banned by the brief (BQ2's chat uses it anyway).

## Look

The pages match the BQ2 Helical dashboards (`helical/dashboard-theme/`): a
forest-green header band with KPI cards over its edge, a cream page, white
rounded cards, Plus Jakarta Sans, green charts and coloured priority pills.

- `.streamlit/config.toml`: colours, font, radii, chart palettes and table
  colours, through Streamlit's theme. It sits next to `app.py`, so it applies
  wherever the app is started.
- `theme.css`: the band, header cards and card frames, which the theme can't set.
  Streamlit's HTML sanitiser drops the whole stylesheet if it contains a raw
  `<` before a letter, so encode any SVG in it (a test checks this).
- `theme.py`: `hero()` for each page's header, `card()` for its sections and
  the priority pill column.

The font loads from Google Fonts. Without internet the pages fall back to
Inter or Arial, as BQ2's do.

## Where the data comes from

| Shown | Source | Delay |
| --- | --- | --- |
| Overview alert figures and charts, the alert queue (EC2, demo feed) | Kafka `alerts-<run_id>` | ~2 s |
| Transactions screened, repeat accounts, account history, watch-list, model performance | Athena | ~1 min |
| Everything, on a laptop, for `backfill`, or while the stream is unreachable | Athena | ~1 min |

Flink writes each alert to `alerts-<run_id>` as it scores it, but the Iceberg
table only commits at each 60-second checkpoint. `live.py` reads the topic: one
background consumer per process follows every `alerts-*` topic from its start,
keeps only the alert-queue columns (never `label`), and drops resent copies by
`txn_id`. It joins no consumer group, so it commits nothing.

The brokers are private to the VPC, so only the EC2 instance can read them. It
turns live alerts on with `MSK_CLUSTER`, which its service sets; without it, or
until the consumer connects, every page reads Athena exactly as before. Only
alerts are streamed, so figures that need transfers that did not alert (screened
counts, recall, history) stay on Athena and run about a minute behind.

On a live feed the overview refreshes every 2 s. The alert queue keeps the list
it loaded, so a selected alert never moves, and says how many new alerts
arrived; **Refresh** adds them. A new feed appears in the data window picker as
soon as `predemo` creates its topics, and the picker switches to it within
about 30 seconds.

## Credentials

- **Laptop:** the AWS CLI profile `bda` in `~/.aws/`. The dashboard takes the
  profile from `AWS_PROFILE` in the shell and **does not read the root `.env`**,
  whose BQ2 template sets `AWS_PROFILE=aml-bq2`, a different account.
- **EC2:** no keys. The instance role from `infra/dashboard.tf` can query Athena,
  read `gold/`, `model/` and `evaluation/`, look up the MSK brokers and read the
  `alerts-*` topics, nothing else.

## Run it locally

From the repository root, with the `bq1` group installed (`uv sync --group bq1`):

```bash
AWS_PROFILE=bda uv run streamlit run frontend/app.py          # bash
```

```powershell
$env:AWS_PROFILE = "bda"; uv run streamlit run frontend/app.py # PowerShell
```

The `backfill` run (8 Sep to 10 Sep 11:00) stays in the table after teardown,
so the dashboard works without MSK or Flink running; Athena is the only cost.
A demo run appears in the run picker once `pipeline.sh predemo` and `demo` have
written to it, and disappears at `teardown`.

## Run it on EC2

1. Put your address in `infra/terraform.tfvars`:
   `dashboard_allowed_cidr = "<output of curl https://checkip.amazonaws.com>/32"`.
   Without it no instance is created. Streamlit has no login, so keep it to
   your address.
2. `./bq1/scripts/pipeline.sh predemo` packages `frontend/`, uploads it to
   `s3://aml-bq1/artifacts/dashboard.zip`, and the apply creates the instance.
   It prints `dashboard: http://<ip>:8501`; allow about three minutes for it to
   install before the page answers.
3. `./bq1/scripts/pipeline.sh teardown` destroys the instance with the cluster.

After changing the code, `./bq1/scripts/pipeline.sh dashboard-redeploy`
re-uploads and replaces the instance (new IP). If your address changes, edit
`dashboard_allowed_cidr` and run `pipeline.sh provision`; the security group
updates in place.

The instance costs about 3 ¢/hour (t3.small plus its public IPv4).

### When the page does not answer

No SSH; use Session Manager:

```bash
aws ssm start-session --target <instance id> --profile bda
sudo tail -50 /var/log/cloud-init-output.log   # install
sudo journalctl -u aml-dashboard -n 50         # the app
```

## Tests

`pytest tests/test_dashboard.py` covers the Athena result parsing, parameter
quoting, priority tiers, readable factor names, currency handling (including
that its rates match `bq1/flink/features.py`), and that no monitoring query
reads the outcome column. `pytest tests/test_live.py` covers the live feed: the
fields it keeps (never `label`), dropping resent alerts, which topics it reads,
finding the brokers, falling back to Athena, and that the live overview figures
match their queries. Both use stubs and need no AWS access or broker.

# BQ1 analyst dashboard

Streamlit over the Pipeline B gold table, `aml_bq1_catalog.scored`, read through
Athena. It never writes. It is built for the analyst reviewing alerts, so the
monitoring pages never show whether a transfer was actually laundering; a
reviewer works without that.

| Page | Shows |
| --- | --- |
| Overview (landing) | Alerts to review, high priority, accounts involved, value under alert; the five to review first; alerts by priority, over time and by payment format; repeat accounts. Refreshes every 15 s on a live feed |
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
display, and the value-under-alert total counts converted currencies only.

Alerts arrive about once a minute on a live feed: the Iceberg table commits at
each 60-second Flink checkpoint. When a new feed starts, the data window picker
switches to it within about 30 seconds.

## Credentials

- **Laptop:** the AWS CLI profile `bda` in `~/.aws/`. The dashboard takes the
  profile from `AWS_PROFILE` in the shell and **does not read the root `.env`**,
  whose BQ2 template sets `AWS_PROFILE=aml-bq2`, a different account.
- **EC2:** no keys. The instance role from `infra/dashboard.tf` can query Athena
  and read `gold/`, `model/` and `evaluation/`, nothing else.

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
reads the outcome column. It uses a stubbed client and needs no AWS access.

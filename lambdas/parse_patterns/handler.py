"""
Lambda: parse_patterns  (pipeline step 2)

Reads raw/patterns/HI-Small_Patterns.txt and writes curated/patterns/patterns.csv:
one row per laundering transfer with the scheme it belongs to.

The source file groups transfers into blocks:
    BEGIN LAUNDERING ATTEMPT - FAN-OUT:  Max 16-degree Fan-Out
    2022/09/01 00:06,021174,800737690,012,80011F990,2848.96,Euro,2848.96,Euro,ACH,1
    ...
    END LAUNDERING ATTEMPT - FAN-OUT

Trigger: invoked by Airflow with {"bucket": "...", "key": "raw/patterns/HI-Small_Patterns.txt"},
or by an S3 ObjectCreated event on raw/patterns/.
Runtime: Python 3.12, 256 MB, 60 s timeout. Uses only boto3 (already in the Lambda runtime).
"""
import csv
import io
import os
import re

import boto3

s3 = boto3.client("s3")
OUT_KEY = os.environ.get("OUT_KEY", "curated/patterns/patterns.csv")
BEGIN = re.compile(r"^BEGIN LAUNDERING ATTEMPT\s*-\s*(.+?)\s*$", re.I)
HEADER = ["attempt_id", "pattern", "pattern_detail", "ts", "from_bank", "from_account",
          "to_bank", "to_account", "amount_received", "receiving_currency",
          "amount_paid", "payment_currency", "payment_format", "is_laundering"]


def parse(text):
    rows, attempt_id, pattern, detail = [], -1, None, None
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        m = BEGIN.match(line)
        if m:
            attempt_id += 1
            detail = m.group(1).replace(",", ";")          # keep the CSV clean
            pattern = detail.split(":")[0].strip().upper()  # FAN-OUT, CYCLE, STACK ...
            continue
        if line.upper().startswith("END LAUNDERING ATTEMPT"):
            pattern = None
            continue
        if pattern is None:
            continue
        parts = line.split(",")
        if len(parts) != 11:
            continue
        rows.append([attempt_id, pattern, detail] + parts)
    return rows


def lambda_handler(event, context):
    if "Records" in event:                                   # S3 event
        rec = event["Records"][0]["s3"]
        bucket, key = rec["bucket"]["name"], rec["object"]["key"]
    else:                                                    # direct / Airflow invoke
        bucket, key = event["bucket"], event["key"]

    text = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8", "replace")
    rows = parse(text)

    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(HEADER)
    w.writerows(rows)
    s3.put_object(Bucket=bucket, Key=OUT_KEY, Body=buf.getvalue().encode("utf-8"),
                  ContentType="text/csv")

    attempts = len({r[0] for r in rows})
    # Expect 370 attempts and 3,209 rows for HI-Small.
    return {"attempts": attempts, "rows": len(rows), "out": f"s3://{bucket}/{OUT_KEY}"}

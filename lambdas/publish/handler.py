"""
Lambda: publish  (pipeline step 7)

Reads each small mart through Athena and writes a fixed-name JSON file for the dashboard:
    site/data/<mart>.json   e.g. site/data/channel_lift.json

Marts are tiny (tens to hundreds of rows), so SELECT * plus get_query_results is enough.
Runtime: Python 3.12, 256 MB, 120 s. boto3 only.
"""
import json
import os
import time

import boto3

athena = boto3.client("athena")
s3 = boto3.client("s3")
BUCKET = os.environ["AML_BUCKET"]
DB = os.environ.get("ATHENA_DATABASE", "aml_bq2")
OUTPUT = os.environ.get("ATHENA_OUTPUT", f"s3://{BUCKET}/athena-results/")
MARTS = os.environ.get(
    "MARTS",
    "channel_lift,currency_risk,bank_concentration,corridors,amount_hour,typology_profile",
).split(",")


def run(sql):
    qid = athena.start_query_execution(
        QueryString=sql, QueryExecutionContext={"Database": DB},
        ResultConfiguration={"OutputLocation": OUTPUT})["QueryExecutionId"]
    while True:
        state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        raise RuntimeError(f"{sql!r} ended {state}")
    rows, token = [], None
    while True:
        kw = {"QueryExecutionId": qid, "MaxResults": 1000}
        if token:
            kw["NextToken"] = token
        page = athena.get_query_results(**kw)
        rows += page["ResultSet"]["Rows"]
        token = page.get("NextToken")
        if not token:
            break
    header = [c.get("VarCharValue") for c in rows[0]["Data"]]
    return [dict(zip(header, [c.get("VarCharValue") for c in r["Data"]])) for r in rows[1:]]


def lambda_handler(event, context):
    written = {}
    for mart in MARTS:
        data = run(f"SELECT * FROM mart_{mart}")
        body = json.dumps({"mart": mart, "updated": int(time.time()), "rows": data})
        s3.put_object(Bucket=BUCKET, Key=f"site/data/{mart}.json", Body=body.encode(),
                      ContentType="application/json", CacheControl="max-age=30")
        written[mart] = len(data)
    return written

"""
run_athena_sql.py  --  run the repo's Athena SQL files (or one query) from a laptop.

Replaces the aml-bq2-YOUR_TEAM_NAME placeholder with the real bucket (same as the DAG's
sql() helper), strips -- comments, splits on ';' and runs each statement in order,
stopping at the first failure. AWS access comes from the named profile only.

Usage
-----
    export AWS_PROFILE=aml-bq2 AWS_REGION=ap-southeast-1 AML_BUCKET=aml-bq2
    python scripts/run_athena_sql.py sql/01_tables.sql sql/02_curated_transactions.sql
    python scripts/run_athena_sql.py --query "SELECT count(*) FROM aml_bq2.curated_transactions"
"""
import argparse
import os
import re
import sys
import time

import boto3

PLACEHOLDER = "aml-bq2-YOUR_TEAM_NAME"


def statements(sql_text, bucket):
    sql_text = sql_text.replace(PLACEHOLDER, bucket)
    sql_text = re.sub(r"--[^\n]*", "", sql_text)
    return [s.strip() for s in sql_text.split(";") if s.strip()]


def run(athena, sql, database, output, workgroup):
    kw = {"QueryString": sql, "WorkGroup": workgroup,
          "ResultConfiguration": {"OutputLocation": output}}
    if database:
        kw["QueryExecutionContext"] = {"Database": database}
    qid = athena.start_query_execution(**kw)["QueryExecutionId"]
    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = q["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    stats = q.get("Statistics", {})
    info = {"id": qid, "state": state,
            "scanned_mb": round(stats.get("DataScannedInBytes", 0) / 1e6, 1),
            "seconds": round(stats.get("TotalExecutionTimeInMillis", 0) / 1e3, 1),
            "reason": q["Status"].get("StateChangeReason", "")}
    return info


def results(athena, qid):
    rows = []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for r in page["ResultSet"]["Rows"]:
            rows.append([c.get("VarCharValue", "") for c in r["Data"]])
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="*")
    ap.add_argument("--query", help="run one statement and print its result table")
    ap.add_argument("--database", default=os.environ.get("ATHENA_DATABASE", "aml_bq2"))
    ap.add_argument("--workgroup", default=os.environ.get("ATHENA_WORKGROUP", "primary"))
    a = ap.parse_args()

    bucket = os.environ.get("AML_BUCKET", "aml-bq2")
    output = os.environ.get("ATHENA_OUTPUT", f"s3://{bucket}/athena-results/")
    athena = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"),
                           region_name=os.environ.get("AWS_REGION", "ap-southeast-1")).client("athena")

    if a.query:
        info = run(athena, a.query, a.database, output, a.workgroup)
        print(f"{info['state']} ({info['scanned_mb']} MB scanned, {info['seconds']} s) {info['reason']}")
        if info["state"] == "SUCCEEDED":
            for row in results(athena, info["id"]):
                print(" | ".join(row))
        sys.exit(0 if info["state"] == "SUCCEEDED" else 1)

    for path in a.files:
        with open(path) as fh:
            stmts = statements(fh.read(), bucket)
        for i, sql in enumerate(stmts, 1):
            first = " ".join(sql.split())[:90]
            # CREATE DATABASE must not run inside the database it creates
            db = None if sql.upper().startswith("CREATE DATABASE") else a.database
            info = run(athena, sql, db, output, a.workgroup)
            print(f"{path} [{i}/{len(stmts)}] {info['state']} "
                  f"({info['scanned_mb']} MB, {info['seconds']} s): {first}")
            if info["state"] != "SUCCEEDED":
                print(f"  reason: {info['reason']}")
                sys.exit(1)


if __name__ == "__main__":
    main()

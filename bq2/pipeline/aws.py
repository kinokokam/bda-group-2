"""
Thin boto3 adapters for pipeline.rebuild: Athena queries as row dicts, and S3 list/get/copy/upload/delete.
AWS access comes from the named profile only (AWS_PROFILE=aml-bq2).
"""
from __future__ import annotations

import os
import time

import boto3


def session():
    return boto3.Session(profile_name=os.environ.get("AWS_PROFILE"),
                         region_name=os.environ.get("AWS_REGION", "ap-southeast-1"))


class Athena:
    def __init__(self, bucket: str, database: str = "aml_bq2", workgroup: str = "primary", client=None):
        self.client = client or session().client("athena")
        self.database, self.workgroup = database, workgroup
        self.output = os.environ.get("ATHENA_OUTPUT", f"s3://{bucket}/athena-results/")

    def query(self, sql: str) -> list[dict]:
        qid = self.client.start_query_execution(
            QueryString=sql, WorkGroup=self.workgroup, QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output})["QueryExecutionId"]
        while True:
            status = self.client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
            if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)
        if status["State"] != "SUCCEEDED":
            raise RuntimeError(f"Athena {status['State']}: {status.get('StateChangeReason', '')}\n{sql[:300]}")
        rows = []
        for page in self.client.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
            rows += [[c.get("VarCharValue") for c in r["Data"]] for r in page["ResultSet"]["Rows"]]
        if not rows:
            return []
        header, *body = rows
        return [dict(zip(header, r)) for r in body]


class S3:
    def __init__(self, bucket: str, client=None):
        self.client, self.bucket = client or session().client("s3"), bucket

    def keys(self, prefix: str) -> list[str]:
        out = []
        for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            out += [o["Key"] for o in page.get("Contents", [])]
        return sorted(out)

    def copy(self, src: str, dst: str) -> None:
        self.client.copy_object(Bucket=self.bucket, Key=dst, CopySource={"Bucket": self.bucket, "Key": src})

    def get(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def upload(self, path, key: str) -> None:
        self.client.upload_file(str(path), self.bucket, key)

    def delete(self, keys: list[str]) -> None:
        for i in range(0, len(keys), 1000):
            self.client.delete_objects(Bucket=self.bucket,
                                       Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]], "Quiet": True})

"""Run Athena queries against the BQ1 gold table and return typed DataFrames.

Credentials come from boto3's default chain: the profile named by `AWS_PROFILE`
in the shell on a laptop, the instance role on EC2. The root `.env` is
deliberately not read. It is shared with BQ2, whose template sets
`AWS_PROFILE=aml-bq2`, and that account has no `aml_bq1_catalog`.

Defaults match `bq1/scripts/config.sh` and `infra/`, so nothing needs setting
beyond the profile.
"""

import functools
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Optional, Union

import boto3
import pandas as pd

REGION = os.environ.get("AWS_REGION", "ap-southeast-1")
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "aml-bq1")
DATABASE = os.environ.get("GLUE_DATABASE", "aml_bq1_catalog")
BUCKET = os.environ.get("BQ1_BUCKET", "aml-bq1")

POLL_SECONDS = 0.5
TIMEOUT_SECONDS = 60
FINISHED_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}

INTEGER_TYPES = {"tinyint", "smallint", "integer", "int", "bigint"}
FLOAT_TYPES = {"double", "float", "real", "decimal"}


class QueryFailed(RuntimeError):
    """Athena finished a query without success, or it ran past the timeout."""


@functools.lru_cache(maxsize=1)
def session() -> boto3.Session:
    """Return one shared session for the process.

    Returns
    -------
    boto3.Session
        Session on the shell's `AWS_PROFILE`, or the default chain (the EC2
        instance role) when it is unset.
    """
    return boto3.Session(profile_name=os.environ.get("AWS_PROFILE") or None, region_name=REGION)


@functools.lru_cache(maxsize=1)
def athena_client():
    """Return one shared Athena client.

    Clients, unlike sessions, are safe to share between threads, which
    `query_many` relies on.

    Returns
    -------
    botocore client
        Athena client in `REGION`.
    """
    return session().client("athena")


def literal(value: Union[str, int]) -> str:
    """Render a value as an Athena execution parameter.

    Athena substitutes parameters as SQL text, so a string must arrive already
    quoted. Embedded quotes are doubled, which keeps the value a single literal.

    Parameters
    ----------
    value : str | int
        Value to bind to one `?` placeholder.

    Returns
    -------
    str
        The integer as written, or the string as a quoted literal.
    """
    if isinstance(value, bool):
        raise TypeError("booleans are not valid parameters here")
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def cast_column(values: pd.Series, athena_type: str) -> pd.Series:
    """Convert one column of Athena's string cells to its declared type.

    Parameters
    ----------
    values : pd.Series
        Cell values as strings, None where the cell was NULL.
    athena_type : str
        Type name from the result metadata, such as `bigint` or `boolean`.

    Returns
    -------
    pd.Series
        Nullable integer, float, boolean, or the strings unchanged.
    """
    kind = athena_type.lower()
    if kind in INTEGER_TYPES:
        return pd.to_numeric(values).astype("Int64")
    if kind in FLOAT_TYPES:
        return pd.to_numeric(values).astype("float64")
    if kind == "boolean":
        return values.map({"true": True, "false": False}).astype("boolean")
    return values


def to_frame(pages: Iterable[dict]) -> pd.DataFrame:
    """Build a typed DataFrame from `get_query_results` pages.

    The first row of the first page repeats the column names, so it is
    skipped; later pages carry data only.

    Parameters
    ----------
    pages : Iterable[dict]
        Responses from the `get_query_results` paginator.

    Returns
    -------
    pd.DataFrame
        One row per result row, columns cast by `cast_column`.
    """
    columns: Optional[list[tuple[str, str]]] = None
    rows = []
    for page in pages:
        result_set = page["ResultSet"]
        page_rows = result_set["Rows"]
        if columns is None:
            columns = [
                (info["Name"], info["Type"])
                for info in result_set["ResultSetMetadata"]["ColumnInfo"]
            ]
            page_rows = page_rows[1:]
        for row in page_rows:
            rows.append([cell.get("VarCharValue") for cell in row["Data"]])

    if columns is None:
        return pd.DataFrame()

    frame = pd.DataFrame(rows, columns=[name for name, _type in columns], dtype=object)
    for name, athena_type in columns:
        frame[name] = cast_column(frame[name], athena_type)
    return frame


def wait(client, query_id: str) -> None:
    """Block until a query finishes.

    Parameters
    ----------
    client : botocore client
        Athena client.
    query_id : str
        Execution id returned by `start_query_execution`.

    Raises
    ------
    QueryFailed
        If the query failed, was cancelled, or ran past `TIMEOUT_SECONDS`, in
        which case it is stopped so it stops scanning.
    """
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while True:
        status = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            return
        if state in FINISHED_STATES:
            raise QueryFailed(f"{state}: {status.get('StateChangeReason', 'no reason given')}")
        if time.monotonic() > deadline:
            client.stop_query_execution(QueryExecutionId=query_id)
            raise QueryFailed(f"timed out after {TIMEOUT_SECONDS} s")
        time.sleep(POLL_SECONDS)


def query(sql: str, params: Optional[list[Union[str, int]]] = None, client=None) -> pd.DataFrame:
    """Run one statement in the BQ1 workgroup and return its rows.

    The workgroup already names the results location, so none is passed here.

    Parameters
    ----------
    sql : str
        Statement with `?` placeholders for every value a user can influence.
    params : list[str | int], optional
        Values for the placeholders, in order.
    client : botocore client, optional
        Athena client to use; tests pass a stub.

    Returns
    -------
    pd.DataFrame
        Result rows with typed columns.

    Raises
    ------
    QueryFailed
        If the query does not succeed.
    """
    client = client or athena_client()
    request = {
        "QueryString": sql,
        "WorkGroup": WORKGROUP,
        "QueryExecutionContext": {"Database": DATABASE},
    }
    if params:
        request["ExecutionParameters"] = [literal(value) for value in params]
    query_id = client.start_query_execution(**request)["QueryExecutionId"]
    wait(client, query_id)
    pages = client.get_paginator("get_query_results").paginate(QueryExecutionId=query_id)
    return to_frame(pages)


def query_many(
    statements: dict[str, tuple[str, Optional[list[Union[str, int]]]]], client=None
) -> dict[str, pd.DataFrame]:
    """Run several statements at once and return each one's rows by name.

    Athena spends most of a query's second or two queueing, so a page that
    needs six results waits for the slowest rather than the sum.

    Parameters
    ----------
    statements : dict[str, tuple[str, list | None]]
        Name to `(sql, params)`.
    client : botocore client, optional
        Athena client to share; tests pass a stub.

    Returns
    -------
    dict[str, pd.DataFrame]
        Name to result rows.

    Raises
    ------
    QueryFailed
        If any statement fails; the first failure is raised.
    """
    client = client or athena_client()
    with ThreadPoolExecutor(max_workers=len(statements) or 1) as pool:
        futures = {
            name: pool.submit(query, sql, params, client)
            for name, (sql, params) in statements.items()
        }
        return {name: future.result() for name, future in futures.items()}


def read_json(key: str) -> dict:
    """Read a small JSON object from the BQ1 bucket.

    Parameters
    ----------
    key : str
        Object key, such as `model/config.json`.

    Returns
    -------
    dict
        Parsed content.
    """
    body = session().client("s3").get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return json.loads(body)

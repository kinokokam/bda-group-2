"""
Airflow DAG: aml_bq2_batch  (triggered by hand, once)

The one-off exposure pipeline from raw data to results:

    parse_patterns (Lambda) -> curated_transactions (Athena) -> the six marts (Athena)

Each Athena table is rebuilt safely: built into staging, audited against the live table (and, for
curated, the counts measured on 13 Oct), backed up, then published (pipeline/rebuild.py). A failed
audit stops the run with the live tables untouched. Left out on purpose: the FX table (fixed
reference data), fetch_fx (unused ECB rates), publish (the old S3 site) and the XGBoost notebook.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET = os.environ.get("AML_BUCKET", "aml-bq2")
MARTS = ["channel_lift", "currency_risk", "bank_concentration", "corridors", "amount_hour", "typology_profile"]


def parse_patterns(**_):
    """Invoke the parse_patterns Lambda: raw/patterns/ -> curated/patterns/patterns.csv."""
    import json

    from pipeline.aws import session
    out = session().client("lambda").invoke(
        FunctionName="parse_patterns",
        Payload=json.dumps({"bucket": BUCKET, "key": "raw/patterns/HI_Small_Patterns.txt"}).encode())
    body = out["Payload"].read().decode()
    if out.get("FunctionError"):
        raise RuntimeError(f"parse_patterns failed: {body}")
    return body


def rebuild(table: str, ds, **_):
    from pipeline.batch import rebuild as run
    return run(table, BUCKET, run_date=ds)


with DAG(
    dag_id="aml_bq2_batch",
    start_date=datetime(2026, 11, 1),
    schedule=None,                      # by hand: the raw data never changes
    catchup=False,
    tags=["aml", "bq2", "batch"],
) as dag:
    patterns = PythonOperator(task_id="parse_patterns", python_callable=parse_patterns)
    curated = PythonOperator(task_id="curated_transactions", python_callable=rebuild,
                             op_kwargs={"table": "curated_transactions"})
    marts = [PythonOperator(task_id=f"mart_{m}", python_callable=rebuild, op_kwargs={"table": f"mart_{m}"})
             for m in MARTS]

    patterns >> curated >> marts

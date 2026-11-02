"""
The batch DAG's tables: the curated table, then the six marts, each rebuilt safely (rebuild.py).

    python -m pipeline.batch mart_channel_lift          # rebuild one table from the laptop

The curated table must also match the counts measured when it was first built (13 Oct,
AGENT_CONTEXT.md 10.8); the marts must match their live versions.
"""
from __future__ import annotations

import argparse
import os
import re
from datetime import date
from pathlib import Path

from pipeline.rebuild import Table, rebuild_table

SQL_DIR = Path(os.environ.get("AML_SQL_DIR", Path(__file__).resolve().parents[1] / "sql"))
CURATED_KNOWN = {
    "count(*)": 5_078_336,
    "sum(is_laundering)": 5_177,
    "count_if(is_laundering = 1 AND pattern NOT IN ('NONE', 'UNSTRUCTURED'))": 3_209,
    "count_if(amount_paid_usd IS NULL)": 0,
}
MARTS = ["channel_lift", "currency_risk", "bank_concentration", "corridors", "amount_hour", "typology_profile"]


def read_sql(path: Path) -> str:
    """The file's single statement, without -- comments or the closing semicolon."""
    return re.sub(r"--[^\n]*", "", path.read_text()).strip().rstrip(";").strip()


def tables() -> list[Table]:
    curated = Table("curated_transactions", read_sql(SQL_DIR / "02_curated_transactions.sql"),
                    "curated/transactions/", partitioned=True, known=CURATED_KNOWN)
    return [curated] + [Table(f"mart_{m}", read_sql(SQL_DIR / "marts" / f"{m}.sql"), f"marts/{m}/") for m in MARTS]


def rebuild(name: str, bucket: str | None = None, run_date: str | None = None) -> dict:
    from pipeline.aws import S3, Athena
    bucket = bucket or os.environ.get("AML_BUCKET", "aml-bq2")
    table = next(t for t in tables() if t.name == name)
    return rebuild_table(table, Athena(bucket), S3(bucket), bucket, run_date or date.today().isoformat())


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("table", choices=[t.name for t in tables()])
    print(rebuild(ap.parse_args().table))

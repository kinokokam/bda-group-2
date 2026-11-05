"""
Safe rebuild of one Athena table: write -> audit -> back up -> publish (decided 2 Nov).

    1. write    run the table's CTAS into a staging table under s3://<bucket>/staging/<prefix>
    2. audit    the staging table must match the live one (row count and the sum of every numeric
                column, within rounding), plus any known values; on failure stop, live untouched
    3. back up  copy the live files to backup/<run date>/<prefix>
    4. publish  replace the live files with the staging files; a partitioned table re-reads its
                partitions. The table definition is never dropped, so readers keep the same name and path.
    5. clean    drop the staging table and its files

The publish copy is not atomic: for a few seconds the live folder is half old, half new. Fine for a
one-off batch run nobody reads during; versioned folders and a view switch would avoid it.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

DB = "aml_bq2"
PLACEHOLDER = "aml-bq2-YOUR_TEAM_NAME"
NUMERIC = ("bigint", "integer", "int", "smallint", "tinyint", "double", "real", "float", "decimal")


class AuditFailed(RuntimeError):
    pass


@dataclass
class Table:
    name: str                                   # e.g. curated_transactions, mart_channel_lift
    sql: str                                    # the CTAS file's text
    prefix: str                                 # live S3 folder, e.g. curated/transactions/
    partitioned: bool = False
    known: dict[str, float] = field(default_factory=dict)   # SQL expression -> exact expected value


def staging_sql(table: Table, bucket: str) -> str:
    """The table's CTAS with only the table name and its location changed to staging."""
    sql = re.sub(rf"CREATE TABLE {DB}\.{table.name}\b", f"CREATE TABLE {DB}.{table.name}_staging", table.sql, count=1)
    return sql.replace(f"s3://{PLACEHOLDER}/{table.prefix}", f"s3://{bucket}/staging/{table.prefix}")


def _same(a, b) -> bool:
    if a in (None, "") or b in (None, ""):
        return a == b
    return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-6)


def fingerprint(athena, name: str, live: str) -> dict:
    """Row count and the sum of every numeric column of `name`, using the live table's columns."""
    cols = athena.query(f"SELECT column_name, data_type FROM information_schema.columns "
                        f"WHERE table_schema = '{DB}' AND table_name = '{live}'")
    sums = [f'sum(CAST("{c["column_name"]}" AS double)) AS "{c["column_name"]}"' for c in cols
            if c["data_type"].split("(")[0].lower() in NUMERIC]
    return athena.query(f"SELECT count(*) AS n_rows{''.join(', ' + s for s in sums)} FROM {DB}.{name}")[0]


def audit(table: Table, athena) -> None:
    staging = f"{table.name}_staging"
    new, old = fingerprint(athena, staging, table.name), fingerprint(athena, table.name, table.name)
    wrong = [k for k in old if not _same(new.get(k), old[k])]
    if wrong:
        raise AuditFailed(f"{table.name}: staging differs from live in " +
                          ", ".join(f"{k} ({new.get(k)} vs {old[k]})" for k in wrong))
    if table.known:
        exprs = list(table.known)
        row = athena.query("SELECT " + ", ".join(f"{e} AS k{i}" for i, e in enumerate(exprs)) +
                           f" FROM {DB}.{staging}")[0]
        off = [f"{e} = {row[f'k{i}']} (expected {table.known[e]})" for i, e in enumerate(exprs)
               if not _same(row[f"k{i}"], table.known[e])]
        if off:
            raise AuditFailed(f"{table.name}: " + "; ".join(off))


def _clean_staging(table: Table, athena, s3) -> None:
    athena.query(f"DROP TABLE IF EXISTS {DB}.{table.name}_staging")
    s3.delete(s3.keys(f"staging/{table.prefix}"))


def rebuild_table(table: Table, athena, s3, bucket: str, run_date: str) -> dict:
    """athena.query(sql) -> list of row dicts; s3.keys(prefix), s3.copy(src, dst), s3.delete(keys)."""
    _clean_staging(table, athena, s3)
    athena.query(staging_sql(table, bucket))
    audit(table, athena)

    live, staged = s3.keys(table.prefix), s3.keys(f"staging/{table.prefix}")
    for key in live:
        s3.copy(key, f"backup/{run_date}/{key}")
    s3.delete(live)
    for key in staged:
        s3.copy(key, key[len("staging/"):])
    if table.partitioned:
        athena.query(f"MSCK REPAIR TABLE {DB}.{table.name}")
    _clean_staging(table, athena, s3)
    return {"table": table.name, "backed_up": len(live), "published": len(staged)}

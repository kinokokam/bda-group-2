"""
run_marts_local.py - run the Athena mart files on your local DuckDB copy.

Run build_local.py first (it makes aml_local.duckdb). Then, from the same folder:
    python run_marts_local.py marts/*.sql

For each file it turns the Athena "CREATE TABLE aml_bq2.mart_x WITH (...) AS <query>"
into a local DuckDB table, prints the result and saves marts_local/<mart>.csv.
The .sql files stay exactly as Athena needs them; only this script translates.
"""
import os
import re
import sys
import duckdb

con = duckdb.connect("aml_local.duckdb")
os.makedirs("marts_local", exist_ok=True)
head = re.compile(r"CREATE\s+TABLE\s+aml_bq2\.(\w+)\s+WITH\s*\(.*?\)\s*AS\s", re.S | re.I)

for path in sys.argv[1:]:
    sql = re.sub(r"--[^\n]*", "", open(path).read()).strip().rstrip(";")
    m = head.search(sql)
    if not m:
        print(f"skip {path}: no CREATE TABLE aml_bq2.<name> WITH (...) AS header")
        continue
    name, query = m.group(1), sql[m.end():]
    query = query.replace("aml_bq2.", "")                     # local tables have no database prefix
    query = re.sub(r"approx_percentile\(", "quantile_cont(", query, flags=re.I)  # Athena → DuckDB
    con.execute(f"CREATE OR REPLACE TABLE {name} AS {query}")
    con.execute(f"COPY {name} TO 'marts_local/{name}.csv' (HEADER)")
    n = con.execute(f"SELECT count(*) FROM {name}").fetchall()[0][0]
    print(f"\n=== {name}: {n} rows  (saved marts_local/{name}.csv)")
    print(con.sql(f"SELECT * FROM {name} LIMIT 12"))

import re

import pytest

from pipeline.rebuild import AuditFailed, Table, rebuild_table, staging_sql

CTAS = """CREATE TABLE aml_bq2.mart_channel_lift
WITH (
  format = 'PARQUET',
  external_location = 's3://aml-bq2-YOUR_TEAM_NAME/marts/channel_lift/'
) AS
SELECT payment_format, count(*) AS n FROM aml_bq2.curated_transactions GROUP BY 1"""
LIFT = Table("mart_channel_lift", CTAS, "marts/channel_lift/")
LIVE_ROW = {"n_rows": "7", "n": "5078336"}


class FakeS3:
    def __init__(self, keys):
        self.objects = set(keys)

    def keys(self, prefix):
        return sorted(k for k in self.objects if k.startswith(prefix))

    def copy(self, src, dst):
        assert src in self.objects
        self.objects.add(dst)

    def delete(self, keys):
        self.objects -= set(keys)


class FakeAthena:
    """CTAS into a staging location writes files there; fingerprint queries return a fixed row per table."""

    def __init__(self, s3, fingerprints, files=("part-1.parquet",), known_row=None):
        self.s3, self.fingerprints, self.files, self.known_row, self.sql = s3, fingerprints, files, known_row, []

    def query(self, sql):
        self.sql.append(sql)
        if sql.startswith("CREATE TABLE"):
            prefix = re.search(r"s3://[^/]+/([^']+)'", sql).group(1)
            self.s3.objects |= {prefix + f for f in self.files}
        if "information_schema.columns" in sql:
            return [{"column_name": "payment_format", "data_type": "varchar"},
                    {"column_name": "n", "data_type": "bigint"}]
        if sql.startswith("SELECT") and " AS k0" in sql:
            return [self.known_row]
        if sql.startswith("SELECT count(*) AS n_rows"):
            return [self.fingerprints[re.search(r"FROM aml_bq2\.(\w+)", sql).group(1)]]
        return []


def setup(staging_row=LIVE_ROW, files=("part-1.parquet",)):
    s3 = FakeS3({"marts/channel_lift/old-1.parquet", "marts/channel_lift/old-2.parquet"})
    athena = FakeAthena(s3, {"mart_channel_lift": LIVE_ROW, "mart_channel_lift_staging": staging_row}, files)
    return s3, athena


def test_staging_sql_only_changes_the_table_name_and_location():
    assert staging_sql(LIFT, "aml-bq2") == CTAS.replace(
        "aml_bq2.mart_channel_lift\n", "aml_bq2.mart_channel_lift_staging\n").replace(
        "s3://aml-bq2-YOUR_TEAM_NAME/marts/channel_lift/", "s3://aml-bq2/staging/marts/channel_lift/")


def test_a_rebuild_that_matches_the_live_table_replaces_it_and_keeps_a_backup():
    s3, athena = setup()
    rebuild_table(LIFT, athena, s3, "aml-bq2", "2026-11-03")
    assert s3.keys("marts/") == ["marts/channel_lift/part-1.parquet"]
    assert s3.keys("backup/") == ["backup/2026-11-03/marts/channel_lift/old-1.parquet",
                                  "backup/2026-11-03/marts/channel_lift/old-2.parquet"]
    assert s3.keys("staging/") == []
    assert athena.sql[-1] == "DROP TABLE IF EXISTS aml_bq2.mart_channel_lift_staging"


def test_a_failed_audit_leaves_the_live_table_untouched():
    s3, athena = setup(staging_row={"n_rows": "7", "n": "5078335"})
    with pytest.raises(AuditFailed, match="n"):
        rebuild_table(LIFT, athena, s3, "aml-bq2", "2026-11-03")
    assert s3.keys("marts/") == ["marts/channel_lift/old-1.parquet", "marts/channel_lift/old-2.parquet"]
    assert s3.keys("backup/") == []


def test_sums_may_differ_in_the_last_digits():
    """Athena adds doubles in no fixed order, so a rebuild can differ far below any shown digit."""
    s3, athena = setup(staging_row={"n_rows": "7", "n": "5078336.0000000001"})
    rebuild_table(LIFT, athena, s3, "aml-bq2", "2026-11-03")
    assert s3.keys("marts/") == ["marts/channel_lift/part-1.parquet"]


def test_known_counts_are_checked_on_the_staging_table():
    table = Table("mart_channel_lift", CTAS, "marts/channel_lift/", known={"count(*)": 6})
    s3, athena = setup()
    athena.known_row = {"k0": "7"}
    with pytest.raises(AuditFailed, match=r"count\(\*\)"):
        rebuild_table(table, athena, s3, "aml-bq2", "2026-11-03")
    assert s3.keys("marts/") == ["marts/channel_lift/old-1.parquet", "marts/channel_lift/old-2.parquet"]


def test_a_partitioned_table_rereads_its_partitions_after_publishing():
    table = Table("curated_transactions", CTAS.replace("mart_channel_lift", "curated_transactions")
                  .replace("marts/channel_lift/", "curated/transactions/"), "curated/transactions/", partitioned=True)
    s3 = FakeS3({"curated/transactions/txn_date=2022-09-01/old.parquet"})
    athena = FakeAthena(s3, {"curated_transactions": LIVE_ROW, "curated_transactions_staging": LIVE_ROW},
                        files=("txn_date=2022-09-01/new.parquet",))
    rebuild_table(table, athena, s3, "aml-bq2", "2026-11-03")
    assert s3.keys("curated/") == ["curated/transactions/txn_date=2022-09-01/new.parquet"]
    assert "MSCK REPAIR TABLE aml_bq2.curated_transactions" in athena.sql

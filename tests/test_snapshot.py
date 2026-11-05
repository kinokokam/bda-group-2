"""The snapshot: one DuckDB file, synced from S3, that Helical reads (decided 5 Nov)."""
import io
import json

import duckdb
import pandas as pd
import pytest

from pipeline.snapshot import build_snapshot


def checks(day, gate, statuses):
    return {"slice": day, "gate": gate, "path": "build_marts" if gate == "pass" else "quarantine",
            "results": [{"check": c, "status": s} for c, s in statuses.items()]}


PASS = {"recon": "pass", "quality": "pass", "drift": "pass", "volume": "pass"}
FAIL = {"recon": "pass", "quality": "pass", "drift": "fail", "volume": "fail"}
REPORT_1 = {"slice": "2022-09-01", "weekday": "Thursday", "quarantined": True, "failed_checks": ["drift", "volume"],
            "summary": "Failed 2 of 4 checks: drift and volume. The day is quarantined.",
            "biggest_shifts": [{"what": "payment_format: Reinvestment", "today_pct": 43.1, "normal_pct": 0.0}],
            "top_alerts": [{"time": "14:33", "from_bank": "0214322", "to_bank": "0135981", "payment_format": "ACH",
                            "payment_currency": "US Dollar", "amount_usd": 16341.15, "score": 1.0}]}
REPORT_3 = {"slice": "2022-09-03", "weekday": "Saturday", "quarantined": False, "failed_checks": [],
            "summary": "Passed all 4 checks.", "biggest_shifts": [], "top_alerts": []}
DAILY_3 = {"day": "2022-09-03", "weekday": "Saturday", "transfers": 120000, "alerts": 600,
           "laundering_rate_pct": 0.1, "format_mix_pct": {"Cheque": 40.0, "ACH": 60.0}}
ESCALATION_1 = {"escalated_at": "2026-11-03T16:55:11+00:00", "slice": "2022-09-01", "weekday": "Thursday",
                "failed_checks": ["drift", "volume"], "summary": REPORT_1["summary"]}


def parquet(df):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


class FakeStore:
    def __init__(self, objects):
        self.objects = objects

    def keys(self, prefix):
        return sorted(k for k in self.objects if k.startswith(prefix))

    def get(self, key):
        return self.objects[key]


def store():
    js = lambda d: json.dumps(d).encode()  # noqa: E731
    return FakeStore({
        "monitor/checks/2022-09-01.json": js(checks("2022-09-01", "fail", FAIL)),
        "monitor/checks/2022-09-03.json": js(checks("2022-09-03", "pass", PASS)),
        "monitor/reports/2022-09-01.json": js(REPORT_1),
        "monitor/reports/2022-09-03.json": js(REPORT_3),
        "monitor/daily/2022-09-03.json": js(DAILY_3),
        "monitor/escalations.log": js(ESCALATION_1) + b"\n",
        "marts/": b"",
        "marts/channel_lift/run_a": parquet(pd.DataFrame({"payment_format": ["ACH"], "lift": [2.5]})),
        "marts/channel_lift/run_b": parquet(pd.DataFrame({"payment_format": ["Cash"], "lift": [0.4]})),
        "marts/model/metrics.csv": b"metric,value\npr_auc,0.31\n",
    })


@pytest.fixture
def snap(tmp_path):
    path = tmp_path / "aml.duckdb"
    build_snapshot(store(), path)
    con = duckdb.connect(str(path), read_only=True)
    yield con
    con.close()


def rows(con, sql):
    return con.execute(sql).fetchall()


def test_one_row_per_slice_with_a_badge_per_check(snap):
    assert rows(snap, "SELECT slice, weekday, quarantined, recon, quality, drift, volume FROM monitor_slices "
                      "ORDER BY slice") == [
        ("2022-09-01", "Thursday", True, "pass", "pass", "fail", "fail"),
        ("2022-09-03", "Saturday", False, "pass", "pass", "pass", "pass")]


def test_daily_charts_hold_accepted_days_only(snap):
    assert rows(snap, "SELECT day, transfers, alerts, laundering_rate_pct FROM monitor_daily") == [
        ("2022-09-03", 120000, 600, 0.1)]
    assert rows(snap, "SELECT day, payment_format, pct FROM monitor_format_mix ORDER BY pct") == [
        ("2022-09-03", "Cheque", 40.0), ("2022-09-03", "ACH", 60.0)]


def test_check_reports_with_their_shifts_and_alerts(snap):
    assert rows(snap, "SELECT slice, failed_checks, summary FROM monitor_reports ORDER BY slice") == [
        ("2022-09-01", "drift, volume", REPORT_1["summary"]), ("2022-09-03", "", "Passed all 4 checks.")]
    assert rows(snap, "SELECT slice, what, today_pct, normal_pct FROM monitor_shifts") == [
        ("2022-09-01", "payment_format: Reinvestment", 43.1, 0.0)]
    assert rows(snap, "SELECT slice, rank, from_bank, amount_usd FROM monitor_top_alerts") == [
        ("2022-09-01", 1, "0214322", 16341.15)]


def test_escalations(snap):
    assert rows(snap, "SELECT slice, failed_checks, summary FROM monitor_escalations") == [
        ("2022-09-01", "drift, volume", REPORT_1["summary"])]


def test_every_part_file_of_a_mart_and_the_model_tables(snap):
    assert rows(snap, "SELECT payment_format, lift FROM mart_channel_lift ORDER BY lift") == [
        ("Cash", 0.4), ("ACH", 2.5)]
    assert rows(snap, "SELECT metric, value FROM model_metrics") == [("pr_auc", 0.31)]


def test_through_a_day_leaves_out_later_slices(tmp_path):
    """During the replay the page grows one day at a time, even though S3 already holds all days."""
    path = tmp_path / "aml.duckdb"
    build_snapshot(store(), path, through="2022-09-02")
    con = duckdb.connect(str(path), read_only=True)
    assert rows(con, "SELECT slice FROM monitor_slices") == [("2022-09-01",)]
    assert rows(con, "SELECT count(*) FROM monitor_daily") == [(0,)]
    assert rows(con, "SELECT count(*) FROM monitor_escalations") == [(1,)]
    assert rows(con, "SELECT count(*) FROM mart_channel_lift") == [(2,)]   # the analysis never changes


def test_helical_can_open_the_file(tmp_path):
    """Helical's DuckDB 1.5.6 driver reads file format versions 64-68 only (measured 5 Nov)."""
    path = tmp_path / "aml.duckdb"
    build_snapshot(store(), path)
    header = path.read_bytes()[8:20]
    assert header[:4] == b"DUCK"
    assert 64 <= int.from_bytes(header[4:], "little") <= 68


def test_a_rebuild_replaces_the_old_file_whole(tmp_path):
    path = tmp_path / "aml.duckdb"
    build_snapshot(store(), path, through="2022-09-01")
    build_snapshot(store(), path)
    con = duckdb.connect(str(path), read_only=True)
    assert rows(con, "SELECT count(*) FROM monitor_slices") == [(2,)]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["aml.duckdb"]

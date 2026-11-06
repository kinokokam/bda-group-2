"""The two DAGs load and are wired as decided on 2 Nov. Needs Airflow: runs inside the container."""
from pathlib import Path

import pytest

pytest.importorskip("airflow.models")   # bq2/airflow/ (the DAG folder) also imports as "airflow"
from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "bq2" / "airflow" / "dags"
MARTS = [f"mart_{m}" for m in
         ("channel_lift", "currency_risk", "bank_concentration", "corridors", "amount_hour", "typology_profile")]


@pytest.fixture(scope="module")
def bag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def downstream(dag):
    return {t.task_id: sorted(t.downstream_task_ids) for t in dag.tasks}


def test_both_dags_load(bag):
    assert bag.import_errors == {}
    assert sorted(bag.dag_ids) == ["aml_bq2_batch", "aml_bq2_demo"]


def test_both_start_paused_in_the_shared_stack(bag):
    """The team's stack switches new DAGs on; ours write to S3, so a person switches them on."""
    assert all(dag.is_paused_upon_creation for dag in bag.dags.values())


def test_the_monitor_runs_once_per_day_from_1_to_11_sep(bag):
    dag = bag.dags["aml_bq2_demo"]
    days, run = [], dag.next_dagrun_info(None)          # walk the schedule as the scheduler does
    while run is not None:
        days.append(run.data_interval.start.strftime("%Y-%m-%d"))
        run = dag.next_dagrun_info(run.data_interval)
    assert days == [f"2022-09-{d:02d}" for d in range(1, 12)]
    assert dag.catchup and dag.max_active_runs == 1


def test_the_monitor_checks_once_then_accepts_or_quarantines(bag):
    assert downstream(bag.dags["aml_bq2_demo"]) == {
        "run_checks": ["gate"], "gate": ["accept_day", "quarantine"],
        "accept_day": ["report_slice"], "quarantine": ["report_slice"],
        "report_slice": ["send_escalation"], "send_escalation": ["upload_results"], "upload_results": ["sync_snapshot"], "sync_snapshot": []}


def test_the_batch_runs_by_hand_from_schemes_to_marts(bag):
    dag = bag.dags["aml_bq2_batch"]
    assert dag.schedule_interval is None
    assert downstream(dag) == {"parse_patterns": ["curated_transactions"],
                               "curated_transactions": sorted(MARTS), **{m: [] for m in MARTS}}

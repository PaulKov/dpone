"""Outcome XCom identity must follow Airflow's materialized task namespace."""

from types import SimpleNamespace

import pytest
from dpone_airflow_pack import launch_pin_cleanup, outcome
from dpone_airflow_pack.launch_pin_wiring import attach_outcome_and_cleanup


@pytest.mark.parametrize("prefix", ["", "transfer.", "parent.transfer."])
@pytest.mark.parametrize("pin_enabled", [False, True])
def test_outcome_uses_runtime_identity_without_changing_local_gate_name(monkeypatch, prefix, pin_enabled):
    class Operator:
        def __init__(self, **kwargs):
            self.task_id = prefix + kwargs["task_id"]
            self.op_kwargs = kwargs["op_kwargs"]

    monkeypatch.setattr(outcome, "python_operator_class", lambda: Operator)
    monkeypatch.setattr(launch_pin_cleanup, "python_operator_class", lambda: Operator)
    runtime = SimpleNamespace(task_id=prefix + "source_events__runtime")
    tasks = attach_outcome_and_cleanup(
        pack={"outcome_gate": {"required_status": "passed"}},
        dag=object(),
        tasks={"dpone_runtime": runtime},
        runtime=runtime,
        upstream_task_id="source_events__runtime",
        pin_enabled=pin_enabled,
        closed_locator=None,
        inline_required_status=None,
        is_mapped=False,
        node=None,
        task_group=None,
        chain=lambda *_: None,
        build_outcome=outcome.build_pack_outcome_task,
        build_cleanup=launch_pin_cleanup.build_pack_launch_pin_cleanup_task,
    )
    gate = tasks["outcome_gate"]
    assert gate.task_id == prefix + "source_events__runtime__outcome_gate"
    assert gate.op_kwargs["upstream_task_id"] == runtime.task_id
    if pin_enabled:
        cleanup = tasks["launch_pin_cleanup"]
        assert cleanup.task_id == prefix + "source_events__runtime__launch_pin_cleanup"
        assert cleanup.op_kwargs["outcome_gate_task_id"] == gate.task_id
    else:
        assert "launch_pin_cleanup" not in tasks

    # The grouped XCom must be consumed rather than mistaken for a missing result.
    ti = SimpleNamespace(xcom_pull=lambda **kw: {"status": "failed"} if kw["task_ids"] == runtime.task_id else None)
    assert outcome._pull_summary(ti=ti, upstream_task_id=gate.op_kwargs["upstream_task_id"]) == {"status": "failed"}


def test_direct_outcome_builder_preserves_legacy_identity(monkeypatch):
    monkeypatch.setattr(outcome, "python_operator_class", lambda: lambda **kw: kw)
    gate = outcome.build_pack_outcome_task(
        pack={"outcome_gate": {"required_status": "passed"}},
        dag=object(),
        upstream_task_id="source_events__runtime",
    )
    assert gate["op_kwargs"]["upstream_task_id"] == "source_events__runtime"


@pytest.mark.parametrize("prefix_group_id", [False, True])
def test_real_airflow_grouped_pack_outcome_identity(prefix_group_id):
    airflow = pytest.importorskip("airflow")
    if not getattr(airflow, "__version__", None):
        pytest.skip("real Airflow distribution is not installed")
    from datetime import UTC, datetime

    from dpone_airflow_pack.pack_tasks import _build_tasks_from_loaded_pack

    try:
        from airflow.sdk import DAG, TaskGroup
    except ImportError:
        from airflow import DAG
        from airflow.utils.task_group import TaskGroup

    with DAG("example_grouped_outcome", schedule=None, start_date=datetime(2026, 1, 1, tzinfo=UTC)) as dag:
        with TaskGroup("parent", prefix_group_id=prefix_group_id):
            with TaskGroup("transfer", prefix_group_id=prefix_group_id):
                tasks = _build_tasks_from_loaded_pack(
                    {
                        "kind": "gitops.airflow_pack",
                        "schema_version": "3",
                        "workload": {"workload_id": "source_events"},
                        "kpo_kwargs": {"task_id": "source_events__runtime", "image": "example:latest"},
                        "outcome_gate": {"required_status": "passed"},
                        "steps": [],
                    },
                    dag=dag,
                    operator_overrides=None,
                    node=None,
                    task_group=None,
                )
    runtime = tasks["dpone_runtime"]
    gate = tasks["outcome_gate"]
    expected = "parent.transfer." if prefix_group_id else ""
    assert runtime.task_id == expected + "source_events__runtime"
    assert gate.task_id == expected + "source_events__runtime__outcome_gate"
    assert gate.op_kwargs["upstream_task_id"] == runtime.task_id
    assert gate.upstream_task_ids == {runtime.task_id}

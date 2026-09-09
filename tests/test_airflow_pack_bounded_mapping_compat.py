from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import pytest


def test_real_airflow_materializes_bounded_mapping_without_discovery_task() -> None:
    airflow = pytest.importorskip("airflow")
    if not getattr(airflow, "__version__", None):
        pytest.skip("real Airflow distribution is not installed")

    from dpone_airflow_pack.pack_tasks import _build_tasks_from_loaded_pack

    try:
        from airflow.sdk import DAG
    except ImportError:
        from airflow import DAG

    plan = _plan()
    dag = DAG(
        dag_id="dpone_bounded_mapping_compat",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        catchup=False,
    )
    tasks = _build_tasks_from_loaded_pack(
        {
            "kind": "gitops.airflow_pack",
            "schema_version": "3",
            "workload": {"workload_id": "orders"},
            "kpo_kwargs": {
                "task_id": "orders__dpone_runtime",
                "name": "orders-dpone-runtime",
                "namespace": "default",
                "image": "dpone:test",
                "cmds": ["/bin/sh", "-ec"],
                "arguments": ["true"],
                "env_vars": {"BASE": "one"},
            },
            "mapping_plan": plan,
            "steps": [],
            "outcome_gate": {"required_status": "passed"},
            "xcom": {},
        },
        dag=dag,
        operator_overrides=None,
        node=None,
        task_group=None,
    )

    runtime = tasks["dpone_runtime"]
    assert sorted(tasks) == ["dpone_runtime"]
    assert runtime.task_id == "orders__dpone_runtime"
    assert runtime.pool == "dpone_backfill"
    assert runtime.max_active_tis_per_dag == 2
    mapped_envs = runtime.expand_input.value["env_vars"]
    assert len(mapped_envs) == 2
    assert all("DPONE_AIRFLOW_MAPPING_ITEM" in env for env in mapped_envs)
    assert len(dag.task_dict) == 1


def _plan() -> dict[str, object]:
    payload: dict[str, object] = {
        "schema": "dpone.airflow-mapping-plan.v1",
        "mode": "visible",
        "backfill_plan_hash": "sha256:" + "b" * 64,
        "chunks_total": 2,
        "items_total": 2,
        "limits": {"max_items": 2, "max_active": 2, "pool": "dpone_backfill"},
        "items": [
            {"item_index": 0, "first_chunk_index": 1, "last_chunk_index": 1, "chunks_count": 1},
            {"item_index": 1, "first_chunk_index": 2, "last_chunk_index": 2, "chunks_count": 1},
        ],
    }
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return {**payload, "plan_fingerprint": "sha256:" + hashlib.sha256(canonical).hexdigest()}

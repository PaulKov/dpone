"""Real-Airflow rendering contract for the dpone interval env templates.

Runs only where Apache Airflow (with the cncf.kubernetes provider) is
importable — locally inside ``.venv-airflow`` and in the
``airflow-pack-compat`` CI matrix. Verifies that a task built from a static
compact pack renders the ``DPONE_*`` env templates into concrete DAG-run
interval values, i.e. the scheduler side of the run-interval contract.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow", reason="Apache Airflow is required for rendering contract tests")
pytest.importorskip(
    "airflow.providers.cncf.kubernetes",
    reason="cncf.kubernetes provider is required for rendering contract tests",
)

INTERVAL_ENV_TEMPLATES = {
    "DPONE_DAG_ID": "{{ dag.dag_id }}",
    "DPONE_DAG_RUN_ID": "{{ run_id }}",
    "DPONE_TRY_NUMBER": "{{ ti.try_number | string }}",
    "DPONE_LOGICAL_DATE": "{{ logical_date | ts if logical_date is defined and logical_date else '' }}",
    "DPONE_INTERVAL_START": "{{ data_interval_start | ts if data_interval_start is defined and data_interval_start else '' }}",
    "DPONE_INTERVAL_END": "{{ data_interval_end | ts if data_interval_end is defined and data_interval_end else '' }}",
}


def _write_pack(tmp_path: Path, *, partitioned: bool = False) -> Path:
    pack = {
        "kind": "gitops.airflow_pack",
        "schema_version": "3",
        "producer": "test",
        "kpo_kwargs": {
            "task_id": "orders__dpone_runtime",
            "name": "dpone-orders",
            "namespace": "etl",
            "image": "dpone-runtime:test",
            "cmds": ["dpone"],
            "arguments": ["run", "manifests/orders.yaml", "--format", "json"],
            "env_vars": dict(INTERVAL_ENV_TEMPLATES),
        },
        "runtime_command": "dpone run manifests/orders.yaml --format json",
        "steps": [],
    }
    if partitioned:
        pack["airflow"] = {
            "execution": {
                "outlets": [
                    {
                        "uri": "dpone://clickhouse/analytics/orders",
                        "partition": {
                            "dimensions": {
                                "business_date": {
                                    "type": "temporal",
                                    "granularity": "day",
                                    "timezone": "UTC",
                                    "key_format": "%Y-%m-%d",
                                    "source": "dag_schedule",
                                }
                            }
                        },
                    }
                ]
            }
        }
    path = tmp_path / "airflow-pack.json"
    path.write_text(json.dumps(pack), encoding="utf-8")
    return path


def _dag_and_runtime_task(pack_path: Path, *, partition_plan: dict[str, str] | None = None):
    import pendulum
    from airflow import DAG
    from dpone_airflow_pack import build_dpone_gitops_task_group_from_pack

    with DAG(
        dag_id="dpone_orders_daily",
        schedule="@daily",
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        catchup=True,
    ) as dag:
        if partition_plan is not None:
            dag._dpone_partition_mode = "native"
            dag._dpone_partition_plan = partition_plan
        tasks = build_dpone_gitops_task_group_from_pack(pack_path, dag=dag)
    return dag, tasks["dpone_runtime"]


def _render_context(dag, *, run_id: str, interval_start, interval_end, partition_key: str | None = None):
    from types import SimpleNamespace

    return {
        "dag": dag,
        "run_id": run_id,
        "ti": SimpleNamespace(try_number=1),
        "logical_date": interval_start,
        "data_interval_start": interval_start,
        "data_interval_end": interval_end,
        "dag_run": SimpleNamespace(partition_key=partition_key),
    }


def test_kpo_env_vars_render_concrete_interval_per_dag_run(tmp_path: Path) -> None:
    import pendulum

    dag, runtime_task = _dag_and_runtime_task(_write_pack(tmp_path))
    start = pendulum.datetime(2025, 1, 1, tz="UTC")
    end = pendulum.datetime(2025, 1, 2, tz="UTC")

    rendered = runtime_task.render_template(
        runtime_task.env_vars,
        _render_context(dag, run_id="scheduled__2025-01-01", interval_start=start, interval_end=end),
    )

    env = _as_mapping(rendered)
    assert env["DPONE_DAG_ID"] == "dpone_orders_daily"
    assert env["DPONE_DAG_RUN_ID"] == "scheduled__2025-01-01"
    assert env["DPONE_TRY_NUMBER"] == "1"
    assert env["DPONE_INTERVAL_START"] == "2025-01-01T00:00:00+00:00"
    assert env["DPONE_INTERVAL_END"] == "2025-01-02T00:00:00+00:00"
    assert env["DPONE_LOGICAL_DATE"] == "2025-01-01T00:00:00+00:00"


def test_kpo_env_vars_render_empty_for_manual_runs_without_interval(tmp_path: Path) -> None:
    dag, runtime_task = _dag_and_runtime_task(_write_pack(tmp_path))

    rendered = runtime_task.render_template(
        runtime_task.env_vars,
        _render_context(dag, run_id="manual__now", interval_start=None, interval_end=None),
    )

    env = _as_mapping(rendered)
    assert env["DPONE_INTERVAL_START"] == ""
    assert env["DPONE_INTERVAL_END"] == ""
    assert env["DPONE_LOGICAL_DATE"] == ""


def test_native_partition_key_renders_into_runtime_env(tmp_path: Path) -> None:
    from dpone_airflow_pack.asset_partitions import detect_partition_capabilities

    if not detect_partition_capabilities().native:
        pytest.skip("installed Airflow has no native partition timetable SDK")
    plan = {
        "mode": "cron_producer",
        "dimension": "business_date",
        "type": "temporal",
        "granularity": "day",
        "timezone": "UTC",
        "key_format": "%Y-%m-%d",
        "source": "dag_schedule",
    }
    dag, runtime_task = _dag_and_runtime_task(
        _write_pack(tmp_path, partitioned=True),
        partition_plan=plan,
    )

    rendered = runtime_task.render_template(
        runtime_task.env_vars,
        _render_context(
            dag,
            run_id="scheduled__2026-07-16",
            interval_start=None,
            interval_end=None,
            partition_key="2026-07-16",
        ),
    )

    env = _as_mapping(rendered)
    assert env["DPONE_PARTITION_KEY"] == "2026-07-16"
    assert env["DPONE_PARTITION_DIMENSION"] == "business_date"
    assert env["DPONE_PARTITION_MODE"] == "native"


def test_kpo_env_vars_render_empty_when_interval_keys_are_missing(tmp_path: Path) -> None:
    """Airflow 3 manual triggers may omit interval keys from the render context."""

    dag, runtime_task = _dag_and_runtime_task(_write_pack(tmp_path))
    context = {
        "dag": dag,
        "run_id": "manual__now",
        "ti": __import__("types").SimpleNamespace(try_number=1),
    }

    rendered = runtime_task.render_template(runtime_task.env_vars, context)

    env = _as_mapping(rendered)
    assert env["DPONE_INTERVAL_START"] == ""
    assert env["DPONE_INTERVAL_END"] == ""
    assert env["DPONE_LOGICAL_DATE"] == ""


def _as_mapping(rendered) -> dict[str, str]:
    """KPO may keep env_vars as a dict or convert to V1EnvVar objects."""

    if isinstance(rendered, dict):
        return {str(key): str(value) for key, value in rendered.items()}
    return {str(item.name): str(item.value) for item in rendered}

from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest
from dpone_airflow_pack.dag_loader import load_dpone_dags
from dpone_airflow_pack.dag_spec_loader import load_dag_spec_file

from dpone.gitops.airflow_dag_spec import compute_spec_fingerprint
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.readiness.airflow_self_service_templates import RECIPE_DEFAULTS, dag_spec_payload, pipeline_payload


def test_preview_template_is_a_loadable_single_workload_dag_spec() -> None:
    pipeline_id = "orders_daily"
    source = pipeline_payload(
        pipeline_id,
        "mssql-to-clickhouse-incremental",
        RECIPE_DEFAULTS["mssql-to-clickhouse-incremental"],
    )

    pack = {
        "process_plans": {
            pipeline_id: {
                "selector": pipeline_id,
                "dag_node": {
                    "process_name": pipeline_id,
                    "visibility": "inline",
                    "task_group": None,
                    "estimated_visible_tasks": 1,
                },
            }
        }
    }

    payload = dag_spec_payload(pipeline_id, source, pack=pack)

    assert payload["nodes"] == [
        {
            "node_id": pipeline_id,
            "workload_id": pipeline_id,
            "selector": pipeline_id,
            "task_group": None,
            "visibility": "inline",
            "estimated_visible_tasks": 1,
            "pack_ref": f"cached://workloads/{pipeline_id}",
            "pack_path": f".dpone/gitops/airflow/{pipeline_id}/airflow-pack.json",
        }
    ]
    assert payload["topological_order"] == [pipeline_id]
    assert payload["spec_fingerprint"] == compute_spec_fingerprint(payload)


def test_preview_uses_pack_owned_node_identity_for_single_explicit_batch() -> None:
    pipeline_id = "orders_daily"
    source = pipeline_payload(
        pipeline_id,
        "mssql-to-clickhouse-incremental",
        RECIPE_DEFAULTS["mssql-to-clickhouse-incremental"],
    )
    pack = {
        "process_plans": {
            pipeline_id: {
                "selector": pipeline_id,
                "dag_node": {
                    "node_id": "orders_daily__orders_daily",
                    "process_name": pipeline_id,
                    "visibility": "inline",
                    "task_group": None,
                    "estimated_visible_tasks": 1,
                },
            }
        }
    }

    payload = dag_spec_payload(pipeline_id, source, pack=pack)

    assert payload["nodes"][0]["node_id"] == "orders_daily__orders_daily"


def test_preview_preserves_process_plan_dependencies() -> None:
    pipeline_id = "orders_daily"
    source = pipeline_payload(
        pipeline_id,
        "mssql-to-clickhouse-incremental",
        RECIPE_DEFAULTS["mssql-to-clickhouse-incremental"],
    )
    pack = {
        "process_plans": {
            "dbo.customers": {
                "selector": "dbo.customers",
                "dag_node": {
                    "process_name": "customers",
                    "visibility": "inline",
                    "task_group": None,
                    "estimated_visible_tasks": 1,
                    "depends_on_process_selectors": ["dbo.orders"],
                },
            },
            "dbo.orders": {
                "selector": "dbo.orders",
                "dag_node": {
                    "process_name": "orders",
                    "visibility": "inline",
                    "task_group": None,
                    "estimated_visible_tasks": 1,
                    "depends_on_process_selectors": [],
                },
            },
        }
    }

    payload = dag_spec_payload(pipeline_id, source, pack=pack)

    assert payload["edges"] == [
        {
            "upstream": "orders_daily__orders",
            "downstream": "orders_daily__customers",
            "reason": "declared",
            "origin": "process_plan_dependency",
        }
    ]
    assert payload["topological_order"] == ["orders_daily__orders", "orders_daily__customers"]


def test_golden_preview_loads_through_public_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True).passed
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    ).passed
    assert service.check("pipelines/orders_daily").passed

    preview = service.preview("orders_daily")

    assert preview.passed
    index_path = tmp_path / ".dpone-cache" / "current" / "airflow-index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    assert [item["id"] for item in index["dag_specs"]] == ["orders_daily"]
    assert [item["id"] for item in index["workload_packs"]] == ["orders_daily"]
    spec_ref = str(index["dag_specs"][0]["artifact_ref"])
    spec_path = tmp_path / ".dpone-cache" / spec_ref.removeprefix("cache://")
    loaded_spec, issues = load_dag_spec_file(spec_path)
    assert loaded_spec is not None
    assert issues == ()
    assert loaded_spec["nodes"][0]["node_id"] == "orders_daily"
    assert loaded_spec["nodes"][0]["selector"] == "orders_daily"
    assert loaded_spec["nodes"][0]["visibility"] == "inline"
    assert loaded_spec["visible_task_plan"]["estimated_total"] == 1

    materialized_tasks = _install_fake_airflow(monkeypatch)
    globals_dict: dict[str, object] = {}

    report = load_dpone_dags(globals_dict, index_path=index_path)

    assert report.loaded == ("orders_daily",)
    assert report.skipped == ()
    assert report.errors == ()
    assert "orders_daily" in globals_dict
    assert [task["task_id"] for task in materialized_tasks] == ["orders_daily__dpone_runtime"]

    repeated = service.preview("orders_daily")
    assert repeated.passed
    repeated_index = json.loads(index_path.read_text(encoding="utf-8"))
    repeated_ref = str(repeated_index["dag_specs"][0]["artifact_ref"])
    repeated_spec_path = tmp_path / ".dpone-cache" / repeated_ref.removeprefix("cache://")
    repeated_spec, repeated_issues = load_dag_spec_file(repeated_spec_path)
    assert repeated_issues == ()
    assert repeated_spec is not None
    assert repeated_spec["spec_fingerprint"] == loaded_spec["spec_fingerprint"]
    assert repeated_spec["nodes"] == loaded_spec["nodes"]


def test_preview_preserves_authored_group_visibility(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True).passed
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    ).passed
    pipeline_path = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    source = pipeline_path.read_text(encoding="utf-8")
    pipeline_path.write_text(
        source.replace(
            "- name: orders_daily\n",
            "- name: orders_daily\n  execution:\n    visibility: group\n",
            1,
        ),
        encoding="utf-8",
    )

    preview = service.preview("orders_daily")

    assert preview.passed
    index_path = tmp_path / ".dpone-cache" / "current" / "airflow-index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    spec_ref = str(index["dag_specs"][0]["artifact_ref"])
    spec_path = tmp_path / ".dpone-cache" / spec_ref.removeprefix("cache://")
    loaded_spec, issues = load_dag_spec_file(spec_path)
    assert issues == ()
    assert loaded_spec is not None
    assert loaded_spec["nodes"][0]["visibility"] == "group"
    assert loaded_spec["nodes"][0]["estimated_visible_tasks"] == 2
    assert loaded_spec["visible_task_plan"]["estimated_total"] == 2


def _install_fake_airflow(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, object]]:
    materialized_tasks: list[dict[str, object]] = []
    airflow = types.ModuleType("airflow")
    providers = types.ModuleType("airflow.providers")
    standard = types.ModuleType("airflow.providers.standard")
    operators = types.ModuleType("airflow.providers.standard.operators")
    empty_module = types.ModuleType("airflow.providers.standard.operators.empty")

    class DAG:
        def __init__(self, *, dag_id: str, schedule: object = None, **kwargs: object) -> None:
            self.dag_id = dag_id
            self.schedule = schedule
            self.kwargs = kwargs

    class EmptyOperator:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            materialized_tasks.append(dict(kwargs))

        def __rshift__(self, other: object) -> object:
            return other

    airflow.DAG = DAG
    empty_module.EmptyOperator = EmptyOperator
    for name, module in {
        "airflow": airflow,
        "airflow.providers": providers,
        "airflow.providers.standard": standard,
        "airflow.providers.standard.operators": operators,
        "airflow.providers.standard.operators.empty": empty_module,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    return materialized_tasks

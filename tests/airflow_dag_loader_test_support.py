from __future__ import annotations

import hashlib
import sys
import types
from pathlib import Path

import pytest
from dpone_airflow_pack.dag_spec_loader import compute_dag_spec_fingerprint


def sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def install_fake_airflow(monkeypatch: pytest.MonkeyPatch) -> None:
    airflow = types.ModuleType("airflow")
    providers = types.ModuleType("airflow.providers")
    standard = types.ModuleType("airflow.providers.standard")
    operators = types.ModuleType("airflow.providers.standard.operators")
    empty_mod = types.ModuleType("airflow.providers.standard.operators.empty")

    class DAG:
        def __init__(
            self,
            *,
            dag_id: str,
            schedule: object = None,
            start_date: object = None,
            **kwargs: object,
        ) -> None:
            self.kwargs = {
                "dag_id": dag_id,
                "schedule": schedule,
                "start_date": start_date,
                **kwargs,
            }

        def __enter__(self) -> DAG:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    class EmptyOperator:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

    empty_mod.EmptyOperator = EmptyOperator
    airflow.DAG = DAG
    for name, module in {
        "airflow": airflow,
        "airflow.providers": providers,
        "airflow.providers.standard": standard,
        "airflow.providers.standard.operators": operators,
        "airflow.providers.standard.operators.empty": empty_mod,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)


def minimal_pack(*, task_id: str) -> dict[str, object]:
    return {
        "kind": "gitops.airflow_pack",
        "kpo_kwargs": {
            "task_id": task_id,
            "name": task_id.replace("_", "-"),
            "namespace": "airflow",
        },
        "runtime_command": "echo ok",
        "steps": [],
    }


def legacy_dag_spec_payload(dag_id: str) -> dict[str, object]:
    payload: dict[str, object] = {
        "kind": "gitops.airflow_dag_spec",
        "schema_version": "1",
        "producer": "test",
        "dag_id": dag_id,
        "schedule": None,
        "start_date": "2026-07-07",
        "nodes": [
            {
                "node_id": "app",
                "workload_id": "app",
                "pack_path": ".dpone/gitops/airflow/app/airflow-pack.json",
            }
        ],
        "edges": [],
        "topological_order": ["app"],
    }
    payload["spec_fingerprint"] = compute_dag_spec_fingerprint(payload)
    return payload


def legacy_dag_spec_path(repo_root: Path, dag_id: str) -> Path:
    spec_dir = repo_root / ".dpone/gitops/airflow/_dags"
    spec_dir.mkdir(parents=True, exist_ok=True)
    return spec_dir / f"{dag_id}.dag-spec.json"


__all__ = [
    "install_fake_airflow",
    "legacy_dag_spec_path",
    "legacy_dag_spec_payload",
    "minimal_pack",
    "sha256",
]

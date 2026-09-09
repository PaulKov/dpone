from __future__ import annotations

import json
from pathlib import Path

import pytest
from dpone_airflow_pack.cache_activation_contract import cache_write_lease
from dpone_airflow_pack.dag_loader import load_dpone_dags
from dpone_airflow_pack.dag_spec_loader import compute_dag_spec_fingerprint

from tests.airflow_dag_loader_test_support import (
    install_fake_airflow,
    minimal_pack,
    sha256,
)

RELEASE_ID = "sha256:" + "a" * 64
DEPLOYMENT_ID = "sha256:" + "b" * 64


def test_local_preview_materializes_empty_operator_without_runtime_wiring(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_fake_airflow(monkeypatch)
    cache = tmp_path / ".dpone-cache"
    with cache_write_lease(cache):
        pass
    release_dir = cache / "releases" / RELEASE_ID.replace(":", "-") / "dags"
    packs_dir = cache / "releases" / RELEASE_ID.replace(":", "-") / "packs"
    deployment_dir = cache / "deployments" / "prod" / "sha256-deployment"
    release_dir.mkdir(parents=True)
    packs_dir.mkdir(parents=True)
    deployment_dir.mkdir(parents=True)

    pack_path = packs_dir / "workload_a.airflow-pack.json"
    pack_path.write_text(
        json.dumps(minimal_pack(task_id="workload_a__dpone_runtime")),
        encoding="utf-8",
    )
    spec_payload = {
        "kind": "gitops.airflow_dag_spec",
        "schema_version": "1",
        "producer": "test",
        "dag_id": "DAG__preview",
        "schedule": None,
        "start_date": "2026-07-07",
        "catchup": False,
        "nodes": [
            {
                "node_id": "workload_a",
                "workload_id": "workload_a",
                "pack_ref": "cached://workloads/workload_a",
                "pack_path": ".dpone/gitops/airflow/workload_a/airflow-pack.json",
            }
        ],
        "edges": [],
        "topological_order": ["workload_a"],
    }
    spec_payload["spec_fingerprint"] = compute_dag_spec_fingerprint(spec_payload)
    spec_path = release_dir / "DAG__preview.dag-spec.json"
    spec_path.write_text(json.dumps(spec_payload), encoding="utf-8")

    index_path = deployment_dir / "airflow-index.json"
    index_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [
                    {
                        "id": "DAG__preview",
                        "artifact_ref": (
                            f"cache://releases/{RELEASE_ID.replace(':', '-')}/dags/DAG__preview.dag-spec.json"
                        ),
                        "sha256": sha256(spec_path),
                        "bytes": spec_path.stat().st_size,
                    }
                ],
                "workload_packs": [
                    {
                        "id": "workload_a",
                        "artifact_ref": (
                            f"cache://releases/{RELEASE_ID.replace(':', '-')}/packs/workload_a.airflow-pack.json"
                        ),
                        "sha256": sha256(pack_path),
                        "bytes": pack_path.stat().st_size,
                    }
                ],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    wire_calls = {"count": 0}
    created_task_ids: list[str] = []

    def forbid_wire_pack_workload(*_args: object, **_kwargs: object) -> object:
        wire_calls["count"] += 1
        raise AssertionError("runtime wiring is forbidden in local preview")

    class PreviewTask:
        def __init__(self, *, task_id: str, **_kwargs: object) -> None:
            created_task_ids.append(task_id)

    monkeypatch.setattr(
        "dpone_airflow_pack.dag_materializer.wire_pack_workload",
        forbid_wire_pack_workload,
    )
    monkeypatch.setattr(
        "dpone_airflow_pack.dag_materializer._empty_operator_class",
        lambda: PreviewTask,
    )
    globals_dict: dict[str, object] = {}
    report = load_dpone_dags(globals_dict, index_path=index_path)

    assert report.errors == ()
    assert report.loaded == ("DAG__preview",)
    assert wire_calls["count"] == 0
    assert created_task_ids == ["workload_a__dpone_runtime"]
    assert "DAG__preview" in globals_dict

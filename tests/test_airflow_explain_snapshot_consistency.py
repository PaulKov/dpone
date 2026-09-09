from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.readiness import airflow_active_index
from dpone.readiness.airflow_authoring_check_service import CheckedPipelineSource
from dpone.readiness.airflow_explain_service import AirflowExplainService
from dpone.readiness.airflow_self_service_models import SelfServiceResult

_RELEASE_A = "sha256:" + "a" * 64
_DEPLOYMENT_A = "sha256:" + "b" * 64
_RELEASE_B = "sha256:" + "c" * 64
_DEPLOYMENT_B = "sha256:" + "d" * 64


class _StaticAuthoringCheck:
    def __init__(self, checked: CheckedPipelineSource) -> None:
        self._checked = checked

    def inspect(self, _target: str) -> CheckedPipelineSource:
        return self._checked


def _explain_service(
    root: Path,
    *,
    authoring_result: SelfServiceResult | None = None,
) -> AirflowExplainService:
    source = root / "pipelines" / "orders_daily" / "pipeline.yaml"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("kind: dpone.flow.v1\n", encoding="utf-8")
    checked = CheckedPipelineSource(
        source_path=source,
        payload={"metadata": {"id": "orders_daily"}},
        compilation=SimpleNamespace(semantic_fingerprint="sha256:" + "e" * 64),  # type: ignore[arg-type]
        result=authoring_result or SelfServiceResult(passed=True),
        source_label="pipelines/orders_daily/pipeline.yaml",
    )
    return AirflowExplainService(
        root=root,
        authoring_check=_StaticAuthoringCheck(checked),  # type: ignore[arg-type]
    )


def _write_index(
    path: Path,
    *,
    release_id: str,
    deployment_id: str,
    airflow_bundle_ref: str | None = None,
    pipeline_id: str = "orders_daily",
    dag_specs: list[dict[str, object]] | None = None,
    workload_packs: list[dict[str, object]] | None = None,
) -> None:
    payload: dict[str, object] = {
        "schema": "dpone.airflow-deployment-index.v1",
        "release_id": release_id,
        "deployment_id": deployment_id,
        "runtime_artifact_delivery": {"mode": "local_preview"},
        "dag_specs": dag_specs if dag_specs is not None else [{"id": pipeline_id}],
        "workload_packs": workload_packs or [],
    }
    if airflow_bundle_ref is not None:
        payload["airflow_bundle_ref"] = airflow_bundle_ref
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_cache_artifact(
    cache_root: Path,
    relative_path: str,
    payload: object,
) -> dict[str, object]:
    content = json.dumps(payload, sort_keys=True).encode("utf-8")
    path = cache_root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return {
        "artifact_ref": f"cache://{relative_path}",
        "sha256": "sha256:" + hashlib.sha256(content).hexdigest(),
        "bytes": len(content),
    }


def test_explain_pins_one_confined_index_snapshot_across_pointer_switch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    deployment_a = cache_root / "deployments" / "local-preview" / _DEPLOYMENT_A.replace(":", "-")
    deployment_b = cache_root / "deployments" / "local-preview" / _DEPLOYMENT_B.replace(":", "-")
    _write_index(
        deployment_a / "airflow-index.json",
        release_id=_RELEASE_A,
        deployment_id=_DEPLOYMENT_A,
    )
    _write_index(
        deployment_b / "airflow-index.json",
        release_id=_RELEASE_B,
        deployment_id=_DEPLOYMENT_B,
    )
    current = cache_root / "current"
    current.symlink_to(deployment_a.relative_to(cache_root), target_is_directory=True)

    confined_reads = 0
    direct_reopens = 0
    original_confined_read = airflow_active_index.read_confined_file
    original_path_open = Path.open

    def switching_confined_read(
        root: Path,
        relative_path: str,
        *,
        max_bytes: int,
        follow_in_root_symlinks: bool = False,
    ) -> bytes:
        nonlocal confined_reads
        content = original_confined_read(
            root,
            relative_path,
            max_bytes=max_bytes,
            follow_in_root_symlinks=follow_in_root_symlinks,
        )
        if relative_path.endswith("/airflow-index.json"):
            confined_reads += 1
            current.unlink()
            current.symlink_to(deployment_b.relative_to(cache_root), target_is_directory=True)
        return content

    def tracking_path_open(path: Path, *args: object, **kwargs: object) -> Any:
        nonlocal direct_reopens
        if path.name == "airflow-index.json" and ".dpone-cache" in path.parts:
            direct_reopens += 1
        return original_path_open(path, *args, **kwargs)

    monkeypatch.setattr(airflow_active_index, "read_confined_file", switching_confined_read)
    monkeypatch.setattr(Path, "open", tracking_path_open)

    result = _explain_service(tmp_path).explain("orders_daily")

    assert result.details is not None
    diagnostics = result.details["operator_diagnostics"]
    artifact_state = result.details["artifact_state"]
    assert confined_reads == 1
    assert direct_reopens == 0
    assert current.readlink() == deployment_b.relative_to(cache_root)
    assert diagnostics["deployment_id"] == _DEPLOYMENT_A
    assert artifact_state["published_deployment_id"] == _DEPLOYMENT_A


def test_explain_failed_operator_diagnostics_fail_aggregate_result(tmp_path: Path) -> None:
    current = tmp_path / ".dpone-cache" / "current"
    current.mkdir(parents=True)
    (current / "airflow-index.json").write_text("{not-json", encoding="utf-8")

    result = _explain_service(tmp_path).explain("orders_daily")

    assert result.passed is False
    assert result.exit_code == 1
    assert len(result.errors) == 1
    error = result.errors[0]
    assert error["schema"] == "dpone.error.v1"
    assert error["code"] == "DPONE_AIRFLOW_INDEX_INVALID"
    assert error["stage"] == "airflow_operator_diagnostics"
    assert error["severity"] == "error"
    assert error["entity"] == {
        "kind": "airflow_operator_check",
        "id": "DPONE_AIRFLOW_INDEX_INVALID",
    }
    assert tmp_path.as_posix() not in json.dumps(result.to_dict())
    assert result.details is not None
    assert result.details["operator_diagnostics"]["summary"]["failed"] == 1


def test_explain_missing_cache_remains_successfully_planned(tmp_path: Path) -> None:
    result = _explain_service(tmp_path).explain("orders_daily")

    assert result.passed is True
    assert result.errors == ()
    assert result.exit_code is None
    assert result.details is not None
    assert result.details["operator_diagnostics"]["status"] == "planned"
    assert result.details["operator_diagnostics"]["summary"]["failed"] == 0


def test_active_index_snapshot_is_deeply_immutable(tmp_path: Path) -> None:
    current = tmp_path / ".dpone-cache" / "current"
    _write_index(
        current / "airflow-index.json",
        release_id=_RELEASE_A,
        deployment_id=_DEPLOYMENT_A,
    )
    snapshot = airflow_active_index.load_active_index_snapshot(tmp_path)

    assert snapshot.index is not None
    with pytest.raises(TypeError):
        snapshot.index["deployment_id"] = _DEPLOYMENT_B
    delivery = snapshot.index["runtime_artifact_delivery"]
    assert isinstance(delivery, Mapping)
    with pytest.raises(TypeError):
        delivery["mode"] = "init_fetch"


def test_explain_operator_warning_does_not_fail_aggregate_result(tmp_path: Path) -> None:
    current = tmp_path / ".dpone-cache" / "current"
    _write_index(
        current / "airflow-index.json",
        release_id=_RELEASE_A,
        deployment_id=_DEPLOYMENT_A,
        airflow_bundle_ref="s3://dpone-dags/prod",
    )

    result = _explain_service(tmp_path).explain("orders_daily")

    assert result.passed is True
    assert result.errors == ()
    assert result.exit_code is None
    assert result.details is not None
    assert result.details["operator_diagnostics"]["status"] == "operator_warnings"
    assert result.details["operator_diagnostics"]["summary"]["warning"] == 1


def test_explain_failed_authoring_does_not_project_unrelated_active_deployment(tmp_path: Path) -> None:
    current = tmp_path / ".dpone-cache" / "current"
    _write_index(
        current / "airflow-index.json",
        release_id=_RELEASE_A,
        deployment_id=_DEPLOYMENT_A,
    )
    authoring_failure = SelfServiceResult(
        passed=False,
        errors=(
            {
                "schema": "dpone.error.v1",
                "code": "DPONE_PIPELINE_SOURCE_NOT_FOUND",
                "stage": "self_service",
                "severity": "error",
                "message": "Pipeline source was not found.",
            },
        ),
        exit_code=1,
    )

    result = _explain_service(tmp_path, authoring_result=authoring_failure).explain("missing")

    assert result.passed is False
    assert result.details is not None
    assert result.details["operator_diagnostics"]["status"] == "not_applicable"
    assert result.details["operator_diagnostics"]["operator_pinning"] == "not_applicable"
    assert "deployment_id" not in result.details["operator_diagnostics"]
    assert result.details["artifact_state"]["dag_spec"] == "unavailable"
    assert result.details["artifact_state"]["airflow_pack"] == "unavailable"


def test_explain_valid_source_does_not_project_unrelated_active_deployment(
    tmp_path: Path,
) -> None:
    current = tmp_path / ".dpone-cache" / "current"
    _write_index(
        current / "airflow-index.json",
        release_id=_RELEASE_A,
        deployment_id=_DEPLOYMENT_A,
        pipeline_id="another_pipeline",
    )

    result = _explain_service(tmp_path).explain("orders_daily")

    assert result.details is not None
    artifact_state = result.details["artifact_state"]
    assert artifact_state["published_deployment_id"] is None
    assert artifact_state["dag_spec"] == "planned"
    assert artifact_state["airflow_pack"] == "planned"
    diagnostics = result.details["operator_diagnostics"]
    assert diagnostics["status"] == "planned"
    assert "release_id" not in diagnostics
    assert "deployment_id" not in diagnostics
    assert "workload_operators" not in diagnostics


def test_explain_shared_dag_projects_only_target_workload(
    tmp_path: Path,
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    shared_dag = _write_cache_artifact(
        cache_root,
        f"releases/{_RELEASE_A.replace(':', '-')}/dags/shared_daily.dag-spec.json",
        {
            "schema": "dpone.airflow-dag-spec.v1",
            "dag_id": "shared_daily",
            "nodes": [
                {"id": "load_orders", "workload_id": "orders_daily"},
                {"id": "load_customers", "workload_id": "customers_daily"},
            ],
        },
    )
    orders_pack = _write_cache_artifact(
        cache_root,
        f"releases/{_RELEASE_A.replace(':', '-')}/packs/orders_daily.airflow-pack.json",
        {
            "schema": "dpone.airflow-pack.v3",
            "workload": {"workload_id": "orders_daily"},
            "kpo_kwargs": {"task_id": "load_orders"},
        },
    )
    customers_pack = _write_cache_artifact(
        cache_root,
        f"releases/{_RELEASE_A.replace(':', '-')}/packs/customers_daily.airflow-pack.json",
        {
            "schema": "dpone.airflow-pack.v3",
            "workload": {"workload_id": "customers_daily"},
            "kpo_kwargs": {"task_id": "load_customers"},
        },
    )
    _write_index(
        cache_root / "current" / "airflow-index.json",
        release_id=_RELEASE_A,
        deployment_id=_DEPLOYMENT_A,
        dag_specs=[{"id": "shared_daily", **shared_dag}],
        workload_packs=[
            {"id": "orders_daily", **orders_pack},
            {"id": "customers_daily", **customers_pack},
        ],
    )

    result = _explain_service(tmp_path).explain("orders_daily")

    assert result.details is not None
    diagnostics = result.details["operator_diagnostics"]
    assert diagnostics["deployment_id"] == _DEPLOYMENT_A
    assert [item["workload_id"] for item in diagnostics["workload_operators"]] == ["orders_daily"]
    assert result.details["artifact_state"]["published_deployment_id"] == _DEPLOYMENT_A
    assert result.details["artifact_state"]["dag_spec"] == "stale"


@pytest.mark.parametrize("missing_field", ["sha256", "bytes"])
def test_explain_shared_dag_requires_complete_integrity_descriptor(
    tmp_path: Path,
    missing_field: str,
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    shared_dag = _write_cache_artifact(
        cache_root,
        f"releases/{_RELEASE_A.replace(':', '-')}/dags/shared_daily.dag-spec.json",
        {
            "schema": "dpone.airflow-dag-spec.v1",
            "dag_id": "shared_daily",
            "nodes": [{"id": "load_orders", "workload_id": "orders_daily"}],
        },
    )
    shared_dag.pop(missing_field)
    _write_index(
        cache_root / "current" / "airflow-index.json",
        release_id=_RELEASE_A,
        deployment_id=_DEPLOYMENT_A,
        dag_specs=[{"id": "shared_daily", **shared_dag}],
    )

    result = _explain_service(tmp_path).explain("orders_daily")

    assert result.details is not None
    assert result.details["artifact_state"]["published_deployment_id"] is None
    assert result.details["operator_diagnostics"]["status"] == "planned"

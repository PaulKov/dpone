"""Producer-to-provider contracts for domain-first Airflow previews."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from dpone_airflow_pack import dag_loader
from dpone_airflow_pack.dag_loader import load_dpone_dags

from dpone.readiness import airflow_preview_retirement
from dpone.readiness.airflow_self_service import AirflowSelfServiceService
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.readiness.project_selection_preview import ProjectSelectionPreviewService


def _project(root: Path) -> AirflowSelfServiceService:
    service = build_airflow_self_service_service(root=root)
    assert service.init_project(airflow=True, layout="domain_first").passed
    return service


def _domain(service: AirflowSelfServiceService, domain: str) -> None:
    assert service.init_domain(
        domain=domain,
        owner_team=f"data-{domain}",
        owner_contact=f"{domain}@example.com",
        approver_team="data-platform",
    ).passed


def _pipeline(service: AirflowSelfServiceService, pipeline_id: str, domain: str) -> None:
    assert service.init_pipeline(
        pipeline_id=pipeline_id,
        domain=domain,
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    ).passed


def _install_parse_safe_materializer(monkeypatch: pytest.MonkeyPatch) -> None:
    def materialize(payload: dict[str, object], **_: object) -> SimpleNamespace:
        return SimpleNamespace(dag_id=payload["dag_id"])

    monkeypatch.setattr(dag_loader, "_materialize_dag_spec", materialize)


def test_single_preview_loads_idempotently_and_retirement_removes_provider_dag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_parse_safe_materializer(monkeypatch)
    service = _project(tmp_path)
    _domain(service, "crm")
    _pipeline(service, "orders_daily", "crm")
    assert service.preview("orders_daily").passed
    index_path = tmp_path / ".dpone-cache/current/airflow-index.json"
    namespace: dict[str, object] = {}

    first = load_dpone_dags(namespace, index_path=index_path)
    second = load_dpone_dags(namespace, index_path=index_path)

    assert first.loaded == ("orders_daily",)
    assert second.loaded == ()
    assert second.skipped == ({"dag_id": "orders_daily", "reason": "duplicate_dag_id"},)
    source = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["metadata"]["airflow"] = False
    source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    retired = service.preview("orders_daily")
    retired_namespace: dict[str, object] = {}
    retired_report = load_dpone_dags(retired_namespace, index_path=index_path)

    assert not retired.passed
    assert retired.details["artifact_state"] == "retired"
    assert retired_report.loaded == ()
    assert retired_namespace == {}


def test_project_preview_is_consumable_by_provider_loader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_parse_safe_materializer(monkeypatch)
    service = _project(tmp_path)
    _domain(service, "crm")
    _pipeline(service, "orders_daily", "crm")
    _pipeline(service, "customers_daily", "crm")

    preview = ProjectSelectionPreviewService(root=tmp_path).preview(
        target=tmp_path,
        select=(),
        exclude=(),
        state_path=None,
        selectors_path="selectors.yml",
        max_selected=100,
    )
    namespace: dict[str, object] = {}
    report = load_dpone_dags(
        namespace,
        index_path=tmp_path / ".dpone-cache/current/airflow-index.json",
    )

    assert preview.passed
    assert report.loaded == ("customers_daily", "orders_daily")
    assert set(namespace) == {"customers_daily", "orders_daily"}


def test_retirement_rechecks_authority_inside_promotion_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    _pipeline(service, "orders_daily", "crm")
    enabled = service.preview("orders_daily")
    assert enabled.passed
    source = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["metadata"]["airflow"] = False
    source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    pointer_path = tmp_path / ".dpone-cache/current-pointer.json"
    original_pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    original_sync = airflow_preview_retirement.local_cache_sync_result

    def mutate_then_sync(**kwargs: object):
        ownership = tmp_path / "workloads/crm/ownership.yaml"
        ownership.write_bytes(ownership.read_bytes() + b"\n")
        return original_sync(**kwargs)

    monkeypatch.setattr(airflow_preview_retirement, "local_cache_sync_result", mutate_then_sync)

    result = service.preview("orders_daily")

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD"
    assert json.loads(pointer_path.read_text(encoding="utf-8")) == original_pointer


def test_retirement_precommit_snapshot_is_the_promotion_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    service = _project(tmp_path)
    _domain(service, "crm")
    _pipeline(service, "orders_daily", "crm")
    assert service.preview("orders_daily").passed
    source = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["metadata"]["airflow"] = False
    source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    original_commit = DeploymentCacheMaterializer._commit_promotion

    def reenable_then_commit(
        materializer: DeploymentCacheMaterializer,
        *,
        deployment_path: Path,
        pointer: dict[str, object],
    ):
        current = yaml.safe_load(source.read_text(encoding="utf-8"))
        current["metadata"]["airflow"] = True
        source.write_text(yaml.safe_dump(current, sort_keys=False), encoding="utf-8")
        return original_commit(materializer, deployment_path=deployment_path, pointer=pointer)

    monkeypatch.setattr(DeploymentCacheMaterializer, "_commit_promotion", reenable_then_commit)

    result = service.preview("orders_daily")

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_AIRFLOW_DISABLED"
    current_index = json.loads((tmp_path / ".dpone-cache/current/airflow-index.json").read_text(encoding="utf-8"))
    assert current_index["dag_specs"] == []


def test_retirement_cas_is_bound_to_inspected_single_workload_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    _pipeline(service, "orders_daily", "crm")
    _pipeline(service, "customers_daily", "crm")
    assert service.preview("orders_daily").passed
    source = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["metadata"]["airflow"] = False
    source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    original_sync = airflow_preview_retirement.local_cache_sync_result

    def promote_new_project_preview_then_sync(**kwargs: object):
        promoted = ProjectSelectionPreviewService(root=tmp_path).preview(
            target=tmp_path,
            select=("id:customers_daily",),
            exclude=(),
            state_path=None,
            selectors_path="selectors.yml",
            max_selected=100,
        )
        assert promoted.passed
        return original_sync(**kwargs)

    monkeypatch.setattr(
        airflow_preview_retirement,
        "local_cache_sync_result",
        promote_new_project_preview_then_sync,
    )

    result = service.preview("orders_daily")
    current_index = json.loads((tmp_path / ".dpone-cache/current/airflow-index.json").read_text(encoding="utf-8"))

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_CURRENT_POINTER_CAS_MISMATCH"
    assert [item["id"] for item in current_index["dag_specs"]] == ["customers_daily"]


def test_empty_retirement_release_is_reusable_for_different_workloads(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    _pipeline(service, "orders_daily", "crm")
    _pipeline(service, "customers_daily", "crm")

    for pipeline_id in ("orders_daily", "customers_daily"):
        assert service.preview(pipeline_id).passed
        source = tmp_path / f"workloads/crm/pipelines/{pipeline_id}/pipeline.yaml"
        payload = yaml.safe_load(source.read_text(encoding="utf-8"))
        payload["metadata"]["airflow"] = False
        source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

        result = service.preview(pipeline_id)

        assert not result.passed
        assert result.details["artifact_state"] == "retired"

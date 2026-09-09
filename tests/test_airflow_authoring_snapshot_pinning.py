from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import yaml

from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.readiness.airflow_authoring_check_service import AirflowAuthoringCheckService
from dpone.readiness.airflow_local_safe_sample_deployment import (
    ensure_local_safe_sample_deployment,
)
from dpone.readiness.airflow_self_service import AirflowSelfServiceService
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.readiness.project_selection_loader import ProjectSelectionLoader
from dpone.readiness.project_selection_preview import ProjectSelectionPreviewService
from tests.mssql_asset_registry_fixtures import write_mssql_connection_registry

_SOURCE_DRIFT_CODE = "DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD"


def _scaffold_project(root: Path) -> tuple[AirflowSelfServiceService, Path]:
    source_path = root / "pipelines/orders_daily/pipeline.yaml"
    source_path.parent.mkdir(parents=True)
    source_path.write_text(
        yaml.safe_dump(
            {
                "kind": "dpone.flow.v1",
                "authoring": {
                    "mode": "flow",
                    "source": "pipelines/orders_daily/pipeline.yaml",
                },
                "metadata": {
                    "id": "orders_daily",
                    "domain": "sales",
                    "owner": "data-platform",
                },
                "processes": [
                    {
                        "name": "orders_daily",
                        "source": {
                            "type": "mssql",
                            "connection_ref": "mssql_dev",
                            "table": {"schema": "dbo", "name": "orders"},
                        },
                        "sink": {
                            "type": "clickhouse",
                            "connection_ref": "clickhouse_dev",
                            "table": {"schema": "analytics", "name": "orders"},
                            "strategy": {"mode": "incremental_merge", "unique_key": "id"},
                        },
                    }
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (root / "dpone.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.project.v1",
                "authoring": {"primary_source_policy": "one_per_pipeline"},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    domain_path = root / "domains/sales.yaml"
    domain_path.parent.mkdir(parents=True)
    domain_path.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.domain-catalog.v1",
                "domain": "sales",
                "workloads": {
                    "orders_daily": {
                        "authoring_source": "pipelines/orders_daily/pipeline.yaml",
                    }
                },
                "dags": {
                    "orders_daily": {
                        "workloads": ["orders_daily"],
                        "schedule": None,
                        "start_date": "2026-01-01",
                        "catchup": False,
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    write_mssql_connection_registry(root, include=("mssql_dev",))
    service = build_airflow_self_service_service(root=root)
    return service, source_path


def _mutate_source_when_pack_build_starts(
    *,
    source_path: Path,
    monkeypatch: Any,
) -> None:
    original_build = AirflowCompactPackBuilder.build
    mutated = False

    def build_after_source_drift(builder: AirflowCompactPackBuilder, **kwargs: Any) -> Any:
        nonlocal mutated
        if not mutated:
            payload = yaml.safe_load(source_path.read_text(encoding="utf-8"))
            payload["metadata"]["owner"] = "concurrent-writer"
            source_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
            mutated = True
        return original_build(builder, **kwargs)

    monkeypatch.setattr(AirflowCompactPackBuilder, "build", build_after_source_drift)


def _assert_no_cache_artifacts(root: Path) -> None:
    cache_root = root / ".dpone-cache"
    assert not (cache_root / "releases").exists()
    assert not (cache_root / "deployments").exists()
    assert not (cache_root / "current").exists()


def test_check_captures_primary_bytes_and_digest_from_one_snapshot(tmp_path: Path) -> None:
    _, source_path = _scaffold_project(tmp_path)
    expected_bytes = source_path.read_bytes()

    checked = AirflowAuthoringCheckService(root=tmp_path).inspect("orders_daily")

    assert checked.result.passed is True
    assert checked.source_bytes == expected_bytes
    assert checked.source_sha256 == "sha256:" + hashlib.sha256(expected_bytes).hexdigest()
    assert checked.source_label == "pipelines/orders_daily/pipeline.yaml"


def test_selected_source_digest_is_computed_from_the_compiled_bytes(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _, source_path = _scaffold_project(tmp_path)
    expected_bytes = source_path.read_bytes()
    from dpone.manifest import project_selection_flat

    confined_read = project_selection_flat.read_confined_file
    mutated = False

    def read_then_mutate(*args: Any, **kwargs: Any) -> bytes:
        nonlocal mutated
        content = confined_read(*args, **kwargs)
        relative_path = str(args[1] if len(args) > 1 else kwargs["relative_path"])
        if relative_path == "pipelines/orders_daily/pipeline.yaml" and not mutated:
            payload = yaml.safe_load(content)
            payload["metadata"]["owner"] = "concurrent-writer"
            source_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
            mutated = True
        return content

    monkeypatch.setattr(project_selection_flat, "read_confined_file", read_then_mutate)

    outcome = ProjectSelectionLoader(root=tmp_path).load(".")

    checked = outcome.checked_sources["orders_daily"]
    assert checked.payload["metadata"].get("owner") != "concurrent-writer"
    assert checked.source_sha256 == "sha256:" + hashlib.sha256(expected_bytes).hexdigest()


def test_single_preview_rejects_primary_source_drift_before_promotion(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    service, source_path = _scaffold_project(tmp_path)
    _mutate_source_when_pack_build_starts(source_path=source_path, monkeypatch=monkeypatch)

    result = service.preview("orders_daily")

    assert result.passed is False
    assert [error["code"] for error in result.errors] == [_SOURCE_DRIFT_CODE]
    _assert_no_cache_artifacts(tmp_path)


def test_selected_preview_uses_the_source_digest_captured_during_selection(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _, source_path = _scaffold_project(tmp_path)
    _mutate_source_when_pack_build_starts(source_path=source_path, monkeypatch=monkeypatch)

    result = ProjectSelectionPreviewService(root=tmp_path).preview(
        target=".",
        select=("id:orders_daily",),
        exclude=(),
        state_path=None,
        selectors_path="selectors.yaml",
        max_selected=500,
    )

    assert result.passed is False
    assert [error["code"] for error in result.errors] == [_SOURCE_DRIFT_CODE]
    _assert_no_cache_artifacts(tmp_path)


def test_safe_sample_rejects_primary_source_drift_before_release_materialization(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _, source_path = _scaffold_project(tmp_path)
    _mutate_source_when_pack_build_starts(source_path=source_path, monkeypatch=monkeypatch)

    result = ensure_local_safe_sample_deployment(
        root=tmp_path,
        pipeline_source_path="orders_daily",
        policy_environment="development",
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error["code"] == _SOURCE_DRIFT_CODE
    _assert_no_cache_artifacts(tmp_path)

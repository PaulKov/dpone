"""Regression coverage for MSSQL pack/dbt-publish review gaps (#527)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.gitops.airflow_asset_uri import (
    MSSQL_ASSET_URI_INVALID,
    resolve_explicit_mssql_uri,
    resolve_mssql_asset_registry,
)
from dpone.gitops.airflow_compact_pack_outlets import canonical_declared_outlet_map
from dpone.gitops.airflow_dag_spec_artifacts import AirflowDagSpecArtifactWriter
from dpone.gitops.airflow_mssql_registry_path import (
    MSSQL_REGISTRY_ENVIRONMENT_MISMATCH,
    MSSQL_REGISTRY_PATH_INVALID,
    MSSQL_REGISTRY_SOURCE_AMBIGUOUS,
)
from dpone.gitops.airflow_mssql_registry_snapshot import MSSQL_REGISTRY_SNAPSHOT_DRIFT
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
from dpone.readiness.dbt_airflow_pack_adapter import DbtAirflowPackBuilder
from dpone.readiness.dbt_connection_registry_staging import stage_project_connection_registries
from dpone.services.gitops.airflow_compact_pack_service import GitOpsAirflowCompactPackService
from tests.test_airflow_asset_uri_mssql_aip60 import _write_registry

_MSSQL_ALIASES = (
    "mssql",
    "MSSQL",
    "microsoft mssql",
    "microsoft_mssql",
    "odbc",
    "sqlserver",
    "sql_server",
    "sql-server",
)


def _write_platform_registry(
    root: Path,
    *,
    env: str,
    host: str,
    port: int = 1433,
    instance: str | None = None,
    document_environment: str | None = None,
) -> Path:
    path = root / "platform" / "connection-registries" / f"{env}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    authority: dict[str, object] = {"host": host, "port": port}
    if instance is not None:
        authority["instance"] = instance
    path.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": env if document_environment is None else document_environment,
                "connections": {
                    "mssql_example": {
                        "type": "mssql",
                        "connection": {"asset_authority": authority, "database": "DWH"},
                        "credentials": {
                            "resolver": "airflow_connection",
                            "connection_id": "mssql_example",
                        },
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _workload(
    tmp_path: Path, *, outlets: list[object], sink: dict[str, object] | None = None
) -> GitOpsWorkloadDefinition:
    manifest = tmp_path / "pipelines" / "demo" / "pipeline.yaml"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "name": "demo",
        "source": {
            "type": "postgres",
            "connection_id": "pg",
            "table": {"schema": "public", "name": "t"},
        },
        "sink": sink
        or {
            "type": "postgres",
            "connection_id": "pg",
            "table": {"schema": "public", "name": "t2"},
            "mode": "append",
        },
    }
    manifest.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return GitOpsWorkloadDefinition(
        workload_id="demo",
        manifest=manifest.relative_to(tmp_path).as_posix(),
        domain="demo",
        catalog_path="domains/demo.yaml",
        effective_config={"airflow": {"execution": {"outlets": outlets}}},
        provenance={},
    )


@pytest.mark.parametrize("sink_type", _MSSQL_ALIASES)
def test_dbt_release_keeps_logical_asset_ref_not_physical_uri(tmp_path: Path, sink_type: str) -> None:
    project = tmp_path / "project"
    build = tmp_path / "build"
    _write_platform_registry(project, env="prod", host="sql-prod.internal", port=1444)
    stage_project_connection_registries(project, build)

    workload = _workload(
        build,
        outlets=[
            {
                "asset_ref": {
                    "engine": "mssql",
                    "connection_ref": "mssql_example",
                    "database": "DWH",
                    "schema": "dbo",
                    "table": "orders",
                }
            }
        ],
        sink={
            "type": sink_type,
            "connection_ref": "mssql_example",
            "table": {"database": "DWH", "schema": "dbo", "name": "orders"},
            "mode": "append",
        },
    )
    report = DbtAirflowPackBuilder().build(
        workload=workload,
        output_path="packs/demo.airflow-pack.json",
        repo_root=build,
        outlet_binding="logical",
    )
    assert report.passed
    outlets = report.to_jsonable()["airflow"]["execution"]["outlets"]
    assert outlets
    assert all(isinstance(item, dict) and "asset_ref" in item for item in outlets)
    assert not any("mssql://" in json.dumps(item) for item in outlets)
    assert not any("sql-prod.internal" in json.dumps(item) for item in outlets)


def test_dbt_release_rejects_physical_mssql_uri_in_logical_binding(tmp_path: Path) -> None:
    workload = _workload(tmp_path, outlets=["mssql://sql-prod.internal:1433/DWH/dbo/orders"])
    report = DbtAirflowPackBuilder().build(
        workload=workload,
        output_path="packs/demo.airflow-pack.json",
        repo_root=tmp_path,
        outlet_binding="logical",
    )
    assert report.passed is False
    assert any(blocker.code == MSSQL_ASSET_URI_INVALID for blocker in report.blockers)


def test_logical_outlets_union_inferred_mssql_with_declared_s3(tmp_path: Path) -> None:
    """Declared must UNION with inferred — not replace the MSSQL sink asset_ref."""

    workload = _workload(
        tmp_path,
        outlets=[{"uri": "s3://bucket/path/orders.parquet"}],
        sink={
            "type": "mssql",
            "connection_ref": "mssql_example",
            "table": {"database": "DWH", "schema": "dbo", "name": "orders"},
            "mode": "append",
        },
    )
    report = DbtAirflowPackBuilder().build(
        workload=workload,
        output_path="packs/demo.airflow-pack.json",
        repo_root=tmp_path,
        outlet_binding="logical",
    )
    assert report.passed
    outlets = report.to_jsonable()["airflow"]["execution"]["outlets"]
    assert any(isinstance(item, dict) and item.get("uri") == "s3://bucket/path/orders.parquet" for item in outlets)
    assert any(
        isinstance(item, dict) and isinstance(item.get("asset_ref"), dict) and item["asset_ref"]["table"] == "orders"
        for item in outlets
    )


def test_logical_outlets_dedupe_same_asset_ref(tmp_path: Path) -> None:
    asset_ref = {
        "engine": "mssql",
        "connection_ref": "mssql_example",
        "database": "DWH",
        "schema": "dbo",
        "table": "orders",
    }
    workload = _workload(
        tmp_path,
        outlets=[{"asset_ref": dict(asset_ref)}],
        sink={
            "type": "mssql",
            "connection_ref": "mssql_example",
            "table": {"database": "DWH", "schema": "dbo", "name": "orders"},
            "mode": "append",
        },
    )
    report = DbtAirflowPackBuilder().build(
        workload=workload,
        output_path="packs/demo.airflow-pack.json",
        repo_root=tmp_path,
        outlet_binding="logical",
    )
    assert report.passed
    outlets = report.to_jsonable()["airflow"]["execution"]["outlets"]
    mssql_outlets = [item for item in outlets if isinstance(item, dict) and isinstance(item.get("asset_ref"), dict)]
    assert len(mssql_outlets) == 1
    assert mssql_outlets[0]["asset_ref"] == asset_ref
    assert "provenance" not in mssql_outlets[0]


def test_logical_outlets_declared_partition_enriches_inferred(tmp_path: Path) -> None:
    """Declared partition metadata must enrich inferred logical outlet (same identity)."""

    asset_ref = {
        "engine": "mssql",
        "connection_ref": "mssql_example",
        "database": "DWH",
        "schema": "dbo",
        "table": "orders",
    }
    partition = {"dimensions": {"ds": {"type": "temporal", "granularity": "day", "timezone": "UTC"}}}
    workload = _workload(
        tmp_path,
        outlets=[{"asset_ref": dict(asset_ref), "partition": partition}],
        sink={
            "type": "mssql",
            "connection_ref": "mssql_example",
            "table": {"database": "DWH", "schema": "dbo", "name": "orders"},
            "mode": "append",
        },
    )
    report = DbtAirflowPackBuilder().build(
        workload=workload,
        output_path="packs/demo.airflow-pack.json",
        repo_root=tmp_path,
        outlet_binding="logical",
    )
    assert report.passed
    outlets = report.to_jsonable()["airflow"]["execution"]["outlets"]
    mssql_outlets = [item for item in outlets if isinstance(item, dict) and isinstance(item.get("asset_ref"), dict)]
    assert len(mssql_outlets) == 1
    assert mssql_outlets[0]["asset_ref"] == asset_ref
    assert mssql_outlets[0]["partition"] == partition
    assert "provenance" not in mssql_outlets[0]


def test_logical_outlets_two_declared_different_partitions_is_blocker(tmp_path: Path) -> None:
    asset_ref = {
        "engine": "mssql",
        "connection_ref": "mssql_example",
        "database": "DWH",
        "schema": "dbo",
        "table": "orders",
    }
    workload = _workload(
        tmp_path,
        outlets=[
            {
                "asset_ref": dict(asset_ref),
                "partition": {"dimensions": {"ds": {"type": "temporal", "granularity": "day", "timezone": "UTC"}}},
            },
            {
                "asset_ref": dict(asset_ref),
                "partition": {"dimensions": {"ds": {"type": "temporal", "granularity": "hour", "timezone": "UTC"}}},
            },
        ],
        sink={
            "type": "postgres",
            "connection_id": "pg",
            "table": {"schema": "public", "name": "t2"},
            "mode": "append",
        },
    )
    report = DbtAirflowPackBuilder().build(
        workload=workload,
        output_path="packs/demo.airflow-pack.json",
        repo_root=tmp_path,
        outlet_binding="logical",
    )
    assert report.passed is False
    assert any(blocker.code == MSSQL_ASSET_URI_INVALID for blocker in report.blockers)
    assert any("conflicting metadata" in blocker.message for blocker in report.blockers)


def test_logical_outlets_retains_distinct_asset_ref_identities(tmp_path: Path) -> None:
    orders_ref = {
        "engine": "mssql",
        "connection_ref": "mssql_example",
        "database": "DWH",
        "schema": "dbo",
        "table": "orders",
    }
    items_ref = {
        "engine": "mssql",
        "connection_ref": "mssql_example",
        "database": "DWH",
        "schema": "dbo",
        "table": "items",
    }
    workload = _workload(
        tmp_path,
        outlets=[{"asset_ref": dict(items_ref)}],
        sink={
            "type": "mssql",
            "connection_ref": "mssql_example",
            "table": {"database": "DWH", "schema": "dbo", "name": "orders"},
            "mode": "append",
        },
    )
    report = DbtAirflowPackBuilder().build(
        workload=workload,
        output_path="packs/demo.airflow-pack.json",
        repo_root=tmp_path,
        outlet_binding="logical",
    )
    assert report.passed
    outlets = report.to_jsonable()["airflow"]["execution"]["outlets"]
    tables = {
        item["asset_ref"]["table"]
        for item in outlets
        if isinstance(item, dict) and isinstance(item.get("asset_ref"), dict)
    }
    assert tables == {"orders", "items"}
    # Declared exact asset_ref for a distinct identity is retained alongside inferred.
    assert any(
        isinstance(item, dict) and item.get("asset_ref") == items_ref and "provenance" not in item for item in outlets
    )
    assert any(
        isinstance(item, dict) and item.get("asset_ref") == orders_ref and item.get("provenance") for item in outlets
    )


def test_explicit_portless_adopts_registry_port_1444(tmp_path: Path) -> None:
    _write_registry(tmp_path, host="sql-prod.internal", port=1444)
    registry = resolve_mssql_asset_registry(tmp_path, env="dev")
    resolution = resolve_explicit_mssql_uri(
        "mssql://sql-prod.internal/DWH/dbo/orders",
        registry=registry,
    )
    assert resolution.uri == "mssql://sql-prod.internal:1444/DWH/dbo/orders"


def test_explicit_missing_instance_is_blocker(tmp_path: Path) -> None:
    _write_registry(tmp_path, host="sql-prod.internal", port=1433, instance="MSSQL01")
    registry = resolve_mssql_asset_registry(tmp_path, env="dev")
    resolution = resolve_explicit_mssql_uri(
        "mssql://sql-prod.internal:1433/DWH/dbo/orders",
        registry=registry,
    )
    assert resolution.uri is None
    assert resolution.issues[0].code == MSSQL_ASSET_URI_INVALID


def test_explicit_wrong_port_or_instance_is_blocker(tmp_path: Path) -> None:
    _write_registry(tmp_path, host="sql-prod.internal", port=1444, instance="MSSQL01")
    registry = resolve_mssql_asset_registry(tmp_path, env="dev")
    wrong_port = resolve_explicit_mssql_uri(
        "mssql://sql-prod.internal:1433/mssql01/DWH/dbo/orders",
        registry=registry,
    )
    wrong_instance = resolve_explicit_mssql_uri(
        "mssql://sql-prod.internal:1444/mssql02/DWH/dbo/orders",
        registry=registry,
    )
    assert wrong_port.uri is None and wrong_port.issues[0].code == MSSQL_ASSET_URI_INVALID
    assert wrong_instance.uri is None and wrong_instance.issues[0].code == MSSQL_ASSET_URI_INVALID


def test_registry_identity_is_relative_with_sha256_prefix(tmp_path: Path) -> None:
    _write_registry(tmp_path, host="sql-prod.internal", port=1433)
    path = tmp_path / ".dpone" / "registry" / "connection-registries" / "dev.yaml"
    registry = resolve_mssql_asset_registry(tmp_path, env="dev")
    assert registry.ok
    assert registry.content_sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    assert registry.identity()["registry_path"] == ".dpone/registry/connection-registries/dev.yaml"
    assert str(registry.identity()["registry_sha256"]).startswith("sha256:")
    assert str(registry.identity()["authority_digest"]).startswith("sha256:")


def test_registry_snapshot_drift_is_detected(tmp_path: Path) -> None:
    _write_registry(tmp_path, host="sql-prod.internal", port=1433)
    path = tmp_path / ".dpone" / "registry" / "connection-registries" / "dev.yaml"
    registry = resolve_mssql_asset_registry(tmp_path, env="dev")
    assert registry.verify_unchanged(tmp_path) is None
    path.write_text(path.read_text(encoding="utf-8") + "# drift\n", encoding="utf-8")
    drift = registry.verify_unchanged(tmp_path)
    assert drift is not None
    assert drift.code == MSSQL_REGISTRY_SNAPSHOT_DRIFT
    assert registry.identity()["registry_sha256"] is not None
    assert {auth.port for auth in registry.authorities.values()} == {1433}


def test_registry_mapping_is_immutable(tmp_path: Path) -> None:
    _write_registry(tmp_path, host="sql-prod.internal")
    registry = resolve_mssql_asset_registry(tmp_path, env="dev")
    with pytest.raises(TypeError):
        registry.authorities["mssql_example"] = registry.authorities["mssql_example"]  # type: ignore[index]


def test_canonical_dedupe_conflicting_metadata_is_blocker(tmp_path: Path) -> None:
    _write_registry(tmp_path, host="sql-prod.internal", port=1433)
    registry = resolve_mssql_asset_registry(tmp_path, env="dev")
    _map, blockers = canonical_declared_outlet_map(
        [
            "mssql://sql-prod.internal/DWH/dbo/orders",
            {
                "uri": "mssql://sql-prod.internal:1433/DWH/dbo/orders",
                "partition": {"dimensions": {"ds": {"type": "temporal", "granularity": "day", "timezone": "UTC"}}},
            },
        ],
        mssql_registry=registry,
    )
    assert any(item.code == MSSQL_ASSET_URI_INVALID for item in blockers)
    assert any("conflicting metadata" in item.message for item in blockers)


def test_stage_rejects_symlinked_registry_directory(tmp_path: Path) -> None:
    project = tmp_path / "project"
    outside = tmp_path / "outside" / "connection-registries"
    outside.mkdir(parents=True)
    (outside / "dev.yaml").write_text("schema: dpone.connection-registry.v1\nconnections: {}\n", encoding="utf-8")
    platform = project / "platform"
    platform.mkdir(parents=True)
    (platform / "connection-registries").symlink_to(outside, target_is_directory=True)
    staged = stage_project_connection_registries(project, tmp_path / "build")
    assert staged == ()


def test_direct_resolver_rejects_symlink_outside_repo(tmp_path: Path) -> None:
    outside = tmp_path / "outside" / "prod.yaml"
    outside.parent.mkdir(parents=True)
    outside.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {},
            }
        ),
        encoding="utf-8",
    )
    link = tmp_path / "platform" / "connection-registries" / "prod.yaml"
    link.parent.mkdir(parents=True)
    link.symlink_to(outside)
    registry = resolve_mssql_asset_registry(tmp_path, env="prod")
    assert registry.ok is False
    assert registry.issues[0].code == MSSQL_REGISTRY_PATH_INVALID


def test_env_prod_with_snapshot_env_dev_is_blocker(tmp_path: Path) -> None:
    _write_registry(tmp_path, host="sql-dev.internal")
    registry = resolve_mssql_asset_registry(tmp_path, env="dev")
    from dpone.gitops.airflow_compact_pack_outlets import execution_policy_with_inferred_outlets

    workload = _workload(tmp_path, outlets=["mssql://sql-dev.internal:1433/DWH/dbo/orders"])
    _execution, _warnings, blockers = execution_policy_with_inferred_outlets(
        workload=workload,
        execution={"outlets": ["mssql://sql-dev.internal:1433/DWH/dbo/orders"]},
        repo_root=tmp_path,
        env="prod",
        mssql_registry=registry,
        outlet_binding="physical",
    )
    assert any(item.code == MSSQL_REGISTRY_ENVIRONMENT_MISMATCH for item in blockers)


def test_document_environment_mismatch_is_blocker(tmp_path: Path) -> None:
    _write_platform_registry(
        tmp_path,
        env="prod",
        host="sql-prod.internal",
        document_environment="dev",
    )
    registry = resolve_mssql_asset_registry(tmp_path, env="prod")
    assert registry.ok is False
    assert registry.issues[0].code == MSSQL_REGISTRY_ENVIRONMENT_MISMATCH


def test_both_registry_locations_are_ambiguous_blocker(tmp_path: Path) -> None:
    _write_registry(tmp_path, host="sql-a.internal")
    _write_platform_registry(tmp_path, env="dev", host="sql-b.internal")
    registry = resolve_mssql_asset_registry(tmp_path, env="dev")
    assert registry.ok is False
    assert registry.issues[0].code == MSSQL_REGISTRY_SOURCE_AMBIGUOUS


@dataclass
class _MemoryFs:
    writes: list[Path]

    def write_text(self, path: Path, data: str, encoding: str = "utf-8") -> None:
        del encoding
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(data, encoding="utf-8")
        self.writes.append(path)


def test_pack_view_skips_write_on_mssql_blockers(tmp_path: Path) -> None:
    # No registry → explicit typo host fails closed; pack_view must not write.
    workload = _workload(
        tmp_path,
        outlets=["mssql://sql-typo.internal:1433/DWH/dbo/orders"],
    )
    catalog = SimpleNamespace(
        env="dev",
        workloads=(workload,),
        by_id=lambda workload_id: (
            workload if workload_id == workload.workload_id else (_ for _ in ()).throw(KeyError())
        ),
    )
    fs = _MemoryFs(writes=[])
    service = GitOpsAirflowCompactPackService(
        ctx=SimpleNamespace(settings=SimpleNamespace(repo_root=tmp_path), fs=fs),
        dag_spec_writer=AirflowDagSpecArtifactWriter(repo_root=tmp_path),
    )
    service._resolve_catalog = lambda args: catalog  # type: ignore[method-assign]
    args = SimpleNamespace(
        workload=workload.workload_id,
        workload_set="workloads.yaml",
        env="dev",
        output_path="packs/demo.airflow-pack.json",
        mode="plan",
        runner_policy=None,
        include_live_gates=False,
    )
    view = service.pack_view(args)
    assert view.report.passed is False
    assert any(blocker.code == MSSQL_ASSET_URI_INVALID for blocker in view.report.blockers)
    assert fs.writes == []
    assert not (tmp_path / "packs" / "demo.airflow-pack.json").exists()


def test_connection_ref_equal_hostname_is_not_false_blocker(tmp_path: Path) -> None:
    # Registry key may literally equal the physical hostname; that must not block.
    path = tmp_path / ".dpone" / "registry" / "connection-registries" / "dev.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "dev",
                "connections": {
                    "sql-prod.internal": {
                        "type": "mssql",
                        "connection": {
                            "asset_authority": {"host": "sql-prod.internal", "port": 1433},
                            "database": "DWH",
                        },
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    registry = resolve_mssql_asset_registry(tmp_path, env="dev")
    resolution = resolve_explicit_mssql_uri(
        "mssql://sql-prod.internal:1433/DWH/dbo/orders",
        registry=registry,
    )
    assert resolution.uri == "mssql://sql-prod.internal:1433/DWH/dbo/orders"

"""Regressions discovered by the final domain-first fresh-context review."""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
import yaml

from dpone.contracts.workload_index import (
    WORKLOAD_IDENTITY_FIELDS,
    WORKLOAD_INDEX_FIELDS,
    WORKLOAD_INDEX_ITEM_FIELDS,
)
from dpone.gitops.schema_self_service_authoring_contracts import workload_index_contract
from dpone.manifest import authoring_folder
from dpone.manifest import project_discovery_namespace as namespace_module
from dpone.manifest import project_discovery_scan as discovery_scan_module
from dpone.manifest.authoring_folder import collect_sql_file_dependencies
from dpone.manifest.confined_files import ConfinedFileError
from dpone.manifest.project_config import ProjectConfigError, resolve_project_layout
from dpone.manifest.project_discovery import ProjectDiscoveryService
from dpone.manifest.project_discovery_namespace import observe_namespace
from dpone.manifest.project_layout_authority import detect_authoring_layout
from dpone.readiness.airflow_scaffold_apply import ScaffoldApplier, ScaffoldApplyPlan, ScaffoldFileSystem
from dpone.readiness.airflow_self_service import (
    AirflowSelfServiceService,
    AuthoringLockFactory,
    Change,
    SelfServiceResult,
    build_airflow_self_service_service,
)
from dpone.readiness.airflow_self_service_models import Change as ModelChange
from dpone.services.workload_index_contract import (
    validate_workload_index,
    workload_index_from_snapshot,
)


def _init_domain_first_project(root: Path):
    service = build_airflow_self_service_service(root=root)
    assert service.init_project(airflow=True, layout="domain_first").passed
    assert service.init_domain(
        domain="crm",
        owner_team="data-crm",
        owner_contact="crm@example.com",
        approver_team="data-platform",
    ).passed
    return service


def test_pipeline_init_preserves_incomplete_scaffold_recovery_receipt(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = _init_domain_first_project(tmp_path)
    real_create = ScaffoldFileSystem.create
    create_calls = 0

    def fail_second_create(self: ScaffoldFileSystem, file):
        nonlocal create_calls
        create_calls += 1
        if create_calls == 2:
            raise OSError("password=must-not-leak")
        return real_create(self, file)

    def fail_rollback(self: ScaffoldFileSystem, created):
        del self, created
        raise OSError("token=must-not-leak")

    monkeypatch.setattr(ScaffoldFileSystem, "create", fail_second_create)
    monkeypatch.setattr(ScaffoldFileSystem, "rollback", fail_rollback)

    result = service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    )
    payload = result.to_dict()

    assert result.passed is False
    assert result.exit_code == 4
    assert result.errors[0]["code"] == "DPONE_SCAFFOLD_APPLY_FAILED"
    assert payload["recovery_required"] is True
    assert payload["rollback_journal"]["entries"]
    assert any(change["action"] == "recovery_required" for change in payload["changes"])
    assert "must-not-leak" not in str(payload)


def test_all_scaffold_entrypoints_preserve_recovery_receipts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    receipt = ScaffoldApplyPlan(
        changes=(ModelChange("recovery_required", "dpone.yaml", "Inspect rollback journal."),),
        rollback_journal={"schema": "dpone.scaffold-rollback-journal.v1", "entries": [{"path": "dpone.yaml"}]},
        rollback_issues=("dpone.yaml: rollback failed",),
        recovery_artifacts=("dpone.yaml",),
        apply_failed=True,
    )

    def fail_apply(*args: Any, **kwargs: Any):
        del args, kwargs
        error = OSError("password=must-not-leak")
        error.scaffold_receipt = receipt
        raise error

    monkeypatch.setattr(ScaffoldApplier, "apply", fail_apply)
    project_result = service.init_project(airflow=False, layout="domain_first")
    assert project_result.errors[0]["code"] == "DPONE_SCAFFOLD_APPLY_FAILED"
    assert project_result.errors[0]["stage"] == "init_project"
    assert project_result.to_dict()["recovery_required"] is True
    assert "must-not-leak" not in str(project_result.to_dict())

    monkeypatch.undo()
    assert service.init_project(airflow=False, layout="domain_first").passed
    assert service.init_domain(
        domain="crm",
        owner_team="data-crm",
        owner_contact="crm@example.com",
        approver_team="data-platform",
    ).passed

    monkeypatch.setattr(ScaffoldApplier, "apply", fail_apply)
    domain_result = service.init_domain(
        domain="finance",
        owner_team="data-finance",
        owner_contact="finance@example.com",
        approver_team="data-platform",
    )
    assert domain_result.errors[0]["code"] == "DPONE_SCAFFOLD_APPLY_FAILED"
    assert domain_result.errors[0]["stage"] == "init_domain"
    assert domain_result.to_dict()["recovery_required"] is True


def test_apply_time_conflict_is_a_structured_safety_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = _init_domain_first_project(tmp_path)
    real_create = ScaffoldFileSystem.create
    calls = 0

    def create_then_conflict(self: ScaffoldFileSystem, file):
        nonlocal calls
        calls += 1
        if calls == 2:
            from dpone.readiness.airflow_pipeline_source import ConcurrentAuthoringCreate

            raise ConcurrentAuthoringCreate(None)
        return real_create(self, file)

    monkeypatch.setattr(ScaffoldFileSystem, "create", create_then_conflict)

    result = service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    )

    assert result.passed is False
    assert result.exit_code == 4
    assert result.errors[0]["code"] == "DPONE_SCAFFOLD_APPLY_FAILED"
    assert not (tmp_path / "workloads/crm/pipelines/orders_daily").exists()


def test_project_root_replacement_cannot_redirect_pipeline_writes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    original = tmp_path / "project-original"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    swapped = False

    @contextmanager
    def replacing_lock(_root: Path):
        nonlocal swapped
        if not swapped:
            swapped = True
            root.rename(original)
            root.symlink_to(outside, target_is_directory=True)
        yield

    service = AirflowSelfServiceService(
        root=root,
        authoring_lock=replacing_lock,
    )
    result = service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    )

    assert result.passed is False
    assert not (outside / "pipelines/orders_daily/pipeline.yaml").exists()


def test_real_directory_root_replacement_cannot_redirect_project_writes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    original = tmp_path / "project-original"
    root.mkdir()
    replacement = tmp_path / "replacement"
    replacement.mkdir()

    @contextmanager
    def replacing_lock(_root: Path):
        root.rename(original)
        replacement.rename(root)
        yield

    service = AirflowSelfServiceService(
        root=root,
        authoring_lock=replacing_lock,
    )
    result = service.init_project(airflow=True, layout="domain_first")

    assert result.passed is False
    assert not (root / "dpone.yaml").exists()
    assert not (original / "dpone.yaml").exists()


def test_sql_dependency_hashing_stops_at_the_file_byte_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(authoring_folder, "MAX_SQL_DEPENDENCY_BYTES", 16)
    (tmp_path / "query.sql").write_bytes(b"x" * 17)

    with pytest.raises(ConfinedFileError) as exc:
        collect_sql_file_dependencies(
            {"processes": [{"source": {"sql_file": "query.sql"}}]},
            source_path=tmp_path / "pipeline.yaml",
            project_root=tmp_path,
        )

    assert exc.value.code == "file_too_large"


@pytest.mark.parametrize(
    "payload",
    (
        {"schema": "dpone.project.v1", "authoring": None},
        {"schema": "dpone.project.v1", "authoring": {"primary_source_policy": None}},
        {"schema": "dpone.project.v1", "airflow": None},
        {"schema": "dpone.project.v1", "airflow": {"enabled": None}},
        {"schema": "dpone.project.v1", "airflow": {"index_path": None}},
        {"schema": "dpone.project.v1", "layout": None},
    ),
)
def test_project_config_explicit_null_never_means_absent(
    tmp_path: Path,
    payload: dict[str, object],
) -> None:
    (tmp_path / "dpone.yaml").write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )

    with pytest.raises(ProjectConfigError):
        resolve_project_layout(tmp_path)


def test_hidden_namespace_entries_consume_the_scan_budget(tmp_path: Path) -> None:
    root = tmp_path / "project"
    namespace = root / "workloads"
    namespace.mkdir(parents=True)
    for index in range(3):
        (namespace / f".ignored-{index}").write_text("ignored\n", encoding="utf-8")

    observed = observe_namespace(root, namespace, limit=10, scan_limit=2)

    assert observed is not None
    assert observed.children == ()
    assert observed.scanned_entries == 3
    assert observed.scan_exceeded is True


def test_hidden_entries_cannot_bypass_the_project_scan_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _init_domain_first_project(tmp_path)
    monkeypatch.setattr(discovery_scan_module, "MAX_PROJECT_DISCOVERY_ENTRIES", 2)
    for index in range(3):
        (tmp_path / "workloads" / f".ignored-{index}").write_text(
            "ignored\n",
            encoding="utf-8",
        )

    snapshot = ProjectDiscoveryService(tmp_path).discover()

    assert snapshot.ok is False
    assert snapshot.issues[0].code == "DPONE_DISCOVERY_LIMIT_EXCEEDED"


@pytest.mark.parametrize("invalid_path", ("workloads/", "workloads//crm"))
def test_workload_index_rejects_noncanonical_project_paths(
    tmp_path: Path,
    invalid_path: str,
) -> None:
    service = _init_domain_first_project(tmp_path)
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    ).passed
    payload = workload_index_from_snapshot(ProjectDiscoveryService(tmp_path).discover())
    payload["layout_root"] = invalid_path

    with pytest.raises(ValueError, match="layout root is invalid"):
        validate_workload_index(payload)


def test_namespace_observation_rejects_directory_replaced_by_symlink_before_open(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "project"
    target = root / "workloads"
    outside = tmp_path / "outside"
    target.mkdir(parents=True)
    outside.mkdir()
    (outside / "must-not-observe").write_text("external\n", encoding="utf-8")
    original = root / "workloads-original"
    real_lstat = os.lstat
    swapped = False

    def lstat_then_swap(path: str | bytes | os.PathLike[str] | os.PathLike[bytes], *args: Any, **kwargs: Any):
        nonlocal swapped
        metadata = real_lstat(path, *args, **kwargs)
        if Path(path) == target and not swapped:
            swapped = True
            target.rename(original)
            target.symlink_to(outside, target_is_directory=True)
        return metadata

    monkeypatch.setattr(namespace_module.os, "lstat", lstat_then_swap)

    assert observe_namespace(root, target, limit=10) is None


def test_flat_authority_rejects_directory_replaced_by_symlink_before_open(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "project"
    target = root / "pipelines"
    outside = tmp_path / "outside"
    target.mkdir(parents=True)
    outside.mkdir()
    (outside / "external.yaml").write_text("external\n", encoding="utf-8")
    original = root / "pipelines-original"
    real_lstat = os.lstat
    swapped = False

    def lstat_then_swap(path: str | bytes | os.PathLike[str] | os.PathLike[bytes], *args: Any, **kwargs: Any):
        nonlocal swapped
        metadata = real_lstat(path, *args, **kwargs)
        if Path(path) == target and not swapped:
            swapped = True
            target.rename(original)
            target.symlink_to(outside, target_is_directory=True)
        return metadata

    monkeypatch.setattr(namespace_module.os, "lstat", lstat_then_swap)

    assert detect_authoring_layout(root) == "unsafe"


def test_flat_project_ignores_unrelated_nonempty_workloads_directory(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="flat").passed
    unrelated = tmp_path / "workloads/model-training"
    unrelated.mkdir(parents=True)
    (unrelated / "README.md").write_text("Not dpone authoring authority.\n", encoding="utf-8")

    result = service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    )

    assert result.passed is True
    assert service.check("orders_daily").passed is True


def test_flat_project_still_rejects_structural_domain_first_authority(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="flat").passed
    ownership = tmp_path / "workloads/crm/ownership.yaml"
    ownership.parent.mkdir(parents=True)
    ownership.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.domain-ownership.v1",
                "domain": "crm",
                "owner": {"team": "data-crm", "contact": "crm@example.com"},
                "approvers": {"github_team": "data-platform"},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    )

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED"


def test_flat_project_rejects_symlinked_domain_entry(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="flat").passed
    outside = tmp_path / "outside-domain"
    outside.mkdir()
    workloads = tmp_path / "workloads"
    workloads.mkdir()
    (workloads / "crm").symlink_to(outside, target_is_directory=True)

    result = service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    )

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED"


def test_workload_index_uses_one_canonical_field_vocabulary(tmp_path: Path) -> None:
    service = _init_domain_first_project(tmp_path)
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    ).passed

    payload = workload_index_from_snapshot(ProjectDiscoveryService(tmp_path).discover())

    assert set(payload) == set(WORKLOAD_INDEX_FIELDS)
    assert set(payload["workloads"][0]) == set(WORKLOAD_INDEX_ITEM_FIELDS)
    schema = workload_index_contract().schema
    assert tuple(schema["required"]) == WORKLOAD_INDEX_FIELDS
    assert tuple(schema["properties"]["workloads"]["items"]["required"]) == WORKLOAD_INDEX_ITEM_FIELDS
    assert tuple(field for field in WORKLOAD_INDEX_ITEM_FIELDS if field != "workload_fingerprint") == (
        WORKLOAD_IDENTITY_FIELDS
    )


def test_airflow_self_service_facade_preserves_legacy_exports() -> None:
    from dpone.readiness import airflow_self_service

    assert airflow_self_service.__all__ == [
        "AirflowSelfServiceService",
        "AuthoringLockFactory",
        "Change",
        "SelfServiceResult",
        "build_airflow_self_service_service",
    ]
    assert AirflowSelfServiceService is airflow_self_service.AirflowSelfServiceService
    assert AuthoringLockFactory is airflow_self_service.AuthoringLockFactory
    assert Change is airflow_self_service.Change
    assert SelfServiceResult is airflow_self_service.SelfServiceResult


def test_flat_selection_does_not_depend_on_domain_first_variant() -> None:
    source = Path("src/dpone/manifest/project_selection_flat.py").read_text(encoding="utf-8")

    assert "project_selection_domain_first" not in source


def test_self_service_canonicalizes_symlinked_parent_alias(tmp_path: Path) -> None:
    actual_parent = tmp_path / "actual"
    actual_root = actual_parent / "project"
    actual_root.mkdir(parents=True)
    alias_parent = tmp_path / "alias"
    alias_parent.symlink_to(actual_parent, target_is_directory=True)

    service = build_airflow_self_service_service(root=alias_parent / "project")

    assert service.init_project(airflow=False, layout="flat").passed
    assert (actual_root / "dpone.yaml").is_file()


def test_legacy_direct_pipeline_init_creates_a_missing_flat_root(tmp_path: Path) -> None:
    root = tmp_path / "new-project"

    result = build_airflow_self_service_service(root=root).init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    )

    assert result.passed is True
    assert (root / "pipelines/orders_daily/pipeline.yaml").is_file()

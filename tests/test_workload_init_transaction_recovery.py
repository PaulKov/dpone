"""Regression tests for workload-init filesystem transaction boundaries."""

from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
import yaml

from dpone.readiness.airflow_pipeline_source import ConfinedFileRollbackOutcome
from dpone.readiness.airflow_scaffold_apply import ScaffoldApplier, ScaffoldFile, ScaffoldFileSystem
from dpone.readiness.workload_init_catalog_merge import plan_domain_catalog_patch
from dpone.readiness.workload_init_service import WorkloadInitRequest, WorkloadInitService
from dpone.readiness.workload_init_templates import dag_declaration

_DOMAIN = "marketing"
_WORKLOAD_ID = "sample_web_sync"
_DAG_ID = "DAG__marketing__sample_web_sync__sync"
_MANIFEST_REF = "../../../workloads/marketing/dpone/manifests/sample_web_sync.yaml"


def test_scaffold_rollback_continues_and_retains_every_recovery_signal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = (
        ScaffoldFile(Path("first.txt"), "first\n"),
        ScaffoldFile(Path("second.txt"), "second\n"),
        ScaffoldFile(Path("third.txt"), "third\n"),
        ScaffoldFile(Path("trigger.txt"), "trigger\n"),
    )
    filesystem = ScaffoldFileSystem(tmp_path)
    original_create = filesystem.create
    original_rollback = filesystem.rollback
    rollback_events: list[str] = []
    outcome_recovery = Path(".third.txt.dpone-rollback-recovery")
    error_recovery = Path(".second.txt.dpone-rollback-recovery")

    def fail_final_create(file: ScaffoldFile):
        if file.path == files[-1].path:
            raise OSError("injected create failure")
        return original_create(file)

    def rollback_with_faults(receipt):
        rollback_events.append(receipt.path.as_posix())
        if receipt.path == files[1].path:
            (tmp_path / error_recovery).write_text("second recovery\n", encoding="utf-8")
            error = RuntimeError("injected rollback failure")
            error.recovery_artifacts = (error_recovery.as_posix(),)
            raise error
        outcome = original_rollback(receipt)
        if receipt.path == files[2].path:
            (tmp_path / outcome_recovery).write_text("third recovery\n", encoding="utf-8")
            return ConfinedFileRollbackOutcome(
                path=outcome.path,
                removed=outcome.removed,
                preserved=True,
                recovery_path=outcome_recovery,
            )
        return outcome

    monkeypatch.setattr(filesystem, "create", fail_final_create)
    monkeypatch.setattr(filesystem, "rollback", rollback_with_faults)

    with pytest.raises(OSError, match="injected create failure") as caught:
        ScaffoldApplier(tmp_path, filesystem=filesystem).apply(files)

    receipt = caught.value.scaffold_receipt
    assert rollback_events == ["third.txt", "second.txt", "first.txt"]
    assert receipt.recovery_required is True
    assert receipt.rollback_issues == ("second.txt: injected rollback failure",)
    assert set(receipt.recovery_artifacts) == {
        outcome_recovery.as_posix(),
        error_recovery.as_posix(),
    }
    journal_entry = receipt.rollback_journal["entries"][0]
    assert journal_entry["action"] == "verify_and_remove"
    assert journal_entry["kind"] == "file"
    assert journal_entry["path"] == "second.txt"
    assert journal_entry["require_empty"] is False
    assert isinstance(journal_entry["device"], int)
    assert isinstance(journal_entry["inode"], int)
    assert next(change for change in receipt.changes if change.path == "second.txt").action == "recovery_required"
    assert {change.path for change in receipt.changes if change.action == "recovery"} == {
        outcome_recovery.as_posix(),
        error_recovery.as_posix(),
    }
    assert not (tmp_path / "first.txt").exists()
    assert (tmp_path / "second.txt").read_text(encoding="utf-8") == "second\n"
    assert not (tmp_path / "third.txt").exists()


def test_workload_init_surfaces_unresolved_scaffold_rollback_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_create = ScaffoldFileSystem.create
    original_rollback = ScaffoldFileSystem.rollback
    failed_path = Path("workloads/marketing/dpone/sql/sample_web_sync.sql")
    recovery_path = failed_path.with_name(".sample_web_sync.sql.dpone-recovery-test")
    rollback_events: list[str] = []

    def fail_docs_create(self: ScaffoldFileSystem, file: ScaffoldFile):
        if file.path.as_posix().endswith("/docs/sample_web_sync.md"):
            raise OSError("injected scaffold create failure")
        return original_create(self, file)

    def fail_sql_rollback(self: ScaffoldFileSystem, receipt):
        rollback_events.append(receipt.path.as_posix())
        if receipt.path == failed_path:
            target = tmp_path / recovery_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("preserved recovery bytes\n", encoding="utf-8")
            error = RuntimeError("injected persistent rollback failure")
            error.recovery_artifacts = (recovery_path.as_posix(),)
            raise error
        return original_rollback(self, receipt)

    monkeypatch.setattr(ScaffoldFileSystem, "create", fail_docs_create)
    monkeypatch.setattr(ScaffoldFileSystem, "rollback", fail_sql_rollback)

    result = WorkloadInitService(repo_root=tmp_path).init(_request(apply=True))

    assert result.passed is False
    assert rollback_events == [
        failed_path.as_posix(),
        "workloads/marketing/dpone/manifests/sample_web_sync.yaml",
        "dpone_workloads/gitops/gitops.yaml",
        failed_path.as_posix(),
    ]
    assert result.details is not None
    journal_entry = result.details["rollback_journal"]["entries"][0]
    assert journal_entry["action"] == "verify_and_remove"
    assert journal_entry["kind"] == "file"
    assert journal_entry["path"] == failed_path.as_posix()
    assert journal_entry["require_empty"] is False
    assert isinstance(journal_entry["device"], int)
    assert isinstance(journal_entry["inode"], int)
    assert result.details["unit_of_work"] == {
        "status": "recovery_required",
        "recovery_artifacts": [recovery_path.as_posix()],
        "issues": [f"{failed_path.as_posix()}: injected persistent rollback failure"],
    }
    assert (tmp_path / failed_path).exists()
    assert (tmp_path / recovery_path).read_text(encoding="utf-8") == "preserved recovery bytes\n"
    assert not (tmp_path / "dpone_workloads/gitops/domains/marketing.yaml").exists()
    assert not (tmp_path / "ownership.yaml").exists()


@pytest.mark.parametrize("dangling", (False, True), ids=("outside-root", "dangling"))
def test_workload_init_rejects_workload_set_symlink_without_mutation(
    tmp_path: Path,
    dangling: bool,
) -> None:
    repo_root = tmp_path / "project"
    workload_set = repo_root / "dpone_workloads/gitops/gitops.yaml"
    workload_set.parent.mkdir(parents=True)
    outside = tmp_path / "outside-workload-set.yaml"
    if not dangling:
        outside.write_text("outside: unchanged\n", encoding="utf-8")
    try:
        workload_set.symlink_to(outside)
    except (NotImplementedError, OSError) as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")
    before = _tree_snapshot(repo_root)

    result = WorkloadInitService(repo_root=repo_root).init(_request(apply=True))

    assert result.passed is False
    assert any(
        change.action == "conflict" and change.path == "dpone_workloads/gitops/gitops.yaml" for change in result.changes
    )
    assert _tree_snapshot(repo_root) == before
    assert workload_set.is_symlink()
    if not dangling:
        assert outside.read_text(encoding="utf-8") == "outside: unchanged\n"


def test_workload_init_plan_is_zero_write_and_repeat_apply_is_idempotent(tmp_path: Path) -> None:
    repo_root = tmp_path / "project"
    repo_root.mkdir()
    service = WorkloadInitService(repo_root=repo_root)

    planned = service.init(_request(apply=False))

    assert planned.passed is True
    assert _tree_snapshot(repo_root) == ()

    first = service.init(_request(apply=True))
    committed = _tree_snapshot(repo_root)
    second = service.init(_request(apply=True))

    assert first.passed is True
    assert second.passed is True
    assert committed
    assert _tree_snapshot(repo_root) == committed
    assert all(change.action == "no_op" for change in second.changes)


def test_existing_exact_domain_dag_declaration_is_no_op(tmp_path: Path) -> None:
    requested = _requested_dag()
    path = _write_domain_catalog(tmp_path, requested)
    before = path.read_bytes()

    plan = _plan_domain_catalog(tmp_path, requested)

    assert plan.action == "no_op"
    assert path.read_bytes() == before


def test_existing_exact_domain_dag_preserves_user_yaml_formatting(tmp_path: Path) -> None:
    requested = _requested_dag()
    path = _write_domain_catalog(tmp_path, requested)
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False, default_flow_style=True),
        encoding="utf-8",
    )
    before = path.read_bytes()

    plan = _plan_domain_catalog(tmp_path, requested)

    assert plan.action == "no_op"
    assert plan.desired_text == before.decode("utf-8")
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    ("field_path", "existing_value"),
    (
        pytest.param(("schedule",), "@hourly", id="schedule"),
        pytest.param(("default_args", "owner"), "another_team", id="owner"),
        pytest.param(("default_args", "retries"), 7, id="retries"),
        pytest.param(("timezone",), "UTC", id="timezone"),
        pytest.param(("workloads",), ["another_workload"], id="workloads"),
        pytest.param(("unknown_user_field",), {"policy": "preserve"}, id="unknown-field"),
    ),
)
def test_existing_different_domain_dag_declaration_is_conflict(
    tmp_path: Path,
    field_path: tuple[str, ...],
    existing_value: Any,
) -> None:
    requested = _requested_dag()
    existing = deepcopy(requested)
    _set_nested(existing, field_path, existing_value)
    path = _write_domain_catalog(tmp_path, existing)
    before = path.read_bytes()

    plan = _plan_domain_catalog(tmp_path, requested)

    assert plan.action == "conflict"
    assert plan.reason is not None
    assert "DAG" in plan.reason
    assert path.read_bytes() == before


def _request(*, apply: bool) -> WorkloadInitRequest:
    return WorkloadInitRequest(
        workload_ref=f"{_DOMAIN}/{_WORKLOAD_ID}",
        source="clickhouse",
        sink="mssql",
        strategy="full_refresh",
        layout="catalog",
        apply=apply,
    )


def _requested_dag() -> dict[str, Any]:
    return dag_declaration(
        dag_id=_DAG_ID,
        workload_id=_WORKLOAD_ID,
        schedule="0 6 * * *",
        owner="data_platform",
        timezone="Europe/Moscow",
    )


def _write_domain_catalog(root: Path, declaration: dict[str, Any]) -> Path:
    path = root / "dpone_workloads/gitops/domains/marketing.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(
        yaml.safe_dump(
            {
                "domain": _DOMAIN,
                "workloads": {_WORKLOAD_ID: {"manifest": _MANIFEST_REF}},
                "dags": {_DAG_ID: declaration},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _plan_domain_catalog(root: Path, declaration: dict[str, Any]):
    return plan_domain_catalog_patch(
        repo_root=root,
        path=Path("dpone_workloads/gitops/domains/marketing.yaml"),
        domain=_DOMAIN,
        workload_id=_WORKLOAD_ID,
        manifest_ref=_MANIFEST_REF,
        dag_id=_DAG_ID,
        dag_declaration=declaration,
    )


def _set_nested(payload: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    target = payload
    for key in path[:-1]:
        nested = target[key]
        assert isinstance(nested, dict)
        target = nested
    target[path[-1]] = value


def _tree_snapshot(root: Path) -> tuple[tuple[str, str, bytes | str], ...]:
    snapshot: list[tuple[str, str, bytes | str]] = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            snapshot.append((relative, "symlink", os.readlink(path)))
        elif path.is_file():
            snapshot.append((relative, "file", path.read_bytes()))
        elif path.is_dir():
            snapshot.append((relative, "directory", ""))
    return tuple(snapshot)

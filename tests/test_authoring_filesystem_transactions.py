from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

import dpone.manifest.authoring_migration_io as migration_io
import dpone.manifest.confined_mutations as confined_mutations
import dpone.readiness.airflow_authoring_directories as authoring_directories
import dpone.readiness.airflow_loader_migration as loader_migration
import dpone.readiness.airflow_pipeline_source as pipeline_source
from dpone.manifest.authoring_migration_io import AuthoringMigrationFileSystem, AuthoringMigrationIoError
from dpone.manifest.confined_mutations import ConfinedMutationError, ConfinedReplaceOutcome
from dpone.readiness.airflow_loader_migration import AirflowLoaderMigrator, AirflowLoaderUpgrade
from dpone.readiness.airflow_pipeline_source import ConfinedAuthoringFileSystem
from dpone.readiness.airflow_scaffold_apply import (
    ScaffoldApplier,
    ScaffoldFile,
    ScaffoldFileSystem,
)
from dpone.readiness.workload_init_scaffold_rollback import rollback_scaffold_files


def test_migration_late_concurrent_winner_is_not_overwritten(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "pipeline.yaml"
    original = b"kind: dpone.flow.v1\nmetadata: {id: original}\n"
    desired = b"kind: dpone.flow.v1\nmetadata: {id: desired}\n"
    winner = b"kind: dpone.flow.v1\nmetadata: {id: concurrent-winner}\n"
    source.write_bytes(original)
    filesystem = AuthoringMigrationFileSystem(tmp_path)
    snapshot = filesystem.read_source("pipeline.yaml")
    recovery_name = ".pipeline.yaml.dpone-recovery-test"

    def preserve_source_and_install_winner(
        parent_fd: int,
        name: str,
        replacement_name: str,
        **_: object,
    ) -> None:
        os.rename(
            name,
            recovery_name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        source.write_bytes(winner)
        raise ConfinedMutationError(
            "source_changed",
            "Injected late source race.",
            recovery_name=recovery_name,
        )

    monkeypatch.setattr(migration_io, "replace_file_if_digest", preserve_source_and_install_winner)

    with pytest.raises(AuthoringMigrationIoError) as caught:
        filesystem.apply(
            source=snapshot,
            desired_source=desired,
            fragment_relative=None,
            desired_fragment=None,
        )

    assert caught.value.code == "DPONE_AUTHORING_MIGRATION_SOURCE_CHANGED"
    assert source.read_bytes() == winner
    recovery = tuple(tmp_path.glob(".pipeline.yaml.dpone-recovery-*"))
    assert len(recovery) == 1
    assert caught.value.recovery_path == recovery[0].name
    assert recovery[0].read_bytes() == original


def test_scaffold_rollback_does_not_delete_late_concurrent_winner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    filesystem = ConfinedAuthoringFileSystem(tmp_path)
    relative = Path("dpone.yaml")
    created = filesystem.create(relative, b"dpone-owned\n")
    assert created is not None
    winner = b"concurrent-user-content\n"
    real_unlink = confined_mutations.os.unlink

    def install_winner_before_quarantine_delete(
        name: str,
        *,
        dir_fd: int,
    ) -> None:
        if name.startswith(".dpone-rollback-"):
            (tmp_path / relative).write_bytes(winner)
        real_unlink(name, dir_fd=dir_fd)

    monkeypatch.setattr(confined_mutations.os, "unlink", install_winner_before_quarantine_delete)

    outcome = filesystem.rollback(created)

    assert outcome.removed is True
    assert outcome.preserved is True
    assert outcome.recovery_path is None
    assert (tmp_path / relative).read_bytes() == winner


def test_scaffold_rollback_surfaces_recovery_and_retry_preserves_both_versions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    filesystem = ConfinedAuthoringFileSystem(tmp_path)
    relative = Path("dpone.yaml")
    created = filesystem.create(relative, b"dpone-owned\n")
    assert created is not None
    displaced = b"first-concurrent-version\n"
    winner = b"second-concurrent-version\n"
    (tmp_path / relative).write_bytes(displaced)
    real_link = confined_mutations.os.link
    raced = False

    def install_winner_before_restore(
        src: str,
        dst: str,
        *,
        src_dir_fd: int,
        dst_dir_fd: int,
        follow_symlinks: bool,
    ) -> None:
        nonlocal raced
        if not raced and src.startswith(".dpone-rollback-"):
            raced = True
            (tmp_path / relative).write_bytes(winner)
        real_link(
            src,
            dst,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(confined_mutations.os, "link", install_winner_before_restore)

    outcome = filesystem.rollback(created)

    assert outcome.removed is False
    assert outcome.preserved is True
    assert outcome.recovery_path is not None
    recovery = tmp_path / outcome.recovery_path
    assert (tmp_path / relative).read_bytes() == winner
    assert recovery.read_bytes() == displaced

    retry = filesystem.rollback(created)

    assert retry.removed is False
    assert retry.preserved is True
    assert retry.recovery_path is None
    assert (tmp_path / relative).read_bytes() == winner
    assert recovery.read_bytes() == displaced


def test_scaffold_rollback_reports_unresolved_created_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    filesystem = ConfinedAuthoringFileSystem(tmp_path)
    relative = Path("nested/pipeline/pipeline.yaml")
    created = filesystem.create(relative, b"dpone-owned\n")
    assert created is not None
    real_rmdir = authoring_directories.os.rmdir

    def reject_pipeline_directory(name: str, *, dir_fd: int) -> None:
        if name == "pipeline":
            raise PermissionError("injected directory cleanup failure")
        real_rmdir(name, dir_fd=dir_fd)

    monkeypatch.setattr(authoring_directories.os, "rmdir", reject_pipeline_directory)

    outcome = filesystem.rollback(created)
    report = rollback_scaffold_files([(0, created)], rollback=lambda _: outcome)

    assert outcome.removed is True
    assert outcome.directory_recovery_paths == (Path("nested/pipeline"),)
    assert report.unresolved_paths == ("nested/pipeline",)
    assert report.issues == ("nested/pipeline: created directory rollback could not be verified",)
    assert report.receipt_changes[0][1].action == "recovery_required"


def test_scaffold_failure_before_file_receipt_reports_preserved_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    file = ScaffoldFile(
        Path("nested/pipeline/pipeline.yaml"),
        "schema: dpone.flow.v1\n",
    )

    def fail_install(*args, **kwargs):
        del args, kwargs
        concurrent = tmp_path / "nested/pipeline/concurrent.txt"
        concurrent.write_text("user-owned\n", encoding="utf-8")
        raise PermissionError("injected install failure")

    monkeypatch.setattr(pipeline_source.os, "link", fail_install)

    with pytest.raises(PermissionError) as exc:
        ScaffoldApplier(tmp_path).apply((file,))

    receipt = exc.value.scaffold_receipt
    assert receipt.apply_failed is True
    assert "nested/pipeline" in receipt.recovery_artifacts
    assert any("nested/pipeline" in issue for issue in receipt.rollback_issues)
    assert (tmp_path / "nested/pipeline/concurrent.txt").read_text(encoding="utf-8") == "user-owned\n"


def test_partial_directory_cleanup_failure_keeps_identity_bound_recovery_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_open = authoring_directories.os.open
    real_rmdir = authoring_directories.os.rmdir
    pipeline_open_attempts = 0

    def fail_open_after_pipeline_creation(path, flags, *args, **kwargs):
        nonlocal pipeline_open_attempts
        if path == "pipeline":
            pipeline_open_attempts += 1
            if pipeline_open_attempts == 2:
                raise PermissionError("injected parent open failure")
        return real_open(path, flags, *args, **kwargs)

    def fail_pipeline_cleanup(name: str, *, dir_fd: int) -> None:
        if name == "pipeline":
            raise PermissionError("injected directory cleanup failure")
        real_rmdir(name, dir_fd=dir_fd)

    monkeypatch.setattr(authoring_directories.os, "open", fail_open_after_pipeline_creation)
    monkeypatch.setattr(authoring_directories.os, "rmdir", fail_pipeline_cleanup)

    with pytest.raises(PermissionError) as caught:
        ScaffoldApplier(tmp_path).apply(
            (ScaffoldFile(Path("nested/pipeline/pipeline.yaml"), "schema: dpone.flow.v1\n"),)
        )

    receipt = caught.value.scaffold_receipt
    assert receipt.recovery_required is True
    entries = receipt.rollback_journal["entries"]
    entry = next(item for item in entries if item["path"] == "nested/pipeline")
    assert entry["action"] == "verify_and_remove"
    assert entry["kind"] == "directory"
    assert entry["require_empty"] is True
    assert isinstance(entry["device"], int)
    assert isinstance(entry["inode"], int)
    assert {item["path"] for item in entries} == {"nested", "nested/pipeline"}
    assert (tmp_path / "nested/pipeline").is_dir()


def test_compensate_rebuilds_recovery_journal_after_partial_rollback_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = (
        ScaffoldFile(Path("first.yaml"), "one\n"),
        ScaffoldFile(Path("second.yaml"), "two\n"),
    )
    applier = ScaffoldApplier(tmp_path)
    plan = applier.apply(files)
    assert len(plan.created_files) == 2
    assert any(item.get("action") == "delete" for item in plan.rollback_journal["entries"])

    calls = 0
    real_rollback = applier._filesystem.rollback

    def flaky_rollback(created):
        nonlocal calls
        calls += 1
        if created.path.as_posix() == "second.yaml":
            raise PermissionError("injected rollback failure")
        return real_rollback(created)

    monkeypatch.setattr(applier._filesystem, "rollback", flaky_rollback)

    compensated = applier.compensate(plan)

    assert compensated.apply_failed
    assert compensated.recovery_required
    entries = compensated.rollback_journal["entries"]
    second_entry = next(item for item in entries if item["path"] == "second.yaml")
    assert second_entry["action"] == "verify_and_remove"
    assert isinstance(second_entry["device"], int)
    assert isinstance(second_entry["inode"], int)
    assert not any(item.get("action") == "delete" for item in entries)
    assert any(change.action == "recovery_required" for change in compensated.changes)


def test_scaffold_diff_uses_bounded_yaml_before_object_construction(
    tmp_path: Path,
) -> None:
    path = tmp_path / "pipeline.yaml"
    path.write_text("value: " + "[" * 1_500 + "0" + "]" * 1_500 + "\n", encoding="utf-8")

    plan = ScaffoldApplier(tmp_path).plan((ScaffoldFile(Path("pipeline.yaml"), "schema: dpone.flow.v1\n"),))

    assert plan.has_conflict is True
    assert "unparseable structured content omitted by dpone" in plan.changes[0].diff


def test_loader_migration_preserves_displaced_bytes_when_cleanup_is_incomplete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dags = tmp_path / "dags"
    dags.mkdir()
    loader = dags / "dpone.py"
    original = b"# generated loader v1\n"
    desired = b"# generated loader v2\n"
    loader.write_bytes(original)
    upgrade = AirflowLoaderUpgrade(
        content=original,
        expected_sha256="sha256:" + hashlib.sha256(original).hexdigest(),
    )

    def commit_without_cleanup(
        parent_fd: int,
        name: str,
        replacement_name: str,
        **_: object,
    ) -> ConfinedReplaceOutcome:
        displaced = f".{name}.displaced"
        loader_migration.os.rename(name, displaced, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        loader_migration.os.rename(
            replacement_name,
            name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        loader_migration.os.rename(
            displaced,
            replacement_name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        return ConfinedReplaceOutcome(
            committed=True,
            cleanup_required=True,
            recovery_name=replacement_name,
        )

    monkeypatch.setattr(loader_migration, "replace_file_if_digest", commit_without_cleanup)

    receipt = AirflowLoaderMigrator(tmp_path).apply(upgrade, desired)

    assert receipt.committed is True
    assert receipt.cleanup_required is True
    assert len(receipt.recovery_artifacts) == 1
    assert (tmp_path / receipt.recovery_artifacts[0]).read_bytes() == original
    assert loader.read_bytes() == desired


def test_scaffold_plan_conflict_marks_every_create_not_applied(tmp_path: Path) -> None:
    (tmp_path / "second.txt").write_text("user-owned\n", encoding="utf-8")
    files = (
        ScaffoldFile(Path("first.txt"), "first-operation-content\n"),
        ScaffoldFile(Path("second.txt"), "second-operation-content\n"),
        ScaffoldFile(Path("third.txt"), "third-operation-content\n"),
    )

    result = ScaffoldApplier(tmp_path).apply(files)

    assert [change.action for change in result.changes] == [
        "not_applied",
        "conflict",
        "not_applied",
    ]
    assert not (tmp_path / files[0].path).exists()
    assert (tmp_path / files[1].path).read_text(encoding="utf-8") == "user-owned\n"
    assert not (tmp_path / files[2].path).exists()


def test_partial_scaffold_conflict_reports_rollback_and_not_applied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = (
        ScaffoldFile(Path("first.txt"), "first-operation-content\n"),
        ScaffoldFile(Path("second.txt"), "second-operation-content\n"),
        ScaffoldFile(Path("third.txt"), "third-operation-content\n"),
    )
    filesystem = ScaffoldFileSystem(tmp_path)
    create = filesystem.create

    def compete_on_second(file: ScaffoldFile):
        if file.path == files[1].path:
            (tmp_path / file.path).write_text("concurrent-user-content\n", encoding="utf-8")
        return create(file)

    monkeypatch.setattr(filesystem, "create", compete_on_second)

    result = ScaffoldApplier(tmp_path, filesystem=filesystem).apply(files)

    assert [change.action for change in result.changes] == [
        "rolled_back",
        "conflict",
        "not_applied",
    ]
    assert not (tmp_path / files[0].path).exists()
    assert (tmp_path / files[1].path).read_text(encoding="utf-8") == "concurrent-user-content\n"
    assert not (tmp_path / files[2].path).exists()
    assert result.rollback_journal["entries"] == []


def test_scaffold_rollback_removes_only_its_empty_nested_directories(tmp_path: Path) -> None:
    pipelines = tmp_path / "workloads" / "crm" / "pipelines"
    pipelines.mkdir(parents=True)
    files = (
        ScaffoldFile(
            Path("workloads/crm/pipelines/orders_daily/pipeline.yaml"),
            "schema: dpone.flow.v1\n",
        ),
        ScaffoldFile(
            Path("workloads/crm/pipelines/orders_daily/tests/pipeline.test.yaml"),
            "schema: dpone.test.v1\n",
        ),
    )

    result = ScaffoldApplier(tmp_path).apply(files, postcondition=lambda: False)

    assert [change.action for change in result.changes] == ["rolled_back", "rolled_back", "conflict"]
    assert pipelines.is_dir()
    assert not (pipelines / "orders_daily").exists()


def test_final_scaffold_read_failure_rolls_back_created_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = (
        ScaffoldFile(Path("first.txt"), "first-operation-content\n"),
        ScaffoldFile(Path("second.txt"), "second-operation-content\n"),
    )
    filesystem = ScaffoldFileSystem(tmp_path)
    read = filesystem.read
    create = filesystem.create
    created = 0

    def count_create(file: ScaffoldFile):
        nonlocal created
        result = create(file)
        if result is not None:
            created += 1
        return result

    def fail_final_read(path: Path):
        if created == len(files):
            raise PermissionError("simulated final read failure")
        return read(path)

    monkeypatch.setattr(filesystem, "create", count_create)
    monkeypatch.setattr(filesystem, "read", fail_final_read)

    result = ScaffoldApplier(tmp_path, filesystem=filesystem).apply(files)

    assert result.has_conflict
    assert any(change.path == "project-authority" for change in result.changes)
    assert not (tmp_path / "first.txt").exists()
    assert not (tmp_path / "second.txt").exists()
    assert result.rollback_journal["entries"] == []


def test_partial_scaffold_surfaces_preserved_recovery_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = (
        ScaffoldFile(Path("first.txt"), "first-operation-content\n"),
        ScaffoldFile(Path("second.txt"), "second-operation-content\n"),
        ScaffoldFile(Path("third.txt"), "third-operation-content\n"),
    )
    filesystem = ScaffoldFileSystem(tmp_path)
    create = filesystem.create
    displaced = "first-concurrent-version\n"
    winner = "second-concurrent-version\n"
    real_link = confined_mutations.os.link
    raced = False

    def compete_on_second(file: ScaffoldFile):
        if file.path == files[1].path:
            (tmp_path / files[0].path).write_text(displaced, encoding="utf-8")
            (tmp_path / file.path).write_text("second-file-winner\n", encoding="utf-8")
        return create(file)

    def install_winner_before_restore(
        src: str,
        dst: str,
        *,
        src_dir_fd: int,
        dst_dir_fd: int,
        follow_symlinks: bool,
    ) -> None:
        nonlocal raced
        if not raced and src.startswith(".dpone-rollback-"):
            raced = True
            (tmp_path / files[0].path).write_text(winner, encoding="utf-8")
        real_link(
            src,
            dst,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(filesystem, "create", compete_on_second)
    monkeypatch.setattr(confined_mutations.os, "link", install_winner_before_restore)

    result = ScaffoldApplier(tmp_path, filesystem=filesystem).apply(files)

    assert [change.action for change in result.changes[:3]] == [
        "preserved",
        "conflict",
        "not_applied",
    ]
    recovery = next(change for change in result.changes if change.action == "recovery")
    assert recovery.path.startswith(".dpone-rollback-")
    assert (tmp_path / recovery.path).read_text(encoding="utf-8") == displaced
    assert (tmp_path / files[0].path).read_text(encoding="utf-8") == winner
    assert not (tmp_path / files[2].path).exists()

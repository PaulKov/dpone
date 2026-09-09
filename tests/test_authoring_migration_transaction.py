from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

import dpone.manifest.authoring_migration_io as migration_io
import dpone.manifest.authoring_migration_transaction as migration_transaction
from dpone.manifest.authoring_migration_io import (
    AuthoringMigrationFileSystem,
    AuthoringMigrationIoError,
)
from dpone.manifest.confined_mutations import ConfinedMutationError


def _migration_source(
    tmp_path: Path,
    *,
    nested: bool = False,
) -> tuple[AuthoringMigrationFileSystem, Path, bytes, bytes]:
    source = tmp_path / "pipelines" / "orders" / "pipeline.yaml" if nested else tmp_path / "pipeline.yaml"
    source.parent.mkdir(parents=True, exist_ok=True)
    original = b"kind: dpone.flow.v1\nmetadata: {id: original}\n"
    desired = b"kind: dpone.flow.v1\nmetadata: {id: migrated}\n"
    source.write_bytes(original)
    return AuthoringMigrationFileSystem(tmp_path), source, original, desired


def test_preexisting_identical_fragment_remains_unowned_during_identity_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    filesystem, source, original, desired_source = _migration_source(tmp_path)
    fragment = tmp_path / "connections.yaml"
    desired_fragment = b"connections: {}\n"
    fragment.write_bytes(desired_fragment)
    snapshot = filesystem.read_source(source)
    create_or_compare = migration_io._create_or_compare
    root_replace_called = False
    rollback_called = False

    def replace_fragment_after_compare(
        parent_fd: int,
        name: str,
        desired: bytes,
        *,
        file_mode: int,
    ) -> object:
        state = create_or_compare(parent_fd, name, desired, file_mode=file_mode)
        concurrent = tmp_path / ".connections.concurrent"
        concurrent.write_bytes(desired_fragment)
        os.replace(concurrent, fragment)
        return state

    def record_root_replace(*args: object, **kwargs: object) -> None:
        nonlocal root_replace_called
        root_replace_called = True

    def record_rollback(*args: object, **kwargs: object) -> object:
        nonlocal rollback_called
        rollback_called = True
        return SimpleNamespace(recovery_name=None)

    monkeypatch.setattr(migration_io, "_create_or_compare", replace_fragment_after_compare)
    monkeypatch.setattr(migration_io, "replace_file_if_digest", record_root_replace)
    monkeypatch.setattr(migration_transaction, "remove_file_if_owned", record_rollback)

    with pytest.raises(AuthoringMigrationIoError) as caught:
        filesystem.apply(
            source=snapshot,
            desired_source=desired_source,
            fragment_relative="connections.yaml",
            desired_fragment=desired_fragment,
        )

    assert caught.value.code == "DPONE_AUTHORING_MIGRATION_FILE_CONFLICT"
    assert root_replace_called is False
    assert rollback_called is False
    assert source.read_bytes() == original
    assert fragment.read_bytes() == desired_fragment


def test_precommit_rollback_preserves_same_content_concurrent_fragment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    filesystem, source, original, desired_source = _migration_source(tmp_path)
    fragment = tmp_path / "connections.yaml"
    desired_fragment = b"connections: {}\n"
    snapshot = filesystem.read_source(source)

    def install_winner_then_fail(*args: object, **kwargs: object) -> None:
        concurrent = tmp_path / ".connections.concurrent"
        concurrent.write_bytes(desired_fragment)
        os.replace(concurrent, fragment)
        raise ConfinedMutationError(
            "source_changed",
            "Injected pre-commit source race.",
        )

    monkeypatch.setattr(migration_io, "replace_file_if_digest", install_winner_then_fail)

    with pytest.raises(AuthoringMigrationIoError) as caught:
        filesystem.apply(
            source=snapshot,
            desired_source=desired_source,
            fragment_relative="connections.yaml",
            desired_fragment=desired_fragment,
        )

    assert caught.value.code == "DPONE_AUTHORING_MIGRATION_SOURCE_CHANGED"
    assert source.read_bytes() == original
    assert fragment.read_bytes() == desired_fragment


def test_fragment_mutation_before_root_activation_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    filesystem, source, original, desired_source = _migration_source(tmp_path)
    fragment = tmp_path / "connections.yaml"
    desired_fragment = b"connections: {}\n"
    concurrent_fragment = b"connections: {changed: true}\n"
    snapshot = filesystem.read_source(source)
    write_temporary = migration_io._write_temporary
    root_replace_called = False

    def mutate_fragment_after_root_prepare(
        parent_fd: int,
        name: str,
        content: bytes,
        *,
        file_mode: int,
    ) -> str:
        temporary = write_temporary(parent_fd, name, content, file_mode=file_mode)
        if name == source.name:
            fragment.write_bytes(concurrent_fragment)
        return temporary

    def record_root_replace(*args: object, **kwargs: object) -> None:
        nonlocal root_replace_called
        root_replace_called = True

    monkeypatch.setattr(migration_io, "_write_temporary", mutate_fragment_after_root_prepare)
    monkeypatch.setattr(migration_io, "replace_file_if_digest", record_root_replace)

    with pytest.raises(AuthoringMigrationIoError) as caught:
        filesystem.apply(
            source=snapshot,
            desired_source=desired_source,
            fragment_relative="connections.yaml",
            desired_fragment=desired_fragment,
        )

    assert caught.value.code == "DPONE_AUTHORING_MIGRATION_FILE_CONFLICT"
    assert root_replace_called is False
    assert source.read_bytes() == original
    assert fragment.read_bytes() == concurrent_fragment


@pytest.mark.parametrize("raise_committed_error", [False, True])
def test_committed_root_cleanup_failure_retains_fragment_and_relative_recovery_hint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    raise_committed_error: bool,
) -> None:
    filesystem, source, _, desired_source = _migration_source(tmp_path, nested=True)
    fragment = source.with_name("connections.yaml")
    desired_fragment = b"connections: {token: super-secret}\n"
    snapshot = filesystem.read_source(source)
    recovery_name = ".pipeline.yaml.dpone-recovery-test"

    def commit_with_cleanup_required(
        parent_fd: int,
        name: str,
        replacement_name: str,
        **_: object,
    ) -> object:
        os.replace(
            replacement_name,
            name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        if raise_committed_error:
            raise ConfinedMutationError(
                "recovery_required",
                "Injected committed cleanup failure.",
                recovery_name=recovery_name,
                committed=True,
                cleanup_required=True,
            )
        return SimpleNamespace(
            committed=True,
            cleanup_required=True,
            recovery_name=recovery_name,
        )

    monkeypatch.setattr(migration_io, "replace_file_if_digest", commit_with_cleanup_required)

    with pytest.raises(AuthoringMigrationIoError) as caught:
        filesystem.apply(
            source=snapshot,
            desired_source=desired_source,
            fragment_relative="pipelines/orders/connections.yaml",
            desired_fragment=desired_fragment,
        )

    expected_recovery = f"pipelines/orders/{recovery_name}"
    assert caught.value.code == "DPONE_AUTHORING_MIGRATION_APPLY_FAILED"
    assert caught.value.recovery_path == expected_recovery
    assert expected_recovery in str(caught.value)
    assert str(tmp_path) not in str(caught.value)
    assert "super-secret" not in str(caught.value)
    assert source.read_bytes() == desired_source
    assert fragment.read_bytes() == desired_fragment


def test_stable_source_read_rejects_in_place_mutation_before_root_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    filesystem, source, _, desired_source = _migration_source(tmp_path)
    snapshot = filesystem.read_source(source)
    concurrent = b"kind: dpone.flow.v1\nmetadata: {id: modified}\n"
    assert len(concurrent) == len(snapshot.content)
    real_read = migration_io.os.read
    mutated = False
    root_replace_called = False

    def mutate_after_first_read(descriptor: int, size: int) -> bytes:
        nonlocal mutated
        chunk = real_read(descriptor, size)
        if chunk and not mutated:
            mutated = True
            source.write_bytes(concurrent)
        return chunk

    def record_root_replace(*args: object, **kwargs: object) -> None:
        nonlocal root_replace_called
        root_replace_called = True

    monkeypatch.setattr(migration_io.os, "read", mutate_after_first_read)
    monkeypatch.setattr(migration_io, "replace_file_if_digest", record_root_replace)

    with pytest.raises(AuthoringMigrationIoError) as caught:
        filesystem.apply(
            source=snapshot,
            desired_source=desired_source,
            fragment_relative=None,
            desired_fragment=None,
        )

    assert caught.value.code == "DPONE_AUTHORING_MIGRATION_SOURCE_CHANGED"
    assert root_replace_called is False
    assert source.read_bytes() == concurrent

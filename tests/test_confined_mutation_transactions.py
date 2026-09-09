from __future__ import annotations

import hashlib
import json
import os
import secrets
from pathlib import Path
from typing import NoReturn

import pytest

from dpone.manifest import confined_atomic_exchange, confined_mutations
from dpone.manifest.confined_atomic_exchange import (
    AtomicExchangeUnsupported,
    get_native_atomic_exchange,
)
from dpone.manifest.confined_mutations import (
    ConfinedMutationError,
    OwnedFile,
    recover_file_transaction,
    remove_file_if_owned,
    replace_file_if_digest,
)
from dpone.manifest.confined_transaction_journal import transaction_journal_name

_MAX_BYTES = 256 * 1024


class _SimulatedCrash(BaseException):
    pass


def _sha256(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def _open_parent(path: Path) -> int:
    return os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
    )


def _test_exchange(parent_fd: int, left: str, right: str) -> None:
    holding = f".dpone-test-exchange-{secrets.token_hex(8)}"
    os.rename(left, holding, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
    try:
        os.rename(right, left, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        os.rename(holding, right, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
    except BaseException:
        try:
            os.rename(holding, left, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        except OSError:
            pass
        raise


def _replace_path(path: Path, content: bytes) -> None:
    temporary = path.with_name(f".{path.name}.writer-{secrets.token_hex(8)}")
    temporary.write_bytes(content)
    os.replace(temporary, path)


def _fail_unsupported() -> NoReturn:
    raise AtomicExchangeUnsupported("test adapter is unsupported")


def test_native_atomic_exchange_swaps_complete_sibling_names(tmp_path: Path) -> None:
    try:
        exchange = get_native_atomic_exchange()
    except AtomicExchangeUnsupported:
        pytest.skip("native atomic exchange is unsupported on this platform")
    left = tmp_path / "left"
    right = tmp_path / "right"
    left.write_bytes(b"left")
    right.write_bytes(b"right")
    parent_fd = _open_parent(tmp_path)
    try:
        exchange(parent_fd, left.name, right.name)
    finally:
        os.close(parent_fd)

    assert left.read_bytes() == b"right"
    assert right.read_bytes() == b"left"


def test_unsupported_adapter_fails_before_journal_or_namespace_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "pipeline.yaml"
    replacement = tmp_path / ".pipeline.prepared"
    source.write_bytes(b"old")
    replacement.write_bytes(b"new")
    monkeypatch.setattr(confined_mutations, "get_native_atomic_exchange", _fail_unsupported)
    parent_fd = _open_parent(tmp_path)
    try:
        with pytest.raises(ConfinedMutationError) as caught:
            replace_file_if_digest(
                parent_fd,
                source.name,
                replacement.name,
                expected_sha256=_sha256(b"old"),
                max_bytes=_MAX_BYTES,
            )
    finally:
        os.close(parent_fd)

    assert caught.value.code == "atomic_exchange_unsupported"
    assert source.read_bytes() == b"old"
    assert replacement.read_bytes() == b"new"
    assert not (tmp_path / transaction_journal_name(source.name)).exists()


@pytest.mark.parametrize(
    ("crash_phase", "expected_status", "source_content", "replacement_exists"),
    [
        ("journal_durable", "rolled_back", b"old", False),
        ("exchange_linearized", "committed", b"new", False),
        ("old_file_removed", "committed", b"new", False),
    ],
)
def test_crash_phase_recovery_is_deterministic_and_idempotent(
    tmp_path: Path,
    crash_phase: str,
    expected_status: str,
    source_content: bytes,
    replacement_exists: bool,
) -> None:
    source = tmp_path / "pipeline.yaml"
    replacement = tmp_path / ".pipeline.prepared"
    source.write_bytes(b"old")
    replacement.write_bytes(b"new")

    def crash_at(phase: str) -> None:
        if phase == crash_phase:
            raise _SimulatedCrash

    parent_fd = _open_parent(tmp_path)
    try:
        with pytest.raises(_SimulatedCrash):
            replace_file_if_digest(
                parent_fd,
                source.name,
                replacement.name,
                expected_sha256=_sha256(b"old"),
                max_bytes=_MAX_BYTES,
                atomic_exchange=_test_exchange,
                phase_hook=crash_at,
            )

        journal_path = tmp_path / transaction_journal_name(source.name)
        journal_payload = journal_path.read_bytes()
        assert b"old" not in journal_payload
        assert b"new" not in journal_payload
        assert str(tmp_path).encode() not in journal_payload
        assert set(json.loads(journal_payload)) == {
            "desired_sha256",
            "exchange_name",
            "expected_sha256",
            "phase",
            "schema",
            "target_name",
            "version",
        }

        recovered = recover_file_transaction(
            parent_fd,
            source.name,
            max_bytes=_MAX_BYTES,
            atomic_exchange=_test_exchange,
        )
        repeated = recover_file_transaction(
            parent_fd,
            source.name,
            max_bytes=_MAX_BYTES,
            atomic_exchange=_test_exchange,
        )
    finally:
        os.close(parent_fd)

    assert recovered.status == expected_status
    assert recovered.recovery_required is False
    assert repeated.status == "none"
    assert source.read_bytes() == source_content
    assert replacement.exists() is replacement_exists
    assert not (tmp_path / transaction_journal_name(source.name)).exists()


@pytest.mark.parametrize("journal_content", [b"{broken", b"x" * (64 * 1024)])
def test_corrupt_or_oversized_journal_is_preserved_without_mutation(
    tmp_path: Path,
    journal_content: bytes,
) -> None:
    source = tmp_path / "pipeline.yaml"
    replacement = tmp_path / ".pipeline.prepared"
    journal = tmp_path / transaction_journal_name(source.name)
    source.write_bytes(b"old")
    replacement.write_bytes(b"new")
    journal.write_bytes(journal_content)
    parent_fd = _open_parent(tmp_path)
    try:
        outcome = recover_file_transaction(
            parent_fd,
            source.name,
            max_bytes=_MAX_BYTES,
            atomic_exchange=_test_exchange,
        )
    finally:
        os.close(parent_fd)

    assert outcome.status == "recovery_required"
    assert outcome.recovery_required is True
    assert source.read_bytes() == b"old"
    assert replacement.read_bytes() == b"new"
    assert journal.read_bytes() == journal_content


def test_symlink_journal_is_not_followed_or_removed(tmp_path: Path) -> None:
    source = tmp_path / "pipeline.yaml"
    outside = tmp_path / "outside"
    journal = tmp_path / transaction_journal_name(source.name)
    source.write_bytes(b"old")
    outside.write_bytes(b"sensitive")
    journal.symlink_to(outside.name)
    parent_fd = _open_parent(tmp_path)
    try:
        outcome = recover_file_transaction(
            parent_fd,
            source.name,
            max_bytes=_MAX_BYTES,
            atomic_exchange=_test_exchange,
        )
    finally:
        os.close(parent_fd)

    assert outcome.status == "recovery_required"
    assert journal.is_symlink()
    assert outside.read_bytes() == b"sensitive"


def test_post_commit_cleanup_failure_returns_committed_outcome(tmp_path: Path) -> None:
    source = tmp_path / "pipeline.yaml"
    replacement = tmp_path / ".pipeline.prepared"
    source.write_bytes(b"old")
    replacement.write_bytes(b"new")

    def fail_cleanup(phase: str) -> None:
        if phase == "before_old_file_cleanup":
            raise OSError("injected cleanup failure")

    parent_fd = _open_parent(tmp_path)
    try:
        outcome = replace_file_if_digest(
            parent_fd,
            source.name,
            replacement.name,
            expected_sha256=_sha256(b"old"),
            max_bytes=_MAX_BYTES,
            atomic_exchange=_test_exchange,
            phase_hook=fail_cleanup,
        )
        recovered = recover_file_transaction(
            parent_fd,
            source.name,
            max_bytes=_MAX_BYTES,
            atomic_exchange=_test_exchange,
        )
    finally:
        os.close(parent_fd)

    assert outcome.committed is True
    assert outcome.cleanup_required is True
    assert outcome.recovery_name == replacement.name
    assert source.read_bytes() == b"new"
    assert recovered.status == "committed"
    assert not replacement.exists()


def test_post_exchange_directory_sync_failure_is_not_reported_as_precommit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "pipeline.yaml"
    replacement = tmp_path / ".pipeline.prepared"
    source.write_bytes(b"old")
    replacement.write_bytes(b"new")

    def fail_directory_sync(_parent_fd: int) -> None:
        raise OSError("injected directory fsync failure")

    monkeypatch.setattr(confined_atomic_exchange, "sync_directory", fail_directory_sync)
    parent_fd = _open_parent(tmp_path)
    try:
        outcome = replace_file_if_digest(
            parent_fd,
            source.name,
            replacement.name,
            expected_sha256=_sha256(b"old"),
            max_bytes=_MAX_BYTES,
            atomic_exchange=_test_exchange,
        )
    finally:
        os.close(parent_fd)

    assert outcome.committed is True
    assert outcome.cleanup_required is True
    assert outcome.recovery_name == replacement.name
    assert source.read_bytes() == b"new"
    assert replacement.read_bytes() == b"old"
    assert (tmp_path / transaction_journal_name(source.name)).exists()


def test_source_mismatch_exchanges_back_and_preserves_third_writer(tmp_path: Path) -> None:
    source = tmp_path / "pipeline.yaml"
    replacement = tmp_path / ".pipeline.prepared"
    source.write_bytes(b"expected")
    replacement.write_bytes(b"desired")

    def race(phase: str) -> None:
        if phase == "before_exchange":
            _replace_path(source, b"concurrent-source")
        elif phase == "source_mismatch_detected":
            _replace_path(source, b"third-writer")

    parent_fd = _open_parent(tmp_path)
    try:
        with pytest.raises(ConfinedMutationError) as caught:
            replace_file_if_digest(
                parent_fd,
                source.name,
                replacement.name,
                expected_sha256=_sha256(b"expected"),
                max_bytes=_MAX_BYTES,
                atomic_exchange=_test_exchange,
                phase_hook=race,
            )
    finally:
        os.close(parent_fd)

    assert caught.value.code == "source_changed"
    assert caught.value.recovery_name is not None
    assert source.read_bytes() == b"concurrent-source"
    assert (tmp_path / caught.value.recovery_name).read_bytes() == b"third-writer"
    assert not (tmp_path / transaction_journal_name(source.name)).exists()


def test_prepared_replacement_mutation_is_exchanged_back_and_preserved(tmp_path: Path) -> None:
    source = tmp_path / "pipeline.yaml"
    replacement = tmp_path / ".pipeline.prepared"
    source.write_bytes(b"expected")
    replacement.write_bytes(b"desired")

    def mutate_replacement(phase: str) -> None:
        if phase == "before_exchange":
            _replace_path(replacement, b"tampered-replacement")

    parent_fd = _open_parent(tmp_path)
    try:
        with pytest.raises(ConfinedMutationError) as caught:
            replace_file_if_digest(
                parent_fd,
                source.name,
                replacement.name,
                expected_sha256=_sha256(b"expected"),
                max_bytes=_MAX_BYTES,
                atomic_exchange=_test_exchange,
                phase_hook=mutate_replacement,
            )
    finally:
        os.close(parent_fd)

    assert caught.value.code == "replacement_changed"
    assert caught.value.recovery_name is not None
    assert source.read_bytes() == b"expected"
    assert (tmp_path / caught.value.recovery_name).read_bytes() == b"tampered-replacement"
    assert not (tmp_path / transaction_journal_name(source.name)).exists()


def test_transaction_names_are_leaf_only_and_fail_before_mutation(tmp_path: Path) -> None:
    source = tmp_path / "pipeline.yaml"
    replacement = tmp_path / ".pipeline.prepared"
    source.write_bytes(b"old")
    replacement.write_bytes(b"new")
    parent_fd = _open_parent(tmp_path)
    try:
        with pytest.raises(ConfinedMutationError) as caught:
            replace_file_if_digest(
                parent_fd,
                "../pipeline.yaml",
                replacement.name,
                expected_sha256=_sha256(b"old"),
                max_bytes=_MAX_BYTES,
                atomic_exchange=_test_exchange,
            )
    finally:
        os.close(parent_fd)

    assert caught.value.code == "path_invalid"
    assert source.read_bytes() == b"old"
    assert replacement.read_bytes() == b"new"


def test_rollback_preserves_same_content_file_with_different_inode(tmp_path: Path) -> None:
    source = tmp_path / "fragment.yaml"
    source.write_bytes(b"same")
    created = source.stat()
    owned = OwnedFile(
        device=created.st_dev,
        inode=created.st_ino,
        size=created.st_size,
        sha256=_sha256(b"same"),
    )
    _replace_path(source, b"same")
    user_file = source.stat()
    parent_fd = _open_parent(tmp_path)
    try:
        outcome = remove_file_if_owned(parent_fd, source.name, owned=owned, max_bytes=_MAX_BYTES)
    finally:
        os.close(parent_fd)

    assert (user_file.st_dev, user_file.st_ino) != (created.st_dev, created.st_ino)
    assert outcome.removed is False
    assert outcome.preserved is True
    assert source.read_bytes() == b"same"
    assert (source.stat().st_dev, source.stat().st_ino) == (user_file.st_dev, user_file.st_ino)


def test_legacy_content_owned_file_receipt_remains_compatible(tmp_path: Path) -> None:
    source = tmp_path / "fragment.yaml"
    content = b"owned"
    source.write_bytes(content)
    created = source.stat()
    owned = OwnedFile(device=created.st_dev, inode=created.st_ino, content=content)
    parent_fd = _open_parent(tmp_path)
    try:
        outcome = remove_file_if_owned(parent_fd, source.name, owned=owned, max_bytes=_MAX_BYTES)
    finally:
        os.close(parent_fd)

    assert owned.size == len(content)
    assert owned.sha256 == _sha256(content)
    assert outcome.removed is True
    assert not source.exists()

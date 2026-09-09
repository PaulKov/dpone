"""Crash-recoverable mutations for descriptor-confined authoring files."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.confined_atomic_exchange import AtomicExchange
    from dpone.manifest.confined_files import ConfinedFileSnapshot
    from dpone.manifest.confined_transaction_journal import TransactionJournalFile

import hashlib
import os
import secrets
from collections.abc import Callable
from dataclasses import dataclass

from dpone.manifest import confined_atomic_exchange
from dpone.manifest.confined_atomic_exchange import (
    AtomicExchangeUnsupported,
    exchange_back,
    get_native_atomic_exchange,
    leaf_exists,
    remove_snapshot,
    restore_quarantined_leaf,
)
from dpone.manifest.confined_files import ConfinedFileError, read_confined_leaf
from dpone.manifest.confined_transaction_journal import (
    ConfinedRecoveryOutcome,
    TransactionJournalError,
    TransactionJournalRecord,
    create_transaction_journal,
    recover_transaction,
    remove_transaction_journal,
    transaction_journal_name,
    validate_sha256,
    validate_transaction_leaf,
    validate_transaction_names,
)

PhaseHook = Callable[[str], None]


class ConfinedMutationError(OSError):
    """A confined mutation could not complete without risking user bytes."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        recovery_name: str | None = None,
        committed: bool = False,
        cleanup_required: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.recovery_name = recovery_name
        self.committed = committed
        self.cleanup_required = cleanup_required


@dataclass(frozen=True, slots=True, init=False)
class OwnedFile:
    """Exact creation ownership without retaining authoring payload bytes."""

    device: int
    inode: int
    size: int
    sha256: str

    def __init__(
        self,
        device: int,
        inode: int,
        content: bytes | None = None,
        *,
        size: int | None = None,
        sha256: str | None = None,
    ) -> None:
        if content is not None:
            content_size = len(content)
            content_sha256 = _sha256(content)
            if size is not None and size != content_size:
                raise ValueError("Owned file size does not match the compatibility payload.")
            if sha256 is not None and sha256 != content_sha256:
                raise ValueError("Owned file digest does not match the compatibility payload.")
            size = content_size
            sha256 = content_sha256
        if size is None or size < 0 or sha256 is None:
            raise ValueError("Owned file receipt requires a non-negative size and SHA-256 digest.")
        try:
            canonical_sha256 = validate_sha256(sha256)
        except TransactionJournalError as exc:
            raise ValueError("Owned file receipt requires a canonical SHA-256 digest.") from exc
        object.__setattr__(self, "device", device)
        object.__setattr__(self, "inode", inode)
        object.__setattr__(self, "size", size)
        object.__setattr__(self, "sha256", canonical_sha256)


@dataclass(frozen=True, slots=True)
class ConfinedReplaceOutcome:
    """Truthful result at and after the atomic replacement linearization point."""

    committed: bool
    cleanup_required: bool = False
    recovery_name: str | None = None


@dataclass(frozen=True, slots=True)
class ConfinedRollbackOutcome:
    """Describe what a no-clobber rollback actually changed or preserved."""

    removed: bool
    preserved: bool
    recovery_name: str | None = None


def replace_file_if_digest(
    parent_fd: int,
    name: str,
    replacement_name: str,
    *,
    expected_sha256: str,
    max_bytes: int,
    atomic_exchange: AtomicExchange | None = None,
    phase_hook: PhaseHook | None = None,
) -> ConfinedReplaceOutcome:
    """Atomically install prepared bytes if the authoritative digest is unchanged."""

    _validate_replace_inputs(parent_fd, name, replacement_name, expected_sha256, max_bytes)
    exchange = _resolve_exchange(atomic_exchange)
    recovery = recover_file_transaction(
        parent_fd,
        name,
        max_bytes=max_bytes,
        atomic_exchange=exchange,
    )
    if recovery.recovery_required:
        raise ConfinedMutationError(
            "recovery_required",
            "A prior authoring transaction requires recovery before replacement.",
            recovery_name=recovery.recovery_name,
            committed=recovery.committed,
            cleanup_required=recovery.cleanup_required,
        )
    target = _read_for_replace(parent_fd, name, max_bytes=max_bytes, role="source")
    replacement = _read_for_replace(parent_fd, replacement_name, max_bytes=max_bytes, role="replacement")
    if target.sha256 != expected_sha256:
        raise ConfinedMutationError("source_changed", "Authoring source changed before replacement.")
    if target.sha256 == replacement.sha256:
        return ConfinedReplaceOutcome(committed=True)

    record = TransactionJournalRecord(
        target_name=name,
        exchange_name=replacement_name,
        expected_sha256=expected_sha256,
        desired_sha256=replacement.sha256,
    )
    try:
        journal = create_transaction_journal(parent_fd, record)
    except TransactionJournalError as exc:
        raise _journal_mutation_error(exc, target_name=name) from exc
    _notify(phase_hook, "journal_durable")
    _notify(phase_hook, "before_exchange")
    try:
        exchange(parent_fd, name, replacement_name)
    except OSError as exc:
        raise ConfinedMutationError(
            "recovery_required",
            "Atomic replacement did not complete with a provable state.",
            recovery_name=journal.name,
            cleanup_required=True,
        ) from exc
    _notify(phase_hook, "exchange_linearized")
    directory_sync_failed = False
    try:
        confined_atomic_exchange.sync_directory(parent_fd)
    except OSError:
        directory_sync_failed = True

    try:
        displaced = read_confined_leaf(parent_fd, replacement_name, max_bytes=max_bytes)
    except ConfinedFileError as exc:
        raise ConfinedMutationError(
            "recovery_required",
            "The exchanged source cannot be validated safely.",
            recovery_name=journal.name,
            cleanup_required=True,
        ) from exc
    if displaced.sha256 != expected_sha256:
        _notify(phase_hook, "source_mismatch_detected")
        return _raise_after_source_mismatch(
            parent_fd,
            journal=journal,
            displaced_source=displaced,
            prepared=replacement,
            exchange=exchange,
            max_bytes=max_bytes,
        )
    if not confined_atomic_exchange.leaf_matches_snapshot(
        parent_fd,
        name,
        expected=replacement,
        max_bytes=max_bytes,
    ):
        _notify(phase_hook, "replacement_mismatch_detected")
        return _raise_after_source_mismatch(
            parent_fd,
            journal=journal,
            displaced_source=displaced,
            prepared=replacement,
            exchange=exchange,
            max_bytes=max_bytes,
            error_code="replacement_changed",
        )
    if directory_sync_failed:
        return ConfinedReplaceOutcome(
            committed=True,
            cleanup_required=True,
            recovery_name=replacement_name,
        )
    return _finish_committed(
        parent_fd,
        journal=journal,
        displaced=displaced,
        phase_hook=phase_hook,
    )


def recover_file_transaction(
    parent_fd: int,
    name: str,
    *,
    max_bytes: int,
    atomic_exchange: AtomicExchange | None = None,
) -> ConfinedRecoveryOutcome:
    """Recover one deterministic journal by the ADR 0023 digest-state table."""

    if max_bytes < 0:
        raise ValueError("max_bytes must be non-negative")
    exchange = _resolve_exchange(atomic_exchange)
    return recover_transaction(
        parent_fd,
        name,
        max_bytes=max_bytes,
        atomic_exchange=exchange,
    )


def remove_file_if_owned(
    parent_fd: int,
    name: str,
    *,
    owned: OwnedFile,
    max_bytes: int,
) -> ConfinedRollbackOutcome:
    """Quarantine and remove only the exact inode represented by an ownership receipt."""

    try:
        validate_transaction_leaf(parent_fd, name)
    except TransactionJournalError as exc:
        raise ConfinedMutationError("path_invalid", "Rollback path must be a sibling leaf.") from exc
    if max_bytes < 0:
        raise ValueError("max_bytes must be non-negative")
    quarantine_name = _unique_name("rollback")
    try:
        os.rename(name, quarantine_name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
    except FileNotFoundError:
        return ConfinedRollbackOutcome(removed=False, preserved=False)
    except OSError as exc:
        raise ConfinedMutationError("rollback_failed", "Owned file could not be quarantined safely.") from exc
    try:
        confined_atomic_exchange.sync_directory(parent_fd)
        candidate = read_confined_leaf(parent_fd, quarantine_name, max_bytes=max_bytes)
    except (ConfinedFileError, OSError):
        return _preserve_rollback_candidate(parent_fd, quarantine_name, name)
    if not _matches_owned(candidate, owned):
        return _preserve_rollback_candidate(parent_fd, quarantine_name, name)
    try:
        remove_snapshot(parent_fd, quarantine_name, candidate)
    except OSError:
        return ConfinedRollbackOutcome(removed=True, preserved=True, recovery_name=quarantine_name)
    return ConfinedRollbackOutcome(removed=True, preserved=leaf_exists(parent_fd, name))


def _finish_committed(
    parent_fd: int,
    *,
    journal: TransactionJournalFile,
    displaced: ConfinedFileSnapshot,
    phase_hook: PhaseHook | None,
) -> ConfinedReplaceOutcome:
    try:
        _notify(phase_hook, "before_old_file_cleanup")
        remove_snapshot(parent_fd, journal.record.exchange_name, displaced)
        _notify(phase_hook, "old_file_removed")
        remove_transaction_journal(parent_fd, journal)
    except Exception:
        recovery_name = (
            journal.record.exchange_name if leaf_exists(parent_fd, journal.record.exchange_name) else journal.name
        )
        return ConfinedReplaceOutcome(committed=True, cleanup_required=True, recovery_name=recovery_name)
    return ConfinedReplaceOutcome(committed=True)


def _raise_after_source_mismatch(
    parent_fd: int,
    *,
    journal: TransactionJournalFile,
    displaced_source: ConfinedFileSnapshot,
    prepared: ConfinedFileSnapshot,
    exchange: AtomicExchange,
    max_bytes: int,
    error_code: str = "source_changed",
) -> ConfinedReplaceOutcome:
    recovery_hint = journal.name
    try:
        rollback = exchange_back(
            parent_fd,
            target_name=journal.record.target_name,
            exchange_name=journal.record.exchange_name,
            displaced_source=displaced_source,
            desired_sha256=journal.record.desired_sha256,
            desired_identity=prepared.identity,
            atomic_exchange=exchange,
            max_bytes=max_bytes,
        )
        recovery_hint = rollback.recovery_name or recovery_hint
        remove_transaction_journal(parent_fd, journal)
    except Exception as exc:
        raise ConfinedMutationError(
            "recovery_required",
            "Source mismatch could not be rolled back unambiguously.",
            recovery_name=recovery_hint,
            cleanup_required=True,
        ) from exc
    raise ConfinedMutationError(
        error_code,
        "A confined transaction input changed before the atomic replacement.",
        recovery_name=rollback.recovery_name,
        cleanup_required=rollback.cleanup_required,
    )


def _preserve_rollback_candidate(parent_fd: int, quarantine_name: str, name: str) -> ConfinedRollbackOutcome:
    restored = restore_quarantined_leaf(parent_fd, quarantine_name, name)
    recovery_name = None if restored or not leaf_exists(parent_fd, quarantine_name) else quarantine_name
    return ConfinedRollbackOutcome(
        removed=False,
        preserved=leaf_exists(parent_fd, name) or recovery_name is not None,
        recovery_name=recovery_name,
    )


def _read_for_replace(parent_fd: int, name: str, *, max_bytes: int, role: str) -> ConfinedFileSnapshot:
    try:
        return read_confined_leaf(parent_fd, name, max_bytes=max_bytes)
    except ConfinedFileError as exc:
        code = "source_changed" if exc.code in {"file_not_found", "source_changed"} else "unsafe_path"
        raise ConfinedMutationError(code, f"Confined {role} is unavailable or unsafe.") from exc


def _matches_owned(snapshot: ConfinedFileSnapshot, owned: OwnedFile) -> bool:
    identity = snapshot.identity
    return (
        identity.device == owned.device
        and identity.inode == owned.inode
        and identity.size == owned.size
        and snapshot.sha256 == owned.sha256
    )


def _validate_replace_inputs(
    parent_fd: int,
    name: str,
    replacement_name: str,
    expected_sha256: str,
    max_bytes: int,
) -> None:
    if max_bytes < 0:
        raise ValueError("max_bytes must be non-negative")
    try:
        validate_transaction_names(parent_fd, name, replacement_name)
        validate_sha256(expected_sha256)
    except TransactionJournalError as exc:
        raise ConfinedMutationError(exc.code, "Confined replacement input is invalid.") from exc


def _resolve_exchange(exchange: AtomicExchange | None) -> AtomicExchange:
    if exchange is not None:
        return exchange
    try:
        return get_native_atomic_exchange()
    except AtomicExchangeUnsupported as exc:
        raise ConfinedMutationError(
            "atomic_exchange_unsupported",
            "This platform cannot apply confined authoring mutations atomically.",
        ) from exc


def _journal_mutation_error(error: TransactionJournalError, *, target_name: str) -> ConfinedMutationError:
    if error.code in {"path_invalid", "digest_invalid", "journal_too_large"}:
        return ConfinedMutationError(error.code, "Confined transaction journal input is invalid.")
    return ConfinedMutationError(
        "recovery_required",
        "Confined transaction journal requires recovery.",
        recovery_name=_safe_journal_name(target_name),
        cleanup_required=True,
    )


def _safe_journal_name(name: str) -> str | None:
    try:
        return transaction_journal_name(name)
    except TransactionJournalError:
        return None


def _unique_name(operation: str) -> str:
    return f".dpone-{operation}-{secrets.token_hex(16)}"


def _sha256(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def _notify(hook: PhaseHook | None, phase: str) -> None:
    if hook is not None:
        hook(phase)


__all__ = [
    "ConfinedMutationError",
    "ConfinedRecoveryOutcome",
    "ConfinedReplaceOutcome",
    "ConfinedRollbackOutcome",
    "OwnedFile",
    "recover_file_transaction",
    "remove_file_if_owned",
    "replace_file_if_digest",
]

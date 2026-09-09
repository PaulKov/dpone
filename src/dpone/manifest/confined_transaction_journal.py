"""Bounded durable journal codec for one confined file replacement."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.confined_atomic_exchange import AtomicExchange
    from dpone.manifest.confined_files import ConfinedFileSnapshot


import json
import os
import re
import stat
from dataclasses import dataclass
from typing import Any

from dpone.manifest.confined_atomic_exchange import exchange_back, leaf_exists, remove_snapshot
from dpone.manifest.confined_files import ConfinedFileError, ConfinedFileIdentity, read_confined_leaf

_SCHEMA = "dpone.confined-file-transaction"
_VERSION = 1
_PHASE = "prepared"
_JOURNAL_MAX_BYTES = 4096
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_JOURNAL_KEYS = {
    "schema",
    "version",
    "target_name",
    "exchange_name",
    "expected_sha256",
    "desired_sha256",
    "phase",
}


class TransactionJournalError(OSError):
    """A transaction journal is unsafe, ambiguous, or not durable."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class TransactionJournalRecord:
    target_name: str
    exchange_name: str
    expected_sha256: str
    desired_sha256: str
    phase: str = _PHASE


@dataclass(frozen=True, slots=True)
class TransactionJournalFile:
    name: str
    record: TransactionJournalRecord
    identity: ConfinedFileIdentity


@dataclass(frozen=True, slots=True)
class ConfinedRecoveryOutcome:
    """Describe deterministic recovery without presenting ambiguity as success."""

    status: str
    committed: bool = False
    cleanup_required: bool = False
    recovery_required: bool = False
    recovery_name: str | None = None


def transaction_journal_name(target_name: str) -> str:
    """Return the one deterministic sibling journal name for a target leaf."""

    _validate_leaf_syntax(target_name)
    return f".{target_name}.dpone-transaction.json"


def validate_transaction_names(parent_fd: int, target_name: str, exchange_name: str) -> None:
    """Reject traversal, aliases, and names that cannot fit in the parent directory."""

    journal_name = transaction_journal_name(target_name)
    validate_transaction_leaf(parent_fd, target_name)
    validate_transaction_leaf(parent_fd, exchange_name)
    if target_name == exchange_name or journal_name in {target_name, exchange_name}:
        raise TransactionJournalError("path_invalid", "Transaction names must identify distinct sibling leaves.")
    name_max = _name_max(parent_fd)
    if len(os.fsencode(journal_name)) > name_max:
        raise TransactionJournalError("path_invalid", "Transaction leaf name exceeds the filesystem limit.")


def validate_transaction_leaf(parent_fd: int, name: str) -> str:
    """Validate one bounded sibling leaf without resolving or following it."""

    _validate_leaf_syntax(name)
    if len(os.fsencode(name)) > _name_max(parent_fd):
        raise TransactionJournalError("path_invalid", "Transaction leaf name exceeds the filesystem limit.")
    return name


def validate_sha256(value: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise TransactionJournalError("digest_invalid", "Transaction digest must be canonical SHA-256.")
    return value


def create_transaction_journal(
    parent_fd: int,
    record: TransactionJournalRecord,
) -> TransactionJournalFile:
    """Create, fsync, and directory-sync one deterministic journal without clobbering."""

    validate_transaction_names(parent_fd, record.target_name, record.exchange_name)
    validate_sha256(record.expected_sha256)
    validate_sha256(record.desired_sha256)
    if record.phase != _PHASE:
        raise TransactionJournalError("journal_invalid", "Transaction journal phase is invalid.")
    name = transaction_journal_name(record.target_name)
    payload = _encode(record)
    descriptor: int | None = None
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(name, flags, 0o600, dir_fd=parent_fd)
        _write_all(descriptor, payload)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != len(payload):
            raise TransactionJournalError("journal_invalid", "Transaction journal is not a bounded regular file.")
        os.fsync(descriptor)
        identity = ConfinedFileIdentity.from_stat(os.fstat(descriptor))
    except FileExistsError as exc:
        raise TransactionJournalError("journal_exists", "An unresolved transaction journal already exists.") from exc
    except TransactionJournalError:
        raise
    except OSError as exc:
        raise TransactionJournalError("journal_write_failed", "Transaction journal could not be made durable.") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
    _fsync_directory(parent_fd)
    return TransactionJournalFile(name=name, record=record, identity=identity)


def load_transaction_journal(
    parent_fd: int,
    target_name: str,
) -> TransactionJournalFile | None:
    """Load one deterministic journal through a bounded no-follow descriptor read."""

    name = transaction_journal_name(target_name)
    if len(os.fsencode(name)) > _name_max(parent_fd):
        raise TransactionJournalError("path_invalid", "Transaction journal leaf exceeds the filesystem limit.")
    try:
        snapshot = read_confined_leaf(parent_fd, name, max_bytes=_JOURNAL_MAX_BYTES)
    except ConfinedFileError as exc:
        if exc.code == "file_not_found":
            return None
        raise TransactionJournalError("journal_invalid", "Transaction journal is unsafe or unreadable.") from exc
    record = _decode(snapshot.content, expected_target=target_name)
    return TransactionJournalFile(name=name, record=record, identity=snapshot.identity)


def remove_transaction_journal(parent_fd: int, journal: TransactionJournalFile) -> None:
    """Remove only the same regular journal inode that was parsed."""

    try:
        current = os.stat(journal.name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if not stat.S_ISREG(current.st_mode) or ConfinedFileIdentity.from_stat(current) != journal.identity:
        raise TransactionJournalError("journal_changed", "Transaction journal changed before cleanup.")
    try:
        os.unlink(journal.name, dir_fd=parent_fd)
        _fsync_directory(parent_fd)
    except OSError as exc:
        raise TransactionJournalError("journal_cleanup_failed", "Transaction journal cleanup is incomplete.") from exc


def recover_transaction(
    parent_fd: int,
    target_name: str,
    *,
    max_bytes: int,
    atomic_exchange: AtomicExchange,
) -> ConfinedRecoveryOutcome:
    """Apply the ADR 0023 recovery table to one deterministic sibling journal."""

    if max_bytes < 0:
        raise ValueError("max_bytes must be non-negative")
    try:
        validate_transaction_leaf(parent_fd, target_name)
        journal = load_transaction_journal(parent_fd, target_name)
    except TransactionJournalError:
        return _recovery_required(_safe_journal_name(target_name))
    if journal is None:
        return ConfinedRecoveryOutcome(status="none")
    record = journal.record
    try:
        validate_transaction_names(parent_fd, record.target_name, record.exchange_name)
        target = _read_optional(parent_fd, record.target_name, max_bytes=max_bytes)
        exchange_side = _read_optional(parent_fd, record.exchange_name, max_bytes=max_bytes)
    except (ConfinedFileError, TransactionJournalError):
        return _recovery_required(journal.name)
    if target is None:
        return _recovery_required(journal.name)

    target_digest = target.sha256
    exchange_digest = exchange_side.sha256 if exchange_side is not None else None
    if target_digest == record.desired_sha256 and exchange_digest == record.expected_sha256:
        return _finish_recovery(parent_fd, journal, exchange_side, status="committed", committed=True)
    if target_digest == record.expected_sha256 and exchange_digest == record.desired_sha256:
        return _finish_recovery(parent_fd, journal, exchange_side, status="rolled_back", committed=False)
    if target_digest == record.desired_sha256 and exchange_side is None:
        return _finish_recovery(parent_fd, journal, None, status="committed", committed=True)
    if target_digest != record.desired_sha256 or exchange_side is None:
        return _recovery_required(journal.name)
    recovery_hint = journal.name
    try:
        rollback = exchange_back(
            parent_fd,
            target_name=record.target_name,
            exchange_name=record.exchange_name,
            displaced_source=exchange_side,
            desired_sha256=record.desired_sha256,
            desired_identity=target.identity,
            atomic_exchange=atomic_exchange,
            max_bytes=max_bytes,
        )
        recovery_hint = rollback.recovery_name or recovery_hint
        remove_transaction_journal(parent_fd, journal)
    except Exception:
        return _recovery_required(recovery_hint)
    needs_recovery = rollback.recovery_name is not None or rollback.cleanup_required
    return ConfinedRecoveryOutcome(
        status="recovery_required" if needs_recovery else "rolled_back",
        cleanup_required=rollback.cleanup_required,
        recovery_required=needs_recovery,
        recovery_name=rollback.recovery_name,
    )


def _finish_recovery(
    parent_fd: int,
    journal: TransactionJournalFile,
    artifact: ConfinedFileSnapshot | None,
    *,
    status: str,
    committed: bool,
) -> ConfinedRecoveryOutcome:
    try:
        if artifact is not None:
            remove_snapshot(parent_fd, journal.record.exchange_name, artifact)
        remove_transaction_journal(parent_fd, journal)
    except Exception:
        recovery_name = (
            journal.record.exchange_name if leaf_exists(parent_fd, journal.record.exchange_name) else journal.name
        )
        return ConfinedRecoveryOutcome(
            status=status if committed else "recovery_required",
            committed=committed,
            cleanup_required=True,
            recovery_required=True,
            recovery_name=recovery_name,
        )
    return ConfinedRecoveryOutcome(status=status, committed=committed)


def _read_optional(parent_fd: int, name: str, *, max_bytes: int) -> ConfinedFileSnapshot | None:
    try:
        return read_confined_leaf(parent_fd, name, max_bytes=max_bytes)
    except ConfinedFileError as exc:
        if exc.code == "file_not_found":
            return None
        raise


def _recovery_required(recovery_name: str | None) -> ConfinedRecoveryOutcome:
    return ConfinedRecoveryOutcome(
        status="recovery_required",
        cleanup_required=True,
        recovery_required=True,
        recovery_name=recovery_name,
    )


def _safe_journal_name(name: str) -> str | None:
    try:
        return transaction_journal_name(name)
    except TransactionJournalError:
        return None


def _encode(record: TransactionJournalRecord) -> bytes:
    payload = {
        "schema": _SCHEMA,
        "version": _VERSION,
        "target_name": record.target_name,
        "exchange_name": record.exchange_name,
        "expected_sha256": record.expected_sha256,
        "desired_sha256": record.desired_sha256,
        "phase": record.phase,
    }
    encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    if len(encoded) > _JOURNAL_MAX_BYTES:
        raise TransactionJournalError("journal_too_large", "Transaction journal exceeds its byte limit.")
    return encoded


def _decode(content: bytes, *, expected_target: str) -> TransactionJournalRecord:
    try:
        raw = json.loads(content.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, ValueError, TypeError) as exc:
        raise TransactionJournalError("journal_invalid", "Transaction journal is corrupt.") from exc
    if not isinstance(raw, dict) or set(raw) != _JOURNAL_KEYS:
        raise TransactionJournalError("journal_invalid", "Transaction journal shape is invalid.")
    if (
        raw.get("schema") != _SCHEMA
        or type(raw.get("version")) is not int
        or raw.get("version") != _VERSION
        or raw.get("phase") != _PHASE
    ):
        raise TransactionJournalError("journal_invalid", "Transaction journal version or phase is invalid.")
    target_name = _string(raw, "target_name")
    exchange_name = _string(raw, "exchange_name")
    _validate_leaf_syntax(target_name)
    _validate_leaf_syntax(exchange_name)
    if target_name != expected_target or target_name == exchange_name:
        raise TransactionJournalError("journal_invalid", "Transaction journal leaf binding is invalid.")
    return TransactionJournalRecord(
        target_name=target_name,
        exchange_name=exchange_name,
        expected_sha256=validate_sha256(_string(raw, "expected_sha256")),
        desired_sha256=validate_sha256(_string(raw, "desired_sha256")),
        phase=_PHASE,
    )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate journal key")
        result[key] = value
    return result


def _string(value: dict[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str):
        raise TransactionJournalError("journal_invalid", "Transaction journal value is invalid.")
    return item


def _validate_leaf_syntax(name: str) -> None:
    if not isinstance(name, str) or not name or name in {".", ".."} or "/" in name or "\\" in name or "\0" in name:
        raise TransactionJournalError("path_invalid", "Transaction path must be a sibling leaf name.")


def _name_max(parent_fd: int) -> int:
    try:
        return int(os.fpathconf(parent_fd, "PC_NAME_MAX"))
    except (OSError, ValueError):
        return 255


def _write_all(descriptor: int, payload: bytes) -> None:
    remaining = memoryview(payload)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise OSError("Transaction journal write made no progress.")
        remaining = remaining[written:]


def _fsync_directory(parent_fd: int) -> None:
    try:
        os.fsync(parent_fd)
    except OSError as exc:
        raise TransactionJournalError(
            "directory_sync_failed", "Transaction directory could not be made durable."
        ) from exc


__all__ = [
    "TransactionJournalError",
    "TransactionJournalFile",
    "TransactionJournalRecord",
    "ConfinedRecoveryOutcome",
    "create_transaction_journal",
    "load_transaction_journal",
    "recover_transaction",
    "remove_transaction_journal",
    "transaction_journal_name",
    "validate_sha256",
    "validate_transaction_leaf",
    "validate_transaction_names",
]

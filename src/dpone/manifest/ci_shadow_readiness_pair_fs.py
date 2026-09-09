"""Descriptor-confined filesystem operations for a private readiness pair."""

from __future__ import annotations

import os
import secrets
import stat
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import Protocol, cast

from dpone.manifest.ci_shadow_readiness_pair_codec import decode_identity_receipt, digest, identity_receipt
from dpone.manifest.confined_atomic_exchange import AtomicExchange
from dpone.manifest.confined_files import ConfinedFileError, ConfinedFileSnapshot, read_confined_leaf
from dpone.manifest.confined_mutations import (
    ConfinedMutationError,
    ConfinedReplaceOutcome,
    OwnedFile,
    recover_file_transaction,
    remove_file_if_owned,
    replace_file_if_digest,
)

MAX_BYTES = 1_048_576


class ReadinessPairError(OSError):
    """The pair cannot be proven safe; callers must treat it as UNVERIFIED."""


class _Fcntl(Protocol):
    LOCK_EX: int
    LOCK_NB: int
    LOCK_UN: int

    def flock(self, descriptor: int, operation: int) -> None: ...


@contextmanager
def open_directory(directory: Path):
    """Open one non-symlink output directory through a descriptor."""

    fd = os.open(
        directory,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        if not stat.S_ISDIR(os.fstat(fd).st_mode):
            raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")
        yield fd
    finally:
        os.close(fd)


@contextmanager
def pair_lock(parent_fd: int, name: str):
    """Acquire one fail-closed nonblocking POSIX lock without base-import I/O."""

    fd: int | None = None
    try:
        fcntl = cast(_Fcntl, import_module("fcntl"))
        fd = os.open(
            name,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=parent_fd,
        )
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except ImportError as exc:
        raise ReadinessPairError("READINESS_OUTPUT_UNSUPPORTED") from exc
    except OSError as exc:
        raise ReadinessPairError("READINESS_OUTPUT_LOCKED") from exc
    try:
        yield
    finally:
        if fd is not None:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)


def write_new(parent_fd: int, name: str, content: bytes) -> None:
    """Create and durably sync one new, no-follow sibling leaf."""

    fd = os.open(
        name,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        0o600,
        dir_fd=parent_fd,
    )
    try:
        view = memoryview(content)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("Confined leaf write made no progress.")
            view = view[written:]
        os.fsync(fd)
    except OSError as exc:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED") from exc
    finally:
        os.close(fd)
    try:
        os.fsync(parent_fd)
        snapshot = optional(parent_fd, name, max_bytes=MAX_BYTES)
        if snapshot is None or snapshot.content != content:
            raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")
    except OSError as exc:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED") from exc


def optional(parent_fd: int, name: str, *, max_bytes: int = MAX_BYTES) -> ConfinedFileSnapshot | None:
    """Read a bounded regular leaf, returning absent only for ENOENT."""

    try:
        return read_confined_leaf(parent_fd, name, max_bytes=max_bytes)
    except ConfinedFileError as exc:
        if exc.code == "file_not_found":
            return None
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED") from exc


def require(parent_fd: int, name: object, expected: object, *, max_bytes: int = MAX_BYTES) -> ConfinedFileSnapshot:
    """Return only a present leaf whose canonical digest matches exactly."""

    if not isinstance(name, str) or not isinstance(expected, str):
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")
    snapshot = optional(parent_fd, name, max_bytes=max_bytes)
    if snapshot is None or snapshot.sha256 != expected:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")
    return snapshot


def remove_snapshot(parent_fd: int, name: str, snapshot: ConfinedFileSnapshot) -> None:
    """Remove only a just-authenticated inode and sync the parent."""

    try:
        outcome = remove_file_if_owned(
            parent_fd,
            name,
            owned=OwnedFile(
                snapshot.identity.device, snapshot.identity.inode, size=snapshot.identity.size, sha256=snapshot.sha256
            ),
            max_bytes=MAX_BYTES,
        )
        os.fsync(parent_fd)
    except OSError as exc:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED") from exc
    if outcome.preserved or outcome.recovery_name is not None:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")


def same_inode(left: ConfinedFileSnapshot, right: ConfinedFileSnapshot) -> None:
    """Reject a digest-equal but foreign winner."""

    if (left.identity.device, left.identity.inode, left.identity.size) != (
        right.identity.device,
        right.identity.inode,
        right.identity.size,
    ):
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")


def desired_owned(parent_fd: int, name: str, digest: str, device: int, inode: int) -> bool:
    """Return whether a public leaf still denotes the exact prepared inode."""

    snapshot = optional(parent_fd, name)
    return (
        snapshot is not None
        and snapshot.sha256 == digest
        and snapshot.identity.device == device
        and snapshot.identity.inode == inode
    )


def prior_or_absent(parent_fd: int, name: str, backup_digest: str | None) -> bool:
    """Recognize the authenticated pre-install state for recovery planning."""

    snapshot = optional(parent_fd, name)
    return snapshot is None if backup_digest is None else snapshot is not None and snapshot.sha256 == backup_digest


def require_same_snapshot(parent_fd: int, name: str, expected: ConfinedFileSnapshot, *, max_bytes: int) -> None:
    """Reject a same-content foreign inode before a destructive boundary."""

    current = require(parent_fd, name, expected.sha256, max_bytes=max_bytes)
    if (current.identity.device, current.identity.inode, current.identity.size) != (
        expected.identity.device,
        expected.identity.inode,
        expected.identity.size,
    ):
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")


def replace_if_current(
    parent_fd: int,
    name: str,
    stage: str,
    *,
    expected_digest: str,
    max_bytes: int,
    atomic_exchange: AtomicExchange,
) -> ConfinedReplaceOutcome:
    """Replace a journal leaf only through the confined mutation adapter."""

    try:
        return replace_file_if_digest(
            parent_fd,
            name,
            stage,
            expected_sha256=expected_digest,
            max_bytes=max_bytes,
            atomic_exchange=atomic_exchange,
        )
    except (ConfinedMutationError, OSError) as exc:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED") from exc


def recover_transaction(parent_fd: int, name: str, *, max_bytes: int, atomic_exchange: AtomicExchange) -> None:
    """Recover one mutation journal or fail closed when its state is uncertain."""

    try:
        outcome = recover_file_transaction(parent_fd, name, max_bytes=max_bytes, atomic_exchange=atomic_exchange)
    except (ConfinedMutationError, OSError) as exc:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED") from exc
    if outcome.recovery_required:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")


def publish_identity(parent_fd: int, name: str, value: dict[str, object], *, max_bytes: int) -> None:
    """Publish a no-replace receipt bound to both current public leaf inodes."""

    payload = identity_receipt(value, outputs=output_identities(parent_fd, value))
    current = optional(parent_fd, name, max_bytes=max_bytes)
    if current is not None:
        _decode_identity(current.content)
        if current.content != payload:
            raise ReadinessPairError("READINESS_OUTPUT_CONFLICT")
        return
    stage = f".{name}.stage-{secrets.token_hex(16)}"
    write_new(parent_fd, stage, payload)
    try:
        os.link(stage, name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd, follow_symlinks=False)
    except OSError as exc:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED") from exc
    remove_snapshot(parent_fd, stage, require(parent_fd, stage, digest(payload), max_bytes=max_bytes))


def identity_is_current(parent_fd: int, name: str, value: dict[str, object], *, max_bytes: int) -> bool:
    """Prove an identity receipt describes the same logical and inode identities."""

    receipt = optional(parent_fd, name, max_bytes=max_bytes)
    if receipt is None:
        return False
    recorded, outputs = _decode_identity(receipt.content)
    if recorded != value:
        raise ReadinessPairError("READINESS_OUTPUT_CONFLICT")
    if output_identities(parent_fd, value) != outputs:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")
    return True


def require_identity(parent_fd: int, name: str, value: dict[str, object], *, max_bytes: int) -> None:
    """Require a valid receipt still bound to the exact current public leaves."""

    receipt = optional(parent_fd, name, max_bytes=max_bytes)
    if receipt is None:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")
    recorded, outputs = _decode_identity(receipt.content)
    if recorded != value or output_identities(parent_fd, value) != outputs:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED")


def output_identities(parent_fd: int, value: dict[str, object]) -> dict[str, tuple[int, int]]:
    """Return exact device/inode pairs for the logical JSON and Markdown leaves."""

    result: dict[str, tuple[int, int]] = {}
    for role in ("json", "markdown"):
        entry = cast(dict[str, object], value[role])
        snapshot = require(parent_fd, entry["name"], entry["digest"])
        result[role] = (snapshot.identity.device, snapshot.identity.inode)
    return result


def _decode_identity(content: bytes) -> tuple[dict[str, object], dict[str, tuple[int, int]]]:
    try:
        return decode_identity_receipt(content)
    except (TypeError, ValueError) as exc:
        raise ReadinessPairError("READINESS_OUTPUT_UNVERIFIED") from exc

"""Immutable child-identity tombstone storage on the supervisor mount.

This module owns the durability mechanics only: the canonical record encoding,
the exclusive advisory mount lock, protected record directories, and the
staged-then-linked publication that makes every stored tombstone complete.

A tombstone is never created in place. Canonical bytes are written, synced and
verified under a dotted staging name that matches no candidate name, and each
final name is then claimed with an atomic hard link. An interrupted or failing
write therefore leaves at most an inert staging artifact, never an empty or
partial immutable record that would poison a probe slot forever. Nothing here
edits, repairs, deletes or reclaims a final tombstone.

Which names exist, and in which order they are claimed, is allocation policy and
belongs to :mod:`dpone.adapters.composition_child_identity_allocator`.
"""

from __future__ import annotations

import fcntl
import os
import re
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from errno import EACCES, EAGAIN, EIO
from time import monotonic, sleep

from dpone.adapters.composition_supervisor_filesystem import PROTECTED_FLAGS
from dpone.contracts.composition_dbt_outcome import DbtCaptureError
from dpone.contracts.strict_json import StrictJsonError, canonical_json_bytes, strict_json_object

CHILD_IDENTITY_SCHEMA = "dpone.composition-child-identity.v1"
LOCK_NAME = "allocation.lock"
STAGING_PREFIX = ".staging-"
#: Exclusive upper bound on any stored identity, keeping every value a positive
#: signed 32-bit integer for the kernel, container runtime and JSON alike.
MAX_IDENTITY = 2**31

_CREATE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
_LOCK_FLAGS = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC
_LOCK_TIMEOUT_SECONDS = 60.0
_LOCK_POLL_SECONDS = 0.01
_LOCK_CREATE_ATTEMPTS = 64
_MAX_RECORD_BYTES = 4096
_RECORD_FIELDS = frozenset({"schema", "attempt_sha256", "uid", "gid"})
_DIGEST = re.compile("sha256:[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class CompositionChildIdentityRecord:
    """Immutable tombstone naming the exact attempt that owns one identity."""

    attempt_sha256: str
    uid: int
    gid: int

    def to_bytes(self) -> bytes:
        """Return the exact canonical bytes stored under every record name."""

        document = {
            "schema": CHILD_IDENTITY_SCHEMA,
            "attempt_sha256": self.attempt_sha256,
            "uid": self.uid,
            "gid": self.gid,
        }
        return canonical_json_bytes(document) + b"\n"

    @classmethod
    def from_bytes(cls, raw: bytes) -> CompositionChildIdentityRecord:
        """Parse one exactly canonical record; any ambiguity is a rejection."""

        try:
            document = strict_json_object(raw)
        except StrictJsonError:
            raise DbtCaptureError("capture_child_identity_record") from None
        if set(document) != _RECORD_FIELDS or document["schema"] != CHILD_IDENTITY_SCHEMA:
            raise DbtCaptureError("capture_child_identity_record")
        uid, gid = document["uid"], document["gid"]
        if any(type(value) is not int or not 0 < value < MAX_IDENTITY for value in (uid, gid)):
            raise DbtCaptureError("capture_child_identity_record")
        if not isinstance(document["attempt_sha256"], str) or _DIGEST.fullmatch(document["attempt_sha256"]) is None:
            raise DbtCaptureError("capture_child_identity_record")
        record = cls(document["attempt_sha256"], uid, gid)
        if record.to_bytes() != raw:
            raise DbtCaptureError("capture_child_identity_record")
        return record


def require_digest(value: str) -> str:
    """Return the validated hexadecimal record name of one attempt digest."""

    if _DIGEST.fullmatch(value) is None:
        raise DbtCaptureError("capture_child_identity_digest")
    return value.removeprefix("sha256:")


def read_record(parent: int, name: str) -> CompositionChildIdentityRecord | None:
    """Read one bounded, root-owned, read-only regular record, or nothing."""

    try:
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent)
    except FileNotFoundError:
        return None
    except OSError:
        raise DbtCaptureError("capture_child_identity_record") from None
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o222:
            raise DbtCaptureError("capture_child_identity_record")
        raw = b""
        while len(raw) <= _MAX_RECORD_BYTES:
            chunk = os.read(descriptor, _MAX_RECORD_BYTES + 1 - len(raw))
            if not chunk:
                break
            raw += chunk
    except OSError:
        raise DbtCaptureError("capture_child_identity_record") from None
    finally:
        os.close(descriptor)
    if len(raw) > _MAX_RECORD_BYTES:
        raise DbtCaptureError("capture_child_identity_record")
    return CompositionChildIdentityRecord.from_bytes(raw)


@contextmanager
def staged(parent: int, name: str, raw: bytes) -> Iterator[str]:
    """Provide one durable, verified, non-authoritative source for publication.

    The staging name is dotted, so it matches no candidate name, is never parsed
    as authority, and never gates deterministic probing if a crash leaves it
    behind. It is removed again on both the success and the failure path.
    """

    staging = STAGING_PREFIX + name
    try:
        _materialize(parent, staging, raw)
        yield staging
    finally:
        discard(parent, staging)


def publish(source: int, staging: str, target: int, name: str) -> None:
    """Claim one final immutable name atomically, never replacing a winner.

    Both record directories belong to the one supervisor mount. A split or
    cross-device layout cannot be published atomically and is reported as a
    durable-store failure rather than silently copied.
    """

    try:
        os.link(staging, name, src_dir_fd=source, dst_dir_fd=target, follow_symlinks=False)
    except FileExistsError:
        raise DbtCaptureError("capture_child_identity_conflict") from None
    except OSError:
        raise DbtCaptureError("capture_child_identity_publish") from None


def discard(parent: int, staging: str) -> None:
    """Remove one staging artifact; final tombstones are never removed here.

    A staging artifact carries no authority, so an unremovable leftover is inert
    and must not turn an otherwise complete allocation into a failure. Only a
    staging name is ever removable, so no call path can reach a final tombstone.
    """

    if not staging.startswith(STAGING_PREFIX):
        raise DbtCaptureError("capture_child_identity_record")
    try:
        os.unlink(staging, dir_fd=parent)
    except OSError:
        return


def sync(descriptor: int) -> None:
    """Persist one directory entry, reporting an accurate durability failure."""

    try:
        os.fsync(descriptor)
    except OSError:
        raise DbtCaptureError("capture_child_identity_publish") from None


def subdirectory(parent: int, name: str) -> int:
    """Open one root-owned, private record directory, creating it durably."""

    created = True
    try:
        os.mkdir(name, mode=0o700, dir_fd=parent)
    except FileExistsError:
        created = False
    descriptor = os.open(name, PROTECTED_FLAGS, dir_fd=parent)
    try:
        if created:
            os.fchown(descriptor, 0, 0)
            os.fchmod(descriptor, 0o700)
            os.fsync(parent)
        info = os.fstat(descriptor)
        if info.st_uid != 0 or info.st_mode & 0o022:
            raise DbtCaptureError("capture_child_identity_root")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


@contextmanager
def locked(root: int, operation: int) -> Iterator[None]:
    """Serialize every allocator on one private advisory mount lock."""

    descriptor = _open_lock_file(root)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise DbtCaptureError("capture_child_identity_lock")
        _acquire(descriptor, operation)
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _materialize(parent: int, staging: str, raw: bytes) -> None:
    """Write, sync and verify the staged record before it can be published."""

    discard(parent, staging)
    try:
        descriptor = os.open(staging, _CREATE_FLAGS, 0o400, dir_fd=parent)
    except OSError:
        raise DbtCaptureError("capture_child_identity_write") from None
    try:
        os.fchown(descriptor, 0, 0)
        os.fchmod(descriptor, 0o400)
        pending = memoryview(raw)
        while pending:
            written = os.write(descriptor, pending)
            if written <= 0:
                raise OSError(EIO, "short record write")
            pending = pending[written:]
        os.fsync(descriptor)
        info = os.fstat(descriptor)
    except OSError:
        raise DbtCaptureError("capture_child_identity_write") from None
    finally:
        os.close(descriptor)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_size != len(raw)
        or info.st_uid != 0
        or info.st_mode & 0o222
    ):
        raise DbtCaptureError("capture_child_identity_write")


def _open_lock_file(root: int) -> int:
    """Open the shared lock file, tolerating concurrent first creation.

    Shared network and multi-writer mounts, including the macOS filesystems used
    for development, can report a transient ``ENOENT`` to the losing side of a
    concurrent ``O_CREAT``. The lock file carries no state, so retrying the open
    is safe; running without the lock never is.
    """

    for index in range(_LOCK_CREATE_ATTEMPTS):
        try:
            return os.open(LOCK_NAME, _LOCK_FLAGS, 0o600, dir_fd=root)
        except FileNotFoundError:
            if index + 1 == _LOCK_CREATE_ATTEMPTS:
                raise DbtCaptureError("capture_child_identity_lock") from None
            sleep(_LOCK_POLL_SECONDS)
    raise DbtCaptureError("capture_child_identity_lock")


def _acquire(descriptor: int, operation: int) -> None:
    deadline = monotonic() + _LOCK_TIMEOUT_SECONDS
    while True:
        try:
            fcntl.flock(descriptor, operation | fcntl.LOCK_NB)
            return
        except OSError as error:
            if error.errno not in {EACCES, EAGAIN}:
                raise DbtCaptureError("capture_child_identity_lock") from None
            if monotonic() >= deadline:
                raise DbtCaptureError("capture_child_identity_busy") from None
            sleep(_LOCK_POLL_SECONDS)


__all__ = [
    "CHILD_IDENTITY_SCHEMA",
    "LOCK_NAME",
    "MAX_IDENTITY",
    "STAGING_PREFIX",
    "CompositionChildIdentityRecord",
    "discard",
    "locked",
    "publish",
    "read_record",
    "require_digest",
    "staged",
    "subdirectory",
    "sync",
]

"""Persistent, never-reused child UID/GID allocation on the supervisor mount.

The administrator-provisioned ReadWriteMany mount is the durability authority
for protected execution. Exactly one attempt digest owns exactly one child UID
and one child GID forever: allocation is serialized by an exclusive advisory
lock, immutable canonical tombstones are ``fsync``ed together with their parent
directories before the identity is returned, and nothing in this module edits,
repairs, deletes or reclaims an existing record.

A tombstone is never created in place. Canonical bytes are first written, synced
and verified under a dotted staging name that matches no candidate name, and
every final name is then claimed by an atomic hard link. A tombstone that exists
is therefore always complete, and an interrupted write can leave at most an inert
staging artifact instead of an empty record that would poison a probe slot
forever.

Ordering is chosen so that every interruption fails closed. The UID and GID
claims are published first, so a crash can never release a reserved identity to
a later attempt; the attempt tombstone is published last, so its presence proves
the complete reservation is durable. An attempt that finds any record of its own
is rejected instead of receiving a second identity, because a repeated executor
for one attempt is unrecoverable. Reclaiming abandoned identities is a separate
reviewed operation after the parent is ``RETIRED``.
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
from hashlib import sha256
from pathlib import Path
from time import monotonic, sleep

from dpone.adapters.composition_supervisor_filesystem import (
    PROTECTED_FLAGS,
    absolute_supervisor_path,
    open_protected,
    require_supervisor,
)
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_dbt_outcome import DbtCaptureError
from dpone.contracts.composition_supervisor import CompositionSupervisorProjection
from dpone.contracts.strict_json import StrictJsonError, canonical_json_bytes, strict_json_object

CHILD_IDENTITY_SCHEMA = "dpone.composition-child-identity.v1"
MAX_IDENTITY_PROBES = 4096

_ATTEMPTS = "attempts"
_IDENTITIES = "identities"
_LOCK_NAME = "allocation.lock"
_STAGING_PREFIX = ".staging-"
_LOCK_FLAGS = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC
_CREATE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
_LOCK_TIMEOUT_SECONDS = 60.0
_LOCK_POLL_SECONDS = 0.01
_LOCK_CREATE_ATTEMPTS = 64
_MAX_IDENTITY = 2**31
_MAX_RECORD_BYTES = 4096
_RECORD_FIELDS = frozenset({"schema", "attempt_sha256", "uid", "gid"})
_DIGEST = re.compile("sha256:[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class CompositionChildIdentity:
    """One permanently reserved child UID and GID for a single attempt."""

    uid: int
    gid: int


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
        if any(type(value) is not int or not 0 < value < _MAX_IDENTITY for value in (uid, gid)):
            raise DbtCaptureError("capture_child_identity_record")
        if not isinstance(document["attempt_sha256"], str) or _DIGEST.fullmatch(document["attempt_sha256"]) is None:
            raise DbtCaptureError("capture_child_identity_record")
        record = cls(document["attempt_sha256"], uid, gid)
        if record.to_bytes() != raw:
            raise DbtCaptureError("capture_child_identity_record")
        return record


class CompositionChildIdentityAllocator:
    """Allocate one permanent child identity per attempt under an exclusive lock.

    Production composition roots must construct this through
    :meth:`from_projection`, so the reserved range can only come from the
    approved deployment supervisor capability. The explicit range constructor
    stays available for tests and offline tooling; it is not an authority to
    invent, widen or transpose a range.
    """

    def __init__(self, root: Path, *, uid_start: int, gid_start: int, count: int) -> None:
        if (
            type(count) is not int
            or count < 1
            or any(
                type(start) is not int or start < 1 or start + count >= _MAX_IDENTITY
                for start in (uid_start, gid_start)
            )
        ):
            raise DbtCaptureError("capture_child_identity_range")
        self.root = absolute_supervisor_path(root)
        self.uid_start, self.gid_start, self.count = uid_start, gid_start, count

    @classmethod
    def from_projection(
        cls, root: Path, projection: CompositionSupervisorProjection
    ) -> CompositionChildIdentityAllocator:
        """Bind allocation to the approved deployment supervisor capability."""

        projection.require_valid()
        return cls(
            root,
            uid_start=projection.child_uid_start,
            gid_start=projection.child_gid_start,
            count=projection.child_identity_count,
        )

    def allocate(self, attempt: CompositionAttemptIdentity) -> CompositionChildIdentity:
        """Reserve one never-reused identity for exactly this attempt digest.

        A repeated attempt is rejected rather than served, including after an
        interruption that lost only the attempt tombstone. Directory allocation
        and process execution may only start after this call returns.
        """

        attempt.__post_init__()
        digest = attempt.attempt_sha256
        name = _require_digest(digest)
        require_supervisor()
        with self._opened(fcntl.LOCK_EX) as (attempts, identities):
            if _owner(attempts, f"{name}.json", digest) is not None:
                raise DbtCaptureError("capture_child_identity_conflict")
            identity = self._probe(identities, digest)
            record = CompositionChildIdentityRecord(digest, identity.uid, identity.gid).to_bytes()
            with _staged(identities, name, record) as staging:
                for final in (
                    f"uid-{identity.uid}.json",
                    f"gid-{identity.gid}.json",
                    f"{identity.uid}-{identity.gid}.json",
                ):
                    _publish(identities, staging, identities, final)
                _sync(identities)
                _publish(identities, staging, attempts, f"{name}.json")
                _sync(attempts)
        return identity

    def read(self, uid: int) -> CompositionChildIdentityRecord:
        """Return the immutable owner record of one reserved child UID."""

        if type(uid) is not int or not self.uid_start <= uid < self.uid_start + self.count:
            raise DbtCaptureError("capture_child_identity_range")
        require_supervisor()
        with self._opened(fcntl.LOCK_SH) as (_, identities):
            record = _read_record(identities, f"uid-{uid}.json")
        if record is None:
            raise DbtCaptureError("capture_child_identity_unknown")
        return record

    def _probe(self, identities: int, digest: str) -> CompositionChildIdentity:
        """Select the deterministic bounded first free candidate pair."""

        uid = self._free_slot(identities, digest, "uid", self.uid_start)
        gid = self._free_slot(identities, digest, "gid", self.gid_start)
        if _owner(identities, f"{uid}-{gid}.json", digest) is not None:
            raise DbtCaptureError("capture_child_identity_conflict")
        return CompositionChildIdentity(uid, gid)

    def _free_slot(self, identities: int, digest: str, kind: str, start: int) -> int:
        """Return the first unclaimed slot of one axis, within the probe bound."""

        base = int.from_bytes(sha256(kind.encode("ascii") + b"\x00" + digest.encode("ascii")).digest(), "big")
        for step in range(min(self.count, MAX_IDENTITY_PROBES)):
            value = start + (base + step) % self.count
            record = _owner(identities, f"{kind}-{value}.json", digest)
            if record is None:
                return value
            if (record.uid if kind == "uid" else record.gid) != value:
                raise DbtCaptureError("capture_child_identity_conflict")
        raise DbtCaptureError("capture_child_identity_exhausted")

    @contextmanager
    def _opened(self, operation: int) -> Iterator[tuple[int, int]]:
        """Hold the supervisor lock and both record directories, or fail closed."""

        try:
            root = open_protected(self.root)
        except OSError:
            raise DbtCaptureError("capture_child_identity_unavailable") from None
        descriptors: list[int] = []
        try:
            with _locked(root, operation):
                for name in (_ATTEMPTS, _IDENTITIES):
                    descriptors.append(_subdirectory(root, name))
                yield descriptors[0], descriptors[1]
        except OSError:
            raise DbtCaptureError("capture_child_identity_unavailable") from None
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)
            os.close(root)


def _require_digest(value: str) -> str:
    """Return the validated hexadecimal record name of one attempt digest."""

    if _DIGEST.fullmatch(value) is None:
        raise DbtCaptureError("capture_child_identity_digest")
    return value.removeprefix("sha256:")


def _owner(parent: int, name: str, digest: str) -> CompositionChildIdentityRecord | None:
    """Return an existing foreign owner; this attempt's own record is a replay."""

    record = _read_record(parent, name)
    if record is not None and record.attempt_sha256 == digest:
        raise DbtCaptureError("capture_child_identity_replay")
    return record


def _read_record(parent: int, name: str) -> CompositionChildIdentityRecord | None:
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
def _staged(parent: int, name: str, raw: bytes) -> Iterator[str]:
    """Provide one durable, verified, non-authoritative source for publication.

    A final tombstone name is only ever created by linking these already durable
    bytes, so an interrupted or failing write can never publish an empty or
    partial immutable record that would poison a probe slot forever. The staging
    name is dotted, so it matches no candidate name, is never parsed as
    authority, and never gates deterministic probing if a crash leaves it behind.
    """

    staging = _STAGING_PREFIX + name
    try:
        _materialize(parent, staging, raw)
        yield staging
    finally:
        _discard(parent, staging)


def _materialize(parent: int, staging: str, raw: bytes) -> None:
    """Write, sync and verify the staged record before it can be published."""

    _discard(parent, staging)
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


def _publish(source: int, staging: str, target: int, name: str) -> None:
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


def _discard(parent: int, staging: str) -> None:
    """Remove one staging artifact; final tombstones are never removed here.

    A staging artifact carries no authority, so an unremovable leftover is inert
    and must not turn an otherwise complete allocation into a failure.
    """

    try:
        os.unlink(staging, dir_fd=parent)
    except OSError:
        return


def _sync(descriptor: int) -> None:
    """Persist one directory entry, reporting an accurate durability failure."""

    try:
        os.fsync(descriptor)
    except OSError:
        raise DbtCaptureError("capture_child_identity_publish") from None


def _subdirectory(parent: int, name: str) -> int:
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
def _locked(root: int, operation: int) -> Iterator[None]:
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


def _open_lock_file(root: int) -> int:
    """Open the shared lock file, tolerating concurrent first creation.

    Shared network and multi-writer mounts, including the macOS filesystems used
    for development, can report a transient ``ENOENT`` to the losing side of a
    concurrent ``O_CREAT``. The lock file carries no state, so retrying the open
    is safe; running without the lock never is.
    """

    for index in range(_LOCK_CREATE_ATTEMPTS):
        try:
            return os.open(_LOCK_NAME, _LOCK_FLAGS, 0o600, dir_fd=root)
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
    "MAX_IDENTITY_PROBES",
    "CompositionChildIdentity",
    "CompositionChildIdentityAllocator",
    "CompositionChildIdentityRecord",
]

"""Persistent, never-reused child UID/GID allocation on the supervisor mount.

The administrator-provisioned ReadWriteMany mount is the durability authority
for protected execution. Exactly one attempt digest owns exactly one child UID
and one child GID forever: allocation is serialized by an exclusive advisory
lock, immutable canonical tombstones are ``fsync``ed together with their parent
directories before the identity is returned, and nothing in this module edits,
repairs, deletes or reclaims an existing record.

This module owns the allocation policy: range validation, deterministic bounded
probing, replay and conflict rules, and the order in which records are claimed.
Durable storage of those records is
:mod:`dpone.adapters.composition_child_identity_store`, which publishes each
final tombstone atomically from already durable staged bytes, so a stored record
is always complete.

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
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

from dpone.adapters.composition_child_identity_store import (
    CHILD_IDENTITY_SCHEMA,
    MAX_IDENTITY,
    CompositionChildIdentityRecord,
    locked,
    publish,
    read_record,
    require_digest,
    staged,
    subdirectory,
    sync,
)
from dpone.adapters.composition_supervisor_filesystem import (
    absolute_supervisor_path,
    open_protected,
    require_supervisor,
)
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_dbt_outcome import DbtCaptureError
from dpone.contracts.composition_supervisor import CompositionSupervisorProjection

MAX_IDENTITY_PROBES = 4096

_ATTEMPTS = "attempts"
_IDENTITIES = "identities"


@dataclass(frozen=True, slots=True)
class CompositionChildIdentity:
    """One permanently reserved child UID and GID for a single attempt."""

    uid: int
    gid: int


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
                type(start) is not int or start < 1 or start + count >= MAX_IDENTITY for start in (uid_start, gid_start)
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
        name = require_digest(digest)
        require_supervisor()
        with self._opened(fcntl.LOCK_EX) as (attempts, identities):
            if _owner(attempts, f"{name}.json", digest) is not None:
                raise DbtCaptureError("capture_child_identity_conflict")
            identity = self._probe(identities, digest)
            record = CompositionChildIdentityRecord(digest, identity.uid, identity.gid).to_bytes()
            with staged(identities, name, record) as source:
                for final in (
                    f"uid-{identity.uid}.json",
                    f"gid-{identity.gid}.json",
                    f"{identity.uid}-{identity.gid}.json",
                ):
                    publish(identities, source, identities, final)
                sync(identities)
                publish(identities, source, attempts, f"{name}.json")
                sync(attempts)
        return identity

    def read(self, uid: int) -> CompositionChildIdentityRecord:
        """Return the immutable owner record of one reserved child UID."""

        if type(uid) is not int or not self.uid_start <= uid < self.uid_start + self.count:
            raise DbtCaptureError("capture_child_identity_range")
        require_supervisor()
        with self._opened(fcntl.LOCK_SH) as (_, identities):
            record = read_record(identities, f"uid-{uid}.json")
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
            with locked(root, operation):
                for name in (_ATTEMPTS, _IDENTITIES):
                    descriptors.append(subdirectory(root, name))
                yield descriptors[0], descriptors[1]
        except OSError:
            raise DbtCaptureError("capture_child_identity_unavailable") from None
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)
            os.close(root)


def _owner(parent: int, name: str, digest: str) -> CompositionChildIdentityRecord | None:
    """Return an existing foreign owner; this attempt's own record is a replay."""

    record = read_record(parent, name)
    if record is not None and record.attempt_sha256 == digest:
        raise DbtCaptureError("capture_child_identity_replay")
    return record


__all__ = [
    "CHILD_IDENTITY_SCHEMA",
    "MAX_IDENTITY_PROBES",
    "CompositionChildIdentity",
    "CompositionChildIdentityAllocator",
    "CompositionChildIdentityRecord",
]

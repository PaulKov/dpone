"""Connector-neutral capabilities for bounded desired-state CAS access."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.contracts.airflow_desired_state import (
        AirflowDesiredDeployment,
        DesiredStateRevision,
    )
    from dpone.contracts.airflow_desired_state_reconcile import (
        DesiredStateCheckpoint,
        DesiredStateReconcileEvidence,
        DesiredStateRecoveryRecord,
    )


class DesiredStatePortError(RuntimeError):
    """Backend-neutral failure with no vendor payload in its public contract."""


class DesiredStateReadUnavailable(DesiredStatePortError):
    """A bounded read could not establish remote state."""


class DesiredStateConditionalWriteConflict(DesiredStatePortError):
    """The exact create/replace precondition failed; callers must not retry."""


class DesiredStateWriteContention(DesiredStatePortError):
    """A transient conditional-write conflict requires bounded reconciliation."""


class DesiredStateWriteUncertain(DesiredStatePortError):
    """A write timed out after its commit state became ambiguous."""


@dataclass(frozen=True, slots=True)
class ObservedDesiredState:
    """One byte-exact desired state read with its backend revision."""

    body: bytes
    revision: DesiredStateRevision
    desired: AirflowDesiredDeployment


class DesiredStateSourceUnauthorized(DesiredStatePortError):
    """The requested source SHA is not the trusted protected-ref head."""


class DesiredStateSourceUnavailable(DesiredStatePortError):
    """The trusted protected-ref head could not be established."""


class DesiredStateReadStatus(str, Enum):  # noqa: UP042
    """Exhaustive result states for bounded and conditional reads."""

    ABSENT = "absent"
    PRESENT = "present"
    UNCHANGED = "unchanged"


@dataclass(frozen=True, slots=True)
class DesiredStateReadResult:
    """One bounded read without interpreting an opaque revision."""

    status: DesiredStateReadStatus
    body: bytes | None
    revision: DesiredStateRevision | None

    def __post_init__(self) -> None:
        valid = (
            (self.status is DesiredStateReadStatus.ABSENT and self.body is None and self.revision is None)
            or (
                self.status is DesiredStateReadStatus.PRESENT
                and isinstance(self.body, bytes)
                and self.revision is not None
            )
            or (self.status is DesiredStateReadStatus.UNCHANGED and self.body is None and self.revision is not None)
        )
        if not valid:
            raise ValueError("desired-state read result fields do not match its status")

    @classmethod
    def absent(cls) -> DesiredStateReadResult:
        return cls(DesiredStateReadStatus.ABSENT, None, None)

    @classmethod
    def present(cls, body: bytes, revision: DesiredStateRevision) -> DesiredStateReadResult:
        return cls(DesiredStateReadStatus.PRESENT, body, revision)

    @classmethod
    def unchanged(cls, revision: DesiredStateRevision) -> DesiredStateReadResult:
        return cls(DesiredStateReadStatus.UNCHANGED, None, revision)


@dataclass(frozen=True, slots=True)
class DesiredStateWriteResult:
    """Revision observed after one successful conditional write."""

    revision: DesiredStateRevision


class AirflowDesiredStateReader(Protocol):
    """Read one allowlisted desired-state object with a hard body bound."""

    def read(
        self,
        *,
        max_bytes: int,
        if_changed_from: DesiredStateRevision | None = None,
    ) -> DesiredStateReadResult:
        """Return absent, present, or unchanged without listing other keys."""


class AirflowDesiredStateWriter(Protocol):
    """Write only through explicit create/replace preconditions."""

    def create_if_absent(self, body: bytes) -> DesiredStateWriteResult:
        """Create with an absent-state precondition; never overwrite."""

    def replace_if_revision(
        self,
        expected_revision: DesiredStateRevision,
        body: bytes,
    ) -> DesiredStateWriteResult:
        """Replace only when the exact opaque revision still matches."""


class DesiredStateSourceAuthorizer(Protocol):
    """Authorize one protected source identity immediately before mutation."""

    def authorize(self, *, project: str, git_sha: str) -> None:
        """Fail unless the SHA is the trusted protected-ref head."""


class AirflowDesiredStateSnapshotWriter(Protocol):
    """Atomically commit one verified local desired-state snapshot."""

    def commit(self, body: bytes) -> None:
        """Replace the local snapshot atomically or leave the predecessor intact."""


class AirflowDesiredStateSnapshotStore(AirflowDesiredStateSnapshotWriter, Protocol):
    """Read and atomically replace one bounded local desired-state snapshot."""

    def read(self, *, max_bytes: int) -> bytes | None:
        """Return the exact snapshot bytes, or ``None`` when no snapshot exists."""


class DesiredStateReconcilePortError(RuntimeError):
    """Safe local-cache adapter failure exposed to the reconciler."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        state_may_have_changed: bool = False,
    ) -> None:
        self.code = code
        self.state_may_have_changed = state_may_have_changed
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True, slots=True)
class ActiveDesiredDeployment:
    """Current cache identity needed for replay and CAS decisions."""

    release_id: str
    deployment_id: str
    activation_id: str
    attestation_ref: str | None


@dataclass(frozen=True, slots=True)
class DesiredStateActivationResult:
    """Identity returned by a successful local cache activation."""

    activation_id: str
    previous_deployment_id: str | None


class DesiredStateCheckpointReader(Protocol):
    def read(self) -> DesiredStateCheckpoint | None: ...


class DesiredStateCheckpointStore(DesiredStateCheckpointReader, Protocol):
    def commit(self, checkpoint: DesiredStateCheckpoint) -> None: ...


class DesiredStateActivationReceiptStore(Protocol):
    def read(self, activation_id: str) -> DesiredStateReconcileEvidence | None: ...

    def commit(self, evidence: DesiredStateReconcileEvidence) -> None: ...


class DesiredStateRecoveryRecordStore(Protocol):
    def read(self) -> DesiredStateRecoveryRecord | None: ...

    def commit(self, record: DesiredStateRecoveryRecord) -> None: ...


class DesiredDeploymentMaterializer(Protocol):
    def materialize(self, desired: AirflowDesiredDeployment) -> None: ...


class DesiredDeploymentActivator(Protocol):
    def current(self, *, environment: str) -> ActiveDesiredDeployment | None: ...

    def commit_if_current(
        self,
        expected: ActiveDesiredDeployment,
        *,
        environment: str,
        post_validation_commit: Callable[[], None],
    ) -> None: ...

    def activate(
        self,
        desired: AirflowDesiredDeployment,
        *,
        expected_current_deployment_id: str | None,
        remote_precondition: Callable[[], bool],
        post_activation_commit: Callable[[DesiredStateActivationResult], None] | None = None,
    ) -> DesiredStateActivationResult: ...


__all__ = [
    "ActiveDesiredDeployment",
    "AirflowDesiredStateReader",
    "AirflowDesiredStateSnapshotStore",
    "AirflowDesiredStateSnapshotWriter",
    "AirflowDesiredStateWriter",
    "DesiredDeploymentActivator",
    "DesiredDeploymentMaterializer",
    "DesiredStateActivationResult",
    "DesiredStateActivationReceiptStore",
    "DesiredStateCheckpointReader",
    "DesiredStateCheckpointStore",
    "DesiredStateConditionalWriteConflict",
    "DesiredStatePortError",
    "DesiredStateReadResult",
    "DesiredStateReadStatus",
    "DesiredStateReadUnavailable",
    "DesiredStateRecoveryRecordStore",
    "DesiredStateReconcilePortError",
    "DesiredStateSourceAuthorizer",
    "DesiredStateSourceUnauthorized",
    "DesiredStateSourceUnavailable",
    "DesiredStateWriteContention",
    "DesiredStateWriteResult",
    "DesiredStateWriteUncertain",
]

"""Serializable contracts for spawned backfill worker lanes."""

from __future__ import annotations

import pickle
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal, Protocol

PROCESS_TREE_ISOLATION_ERROR = "DPONE_BACKFILL_PROCESS_TREE_ISOLATION_FAILED"
PROCESS_TREE_SIGNAL_ERROR = "DPONE_BACKFILL_PROCESS_TREE_TERMINATION_FAILED"
_UTC = timezone.utc  # noqa: UP017 - mypy target may be older than datetime.UTC.


@dataclass(frozen=True, slots=True)
class ProcessTreeIdentity:
    """Authenticated POSIX identity shared by child and parent processes."""

    pid: int
    process_group_id: int
    session_id: int

    @property
    def isolated(self) -> bool:
        return self.pid > 0 and self.pid == self.process_group_id == self.session_id


@dataclass(frozen=True, slots=True)
class BackfillProcessLaneBootstrap:
    """Top-level child entrypoint plus raw, spawn-serializable runtime input."""

    entrypoint: str
    payload: Any


@dataclass(frozen=True, slots=True)
class ProcessLaneBinding:
    """Exact durable chunk owner bound to one child command."""

    run_key: str
    chunk_index: int
    owner: str

    def __post_init__(self) -> None:
        if not self.run_key or not self.owner or self.chunk_index < 1:
            raise ValueError("backfill.process_lane_binding_invalid")


@dataclass(frozen=True, slots=True)
class ProcessLaneDispatch:
    """Parent-owned dispatch containing only a serializable child projection."""

    command_id: str
    load_config: Any
    binding: ProcessLaneBinding
    parent_context: Any = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if not self.command_id:
            raise ValueError("backfill.process_lane_command_id_required")


class ProcessLaneClaimHandoff(Protocol):
    """Parent-owned continuation for atomically observable durable claims."""

    def __call__(self, dispatch: ProcessLaneDispatch) -> None: ...

    def owns(self, binding: ProcessLaneBinding) -> bool: ...


@dataclass(frozen=True, slots=True)
class ProcessLaneRun:
    """Child-visible subset of a parent dispatch."""

    command_id: str
    load_config: Any
    binding: ProcessLaneBinding

    @classmethod
    def from_dispatch(cls, dispatch: ProcessLaneDispatch) -> ProcessLaneRun:
        """Build and prove the exact child-visible message is serializable."""

        message = cls(dispatch.command_id, dispatch.load_config, dispatch.binding)
        pickle.dumps(message)
        return message


@dataclass(frozen=True, slots=True)
class ProcessLaneStop:
    """Graceful stop command delivered only when a lane is idle."""


@dataclass(frozen=True, slots=True)
class ProcessLaneReady:
    """Validated containment identity emitted before runtime bootstrap."""

    worker_id: int
    pid: int
    process_group_id: int
    session_id: int
    process_tree_isolated: bool
    start_method: str
    faulthandler_enabled: bool


@dataclass(frozen=True, slots=True)
class ProcessLaneContainmentAck:
    """Parent proof that the exact lane process tree is cached for cleanup."""

    worker_id: int
    pid: int
    process_group_id: int
    session_id: int


@dataclass(frozen=True, slots=True)
class ProcessLaneRuntimeReady:
    """Proof that the isolated runtime entered its opener and can accept work."""

    worker_id: int


@dataclass(frozen=True, slots=True)
class ProcessLaneSucceeded:
    """One serializable child result."""

    worker_id: int
    command_id: str
    result: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class ProcessLaneFailed:
    """Sanitized Python failure for one command."""

    worker_id: int
    command_id: str
    error: str


@dataclass(frozen=True, slots=True)
class ProcessLaneBootstrapFailed:
    """Sanitized child bootstrap failure emitted before READY."""

    worker_id: int
    error: str


OperationLeaseAction = Literal["register", "status", "unregister"]


@dataclass(frozen=True, slots=True)
class ProcessLaneOperationLeaseRequest:
    """Exact child-to-parent operation-lease control request."""

    request_id: str
    action: OperationLeaseAction
    worker_id: int
    command_id: str
    binding: ProcessLaneBinding
    operation_key: bytes
    owner_digest: bytes
    epoch: int
    operation: Any | None = field(default=None, repr=False, compare=False)
    portable_scope_binding: Any | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not self.request_id or self.action not in {"register", "status", "unregister"}:
            raise ValueError("backfill.process_lane_operation_request_invalid")
        if len(self.operation_key) != 32 or len(self.owner_digest) != 32 or self.epoch < 1:
            raise ValueError("backfill.process_lane_operation_identity_invalid")
        if self.action == "register" and self.operation is None:
            raise ValueError("backfill.process_lane_operation_registration_required")


@dataclass(frozen=True, slots=True)
class ProcessLaneOperationLeaseReply:
    """Exact parent acknowledgement; mismatched replies are never reusable."""

    request_id: str
    worker_id: int
    command_id: str
    active: bool
    error: str | None = None
    lease_expires_at_utc: datetime | None = None


@dataclass(frozen=True, slots=True)
class ProcessLaneReceiptProbeRequest:
    """Child candidate that requires parent authority before a receipt read."""

    request_id: str
    worker_id: int
    command_id: str
    binding: ProcessLaneBinding
    load_id: str
    portable_scope_binding: Any | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not self.request_id or not self.load_id:
            raise ValueError("backfill.process_lane_receipt_probe_request_invalid")


@dataclass(frozen=True, slots=True)
class ProcessLaneReceiptProbeReply:
    """Exact parent ACK required before the child may probe a receipt."""

    request_id: str
    worker_id: int
    command_id: str
    authorized: bool
    error: str | None = None
    attempt_request: Any | None = field(default=None, repr=False)
    operation_request: Any | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class ProcessLaneDiagnostic:
    """Bounded per-process evidence returned to the parent caller."""

    worker_id: int
    pid: int
    process_group_id: int
    session_id: int
    process_tree_isolated: bool
    start_method: str
    faulthandler_enabled: bool
    exitcode: int | None = None


@dataclass(frozen=True, slots=True)
class ProcessLaneExecutionSummary:
    """Bounded process execution outcome without row or credential payloads."""

    errors: tuple[str, ...]
    lanes: tuple[ProcessLaneDiagnostic, ...]

    @property
    def lanes_started(self) -> int:
        return len(self.lanes)


def operation_identity(operation: Any) -> tuple[bytes, bytes, int]:
    """Project and validate the immutable operation identity used by IPC."""

    operation_key = getattr(operation, "operation_key", None)
    owner_digest = getattr(operation, "owner_digest", None)
    epoch = getattr(operation, "epoch", None)
    if not isinstance(operation_key, bytes) or not isinstance(owner_digest, bytes):
        raise ValueError("backfill.process_lane_operation_identity_invalid")
    if isinstance(epoch, bool) or not isinstance(epoch, int):
        raise ValueError("backfill.process_lane_operation_identity_invalid")
    if len(operation_key) != 32 or len(owner_digest) != 32 or epoch < 1:
        raise ValueError("backfill.process_lane_operation_identity_invalid")
    return operation_key, owner_digest, epoch


def operation_lease_expiry(operation: Any) -> datetime:
    """Return a timezone-aware finite expiry for parent-owned renewal."""

    expiry = getattr(operation, "lease_expires_at_utc", None)
    if not isinstance(expiry, datetime) or expiry.tzinfo is None or expiry.utcoffset() is None:
        raise ValueError("backfill.process_lane_operation_lease_expiry_invalid")
    return expiry


def parent_issued_operation_lease_expiry(load_config: Any) -> datetime:
    """Read the immutable parent-issued lease deadline from a chunk command."""

    options = getattr(load_config, "options", None)
    scope = options.get("__dpone_mssql_operation_scope") if isinstance(options, Mapping) else None
    raw = scope.get("lease_expires_at_utc") if isinstance(scope, Mapping) else None
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("backfill.process_lane_parent_lease_expiry_required")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("backfill.process_lane_parent_lease_expiry_invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("backfill.process_lane_parent_lease_expiry_invalid")
    return parsed.astimezone(_UTC)


__all__ = [
    "BackfillProcessLaneBootstrap",
    "PROCESS_TREE_ISOLATION_ERROR",
    "PROCESS_TREE_SIGNAL_ERROR",
    "ProcessLaneBinding",
    "ProcessLaneBootstrapFailed",
    "ProcessLaneClaimHandoff",
    "ProcessLaneContainmentAck",
    "ProcessLaneDiagnostic",
    "ProcessLaneDispatch",
    "ProcessLaneExecutionSummary",
    "ProcessLaneFailed",
    "ProcessLaneOperationLeaseReply",
    "ProcessLaneOperationLeaseRequest",
    "ProcessLaneReady",
    "ProcessLaneReceiptProbeReply",
    "ProcessLaneReceiptProbeRequest",
    "ProcessLaneRuntimeReady",
    "ProcessLaneRun",
    "ProcessLaneStop",
    "ProcessLaneSucceeded",
    "ProcessTreeIdentity",
    "operation_identity",
    "operation_lease_expiry",
    "parent_issued_operation_lease_expiry",
]

"""Immutable identities for receipt-backed generic SQL Server loads."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import UTC, datetime  # type: ignore[attr-defined]
from enum import StrEnum  # type: ignore[attr-defined]
from typing import Any

CONTRACT_VERSION = "mssql_generic_transaction_v1"


class MssqlTransactionContractError(ValueError):
    """An invocation, operation, or receipt is ambiguous or incomplete."""


class MssqlOperationClaimRejected(RuntimeError):
    """Stable typed rejection for one exact dpone-owned SQL ``THROW`` token."""

    def __init__(self, *, code: str, vendor_token: str) -> None:
        super().__init__(code)
        self.code = code
        self.vendor_token = vendor_token


class MssqlCleanupDisposition(StrEnum):
    """Tell the outer artifact owner which cleanup actions remain safe."""

    CLEANUP_SAFE = "cleanup_safe"
    COMMITTED_SECONDARY_ONLY = "committed_secondary_only"
    PRESERVE_STAGING_EVIDENCE = "preserve_staging_evidence"


@dataclass(frozen=True, slots=True)
class InvocationIdentity:
    """Scheduler-stable identity shared by retries and mapped operations."""

    run_id: str
    process: str
    task_partition: str

    def __post_init__(self) -> None:
        if any(not str(value).strip() for value in (self.run_id, self.process, self.task_partition)):
            raise MssqlTransactionContractError("mssql_transaction.invocation_identity_incomplete")

    @property
    def invocation_digest(self) -> bytes:
        return _digest(
            {
                "version": CONTRACT_VERSION,
                "run_id": self.run_id,
                "process": self.process,
                "task_partition": self.task_partition,
            }
        )


@dataclass(frozen=True, slots=True)
class MssqlAttemptRequest:
    """Invocation-wide request for one immutable route and target binding."""

    invocation: InvocationIdentity
    target_identity: bytes
    route_fingerprint: bytes
    load_id: str
    target_database: str
    target_schema: str
    target_table: str
    strategy: str

    def __post_init__(self) -> None:
        _require_digest(self.target_identity, "target_identity")
        _require_digest(self.route_fingerprint, "route_fingerprint")
        values = (self.load_id, self.target_database, self.target_schema, self.target_table, self.strategy)
        if any(not str(value).strip() for value in values):
            raise MssqlTransactionContractError("mssql_transaction.attempt_request_incomplete")

    @property
    def attempt_key(self) -> bytes:
        """Return the v1-compatible invocation and physical-target row key."""

        return _digest(
            {
                "version": CONTRACT_VERSION,
                "invocation": self.invocation.invocation_digest.hex(),
                "target_identity": self.target_identity.hex(),
            }
        )


@dataclass(frozen=True, slots=True)
class MssqlTransactionAttempt:
    """One invocation-wide source-generation fence for a physical target."""

    request: MssqlAttemptRequest
    generation: int
    is_current_generation: bool = True

    def __post_init__(self) -> None:
        if isinstance(self.generation, bool) or not isinstance(self.generation, int) or self.generation < 1:
            raise MssqlTransactionContractError("mssql_transaction.generation_invalid")
        if not isinstance(self.is_current_generation, bool):
            raise MssqlTransactionContractError("mssql_transaction.current_generation_marker_invalid")

    @property
    def attempt_key(self) -> bytes:
        return self.request.attempt_key

    @property
    def target_identity(self) -> bytes:
        return self.request.target_identity

    @property
    def route_fingerprint(self) -> bytes:
        return self.request.route_fingerprint


@dataclass(frozen=True, slots=True)
class MssqlOperationRequest:
    """One target-wide or planner-proven disjoint scope under an attempt."""

    scope_hash: bytes
    owner_digest: bytes
    lease_expires_at_utc: datetime | None = None

    def __post_init__(self) -> None:
        _require_digest(self.scope_hash, "scope_hash")
        _require_digest(self.owner_digest, "owner_digest")
        if self.lease_expires_at_utc is not None:
            value = self.lease_expires_at_utc
            if value.tzinfo is None or value.utcoffset() is None:
                raise MssqlTransactionContractError("mssql_transaction.lease_expiry_timezone_required")
            object.__setattr__(self, "lease_expires_at_utc", value.astimezone(UTC))

    def operation_key(self, attempt: MssqlTransactionAttempt | MssqlAttemptRequest) -> bytes:
        return _digest(
            {
                "version": CONTRACT_VERSION,
                "attempt_key": attempt.attempt_key.hex(),
                "scope_hash": self.scope_hash.hex(),
            }
        )


@dataclass(frozen=True, slots=True)
class MssqlTransactionOperation:
    """Current owner epoch carried from pre-source admission to target commit."""

    attempt: MssqlTransactionAttempt
    operation_key: bytes
    scope_hash: bytes
    owner_digest: bytes
    epoch: int
    lease_expires_at_utc: datetime | None = None

    def __post_init__(self) -> None:
        if not self.has_canonical_attempt():
            raise MssqlTransactionContractError("mssql_transaction.operation_attempt_invalid")
        for value, field in (
            (self.operation_key, "operation_key"),
            (self.scope_hash, "scope_hash"),
            (self.owner_digest, "owner_digest"),
        ):
            _require_digest(value, field)
        if isinstance(self.epoch, bool) or not isinstance(self.epoch, int) or self.epoch < 1:
            raise MssqlTransactionContractError("mssql_transaction.operation_epoch_invalid")

    def has_canonical_attempt(self) -> bool:
        """Require the exact nested attempt/request types, including after IPC."""

        return isinstance(self.attempt, MssqlTransactionAttempt) and isinstance(
            self.attempt.request,
            MssqlAttemptRequest,
        )

    @property
    def receipt_id(self) -> str:
        return f"mssql-generic-v1:{self.operation_key.hex()}"


@dataclass(frozen=True, slots=True)
class MssqlReceiptMetrics:
    """Exact generic load counters persisted with the business transaction."""

    inserted_rows: int
    updated_rows: int
    total_rows: int
    staging_rows: int | None = None
    replaced_rows: int | None = None
    soft_deleted_rows: int | None = None
    reactivated_rows: int | None = None
    unchanged_rows: int | None = None
    hard_deleted_rows: int | None = None
    active_rows: int | None = None

    def __post_init__(self) -> None:
        for item in fields(self):
            value = getattr(self, item.name)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                raise MssqlTransactionContractError(f"mssql_transaction.metric_invalid:{item.name}")


@dataclass(frozen=True, slots=True)
class MssqlPayloadCommitEvidence:
    """Exact verified payload identity persisted with target business DML."""

    manifest_sha256: bytes
    declared_rows: int
    actual_raw_rows: int
    actual_native_rows: int
    native_contract_sha256: bytes

    def __post_init__(self) -> None:
        _require_digest(self.manifest_sha256, "payload_manifest_sha256")
        _require_digest(self.native_contract_sha256, "native_contract_sha256")
        counts = (self.declared_rows, self.actual_raw_rows, self.actual_native_rows)
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in counts):
            raise MssqlTransactionContractError("mssql_transaction.payload_row_count_invalid")
        if len(set(counts)) != 1:
            raise MssqlTransactionContractError("mssql_transaction.payload_row_count_mismatch")


@dataclass(frozen=True, slots=True)
class MssqlSourceLifecycleEvidence:
    """Truthful completed extraction window with optional native snapshot proof.

    Extraction timestamps share ``clock_authority`` and may therefore be
    ordered.  Snapshot evidence is optional because generic sources cannot
    truthfully claim a database snapshot.  No source timestamp is compared
    with SQL Server target timestamps: those values have different clock
    authorities and ordinary clock skew must not invalidate a commit.
    """

    extraction_started_at_utc: datetime
    extraction_completed_at_utc: datetime
    clock_authority: str
    snapshot_acquired_at_utc: datetime | None = None
    snapshot_authority: str | None = None
    source_token_sha256: bytes | None = None

    def __post_init__(self) -> None:
        for name in ("extraction_started_at_utc", "extraction_completed_at_utc"):
            value = getattr(self, name)
            if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
                raise MssqlTransactionContractError(f"mssql_transaction.{name}_invalid")
        _require_authority(self.clock_authority, "extraction_clock_authority")
        if self.extraction_completed_at_utc < self.extraction_started_at_utc:
            raise MssqlTransactionContractError("mssql_transaction.extraction_lifecycle_order_invalid")
        snapshot = self.snapshot_acquired_at_utc
        if snapshot is not None:
            if snapshot.tzinfo is None or snapshot.utcoffset() != UTC.utcoffset(snapshot):
                raise MssqlTransactionContractError("mssql_transaction.snapshot_acquired_at_utc_invalid")
            _require_authority(self.snapshot_authority, "snapshot_authority")
            if not self.extraction_started_at_utc <= snapshot <= self.extraction_completed_at_utc:
                raise MssqlTransactionContractError("mssql_transaction.snapshot_lifecycle_order_invalid")
        elif self.snapshot_authority is not None:
            raise MssqlTransactionContractError("mssql_transaction.snapshot_authority_without_snapshot")
        if self.source_token_sha256 is not None:
            if snapshot is None:
                raise MssqlTransactionContractError("mssql_transaction.source_token_without_snapshot")
            _require_digest(self.source_token_sha256, "source_token_sha256")


@dataclass(frozen=True, slots=True)
class MssqlGenericCommitReceipt:
    """Durable operation-owner evidence committed with target business DML."""

    receipt_id: str
    operation_key: bytes
    attempt_key: bytes
    target_identity: bytes
    generation: int
    scope_hash: bytes
    operation_epoch: int
    owner_digest: bytes
    route_fingerprint: bytes
    load_id: str
    strategy: str
    mutation_plan_sha256: bytes
    target_before_sha256: bytes
    target_after_sha256: bytes
    loaded_at_utc: datetime
    committed_at_utc: datetime
    payload_evidence: MssqlPayloadCommitEvidence
    source_lifecycle: MssqlSourceLifecycleEvidence
    metrics: MssqlReceiptMetrics

    def __post_init__(self) -> None:
        for value, field in (
            (self.attempt_key, "attempt_key"),
            (self.operation_key, "operation_key"),
            (self.target_identity, "target_identity"),
            (self.scope_hash, "scope_hash"),
            (self.owner_digest, "owner_digest"),
            (self.route_fingerprint, "route_fingerprint"),
            (self.mutation_plan_sha256, "mutation_plan_sha256"),
            (self.target_before_sha256, "target_before_sha256"),
            (self.target_after_sha256, "target_after_sha256"),
        ):
            _require_digest(value, field)
        if (
            not self.receipt_id
            or not self.load_id
            or not self.strategy
            or self.generation < 1
            or self.operation_epoch < 1
        ):
            raise MssqlTransactionContractError("mssql_transaction.receipt_invalid")
        for field_name in ("loaded_at_utc", "committed_at_utc"):
            value = getattr(self, field_name)
            if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
                raise MssqlTransactionContractError(f"mssql_transaction.{field_name}_invalid")
        if self.committed_at_utc < self.loaded_at_utc:
            raise MssqlTransactionContractError("mssql_transaction.commit_before_load_invalid")


@dataclass(frozen=True, slots=True)
class MssqlTransactionAdmission:
    """Either a current operation owner or an exact already-committed replay."""

    operation: MssqlTransactionOperation | None = None
    replay_receipt: MssqlGenericCommitReceipt | None = None

    def __post_init__(self) -> None:
        if (self.operation is None) == (self.replay_receipt is None):
            raise MssqlTransactionContractError("mssql_transaction.admission_invalid")


def route_fingerprint(route_contract: Mapping[str, Any]) -> bytes:
    """Hash the invocation-wide route, excluding load IDs and operation scope."""

    return _digest({"version": CONTRACT_VERSION, "route": dict(route_contract)})


def operation_scope_hash(scope_contract: Mapping[str, Any]) -> bytes:
    """Hash one canonical target-wide or planner-proven disjoint scope."""

    return _digest({"version": CONTRACT_VERSION, "scope": dict(scope_contract)})


def operation_owner_digest(owner_token: str) -> bytes:
    """Protect the lease-owner token while retaining exact equality semantics."""

    if not str(owner_token).strip():
        raise MssqlTransactionContractError("mssql_transaction.operation_owner_missing")
    return _digest({"version": CONTRACT_VERSION, "owner_token": owner_token})


def _digest(value: Mapping[str, Any]) -> bytes:
    try:
        canonical = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise MssqlTransactionContractError("mssql_transaction.identity_not_canonical_json") from exc
    return hashlib.sha256(canonical.encode("utf-8")).digest()


def _require_digest(value: bytes, field: str) -> None:
    if not isinstance(value, bytes) or len(value) != 32:
        raise MssqlTransactionContractError(f"mssql_transaction.{field}_invalid")


def _require_authority(value: str | None, field: str) -> None:
    if value is None or not value.strip() or len(value) > 128:
        raise MssqlTransactionContractError(f"mssql_transaction.{field}_invalid")


__all__ = [
    "CONTRACT_VERSION",
    "InvocationIdentity",
    "MssqlAttemptRequest",
    "MssqlCleanupDisposition",
    "MssqlGenericCommitReceipt",
    "MssqlOperationClaimRejected",
    "MssqlOperationRequest",
    "MssqlPayloadCommitEvidence",
    "MssqlReceiptMetrics",
    "MssqlSourceLifecycleEvidence",
    "MssqlTransactionAdmission",
    "MssqlTransactionAttempt",
    "MssqlTransactionContractError",
    "MssqlTransactionOperation",
    "operation_owner_digest",
    "operation_scope_hash",
    "route_fingerprint",
]

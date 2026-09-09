"""Create-only MSSQL acknowledgement for failed-precommit cleanup."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from typing import Any, Protocol, cast

from dpone.adapters.semantic_refresh_mssql_cleanup_ack_codec import (
    SemanticRefreshMssqlCleanupAckError,
)
from dpone.adapters.semantic_refresh_mssql_cleanup_ack_codec import (
    ack_row as _ack_row,
)
from dpone.adapters.semantic_refresh_mssql_cleanup_ack_codec import (
    resources_from_json as _resources_from_json,
)
from dpone.adapters.semantic_refresh_mssql_cleanup_ack_codec import (
    scratch_from_json as _scratch_from_json,
)
from dpone.ports.semantic_refresh_clickhouse_resources import (
    ClickHouseFailedScratchCleanupReceipt,
    ClickHouseScratchRelationAbsence,
)
from dpone.ports.semantic_refresh_mssql_authority_codec import (
    authority_from_record,
    canonical_authority_record,
)
from dpone.ports.semantic_refresh_mssql_cleanup_ack import (
    MssqlFailedPrecommitCleanupAck,
)
from dpone.ports.semantic_refresh_mssql_resources import (
    MssqlProtectedResourceAllocation,
    MssqlProtectedResourceAllocationClosure,
    mssql_resource_allocation_id,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SAFE_OUTCOMES = {"COMMITTED_WITH_IMAGES", "NOT_INVOKED", "ROLLED_BACK"}


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    autocommit: bool

    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class MssqlSemanticRefreshFailedPrecommitCleanupAckStore:
    """Authenticate both cleanup proofs and persist an immutable acknowledgement."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        if not isinstance(control_schema, str) or _IDENTIFIER.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._control_schema = control_schema

    def persist_exact(
        self,
        *,
        scratch: ClickHouseFailedScratchCleanupReceipt,
        resources: MssqlProtectedResourceAllocationClosure,
    ) -> MssqlFailedPrecommitCleanupAck:
        """Create one ack only while FAILED_PRE_COMMIT state and proofs remain exact."""

        ack = MssqlFailedPrecommitCleanupAck.build(scratch=scratch, resources=resources)
        return self._transaction(
            lambda cursor: self._persist(cursor, ack),
            "failed cleanup acknowledgement persistence failed",
        )

    def load_exact(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlFailedPrecommitCleanupAck:
        """Reload and authenticate one exact COMPLETE acknowledgement."""

        return self._transaction(
            lambda cursor: self._load(cursor, workflow_execution_binding_sha256, operation_id),
            "failed cleanup acknowledgement load failed",
        )

    def _transaction(self, action, label: str) -> MssqlFailedPrecommitCleanupAck:
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            result = action(cursor)
            connection.commit()
            return cast(MssqlFailedPrecommitCleanupAck, result)
        except SemanticRefreshMssqlCleanupAckError:
            _rollback(connection)
            raise
        except Exception as exc:
            _rollback(connection)
            raise SemanticRefreshMssqlCleanupAckError(label) from exc
        finally:
            _close(cursor)
            _close(connection)

    def _persist(self, cursor: _Cursor, ack: MssqlFailedPrecommitCleanupAck) -> MssqlFailedPrecommitCleanupAck:
        bundle = self._require_failed_authority(cursor, ack)
        self._require_allocations(cursor, ack.resources, bundle)
        values = _ack_row(ack)
        cursor.execute(
            f"""
SELECT workflow_execution_id, operation_plan_sha256, attempt_binding_sha256,
       fencing_epoch, LOWER(CONVERT(char(36), target_uuid)),
       scratch_absence_evidence_sha256,
       scratch_receipt_json, resource_allocation_closure_sha256,
       resource_closure_json, cleanup_receipt_sha256, status
FROM {self._table("semantic_refresh_failed_cleanup_acks")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ? AND operation_id = ?;
""".strip(),
            ack.scratch.workflow_execution_binding_sha256,
            ack.scratch.operation_id,
        )
        existing = _row(cursor)
        if existing is not None:
            if existing != values:
                raise SemanticRefreshMssqlCleanupAckError("failed cleanup acknowledgement replay differs")
            return ack
        cursor.execute(
            f"""
INSERT INTO {self._table("semantic_refresh_failed_cleanup_acks")} (
    workflow_execution_binding_sha256, operation_id, workflow_execution_id,
    operation_plan_sha256, attempt_binding_sha256, fencing_epoch, target_uuid,
    scratch_absence_evidence_sha256, scratch_receipt_json,
    resource_allocation_closure_sha256, resource_closure_json,
    cleanup_receipt_sha256, status
)
OUTPUT inserted.cleanup_receipt_sha256
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, N'COMPLETE');
""".strip(),
            ack.scratch.workflow_execution_binding_sha256,
            ack.scratch.operation_id,
            *values[:-1],
        )
        inserted = cursor.fetchone()
        if inserted is None or tuple(inserted) != (ack.cleanup_receipt_sha256,):
            raise SemanticRefreshMssqlCleanupAckError("failed cleanup acknowledgement insert was not exact")
        return ack

    def _load(self, cursor: _Cursor, binding: str, operation_id: str) -> MssqlFailedPrecommitCleanupAck:
        cursor.execute(
            f"""
SELECT scratch_receipt_json, resource_closure_json, cleanup_receipt_sha256, status
FROM {self._table("semantic_refresh_failed_cleanup_acks")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ? AND operation_id = ?;
""".strip(),
            binding,
            operation_id,
        )
        row = _row(cursor)
        if row is None or row[3] != "COMPLETE":
            raise SemanticRefreshMssqlCleanupAckError("COMPLETE failed cleanup acknowledgement is absent")
        ack = MssqlFailedPrecommitCleanupAck(
            scratch=_scratch_from_json(
                str(row[0]),
                receipt_factory=ClickHouseFailedScratchCleanupReceipt,
                relation_factory=ClickHouseScratchRelationAbsence,
            ),
            resources=_resources_from_json(
                str(row[1]),
                closure_factory=MssqlProtectedResourceAllocationClosure,
                allocation_factory=MssqlProtectedResourceAllocation,
            ),
            cleanup_receipt_sha256=str(row[2]),
            status=str(row[3]),
        )
        if ack.scratch.workflow_execution_binding_sha256 != binding or ack.scratch.operation_id != operation_id:
            raise SemanticRefreshMssqlCleanupAckError("failed cleanup acknowledgement identity differs")
        return ack

    def _require_failed_authority(self, cursor: _Cursor, ack: MssqlFailedPrecommitCleanupAck):
        binding = ack.scratch.workflow_execution_binding_sha256
        cursor.execute(
            f"""
SELECT workflow_execution_id, authority_sha256, authority_json, status
FROM {self._table("semantic_refresh_canonical_authorities")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ?;
""".strip(),
            binding,
        )
        row = _row(cursor)
        if row is None:
            raise SemanticRefreshMssqlCleanupAckError("canonical failed cleanup authority is absent")
        bundle = authority_from_record(
            canonical_authority_record(
                workflow_execution_binding_sha256=binding,
                workflow_execution_id=str(row[0]),
                authority_sha256=str(row[1]),
                authority_json=str(row[2]),
                status=str(row[3]),
            )
        )
        operation = next(
            (item for item in bundle.operation_plans if item.operation_id == ack.scratch.operation_id), None
        )
        attempt = next(
            (item for item in bundle.attempt_bindings if item.operation_id == ack.scratch.operation_id), None
        )
        resource = (
            None
            if operation is None
            else next(
                (item for item in bundle.model_resources if item.model_unique_id == operation.model_unique_id),
                None,
            )
        )
        if (
            operation is None
            or attempt is None
            or resource is None
            or (
                ack.scratch.workflow_execution_id,
                ack.scratch.operation_plan_sha256,
                ack.scratch.attempt_binding_sha256,
                ack.scratch.fencing_epoch,
                ack.scratch.target_uuid,
            )
            != (
                bundle.workflow_execution_id,
                operation.operation_plan_sha256,
                attempt.attempt_binding_sha256,
                attempt.fencing_epoch,
                resource.clickhouse_target_uuid,
            )
        ):
            raise SemanticRefreshMssqlCleanupAckError("failed cleanup proof differs from canonical authority")
        cursor.execute(
            f"""
SELECT execution.status, journal.status, journal.mssql_outcome,
       reservation.status, LOWER(CONVERT(char(36), head.target_uuid))
FROM {self._table("semantic_refresh_workflow_executions")} AS execution WITH (UPDLOCK, HOLDLOCK)
JOIN {self._table("semantic_refresh_journals")} AS journal WITH (UPDLOCK, HOLDLOCK)
  ON journal.workflow_id = execution.workflow_id
JOIN {self._table("semantic_refresh_reservations")} AS reservation WITH (UPDLOCK, HOLDLOCK)
  ON reservation.workflow_id = execution.workflow_id
JOIN {self._table("semantic_refresh_target_heads")} AS head WITH (UPDLOCK, HOLDLOCK)
  ON head.database_name = journal.publication_database
 AND head.target_table = journal.publication_target_table
WHERE execution.workflow_execution_binding_sha256 = ?
  AND journal.operation_id = ? AND reservation.reservation_id = ?;
""".strip(),
            binding,
            ack.scratch.operation_id,
            ack.resources.reservation_id,
        )
        state = _row(cursor)
        if (
            state is None
            or state[0:2] != ("FAILED_PRE_COMMIT", "FAILED_PRE_COMMIT")
            or state[2] not in _SAFE_OUTCOMES
            or state[3:] != ("FAILED_PRE_COMMIT", ack.scratch.target_uuid)
        ):
            raise SemanticRefreshMssqlCleanupAckError("cleanup acknowledgement terminal state is unsafe")
        return bundle

    def _require_allocations(self, cursor: _Cursor, closure, bundle) -> None:
        cursor.execute(
            f"""
SELECT allocation_id, reservation_id, resource_kind, amount, status
FROM {self._table("semantic_refresh_resource_allocations")} WITH (UPDLOCK, HOLDLOCK)
WHERE reservation_id = ? ORDER BY allocation_id;
""".strip(),
            closure.reservation_id,
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        expected = {
            (item.allocation_id, item.reservation_id, item.resource_kind, item.amount, "RELEASED")
            for item in closure.allocations
        }
        if not expected.issubset(set(rows)):
            raise SemanticRefreshMssqlCleanupAckError("released allocation proof differs from durable rows")
        allowed = {
            mssql_resource_allocation_id(closure.reservation_id, operation.operation_id, kind)
            for operation in bundle.operation_plans
            for kind in (
                "clickhouse_staging_bytes",
                "prepared_models",
                "retained_generation_bytes",
                "sealed_extract_bytes",
                "shadow_bytes",
            )
        }
        if any(str(row[0]) not in allowed for row in rows):
            raise SemanticRefreshMssqlCleanupAckError("allocation inventory contains a noncanonical row")

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


def _row(cursor: _Cursor) -> tuple[Any, ...] | None:
    value = cursor.fetchone()
    return None if value is None else tuple(value)


def _rollback(connection: _Connection | None) -> None:
    if connection is not None:
        try:
            connection.rollback()
        except Exception:
            pass


def _close(resource: object | None) -> None:
    if resource is not None:
        try:
            resource.close()  # type: ignore[attr-defined]
        except Exception:
            pass


__all__ = [
    "MssqlSemanticRefreshFailedPrecommitCleanupAckStore",
    "SemanticRefreshMssqlCleanupAckError",
]

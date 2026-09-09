"""Vendor-import-free DB-API adapter for semantic-refresh control state."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, Protocol

from dpone.adapters.semantic_refresh_mssql_admission_replay import (
    MssqlAdmissionAuthorityConflict,
    MssqlAdmissionAuthorityQueries,
)
from dpone.adapters.semantic_refresh_mssql_state_errors import (
    SemanticRefreshMssqlStateConflict,
    SemanticRefreshMssqlStateError,
)
from dpone.adapters.semantic_refresh_mssql_state_persistence import (
    MssqlSemanticRefreshStatePersistenceMixin,
)
from dpone.ports.semantic_refresh_mssql import (
    MssqlAdmissionReceipt,
    MssqlAdmissionRequest,
    MssqlGuardClaim,
    MssqlWorkflowSuccessorClaim,
)
from dpone.ports.semantic_refresh_mssql_primitives import _require_digest

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class _DbApiCursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _DbApiCursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> list[tuple[Any, ...]]: ...

    def close(self) -> None: ...


class _DbApiConnection(Protocol):
    autocommit: bool

    def cursor(self) -> _DbApiCursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class MssqlSemanticRefreshStateAdapter(MssqlSemanticRefreshStatePersistenceMixin):
    """Apply workflow admission and successor ownership in serializable transactions."""

    def __init__(
        self,
        connection_factory: Callable[[], _DbApiConnection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        self._connection_factory = connection_factory
        self._control_schema = _require_identifier(control_schema, "control_schema")
        self._authority_queries = MssqlAdmissionAuthorityQueries(self._control_schema)

    def admit(self, request: MssqlAdmissionRequest) -> MssqlAdmissionReceipt:
        """Commit the full guard/binding/reservation/journal set or roll it back."""

        connection: _DbApiConnection | None = None
        cursor: _DbApiCursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            self._begin(cursor)
            receipt = self.admit_in_transaction(cursor, request)
            connection.commit()
        except MssqlAdmissionAuthorityConflict as exc:
            _rollback(connection)
            raise SemanticRefreshMssqlStateConflict(str(exc)) from exc
        except SemanticRefreshMssqlStateError:
            _rollback(connection)
            raise
        except Exception as exc:
            _rollback(connection)
            raise SemanticRefreshMssqlStateError("semantic refresh admission failed") from exc
        finally:
            _close(cursor)
            _close(connection)
        return receipt

    def admit_in_transaction(
        self,
        cursor: _DbApiCursor,
        request: MssqlAdmissionRequest,
    ) -> MssqlAdmissionReceipt:
        """Apply exact admission on an already SERIALIZABLE owner transaction."""

        if self._authority_queries.acknowledge_existing(cursor, request):
            return _admission_receipt(request)
        snapshots = self._authority_queries.require_current_authorities(cursor, request)
        if request.successor_claim is not None:
            self._claim_successor(cursor, request.successor_claim)
        claims = tuple(sorted((request.workflow_guard, *request.resource_guards), key=lambda item: item.resource_id))
        for claim in claims:
            self._acquire_guard(cursor, request, claim)
        self._insert_execution(cursor, request)
        self._insert_reservation(cursor, request)
        self._insert_journals(cursor, request, snapshots)
        return _admission_receipt(request)

    def lock_admissible_guard_epoch(self, cursor: _DbApiCursor, resource_id: str) -> int:
        """Lock one canonical guard and return its current predecessor epoch."""

        self._acquire_application_lock(cursor, resource_id)
        cursor.execute(
            f"""
SELECT fencing_epoch, owner_id, workflow_id, operation_id, status
FROM {self._table("semantic_refresh_guards")} WITH (UPDLOCK, HOLDLOCK)
WHERE resource_id = ?;
""".strip(),
            resource_id,
        )
        row = _row(cursor)
        if row is None:
            raise SemanticRefreshMssqlStateConflict("canonical admission guard is absent")
        epoch, owner_id, workflow_id, operation_id, status = row
        if (
            isinstance(epoch, bool)
            or not isinstance(epoch, int)
            or epoch < 0
            or status not in {"AVAILABLE", "RELEASED"}
            or (status == "AVAILABLE" and (owner_id is not None or workflow_id is not None))
            or (
                status == "RELEASED"
                and (
                    not isinstance(owner_id, str)
                    or not owner_id.strip()
                    or not isinstance(workflow_id, str)
                    or not workflow_id.strip()
                )
            )
        ):
            raise SemanticRefreshMssqlStateConflict("canonical admission guard is not admissible")
        if status == "RELEASED" and operation_id is not None:
            self._require_released_target_cleanup(cursor, str(workflow_id), str(operation_id))
        return epoch

    def _require_released_target_cleanup(
        self,
        cursor: _DbApiCursor,
        workflow_id: str,
        operation_id: str,
    ) -> None:
        cursor.execute(
            f"""
SELECT execution.status, journal.status, ack.status, ack.cleanup_receipt_sha256
FROM {self._table("semantic_refresh_workflow_executions")} AS execution WITH (UPDLOCK, HOLDLOCK)
JOIN {self._table("semantic_refresh_journals")} AS journal WITH (UPDLOCK, HOLDLOCK)
  ON journal.workflow_id = execution.workflow_id AND journal.operation_id = ?
LEFT JOIN {self._table("semantic_refresh_failed_cleanup_acks")} AS ack WITH (UPDLOCK, HOLDLOCK)
  ON ack.workflow_execution_binding_sha256 = execution.workflow_execution_binding_sha256
 AND ack.operation_id = journal.operation_id
WHERE execution.workflow_id = ?;
""".strip(),
            operation_id,
            workflow_id,
        )
        row = _row(cursor)
        if row is None:
            raise SemanticRefreshMssqlStateConflict("released target guard terminal provenance is absent")
        if row[0:2] == ("COMPLETE", "COMPLETE"):
            return
        if row[0:2] != ("FAILED_PRE_COMMIT", "FAILED_PRE_COMMIT") or row[2] != "COMPLETE":
            raise SemanticRefreshMssqlStateConflict(
                "released failed target guard requires complete cleanup acknowledgement"
            )
        digest = row[3]
        try:
            if not isinstance(digest, str):
                raise ValueError
            _require_digest(digest, "cleanup_receipt_sha256")
        except ValueError as exc:
            raise SemanticRefreshMssqlStateConflict(
                "released failed target guard cleanup acknowledgement digest is invalid"
            ) from exc

    def _claim_successor(self, cursor: _DbApiCursor, claim: MssqlWorkflowSuccessorClaim) -> None:
        self._acquire_application_lock(cursor, f"workflow-successor:{claim.predecessor_workflow_id}")
        self._require_failed_cleanup_closure(cursor, claim.predecessor_workflow_id)
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
SET successor_workflow_id = ?, replacement_plan_sha256 = ?
OUTPUT inserted.successor_workflow_id,
       inserted.replacement_plan_sha256,
       inserted.terminal_summary_sha256
WHERE workflow_id = ?
  AND status = N'FAILED_PRE_COMMIT'
  AND terminal_summary_sha256 = ?
  AND successor_workflow_id IS NULL;
""".strip(),
            claim.successor_workflow_id,
            claim.replacement_plan_sha256,
            claim.predecessor_workflow_id,
            claim.predecessor_workflow_summary_sha256,
        )
        changed = _row(cursor)
        accepted = changed == (
            claim.successor_workflow_id,
            claim.replacement_plan_sha256,
            claim.predecessor_workflow_summary_sha256,
        )
        if not accepted:
            cursor.execute(
                f"""
SELECT successor_workflow_id, replacement_plan_sha256, terminal_summary_sha256
FROM {self._table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_id = ? AND status = N'FAILED_PRE_COMMIT';
""".strip(),
                claim.predecessor_workflow_id,
            )
            existing = _row(cursor)
            accepted = existing == (
                claim.successor_workflow_id,
                claim.replacement_plan_sha256,
                claim.predecessor_workflow_summary_sha256,
            )
        if not accepted:
            raise SemanticRefreshMssqlStateConflict("workflow successor conflict")

    def _require_failed_cleanup_closure(self, cursor: _DbApiCursor, workflow_id: str) -> None:
        cursor.execute(
            f"""
SELECT COUNT_BIG(*),
       SUM(CASE WHEN ack.status = N'COMPLETE'
                 AND ack.cleanup_receipt_sha256 IS NOT NULL THEN 1 ELSE 0 END)
FROM {self._table("semantic_refresh_journals")} AS journal WITH (UPDLOCK, HOLDLOCK)
JOIN {self._table("semantic_refresh_workflow_executions")} AS execution WITH (UPDLOCK, HOLDLOCK)
  ON execution.workflow_id = journal.workflow_id
LEFT JOIN {self._table("semantic_refresh_failed_cleanup_acks")} AS ack WITH (UPDLOCK, HOLDLOCK)
  ON ack.workflow_execution_binding_sha256 = execution.workflow_execution_binding_sha256
 AND ack.operation_id = journal.operation_id
WHERE journal.workflow_id = ? AND journal.status = N'FAILED_PRE_COMMIT';
""".strip(),
            workflow_id,
        )
        row = _row(cursor)
        if row is None or row[0] in {None, 0} or row[0] != row[1]:
            raise SemanticRefreshMssqlStateConflict(
                "workflow successor requires complete failed cleanup acknowledgement closure"
            )

    @staticmethod
    def _begin(cursor: _DbApiCursor) -> None:
        cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")

    def _acquire_guard(
        self,
        cursor: _DbApiCursor,
        request: MssqlAdmissionRequest,
        claim: MssqlGuardClaim,
    ) -> None:
        self._acquire_application_lock(cursor, claim.resource_id)
        journal = next(
            (item for item in request.journals if item.target_resource_id == claim.resource_id),
            None,
        )
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_guards")} WITH (UPDLOCK, HOLDLOCK)
SET fencing_epoch = ?, owner_id = ?, workflow_id = ?,
    operation_id = ?, operation_plan_sha256 = ?, attempt_binding_sha256 = ?,
    strategy_authority_sha256 = ?,
    status = N'HELD'
OUTPUT inserted.resource_id
WHERE resource_id = ?
  AND fencing_epoch = ?
  AND status IN (N'AVAILABLE', N'RELEASED');
""".strip(),
            claim.fencing_epoch,
            request.owner_id,
            request.workflow_id,
            journal.operation_id if journal is not None else None,
            journal.operation_plan_sha256 if journal is not None else None,
            journal.attempt_binding_sha256 if journal is not None else None,
            journal.strategy_authority_sha256 if journal is not None else None,
            claim.resource_id,
            claim.expected_predecessor_epoch,
        )
        changed = cursor.fetchone()
        if changed is None or tuple(changed) != (claim.resource_id,):
            raise SemanticRefreshMssqlStateConflict("guard admission conflict")

    @staticmethod
    def _acquire_application_lock(cursor: _DbApiCursor, resource_id: str) -> None:
        cursor.execute(
            """
DECLARE @dpone_lock_result int;
EXEC @dpone_lock_result = sys.sp_getapplock
    @Resource = ?, @LockMode = N'Exclusive',
    @LockOwner = N'Transaction', @LockTimeout = 0;
SELECT @dpone_lock_result;
""".strip(),
            f"dpone:semantic-refresh:{resource_id}",
        )
        result = cursor.fetchone()
        if result is None or isinstance(result[0], bool) or not isinstance(result[0], int) or result[0] < 0:
            raise SemanticRefreshMssqlStateConflict("application guard lock was not acquired")

    def _table(self, table_name: str) -> str:
        return f"[{self._control_schema}].[{table_name}]"


def _require_identifier(value: str, field_name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a simple SQL identifier")
    return value


def _admission_receipt(request: MssqlAdmissionRequest) -> MssqlAdmissionReceipt:
    return MssqlAdmissionReceipt(
        workflow_id=request.workflow_id,
        reservation_id=request.reservation_id,
        guards=(request.workflow_guard, *request.resource_guards),
        preparing_operation_ids=tuple(item.operation_id for item in request.journals),
    )


def _row(cursor: _DbApiCursor) -> tuple[Any, ...] | None:
    value = cursor.fetchone()
    return None if value is None else tuple(value)


def _rollback(connection: _DbApiConnection | None) -> None:
    if connection is not None:
        try:
            connection.rollback()
        except Exception:
            return


def _close(resource: object | None) -> None:
    if resource is not None:
        try:
            resource.close()  # type: ignore[attr-defined]
        except Exception:
            return


__all__ = [
    "MssqlSemanticRefreshStateAdapter",
    "SemanticRefreshMssqlStateConflict",
    "SemanticRefreshMssqlStateError",
]

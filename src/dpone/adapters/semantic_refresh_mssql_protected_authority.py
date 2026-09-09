"""Locked durable operation authority reader for MSSQL semantic refresh."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, Protocol
from uuid import UUID

from dpone.ports.semantic_refresh_mssql_authority import (
    MssqlCanonicalAuthorityRecord,
    MssqlProtectedOperationStateRecord,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> list[tuple[Any, ...]]: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    autocommit: bool

    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class SemanticRefreshMssqlProtectedAuthorityError(RuntimeError):
    """Raised when exact ACTIVE authority and admitted state cannot be read."""


class MssqlSemanticRefreshProtectedOperationState:
    """Read canonical, execution, journal, guard, and image state atomically."""

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

    def load_operation_state(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlProtectedOperationStateRecord:
        """Return one exact locked authority/state record or fail closed."""

        connection = self._connection_factory()
        connection.autocommit = False
        cursor = connection.cursor()
        try:
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            authority = self._authority(cursor, workflow_execution_binding_sha256)
            state = self._state(cursor, authority, operation_id)
            connection.commit()
            return state
        except Exception as exc:
            connection.rollback()
            if isinstance(exc, SemanticRefreshMssqlProtectedAuthorityError):
                raise
            raise SemanticRefreshMssqlProtectedAuthorityError("protected operation authority read failed") from exc
        finally:
            cursor.close()
            connection.close()

    def load_plan_states(
        self,
        *,
        workflow_execution_binding_sha256: str,
    ) -> tuple[MssqlProtectedOperationStateRecord, ...]:
        """Return the full admitted plan state from one SERIALIZABLE snapshot."""

        connection = self._connection_factory()
        connection.autocommit = False
        cursor = connection.cursor()
        try:
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            authority = self._authority(cursor, workflow_execution_binding_sha256)
            rows = self._state_rows(cursor, authority, operation_id=None)
            if not rows:
                raise SemanticRefreshMssqlProtectedAuthorityError("admitted plan operation closure is absent")
            result = tuple(_state_record(authority, row) for row in rows)
            connection.commit()
            return result
        except Exception as exc:
            connection.rollback()
            if isinstance(exc, SemanticRefreshMssqlProtectedAuthorityError):
                raise
            raise SemanticRefreshMssqlProtectedAuthorityError("protected plan authority read failed") from exc
        finally:
            cursor.close()
            connection.close()

    def _authority(self, cursor: _Cursor, binding_sha256: str) -> MssqlCanonicalAuthorityRecord:
        cursor.execute(
            f"""
SELECT workflow_execution_id, authority_sha256, authority_json, status
FROM {self._table("semantic_refresh_canonical_authorities")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ?;
""".strip(),
            binding_sha256,
        )
        row = cursor.fetchone()
        if row is None or cursor.fetchone() is not None or row[3] != "ACTIVE":
            raise SemanticRefreshMssqlProtectedAuthorityError("ACTIVE canonical authority is absent or ambiguous")
        return MssqlCanonicalAuthorityRecord(
            workflow_execution_binding_sha256=binding_sha256,
            workflow_execution_id=str(row[0]),
            authority_sha256=str(row[1]),
            authority_json=str(row[2]),
            status=str(row[3]),
        )

    def _state(
        self,
        cursor: _Cursor,
        authority: MssqlCanonicalAuthorityRecord,
        operation_id: str,
    ) -> MssqlProtectedOperationStateRecord:
        self._execute_state_query(cursor, authority, operation_id=operation_id)
        row = cursor.fetchone()
        if row is None or cursor.fetchone() is not None:
            raise SemanticRefreshMssqlProtectedAuthorityError("admitted operation state is absent or ambiguous")
        return _state_record(authority, tuple(row))

    def _state_rows(
        self,
        cursor: _Cursor,
        authority: MssqlCanonicalAuthorityRecord,
        *,
        operation_id: str | None,
    ) -> tuple[tuple[Any, ...], ...]:
        self._execute_state_query(cursor, authority, operation_id=operation_id)
        return tuple(tuple(row) for row in cursor.fetchall())

    def _execute_state_query(
        self,
        cursor: _Cursor,
        authority: MssqlCanonicalAuthorityRecord,
        *,
        operation_id: str | None,
    ) -> None:
        operation_predicate = "AND journal.operation_id = ?" if operation_id is not None else ""
        parameters: tuple[object, ...] = (
            authority.workflow_execution_binding_sha256,
            authority.authority_sha256,
            *((operation_id,) if operation_id is not None else ()),
        )
        cursor.execute(
            f"""
SELECT execution.workflow_execution_id, execution.workflow_plan_sha256,
       journal.operation_id, journal.operation_plan_sha256,
       journal.attempt_binding_sha256, journal.fencing_epoch, journal.owner_id,
       journal.target_resource_id, guard.status, journal.status, journal.journal_version,
       journal.strategy_authority_json, journal.strategy_authority_sha256,
       journal.target_predecessor_generation_id,
       journal.scope_predecessor_operation_id,
       journal.predecessor_target_generation, journal.predecessor_target_uuid,
       journal.predecessor_target_operation_id, journal.predecessor_scope_revision,
       journal.predecessor_checkpoint_sha256,
       journal.predecessor_checkpoint_operation_id,
       journal.predecessor_checkpoint_version,
       receipt.before_image_relation, receipt.before_image_sha256,
       receipt.after_image_relation, receipt.after_image_sha256
FROM {self._table("semantic_refresh_workflow_executions")} AS execution WITH (UPDLOCK, HOLDLOCK)
JOIN {self._table("semantic_refresh_journals")} AS journal WITH (UPDLOCK, HOLDLOCK)
  ON journal.workflow_id = execution.workflow_id
JOIN {self._table("semantic_refresh_guards")} AS guard WITH (UPDLOCK, HOLDLOCK)
  ON guard.resource_id = journal.target_resource_id
LEFT JOIN {self._table("semantic_refresh_receipts")} AS receipt WITH (UPDLOCK, HOLDLOCK)
  ON receipt.operation_id = journal.operation_id
WHERE execution.workflow_execution_binding_sha256 = ?
  AND execution.canonical_authority_sha256 = ?
  {operation_predicate}
ORDER BY journal.operation_id;
""".strip(),
            *parameters,
        )

    def _table(self, table_name: str) -> str:
        return f"[{self._control_schema}].[{table_name}]"


def _positive(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SemanticRefreshMssqlProtectedAuthorityError(f"{field_name} is invalid")
    return value


def _state_record(
    authority: MssqlCanonicalAuthorityRecord,
    row: tuple[Any, ...],
) -> MssqlProtectedOperationStateRecord:
    return MssqlProtectedOperationStateRecord(
        authority=authority,
        workflow_execution_id=str(row[0]),
        workflow_plan_sha256=str(row[1]),
        operation_id=str(row[2]),
        operation_plan_sha256=str(row[3]),
        attempt_binding_sha256=str(row[4]),
        fencing_epoch=_positive(row[5], "fencing_epoch"),
        owner_id=str(row[6]),
        guard_resource_id=str(row[7]),
        guard_status=str(row[8]),
        journal_status=str(row[9]),
        journal_version=_positive(row[10], "journal_version"),
        strategy_authority_json=str(row[11]),
        strategy_authority_sha256=str(row[12]),
        target_predecessor_generation_id=str(row[13]),
        scope_predecessor_operation_id=_optional_text(row[14]),
        predecessor_target_generation=_positive(row[15], "predecessor_target_generation"),
        predecessor_target_uuid=_uuid_text(row[16]),
        predecessor_target_operation_id=str(row[17]),
        predecessor_scope_revision=_optional_int(row[18]),
        predecessor_checkpoint_sha256=_optional_text(row[19]),
        predecessor_checkpoint_operation_id=_optional_text(row[20]),
        predecessor_checkpoint_version=_optional_int(row[21]),
        before_image_relation=_optional_text(row[22]),
        before_image_sha256=_optional_text(row[23]),
        after_image_relation=_optional_text(row[24]),
        after_image_sha256=_optional_text(row[25]),
    )


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return _positive(value, "optional predecessor integer")


def _optional_text(value: object) -> str | None:
    return None if value is None else str(value)


def _uuid_text(value: object) -> str:
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError) as exc:
        raise SemanticRefreshMssqlProtectedAuthorityError("predecessor target UUID is invalid") from exc


__all__ = [
    "MssqlSemanticRefreshProtectedOperationState",
    "SemanticRefreshMssqlProtectedAuthorityError",
]

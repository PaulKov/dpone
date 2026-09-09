"""Atomic durable workflow summary publication and guard release."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

from dpone.adapters.semantic_refresh_mssql_publication_summary_authority import (
    SemanticRefreshWorkflowSummaryError,
    assert_workflow_resources_released,
    load_canonical_workflow_summary_authority,
)
from dpone.adapters.semantic_refresh_mssql_workflow_summary_validation import (
    canonical_json as _canonical_json,
)
from dpone.adapters.semantic_refresh_mssql_workflow_summary_validation import (
    expected_journals as _expected_journals,
)
from dpone.adapters.semantic_refresh_mssql_workflow_summary_validation import (
    is_digest as _is_digest,
)
from dpone.adapters.semantic_refresh_mssql_workflow_summary_validation import (
    is_uuid as _is_uuid,
)
from dpone.adapters.semantic_refresh_mssql_workflow_summary_validation import (
    journal_resources as _journal_resources,
)
from dpone.adapters.semantic_refresh_mssql_workflow_summary_validation import (
    require_rows as _require_rows,
)
from dpone.adapters.semantic_refresh_mssql_workflow_summary_validation import (
    uuid_text as _uuid_text,
)
from dpone.adapters.semantic_refresh_mssql_workflow_summary_validation import (
    validate_workflow_summary_payload,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


class MssqlSemanticRefreshWorkflowSummaryState:
    """Persist one fully complete summary, then release the whole guard set."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        self._connection_factory = connection_factory
        if _IDENTIFIER.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._control_schema = control_schema

    def persist(self, summary: Mapping[str, object]) -> Mapping[str, object]:
        """Atomically bind terminal journals, summary, reservation and release."""

        normalized = validate_workflow_summary_payload(summary)
        connection = self._connection_factory()
        connection.autocommit = False
        cursor = connection.cursor()
        try:
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            execution_id = str(normalized["workflow_execution_id"])
            self._lock(cursor, execution_id)
            execution = self._execution(cursor, execution_id)
            self._assert_execution(normalized, execution)
            workflow_id = str(execution[0])
            publications = normalized["publications"]
            assert isinstance(publications, list)
            journal_guards = (
                self._assert_terminal_journals(cursor, workflow_id, publications)
                if execution[5] == "COMPLETE"
                else self._assert_journals(cursor, workflow_id, publications)
            )
            authority = load_canonical_workflow_summary_authority(
                cursor,
                table=self._table,
                summary=normalized,
                execution=execution,
            )
            if not set(journal_guards).issubset(authority.guard_resources):
                raise SemanticRefreshWorkflowSummaryError("workflow journal guard is outside canonical closure")
            assert_workflow_resources_released(
                cursor,
                table=self._table,
                workflow_id=workflow_id,
                execution_status=execution[5],
                authority=authority,
            )
            if execution[5] == "COMPLETE":
                self._assert_released(cursor, workflow_id, execution, authority.guard_resources)
            else:
                self._complete(cursor, workflow_id, normalized, authority.guard_resources)
            connection.commit()
        except Exception as exc:
            connection.rollback()
            if isinstance(exc, SemanticRefreshWorkflowSummaryError):
                raise
            raise SemanticRefreshWorkflowSummaryError("durable workflow summary publication failed") from exc
        finally:
            cursor.close()
            connection.close()
        return {
            "persisted": True,
            "status": "FULLY_COMPLETE",
            "terminal_summary_sha256": normalized["terminal_summary_sha256"],
        }

    @staticmethod
    def _lock(cursor: _Cursor, workflow_id: str) -> None:
        cursor.execute(
            """
DECLARE @dpone_lock_result int;
EXEC @dpone_lock_result = sys.sp_getapplock
    @Resource = ?, @LockMode = N'Exclusive',
    @LockOwner = N'Transaction', @LockTimeout = 0;
SELECT @dpone_lock_result;
""".strip(),
            f"dpone:semantic-refresh:summary:{workflow_id}",
        )
        row = cursor.fetchone()
        if row is None or isinstance(row[0], bool) or not isinstance(row[0], int) or row[0] < 0:
            raise SemanticRefreshWorkflowSummaryError("workflow summary lock was not acquired")

    def _execution(self, cursor: _Cursor, workflow_execution_id: str) -> tuple[Any, ...]:
        cursor.execute(
            f"""
SELECT workflow_id, workflow_execution_id, workflow_plan_sha256,
       workflow_execution_binding_sha256, terminal_summary_sha256, status,
       workflow_guard_resource_id, guard_count, terminal_summary_json,
       canonical_authority_sha256, guard_set_sha256
FROM {self._table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_id = ?;
""".strip(),
            workflow_execution_id,
        )
        row = cursor.fetchone()
        if row is None:
            raise SemanticRefreshWorkflowSummaryError("workflow execution is absent")
        return tuple(row)

    @staticmethod
    def _assert_execution(summary: Mapping[str, object], execution: tuple[Any, ...]) -> None:
        if execution[1:4] != (
            summary["workflow_execution_id"],
            summary["workflow_plan_sha256"],
            summary["workflow_execution_binding_sha256"],
        ):
            raise SemanticRefreshWorkflowSummaryError("workflow summary identity differs from execution")
        if execution[5] not in {"PREPARING", "COMPLETE"}:
            raise SemanticRefreshWorkflowSummaryError("workflow execution is not finalizable")
        if execution[5] == "COMPLETE" and (
            execution[4] != summary["terminal_summary_sha256"] or execution[8] != _canonical_json(summary)
        ):
            raise SemanticRefreshWorkflowSummaryError("workflow summary replay differs")

    def _assert_journals(
        self,
        cursor: _Cursor,
        workflow_id: str,
        publications: list[object],
    ) -> tuple[str, ...]:
        cursor.execute(
            f"""
SELECT journal.operation_id, journal.operation_plan_sha256,
       journal.attempt_binding_sha256, journal.status,
       journal.artifact_manifest_sha256, journal.clickhouse_commit_receipt_sha256,
       journal.terminal_receipt_sha256, journal.terminal_target_generation,
       journal.terminal_scope_revision, journal.target_resource_id,
       target_head.target_generation, target_head.target_generation_id,
       target_head.target_uuid, target_head.operation_id,
       journal.terminal_target_generation_id, journal.target_uuid,
       journal.terminal_target_mutation_outcome,
       journal.predecessor_target_operation_id,
       journal.terminal_checkpoint_sha256,
       journal.terminal_checkpoint_version,
       checkpoint_state.checkpoint_sha256,
       checkpoint_state.checkpoint_version,
       checkpoint_state.operation_id,
       scope_head.scope_revision,
       scope_head.operation_id
FROM {self._table("semantic_refresh_journals")} AS journal WITH (UPDLOCK, HOLDLOCK)
JOIN {self._table("semantic_refresh_target_heads")} AS target_head WITH (UPDLOCK, HOLDLOCK)
  ON target_head.database_name = journal.publication_database
 AND target_head.target_table = journal.publication_target_table
JOIN {self._table("semantic_refresh_scope_heads")} AS scope_head WITH (UPDLOCK, HOLDLOCK)
  ON scope_head.database_name = journal.publication_database
 AND scope_head.target_table = journal.publication_target_table
 AND scope_head.scope_id = journal.publication_scope_id
 AND scope_head.operation_id = journal.operation_id
LEFT JOIN {self._table("semantic_refresh_checkpoints")} AS checkpoint_state WITH (UPDLOCK, HOLDLOCK)
  ON checkpoint_state.database_name = journal.publication_database
 AND checkpoint_state.target_table = journal.publication_target_table
 AND checkpoint_state.scope_id = journal.publication_scope_id
WHERE journal.workflow_id = ? ORDER BY journal.operation_id;
""".strip(),
            workflow_id,
        )
        durable = tuple(tuple(row) for row in cursor.fetchall())
        if tuple(row[:9] for row in durable) != _expected_journals(publications):
            raise SemanticRefreshWorkflowSummaryError("workflow terminal journal closure differs")
        for row in durable:
            terminal_owner = row[0] if row[16] == "TARGET_COMMITTED" else row[17]
            if row[16] not in {"TARGET_COMMITTED", "NOT_REQUIRED_EMPTY_SCOPE"} or (
                row[10],
                row[11],
                _uuid_text(row[12]),
                row[13],
            ) != (
                row[7],
                row[14],
                _uuid_text(row[15]),
                terminal_owner,
            ):
                raise SemanticRefreshWorkflowSummaryError("workflow terminal target lineage differs")
            if (row[18], row[19], row[0]) != (row[20], row[21], row[22]):
                raise SemanticRefreshWorkflowSummaryError("workflow terminal checkpoint lineage differs")
            if (row[8], row[0]) != (row[23], row[24]):
                raise SemanticRefreshWorkflowSummaryError("workflow terminal scope lineage differs")
        return _journal_resources(durable)

    def _assert_terminal_journals(
        self,
        cursor: _Cursor,
        workflow_id: str,
        publications: list[object],
    ) -> tuple[str, ...]:
        """Authenticate immutable terminal rows without historical mutable heads."""

        cursor.execute(
            f"""
SELECT journal.operation_id, journal.operation_plan_sha256,
       journal.attempt_binding_sha256, journal.status,
       journal.artifact_manifest_sha256, journal.clickhouse_commit_receipt_sha256,
       journal.terminal_receipt_sha256, journal.terminal_target_generation,
       journal.terminal_scope_revision, journal.target_resource_id,
       journal.terminal_target_generation_id, journal.target_uuid,
       journal.terminal_target_mutation_outcome,
       journal.predecessor_target_operation_id,
       journal.terminal_checkpoint_sha256,
       journal.terminal_checkpoint_version
FROM {self._table("semantic_refresh_journals")} AS journal WITH (UPDLOCK, HOLDLOCK)
WHERE journal.workflow_id = ? ORDER BY journal.operation_id;
""".strip(),
            workflow_id,
        )
        durable = tuple(tuple(row) for row in cursor.fetchall())
        if tuple(row[:9] for row in durable) != _expected_journals(publications):
            raise SemanticRefreshWorkflowSummaryError("workflow terminal journal closure differs")
        for row in durable:
            if (
                len(row) != 16
                or not _is_digest(row[10])
                or not _is_uuid(row[11])
                or row[12] not in {"TARGET_COMMITTED", "NOT_REQUIRED_EMPTY_SCOPE"}
                or not _is_digest(row[14])
                or isinstance(row[15], bool)
                or not isinstance(row[15], int)
                or row[15] <= 0
            ):
                raise SemanticRefreshWorkflowSummaryError("workflow immutable terminal evidence differs")
        return _journal_resources(durable)

    def _complete(
        self,
        cursor: _Cursor,
        workflow_id: str,
        summary: Mapping[str, object],
        expected_guard_resources: tuple[str, ...],
    ) -> None:
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
SET status = N'COMPLETE', terminal_summary_sha256 = ?, terminal_summary_json = ?,
    completed_at_utc = SYSUTCDATETIME()
OUTPUT inserted.workflow_id
WHERE workflow_id = ? AND status = N'PREPARING' AND terminal_summary_sha256 IS NULL;
""".strip(),
            summary["terminal_summary_sha256"],
            _canonical_json(summary),
            workflow_id,
        )
        _require_rows(cursor, ((workflow_id,),), "workflow execution")
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_reservations")} WITH (UPDLOCK, HOLDLOCK)
SET status = N'COMPLETE'
OUTPUT inserted.workflow_id
WHERE workflow_id = ? AND status = N'PREPARING';
""".strip(),
            workflow_id,
        )
        _require_rows(cursor, ((workflow_id,),), "workflow reservation")
        cursor.execute(
            f"SELECT resource_id FROM {self._table('semantic_refresh_guards')} WITH (UPDLOCK, HOLDLOCK) "
            "WHERE workflow_id = ? AND status = N'HELD' ORDER BY resource_id;",
            workflow_id,
        )
        held_resources = tuple(str(row[0]) for row in cursor.fetchall())
        if held_resources != expected_guard_resources:
            raise SemanticRefreshWorkflowSummaryError("workflow guard closure differs")
        cursor.execute(
            f"""
UPDATE {self._table("semantic_refresh_guards")} WITH (UPDLOCK, HOLDLOCK)
SET status = N'RELEASED'
OUTPUT inserted.resource_id
WHERE workflow_id = ? AND status = N'HELD';
""".strip(),
            workflow_id,
        )
        _require_rows(
            cursor,
            tuple((resource_id,) for resource_id in expected_guard_resources),
            "workflow guard set",
        )

    def _assert_released(
        self,
        cursor: _Cursor,
        workflow_id: str,
        execution: tuple[Any, ...],
        expected_guard_resources: tuple[str, ...],
    ) -> None:
        if execution[4] is None:
            raise SemanticRefreshWorkflowSummaryError("complete workflow lacks terminal summary")
        cursor.execute(
            f"SELECT COUNT_BIG(*) FROM {self._table('semantic_refresh_guards')} "
            "WHERE workflow_id = ? AND status = N'HELD';",
            workflow_id,
        )
        held_row = cursor.fetchone()
        cursor.execute(
            f"SELECT resource_id FROM {self._table('semantic_refresh_guards')} "
            "WHERE workflow_id = ? AND status = N'RELEASED' ORDER BY resource_id;",
            workflow_id,
        )
        released = tuple(str(row[0]) for row in cursor.fetchall())
        cursor.execute(
            f"SELECT COUNT_BIG(*) FROM {self._table('semantic_refresh_reservations')} "
            "WHERE workflow_id = ? AND status = N'COMPLETE';",
            workflow_id,
        )
        complete_row = cursor.fetchone()
        held = None if held_row is None else tuple(held_row)
        complete = None if complete_row is None else tuple(complete_row)
        if held != (0,) or released != expected_guard_resources or complete != (1,):
            raise SemanticRefreshWorkflowSummaryError("terminal workflow authority was not released")

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


__all__ = [
    "MssqlSemanticRefreshWorkflowSummaryState",
    "SemanticRefreshWorkflowSummaryError",
    "validate_workflow_summary_payload",
]

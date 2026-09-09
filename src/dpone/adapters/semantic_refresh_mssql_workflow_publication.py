"""Serializable MSSQL reader for durable semantic-refresh model publications."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from dpone.ports.semantic_refresh_workflow_publication import (
    DurableSemanticRefreshModelPublication,
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


class SemanticRefreshWorkflowPublicationReadError(RuntimeError):
    """Raised when the exact workflow journal closure is unavailable."""


class MssqlSemanticRefreshWorkflowPublicationReader:
    """Read execution and journal identities together under SERIALIZABLE."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        if _IDENTIFIER.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._control_schema = control_schema

    def read(
        self,
        *,
        workflow_execution_id: str,
        workflow_execution_binding_sha256: str,
        expected_operation_ids: tuple[str, ...],
    ) -> tuple[DurableSemanticRefreshModelPublication, ...]:
        """Return only the exact admitted operation set in canonical order."""

        if not workflow_execution_id.strip():
            raise ValueError("workflow_execution_id must be non-empty")
        _digest(workflow_execution_binding_sha256, "workflow_execution_binding_sha256")
        canonical_ids = tuple(sorted(set(expected_operation_ids)))
        if not canonical_ids or canonical_ids != expected_operation_ids:
            raise ValueError("expected operation IDs must be canonical and unique")
        for operation_id in canonical_ids:
            _digest(operation_id, "operation_id")
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            cursor.execute(
                f"""
SELECT workflow_id
FROM {self._table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_id = ? AND workflow_execution_binding_sha256 = ?;
""".strip(),
                workflow_execution_id,
                workflow_execution_binding_sha256,
            )
            execution = cursor.fetchone()
            if execution is None or cursor.fetchone() is not None:
                raise SemanticRefreshWorkflowPublicationReadError("workflow execution authority is absent or ambiguous")
            cursor.execute(
                f"""
SELECT journal.operation_id, journal.operation_plan_sha256,
       journal.attempt_binding_sha256, journal.status,
       journal.artifact_manifest_sha256,
       journal.clickhouse_commit_receipt_sha256,
       journal.terminal_receipt_sha256,
       journal.terminal_target_generation, journal.terminal_scope_revision,
       journal.terminal_target_mutation_outcome,
       target.target_generation, scope.scope_revision,
       target.operation_id, scope.operation_id,
       journal.predecessor_target_operation_id
FROM {self._table("semantic_refresh_journals")} AS journal WITH (UPDLOCK, HOLDLOCK)
LEFT JOIN {self._table("semantic_refresh_target_heads")} AS target WITH (UPDLOCK, HOLDLOCK)
  ON target.database_name = journal.publication_database
 AND target.target_table = journal.publication_target_table
LEFT JOIN {self._table("semantic_refresh_scope_heads")} AS scope WITH (UPDLOCK, HOLDLOCK)
  ON scope.database_name = journal.publication_database
 AND scope.target_table = journal.publication_target_table
 AND scope.scope_id = journal.publication_scope_id
WHERE journal.workflow_id = ? ORDER BY journal.operation_id;
""".strip(),
                execution[0],
            )
            rows = tuple(tuple(row) for row in cursor.fetchall())
            if tuple(str(row[0]) for row in rows) != canonical_ids:
                raise SemanticRefreshWorkflowPublicationReadError("workflow journal operation closure differs")
            result = tuple(
                DurableSemanticRefreshModelPublication(
                    operation_id=str(row[0]),
                    operation_plan_sha256=str(row[1]),
                    workflow_execution_binding_sha256=workflow_execution_binding_sha256,
                    attempt_binding_sha256=str(row[2]),
                    status=str(row[3]),
                    artifact_manifest_sha256=(None if row[4] is None else str(row[4])),
                    clickhouse_terminal_receipt_sha256=(None if row[5] is None else str(row[5])),
                    terminal_receipt_sha256=(None if row[6] is None else str(row[6])),
                    target_generation=(None if row[7] is None else int(row[7])),
                    scope_revision=(None if row[8] is None else int(row[8])),
                )
                for row in rows
            )
            if any(row[3] == "COMPLETE" and not _terminal_heads_match(row) for row in rows):
                raise SemanticRefreshWorkflowPublicationReadError(
                    "terminal publication head lineage differs from journal"
                )
            connection.commit()
            return result
        except SemanticRefreshWorkflowPublicationReadError:
            _rollback(connection)
            raise
        except Exception as exc:
            _rollback(connection)
            raise SemanticRefreshWorkflowPublicationReadError("durable workflow publication read failed") from exc
        finally:
            _close(cursor)
            _close(connection)

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


def _digest(value: object, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith("sha256:")
        or len(value) != 71
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{field_name} must be a canonical lowercase sha256 digest")
    return value


def _terminal_heads_match(row: tuple[object, ...]) -> bool:
    operation_id = row[0]
    mutation_outcome = row[9]
    expected_target_owner = row[14] if mutation_outcome == "NOT_REQUIRED_EMPTY_SCOPE" else operation_id
    return (
        mutation_outcome in {"TARGET_COMMITTED", "NOT_REQUIRED_EMPTY_SCOPE"}
        and row[7] == row[10]
        and row[8] == row[11]
        and row[12] == expected_target_owner
        and row[13] == operation_id
    )


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
    "MssqlSemanticRefreshWorkflowPublicationReader",
    "SemanticRefreshWorkflowPublicationReadError",
]

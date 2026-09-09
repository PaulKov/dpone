"""Thin MSSQL reader for persisted predecessor replacement authority."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from dpone.adapters.semantic_refresh_mssql_run_binding import MssqlWorkerRunBindingQueries
from dpone.ports.semantic_refresh_mssql_replacement import (
    MssqlDurableFailedRecoveryAuthority,
    MssqlFailedRecoveryReadRequest,
    MssqlPersistedModelOutcome,
    MssqlPredecessorFailureState,
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


class SemanticRefreshMssqlPredecessorReadError(RuntimeError):
    """Raised when persisted predecessor authority is incomplete."""


class MssqlSemanticRefreshPredecessorStateReader:
    """Read one failed workflow and its terminal model journal closure."""

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
        self._binding_queries = MssqlWorkerRunBindingQueries(control_schema)

    def load_predecessor(self, workflow_id: str) -> MssqlPredecessorFailureState:
        """Return summary and exact outcomes in a serializable snapshot."""

        connection = self._connection_factory()
        connection.autocommit = False
        cursor = connection.cursor()
        try:
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            cursor.execute(
                f"""
SELECT workflow_plan_sha256, workflow_execution_binding_sha256,
       terminal_summary_sha256, status
FROM {self._table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_id = ?;
""".strip(),
                workflow_id,
            )
            execution = cursor.fetchone()
            if execution is None or execution[2] is None or execution[3] != "FAILED_PRE_COMMIT":
                raise SemanticRefreshMssqlPredecessorReadError("replacement predecessor is not terminal")
            cursor.execute(
                f"""
SELECT j.model_unique_id, j.operation_id, j.attempt_binding_sha256,
       j.mssql_outcome, j.mssql_evidence_sha256,
       j.operation_plan_sha256, j.fencing_epoch,
       j.strategy_authority_json, j.strategy_authority_sha256,
       r.before_image_relation, r.before_image_sha256,
       r.after_image_relation, r.after_image_sha256
FROM {self._table("semantic_refresh_journals")} AS j WITH (UPDLOCK, HOLDLOCK)
LEFT JOIN {self._table("semantic_refresh_receipts")} AS r WITH (UPDLOCK, HOLDLOCK)
  ON r.operation_id = j.operation_id
 AND r.operation_plan_sha256 = j.operation_plan_sha256
 AND r.attempt_binding_sha256 = j.attempt_binding_sha256
 AND r.fencing_epoch = j.fencing_epoch
WHERE j.workflow_id = ? AND j.status = N'FAILED_PRE_COMMIT'
ORDER BY model_unique_id;
""".strip(),
                workflow_id,
            )
            rows = tuple(tuple(row) for row in cursor.fetchall())
            if not rows or any(row[2] is None or row[3] is None or row[4] is None for row in rows):
                raise SemanticRefreshMssqlPredecessorReadError("predecessor model outcomes are incomplete")
            connection.commit()
            return MssqlPredecessorFailureState(
                workflow_id=workflow_id,
                workflow_plan_sha256=str(execution[0]),
                workflow_execution_binding_sha256=str(execution[1]),
                terminal_summary_sha256=str(execution[2]),
                status=str(execution[3]),
                models=tuple(MssqlPersistedModelOutcome(*row) for row in rows),
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()

    def load_failed_recovery_authority(
        self,
        request: MssqlFailedRecoveryReadRequest,
    ) -> MssqlDurableFailedRecoveryAuthority:
        """Read the exact ACTIVE authority, failed summary, pack and journals."""

        connection = self._connection_factory()
        connection.autocommit = False
        cursor = connection.cursor()
        try:
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            binding, _, _ = self._binding_queries.load(
                cursor,
                request.workflow_plan_sha256,
                request.workflow_execution_id,
            )
            if (
                binding.record.workflow_execution_binding_sha256 != request.workflow_execution_binding_sha256
                or binding.record.authority_sha256 != request.canonical_authority_sha256
            ):
                raise SemanticRefreshMssqlPredecessorReadError("failed recovery canonical authority conflict")
            cursor.execute(
                f"""
SELECT workflow_execution_id, workflow_plan_sha256, canonical_authority_sha256,
       terminal_summary_sha256, terminal_summary_json, status,
       workflow_execution_binding_sha256
FROM {self._table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 COLLATE Latin1_General_100_BIN2
          = CONVERT(varchar(71), ?) COLLATE Latin1_General_100_BIN2;
""".strip(),
                request.workflow_execution_binding_sha256,
            )
            execution = _row(cursor)
            if (
                execution is None
                or execution[:3]
                != (
                    request.workflow_execution_id,
                    request.workflow_plan_sha256,
                    request.canonical_authority_sha256,
                )
                or not _digest(execution[3])
                or not isinstance(execution[4], str)
                or not execution[4].strip()
                or execution[5] != "FAILED_PRE_COMMIT"
                or execution[6] != request.workflow_execution_binding_sha256
            ):
                raise SemanticRefreshMssqlPredecessorReadError("failed recovery execution authority conflict")
            cursor.execute(
                f"""
SELECT j.model_unique_id, j.operation_id, j.attempt_binding_sha256,
       j.mssql_outcome, j.mssql_evidence_sha256,
       j.operation_plan_sha256, j.fencing_epoch,
       j.strategy_authority_json, j.strategy_authority_sha256,
       r.before_image_relation, r.before_image_sha256,
       r.after_image_relation, r.after_image_sha256, j.status
FROM {self._table("semantic_refresh_journals")} AS j WITH (UPDLOCK, HOLDLOCK)
LEFT JOIN {self._table("semantic_refresh_receipts")} AS r WITH (UPDLOCK, HOLDLOCK)
  ON r.operation_id = j.operation_id
 AND r.operation_plan_sha256 = j.operation_plan_sha256
 AND r.attempt_binding_sha256 = j.attempt_binding_sha256
 AND r.fencing_epoch = j.fencing_epoch
WHERE j.workflow_id = ?
ORDER BY j.model_unique_id;
""".strip(),
                request.workflow_execution_id,
            )
            rows = tuple(tuple(row) for row in cursor.fetchall())
            if not rows or any(
                row[2] is None
                or row[3] is None
                or row[4] is None
                or row[5] is None
                or row[6] is None
                or row[13] != "FAILED_PRE_COMMIT"
                for row in rows
            ):
                raise SemanticRefreshMssqlPredecessorReadError("failed recovery journal authority is incomplete")
            models = tuple(MssqlPersistedModelOutcome(*row[:13]) for row in rows)
            result = MssqlDurableFailedRecoveryAuthority(
                workflow_execution_id=request.workflow_execution_id,
                workflow_plan_sha256=request.workflow_plan_sha256,
                workflow_execution_binding_sha256=request.workflow_execution_binding_sha256,
                plan_bundle_sha256=binding.plan_bundle_sha256,
                canonical_authority_sha256=request.canonical_authority_sha256,
                terminal_summary_sha256=str(execution[3]),
                terminal_summary_json=str(execution[4]),
                models=models,
            )
            connection.commit()
            return result
        except SemanticRefreshMssqlPredecessorReadError:
            connection.rollback()
            raise
        except Exception as exc:
            connection.rollback()
            raise SemanticRefreshMssqlPredecessorReadError("protected failed recovery read failed") from exc
        finally:
            cursor.close()
            connection.close()

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


def _row(cursor: _Cursor) -> tuple[Any, ...] | None:
    value = cursor.fetchone()
    return None if value is None else tuple(value)


def _digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 71
        and value.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in value[7:])
    )


__all__ = [
    "MssqlSemanticRefreshPredecessorStateReader",
    "SemanticRefreshMssqlPredecessorReadError",
]

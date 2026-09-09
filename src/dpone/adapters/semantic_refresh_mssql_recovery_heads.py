"""Serializable MSSQL reader for protected complete-workflow target heads."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from typing import Any, Protocol
from uuid import UUID

from dpone.adapters.semantic_refresh_mssql_run_binding import MssqlWorkerRunBindingQueries
from dpone.ports.semantic_refresh_mssql_recovery_heads import (
    MssqlDurableRecoveryAuthority,
    MssqlDurableRecoveryPublication,
    MssqlDurableRecoveryTargetHead,
    MssqlRecoveryHeadReadRequest,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


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


class SemanticRefreshMssqlRecoveryHeadReadError(RuntimeError):
    """Raised when current target-head evidence is absent or inconsistent."""


class MssqlSemanticRefreshRecoveryHeadReader:
    """Lock completed predecessor journals and their exact current target heads."""

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

    def load_recovery_authority(
        self,
        request: MssqlRecoveryHeadReadRequest,
    ) -> MssqlDurableRecoveryAuthority:
        """Return the exact completed summary and current head closure."""

        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            plan_bundle_sha256, terminal_summary_sha256, terminal_summary_json = self._require_authority(
                cursor,
                request,
            )
            models = tuple(
                self._read_model(
                    cursor,
                    request.workflow_execution_id,
                    request.workflow_execution_binding_sha256,
                    item,
                )
                for item in request.locators
            )
            connection.commit()
            return MssqlDurableRecoveryAuthority(
                workflow_execution_id=request.workflow_execution_id,
                workflow_execution_binding_sha256=request.workflow_execution_binding_sha256,
                workflow_plan_sha256=request.workflow_plan_sha256,
                plan_bundle_sha256=plan_bundle_sha256,
                canonical_authority_sha256=request.canonical_authority_sha256,
                terminal_summary_sha256=terminal_summary_sha256,
                terminal_summary_json=terminal_summary_json,
                predecessor_publications=tuple(item[0] for item in models),
                target_heads=tuple(item[1] for item in models),
            )
        except SemanticRefreshMssqlRecoveryHeadReadError:
            _rollback(connection)
            raise
        except Exception as exc:
            _rollback(connection)
            raise SemanticRefreshMssqlRecoveryHeadReadError("protected MSSQL recovery head read failed") from exc
        finally:
            _close(cursor)
            _close(connection)

    def _require_authority(
        self,
        cursor: _Cursor,
        request: MssqlRecoveryHeadReadRequest,
    ) -> tuple[str, str, str]:
        binding, _, _ = self._binding_queries.load(
            cursor,
            request.workflow_plan_sha256,
            request.workflow_execution_id,
        )
        if (
            binding.record.workflow_execution_binding_sha256 != request.workflow_execution_binding_sha256
            or binding.record.authority_sha256 != request.canonical_authority_sha256
        ):
            raise SemanticRefreshMssqlRecoveryHeadReadError("canonical recovery authority conflict")
        cursor.execute(
            f"""
SELECT workflow_plan_sha256, canonical_authority_sha256, status,
       terminal_summary_sha256, terminal_summary_json,
       workflow_execution_id, workflow_execution_binding_sha256
FROM {self._table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_id COLLATE Latin1_General_100_BIN2
          = CONVERT(nvarchar(512), ?) COLLATE Latin1_General_100_BIN2
  AND workflow_execution_binding_sha256 COLLATE Latin1_General_100_BIN2
          = CONVERT(varchar(71), ?) COLLATE Latin1_General_100_BIN2;
""".strip(),
            request.workflow_execution_id,
            request.workflow_execution_binding_sha256,
        )
        execution = _row(cursor)
        if (
            execution is None
            or execution[:3]
            != (
                request.workflow_plan_sha256,
                request.canonical_authority_sha256,
                "COMPLETE",
            )
            or not _is_digest(execution[3])
            or not isinstance(execution[4], str)
            or not execution[4].strip()
            or execution[5:]
            != (
                request.workflow_execution_id,
                request.workflow_execution_binding_sha256,
            )
        ):
            raise SemanticRefreshMssqlRecoveryHeadReadError("completed recovery execution authority conflict")
        return binding.plan_bundle_sha256, str(execution[3]), str(execution[4])

    def _read_model(
        self,
        cursor: _Cursor,
        workflow_execution_id: str,
        workflow_execution_binding_sha256: str,
        locator,
    ) -> tuple[MssqlDurableRecoveryPublication, MssqlDurableRecoveryTargetHead]:
        predecessor = self._locked_terminal(cursor, locator.predecessor_operation_id)
        if predecessor is None or predecessor[:5] != (
            locator.predecessor_operation_id,
            locator.model_unique_id,
            locator.database_name,
            locator.target_table,
            "COMPLETE",
        ):
            raise SemanticRefreshMssqlRecoveryHeadReadError("predecessor publication journal conflict")
        if predecessor[11] != workflow_execution_id:
            raise SemanticRefreshMssqlRecoveryHeadReadError("predecessor publication execution conflict")
        publication = MssqlDurableRecoveryPublication(
            model_unique_id=locator.model_unique_id,
            operation_id=locator.predecessor_operation_id,
            operation_plan_sha256=str(predecessor[12]),
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            attempt_binding_sha256=str(predecessor[13]),
            artifact_manifest_sha256=str(predecessor[14]),
            clickhouse_terminal_receipt_sha256=str(predecessor[15]),
            terminal_receipt_sha256=str(predecessor[5]),
            target_generation=_positive(predecessor[6], "predecessor target generation"),
            scope_revision=_positive(predecessor[16], "predecessor scope revision"),
        )
        target = self._locked_target(cursor, locator.database_name, locator.target_table)
        terminal = self._locked_terminal(cursor, target[3])
        if terminal is None:
            terminal = predecessor
        if terminal is None:
            raise SemanticRefreshMssqlRecoveryHeadReadError("current target head has no terminal authority")
        terminal_operation_id = str(terminal[0])
        if terminal[1:5] != (
            locator.model_unique_id,
            locator.database_name,
            locator.target_table,
            "COMPLETE",
        ):
            raise SemanticRefreshMssqlRecoveryHeadReadError("terminal recovery journal conflict")
        terminal_receipt = str(terminal[5])
        if not _is_digest(terminal_receipt):
            raise SemanticRefreshMssqlRecoveryHeadReadError("terminal recovery receipt is invalid")
        outcome = str(terminal[9])
        if outcome == "TARGET_COMMITTED":
            owner_operation_id = terminal_operation_id
        elif outcome == "NOT_REQUIRED_EMPTY_SCOPE":
            owner_operation_id = str(terminal[10])
        else:
            raise SemanticRefreshMssqlRecoveryHeadReadError("terminal target mutation outcome is invalid")
        terminal_head = (
            _positive(terminal[6], "terminal target generation"),
            str(terminal[7]),
            _uuid(terminal[8]),
            owner_operation_id,
        )
        if terminal_head != target:
            raise SemanticRefreshMssqlRecoveryHeadReadError("terminal journal differs from current target head")
        return (
            publication,
            MssqlDurableRecoveryTargetHead(
                model_unique_id=locator.model_unique_id,
                clickhouse_target_authority_id=locator.clickhouse_target_authority_id,
                target_generation=target[0],
                target_generation_id=target[1],
                target_uuid=target[2],
                owner_operation_id=target[3],
                terminal_receipt_sha256=terminal_receipt,
            ),
        )

    def _locked_terminal(self, cursor: _Cursor, operation_id: str) -> tuple[Any, ...] | None:
        cursor.execute(
            f"""
SELECT operation_id, model_unique_id, publication_database, publication_target_table,
       status, terminal_receipt_sha256, terminal_target_generation,
       terminal_target_generation_id, target_uuid, terminal_target_mutation_outcome,
       predecessor_target_operation_id, workflow_id, operation_plan_sha256,
       attempt_binding_sha256, artifact_manifest_sha256,
       clickhouse_commit_receipt_sha256, terminal_scope_revision
FROM {self._table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
WHERE operation_id = ?;
""".strip(),
            operation_id,
        )
        return _row(cursor)

    def _locked_target(self, cursor: _Cursor, database_name: str, target_table: str) -> tuple[int, str, str, str]:
        cursor.execute(
            f"""
SELECT target_generation, target_generation_id, target_uuid, operation_id
FROM {self._table("semantic_refresh_target_heads")} WITH (UPDLOCK, HOLDLOCK)
WHERE database_name = ? AND target_table = ?;
""".strip(),
            database_name,
            target_table,
        )
        row = _row(cursor)
        if row is None or not _is_digest(row[1]) or not _is_digest(row[3]):
            raise SemanticRefreshMssqlRecoveryHeadReadError("current target head is invalid")
        return (
            _positive(row[0], "current target generation"),
            str(row[1]),
            _uuid(row[2]),
            str(row[3]),
        )

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


def _row(cursor: _Cursor) -> tuple[Any, ...] | None:
    value = cursor.fetchone()
    return None if value is None else tuple(value)


def _positive(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SemanticRefreshMssqlRecoveryHeadReadError(f"{field_name} must be positive")
    return value


def _uuid(value: object) -> str:
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError) as exc:
        raise SemanticRefreshMssqlRecoveryHeadReadError("target UUID is invalid") from exc


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and _DIGEST.fullmatch(value) is not None


def _rollback(connection: _Connection | None) -> None:
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
    "MssqlSemanticRefreshRecoveryHeadReader",
    "SemanticRefreshMssqlRecoveryHeadReadError",
]

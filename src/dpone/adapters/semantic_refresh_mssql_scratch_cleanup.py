"""Serializable MSSQL reader for failed-precommit scratch cleanup authority."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, Protocol
from uuid import UUID

from dpone.ports.semantic_refresh_mssql_authority_codec import (
    authority_from_record,
    canonical_authority_record,
)
from dpone.ports.semantic_refresh_mssql_replacement import (
    MssqlFailedScratchCleanupReadRequest,
    MssqlFailedScratchCleanupStateRecord,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE_MSSQL_OUTCOMES = {"COMMITTED_WITH_IMAGES", "NOT_INVOKED", "ROLLED_BACK"}


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    autocommit: bool

    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class SemanticRefreshMssqlScratchCleanupReadError(RuntimeError):
    """Raised when failed-precommit cleanup state is absent or unsafe."""


class MssqlSemanticRefreshFailedScratchCleanupReader:
    """Lock and authenticate one bounded failed-precommit scratch boundary."""

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

    def load_failed_scratch_cleanup(
        self,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlFailedScratchCleanupStateRecord:
        """Return one exact failed operation without authorizing its target."""

        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            request = self._request(cursor, workflow_execution_binding_sha256, operation_id)
            self._require_failed_execution(cursor, request)
            journal = self._require_failed_journal(cursor, request)
            self._require_unchanged_target(cursor, request)
            connection.commit()
            return MssqlFailedScratchCleanupStateRecord(
                request=request,
                mssql_outcome=str(journal[7]),
                prepare_plan_sha256=_optional_text(journal[9]),
                prepare_plan_json=_optional_text(journal[10]),
                prepared_receipt_sha256=_optional_text(journal[11]),
                prepared_receipt_json=_optional_text(journal[12]),
            )
        except SemanticRefreshMssqlScratchCleanupReadError:
            _rollback(connection)
            raise
        except Exception as exc:
            _rollback(connection)
            raise SemanticRefreshMssqlScratchCleanupReadError(
                "protected MSSQL failed scratch cleanup read failed"
            ) from exc
        finally:
            _close(cursor)
            _close(connection)

    def _request(
        self,
        cursor: _Cursor,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlFailedScratchCleanupReadRequest:
        cursor.execute(
            f"""
SELECT workflow_execution_id, authority_sha256, authority_json, status
FROM {self._table("semantic_refresh_canonical_authorities")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ?;
""".strip(),
            workflow_execution_binding_sha256,
        )
        row = _row(cursor)
        if row is None:
            raise SemanticRefreshMssqlScratchCleanupReadError("canonical cleanup authority is absent")
        bundle = authority_from_record(
            canonical_authority_record(
                workflow_execution_binding_sha256=workflow_execution_binding_sha256,
                workflow_execution_id=str(row[0]),
                authority_sha256=str(row[1]),
                authority_json=str(row[2]),
                status=str(row[3]),
            )
        )
        operations = tuple(item for item in bundle.operation_plans if item.operation_id == operation_id)
        if len(operations) != 1:
            raise SemanticRefreshMssqlScratchCleanupReadError("cleanup operation is outside canonical authority")
        operation = operations[0]
        attempts = tuple(item for item in bundle.attempt_bindings if item.operation_id == operation_id)
        resources = tuple(item for item in bundle.model_resources if item.model_unique_id == operation.model_unique_id)
        if len(attempts) != 1 or len(resources) != 1:
            raise SemanticRefreshMssqlScratchCleanupReadError("cleanup authority closure is incomplete")
        attempt = attempts[0]
        resource = resources[0]
        return MssqlFailedScratchCleanupReadRequest.build(
            workflow_execution_id=bundle.workflow_execution_id,
            workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
            canonical_authority_sha256=bundle.authority_sha256,
            model_unique_id=operation.model_unique_id,
            operation_id=operation.operation_id,
            operation_plan_sha256=operation.operation_plan_sha256,
            attempt_binding_sha256=attempt.attempt_binding_sha256,
            fencing_epoch=attempt.fencing_epoch,
            target_authority_id=resource.target_authority_id,
            database_name=resource.publication_database,
            target_table=resource.publication_target_table,
            target_generation=resource.target_predecessor_generation,
            target_generation_id=resource.target_predecessor_generation_id,
            target_uuid=resource.clickhouse_target_uuid,
            target_owner_operation_id=resource.target_predecessor_operation_id,
        )

    def _require_failed_execution(
        self,
        cursor: _Cursor,
        request: MssqlFailedScratchCleanupReadRequest,
    ) -> None:
        cursor.execute(
            f"""
SELECT canonical_authority_sha256, status, terminal_summary_sha256, terminal_summary_json
FROM {self._table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_id = ? AND workflow_execution_binding_sha256 = ?;
""".strip(),
            request.workflow_execution_id,
            request.workflow_execution_binding_sha256,
        )
        execution = _row(cursor)
        if (
            execution is None
            or execution[0] != request.canonical_authority_sha256
            or execution[1] != "FAILED_PRE_COMMIT"
            or _DIGEST.fullmatch(str(execution[2])) is None
            or not isinstance(execution[3], str)
            or not execution[3].strip()
        ):
            raise SemanticRefreshMssqlScratchCleanupReadError("cleanup execution is not exact FAILED_PRE_COMMIT")

    def _require_failed_journal(
        self,
        cursor: _Cursor,
        request: MssqlFailedScratchCleanupReadRequest,
    ) -> tuple[Any, ...]:
        cursor.execute(
            f"""
SELECT workflow_id, model_unique_id, operation_plan_sha256,
       attempt_binding_sha256, fencing_epoch, publication_database,
       publication_target_table, mssql_outcome, mssql_evidence_sha256,
       prepare_plan_sha256, prepare_plan_json, prepare_receipt_sha256,
       prepared_receipt_json, target_uuid, status
FROM {self._table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
WHERE operation_id = ?;
""".strip(),
            request.operation_id,
        )
        journal = _row(cursor)
        if (
            journal is None
            or journal[:7]
            != (
                request.workflow_execution_id,
                request.model_unique_id,
                request.operation_plan_sha256,
                request.attempt_binding_sha256,
                request.fencing_epoch,
                request.database_name,
                request.target_table,
            )
            or journal[7] not in _SAFE_MSSQL_OUTCOMES
            or _DIGEST.fullmatch(str(journal[8])) is None
            or (journal[13] is not None and _uuid(journal[13]) != request.target_uuid)
            or journal[14] != "FAILED_PRE_COMMIT"
        ):
            raise SemanticRefreshMssqlScratchCleanupReadError("failed cleanup journal conflict")
        documents = journal[9:13]
        if any(value is None for value in documents) and any(value is not None for value in documents):
            raise SemanticRefreshMssqlScratchCleanupReadError("failed cleanup PREPARE documents are partial")
        return journal

    def _require_unchanged_target(
        self,
        cursor: _Cursor,
        request: MssqlFailedScratchCleanupReadRequest,
    ) -> None:
        cursor.execute(
            f"""
SELECT target_generation, target_generation_id,
       LOWER(CONVERT(char(36), target_uuid)), operation_id
FROM {self._table("semantic_refresh_target_heads")} WITH (UPDLOCK, HOLDLOCK)
WHERE database_name = ? AND target_table = ?;
""".strip(),
            request.database_name,
            request.target_table,
        )
        if _row(cursor) != (
            request.target_generation,
            request.target_generation_id,
            request.target_uuid,
            request.target_owner_operation_id,
        ):
            raise SemanticRefreshMssqlScratchCleanupReadError("protected target changed before scratch cleanup")

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


def _row(cursor: _Cursor) -> tuple[Any, ...] | None:
    value = cursor.fetchone()
    return None if value is None else tuple(value)


def _optional_text(value: object) -> str | None:
    return None if value is None else str(value)


def _uuid(value: object) -> str:
    return str(UUID(str(value)))


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
    "MssqlSemanticRefreshFailedScratchCleanupReader",
    "SemanticRefreshMssqlScratchCleanupReadError",
]

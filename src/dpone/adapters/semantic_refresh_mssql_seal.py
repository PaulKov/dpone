"""Create-only MSSQL persistence and authenticated loading for seal authority."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, Protocol

from dpone.contracts.semantic_refresh_seal_authorization import (
    SemanticRefreshSealAuthorizationReceipt,
)


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


class SemanticRefreshMssqlSealAuthorizationError(RuntimeError):
    """Raised when durable seal authority is absent, invalid, or conflicting."""


class MssqlSemanticRefreshSealAuthorizationStore:
    """Persist and load immutable canonical seal-authorization receipts."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        if not control_schema.replace("_", "a").isalnum() or not control_schema[0].isalpha():
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._control_schema = control_schema
        self._table = f"[{control_schema}].[semantic_refresh_seal_authorizations]"

    def persist_exact(self, receipt: SemanticRefreshSealAuthorizationReceipt) -> None:
        """Create one authorization or acknowledge only an exact replay."""

        if not isinstance(receipt, SemanticRefreshSealAuthorizationReceipt):
            raise TypeError("receipt must be a canonical seal authorization")
        canonical_json = _canonical_json(receipt.to_dict())
        self._transaction(
            lambda cursor: self._persist(cursor, receipt, canonical_json),
            "seal authorization persistence failed",
        )

    def load(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> SemanticRefreshSealAuthorizationReceipt:
        """Load and recompute one immutable receipt by exact run/operation identity."""

        result: list[SemanticRefreshSealAuthorizationReceipt] = []

        def action(cursor: _Cursor) -> None:
            cursor.execute(
                f"""
SELECT seal_authorization_receipt_sha256, seal_authorization_receipt_json
FROM {self._table} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ? AND operation_id = ?;
""".strip(),
                workflow_execution_binding_sha256,
                operation_id,
            )
            row = cursor.fetchone()
            if row is None or cursor.fetchone() is not None:
                raise SemanticRefreshMssqlSealAuthorizationError("seal authorization is absent or ambiguous")
            try:
                receipt = SemanticRefreshSealAuthorizationReceipt.from_mapping(json.loads(str(row[1])))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise SemanticRefreshMssqlSealAuthorizationError("seal authorization document is invalid") from exc
            if (
                receipt.seal_authorization_receipt_sha256 != row[0]
                or receipt.workflow_execution_binding_sha256 != workflow_execution_binding_sha256
                or receipt.operation_id != operation_id
            ):
                raise SemanticRefreshMssqlSealAuthorizationError("seal authorization identity differs")
            result.append(receipt)

        self._transaction(action, "seal authorization load failed")
        return result[0]

    def _persist(
        self,
        cursor: _Cursor,
        receipt: SemanticRefreshSealAuthorizationReceipt,
        canonical_json: str,
    ) -> None:
        self._require_active_state(cursor, receipt)
        cursor.execute(
            f"""
SELECT workflow_execution_binding_sha256, attempt_binding_sha256,
       fencing_epoch, journal_version, seal_authorization_receipt_sha256,
       seal_authorization_receipt_json
FROM {self._table} WITH (UPDLOCK, HOLDLOCK)
WHERE operation_id = ?;
""".strip(),
            receipt.operation_id,
        )
        row = cursor.fetchone()
        expected = (
            receipt.workflow_execution_binding_sha256,
            receipt.attempt_binding_sha256,
            receipt.fencing_epoch,
            receipt.journal_version,
            receipt.seal_authorization_receipt_sha256,
            canonical_json,
        )
        if row is not None:
            if tuple(row) != expected:
                raise SemanticRefreshMssqlSealAuthorizationError("seal authorization replay differs")
            return
        cursor.execute(
            f"""
INSERT INTO {self._table} (
    operation_id, workflow_execution_binding_sha256, attempt_binding_sha256,
    fencing_epoch, journal_version, seal_authorization_receipt_sha256,
    seal_authorization_receipt_json, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
""".strip(),
            receipt.operation_id,
            receipt.workflow_execution_binding_sha256,
            receipt.attempt_binding_sha256,
            receipt.fencing_epoch,
            receipt.journal_version,
            receipt.seal_authorization_receipt_sha256,
            canonical_json,
            receipt.created_at,
        )

    def _require_active_state(
        self,
        cursor: _Cursor,
        receipt: SemanticRefreshSealAuthorizationReceipt,
    ) -> None:
        cursor.execute(
            f"""
SELECT execution.workflow_execution_id, execution.workflow_plan_sha256,
       execution.status, authority.status,
       journal.operation_plan_sha256, journal.attempt_binding_sha256,
       journal.fencing_epoch, journal.journal_version, journal.status,
       guard.fencing_epoch, guard.workflow_id, guard.operation_id,
       guard.operation_plan_sha256, guard.attempt_binding_sha256, guard.status
FROM {self._state_table("semantic_refresh_workflow_executions")} AS execution
     WITH (UPDLOCK, HOLDLOCK)
JOIN {self._state_table("semantic_refresh_canonical_authorities")} AS authority
     WITH (UPDLOCK, HOLDLOCK)
  ON authority.workflow_execution_binding_sha256 = execution.workflow_execution_binding_sha256
 AND authority.authority_sha256 = execution.canonical_authority_sha256
JOIN {self._state_table("semantic_refresh_journals")} AS journal
     WITH (UPDLOCK, HOLDLOCK)
  ON journal.workflow_id = execution.workflow_id
JOIN {self._state_table("semantic_refresh_guards")} AS guard
     WITH (UPDLOCK, HOLDLOCK)
  ON guard.resource_id = journal.target_resource_id
WHERE execution.workflow_execution_binding_sha256 = ?
  AND journal.operation_id = ?;
""".strip(),
            receipt.workflow_execution_binding_sha256,
            receipt.operation_id,
        )
        row = cursor.fetchone()
        if row is None or cursor.fetchone() is not None:
            raise SemanticRefreshMssqlSealAuthorizationError("active seal admission state is absent or ambiguous")
        expected = (
            receipt.workflow_execution_id,
            receipt.workflow_plan_sha256,
            "PREPARING",
            "ACTIVE",
            receipt.operation_plan_sha256,
            receipt.attempt_binding_sha256,
            receipt.fencing_epoch,
            receipt.journal_version,
            "PREPARING",
            receipt.fencing_epoch,
            receipt.workflow_execution_id,
            receipt.operation_id,
            receipt.operation_plan_sha256,
            receipt.attempt_binding_sha256,
            "HELD",
        )
        if tuple(row) != expected:
            raise SemanticRefreshMssqlSealAuthorizationError("active seal admission state differs")

    def _state_table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"

    def _transaction(self, action: Callable[[_Cursor], None], label: str) -> None:
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            action(cursor)
            connection.commit()
        except SemanticRefreshMssqlSealAuthorizationError:
            _rollback(connection)
            raise
        except Exception as exc:
            _rollback(connection)
            raise SemanticRefreshMssqlSealAuthorizationError(label) from exc
        finally:
            _close(cursor)
            _close(connection)


def _canonical_json(value: dict[str, object]) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


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
    "MssqlSemanticRefreshSealAuthorizationStore",
    "SemanticRefreshMssqlSealAuthorizationError",
]

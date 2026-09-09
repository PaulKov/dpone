"""Serializable MSSQL locator for worker-time run and attempt authority."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from dpone.adapters.semantic_refresh_mssql_run_binding import (
    MssqlWorkerRunBindingQueries,
    SemanticRefreshMssqlWorkerRunBindingAuthorityError,
)
from dpone.ports.semantic_refresh_mssql_run_authority import (
    MssqlWorkerAttemptAuthority,
    MssqlWorkerRunAuthority,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_ACTIVE_JOURNAL_STATES = {
    "COMMITTED_INCOMPLETE",
    "COMMITTING",
    "COMMIT_UNKNOWN",
    "COMPLETE",
    "PREPARED",
    "PREPARING",
    "TARGET_COMMITTED",
}


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


class SemanticRefreshMssqlWorkerRunAuthorityError(RuntimeError):
    """Raised when worker-time run authority is absent, ambiguous, or stale."""


class MssqlSemanticRefreshWorkerRunAuthority:
    """Locate one ACTIVE run and expose attempts only after atomic admission."""

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

    def locate(
        self,
        workflow_plan_sha256: str,
        workflow_execution_id: str,
    ) -> MssqlWorkerRunAuthority:
        """Return exact pack/binding authority and admitted attempt closure."""

        if _DIGEST.fullmatch(workflow_plan_sha256) is None:
            raise ValueError("workflow_plan_sha256 must be a lowercase sha256 digest")
        if not isinstance(workflow_execution_id, str) or not workflow_execution_id.strip():
            raise ValueError("workflow_execution_id must be non-empty text")
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            binding, bundle, request = self._binding_queries.load(
                cursor,
                workflow_plan_sha256,
                workflow_execution_id,
            )
            attempts = self._admitted_attempts(cursor, bundle, request)
            result = MssqlWorkerRunAuthority(
                record=binding.record,
                workflow_plan_sha256=binding.workflow_plan_sha256,
                pack_fingerprint=binding.pack_fingerprint,
                activation_authority_receipt_sha256=(binding.activation_authority_receipt_sha256),
                authority_store_ref=binding.authority_store_ref,
                plan_bundle_sha256=binding.plan_bundle_sha256,
                run_execution_bundle_sha256=binding.run_execution_bundle_sha256,
                run_guard_closure=binding.run_guard_closure,
                projection_identity=binding.projection_identity,
                admission_status="REGISTERED" if not attempts else "ADMITTED",
                attempts=attempts,
            )
            connection.commit()
            return result
        except SemanticRefreshMssqlWorkerRunAuthorityError:
            _rollback(connection)
            raise
        except SemanticRefreshMssqlWorkerRunBindingAuthorityError as exc:
            _rollback(connection)
            raise SemanticRefreshMssqlWorkerRunAuthorityError(str(exc)) from exc
        except Exception as exc:
            _rollback(connection)
            raise SemanticRefreshMssqlWorkerRunAuthorityError(
                "protected MSSQL worker run authority lookup failed"
            ) from exc
        finally:
            _close(cursor)
            _close(connection)

    def _admitted_attempts(self, cursor: _Cursor, bundle, request) -> tuple[MssqlWorkerAttemptAuthority, ...]:
        cursor.execute(
            f"""
SELECT workflow_plan_sha256, workflow_execution_binding_sha256,
       canonical_authority_sha256, guard_set_sha256, journal_set_sha256,
       workflow_guard_resource_id, guard_count, owner_id, status
FROM {self._table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_id = ?;
""".strip(),
            bundle.workflow_execution_id,
        )
        execution = _row(cursor)
        if execution is None:
            self._require_registered_without_partial_admission(cursor, request)
            return ()
        if execution != (
            bundle.workflow_plan.workflow_plan_sha256,
            bundle.execution_binding.workflow_execution_binding_sha256,
            bundle.authority_sha256,
            request.guard_set_sha256,
            request.journal_set_sha256,
            bundle.workflow_guard.resource_id,
            len(request.resource_guards) + 1,
            bundle.owner_id,
            "PREPARING",
        ):
            raise SemanticRefreshMssqlWorkerRunAuthorityError("worker execution authority is not active admission")
        self._reservation(cursor, request)
        self._guard_closure(cursor, request)
        return self._journal_attempts(cursor, bundle, request)

    def _require_registered_without_partial_admission(self, cursor: _Cursor, request) -> None:
        cursor.execute(
            f"""
SELECT COUNT_BIG(*)
FROM {self._table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_id = ?;
""".strip(),
            request.workflow_id,
        )
        if _row(cursor) != (0,):
            raise SemanticRefreshMssqlWorkerRunAuthorityError("registered worker run has partial journals")
        cursor.execute(
            f"""
SELECT reservation_id
FROM {self._table("semantic_refresh_reservations")} WITH (UPDLOCK, HOLDLOCK)
WHERE reservation_id = ? OR workflow_id = ?;
""".strip(),
            request.reservation_id,
            request.workflow_id,
        )
        if tuple(cursor.fetchall()):
            raise SemanticRefreshMssqlWorkerRunAuthorityError("registered worker run has a partial reservation")
        cursor.execute(
            f"""
SELECT COUNT_BIG(*)
FROM {self._table("semantic_refresh_guards")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_id = ?;
""".strip(),
            request.workflow_id,
        )
        if _row(cursor) != (0,):
            raise SemanticRefreshMssqlWorkerRunAuthorityError("registered worker run has partial held guards")

    def _reservation(self, cursor: _Cursor, request) -> None:
        cursor.execute(
            f"""
SELECT reservation_id, workflow_id, workflow_execution_binding_sha256, status,
       max_prepared_models, max_sealed_extract_bytes,
       max_clickhouse_staging_bytes, max_shadow_bytes, max_peak_bytes
FROM {self._table("semantic_refresh_reservations")} WITH (UPDLOCK, HOLDLOCK)
WHERE reservation_id = ? OR workflow_id = ?;
""".strip(),
            request.reservation_id,
            request.workflow_id,
        )
        budget = request.resource_budget
        expected = (
            request.reservation_id,
            request.workflow_id,
            request.workflow_execution_binding_sha256,
            "PREPARING",
            budget.max_workflow_prepared_models,
            budget.max_workflow_sealed_extract_bytes,
            budget.max_workflow_clickhouse_staging_bytes,
            budget.max_workflow_shadow_bytes,
            budget.max_workflow_peak_bytes,
        )
        if tuple(tuple(row) for row in cursor.fetchall()) != (expected,):
            raise SemanticRefreshMssqlWorkerRunAuthorityError("worker reservation authority differs")

    def _guard_closure(self, cursor: _Cursor, request) -> None:
        journals = {item.target_resource_id: item for item in request.journals}
        claims = (request.workflow_guard, *request.resource_guards)
        for claim in claims:
            journal = journals.get(claim.resource_id)
            cursor.execute(
                f"""
SELECT fencing_epoch, owner_id, workflow_id, operation_id,
       operation_plan_sha256, attempt_binding_sha256,
       strategy_authority_sha256, status
FROM {self._table("semantic_refresh_guards")} WITH (UPDLOCK, HOLDLOCK)
WHERE resource_id = ?;
""".strip(),
                claim.resource_id,
            )
            expected = (
                claim.fencing_epoch,
                request.owner_id,
                request.workflow_id,
                None if journal is None else journal.operation_id,
                None if journal is None else journal.operation_plan_sha256,
                None if journal is None else journal.attempt_binding_sha256,
                None if journal is None else journal.strategy_authority_sha256,
                "HELD",
            )
            if _row(cursor) != expected:
                raise SemanticRefreshMssqlWorkerRunAuthorityError("worker guard authority differs")
        cursor.execute(
            f"""
SELECT COUNT_BIG(*)
FROM {self._table("semantic_refresh_guards")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_id = ?;
""".strip(),
            request.workflow_id,
        )
        if _row(cursor) != (len(claims),):
            raise SemanticRefreshMssqlWorkerRunAuthorityError("worker guard set closure differs")

    def _journal_attempts(self, cursor: _Cursor, bundle, request) -> tuple[MssqlWorkerAttemptAuthority, ...]:
        cursor.execute(
            f"""
SELECT j.model_unique_id, j.operation_id, j.operation_plan_sha256,
       j.attempt_binding_sha256, j.fencing_epoch, j.owner_id, j.status,
       j.target_resource_id, j.strategy_authority_sha256
FROM {self._table("semantic_refresh_journals")} AS j WITH (UPDLOCK, HOLDLOCK)
WHERE j.workflow_id = ?
ORDER BY j.operation_id;
""".strip(),
            bundle.workflow_execution_id,
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        operations = {item.operation_id: item for item in bundle.operation_plans}
        attempts = {item.operation_id: item for item in bundle.attempt_bindings}
        resources = {item.model_unique_id: item for item in bundle.model_resources}
        journals = {item.operation_id: item for item in request.journals}
        if len(rows) != len(operations):
            raise SemanticRefreshMssqlWorkerRunAuthorityError("worker journal attempt closure differs")
        result: list[MssqlWorkerAttemptAuthority] = []
        for row in rows:
            operation = operations.get(str(row[1]))
            attempt = attempts.get(str(row[1]))
            journal = journals.get(str(row[1]))
            if operation is None or attempt is None or journal is None:
                raise SemanticRefreshMssqlWorkerRunAuthorityError("worker journal operation is unauthorized")
            resource = resources[operation.model_unique_id]
            expected = (
                operation.model_unique_id,
                operation.operation_id,
                operation.operation_plan_sha256,
                attempt.attempt_binding_sha256,
                attempt.fencing_epoch,
                bundle.owner_id,
                resource.target_resource_id,
                journal.strategy_authority_sha256,
            )
            if row[:6] != expected[:6] or row[6] not in _ACTIVE_JOURNAL_STATES or row[7:] != expected[6:]:
                raise SemanticRefreshMssqlWorkerRunAuthorityError("worker journal attempt authority differs")
            if resource.target_resource_id not in {item.resource_id for item in bundle.resource_guards}:
                raise SemanticRefreshMssqlWorkerRunAuthorityError("worker target guard is outside canonical closure")
            result.append(
                MssqlWorkerAttemptAuthority(
                    model_unique_id=operation.model_unique_id,
                    operation_id=operation.operation_id,
                    operation_plan_sha256=operation.operation_plan_sha256,
                    attempt_binding_sha256=attempt.attempt_binding_sha256,
                    fencing_epoch=attempt.fencing_epoch,
                    owner_id=bundle.owner_id,
                    task_id=attempt.task_id,
                    try_number=attempt.try_number,
                    pod_uid=attempt.pod_uid,
                    journal_status=str(row[6]),
                )
            )
        return tuple(sorted(result))

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
    "MssqlSemanticRefreshWorkerRunAuthority",
    "SemanticRefreshMssqlWorkerRunAuthorityError",
]

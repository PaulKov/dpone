"""Authority-bound MSSQL resource ledger for publication mutations."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import Any, Protocol, cast

from dpone.adapters.semantic_refresh_mssql_resources import (
    MssqlSemanticRefreshResourceLedger,
    SemanticRefreshResourceReservationError,
)
from dpone.ports.semantic_refresh_mssql_admission_authority import compose_admission
from dpone.ports.semantic_refresh_mssql_authority_codec import (
    authority_from_record,
    canonical_authority_record,
)
from dpone.ports.semantic_refresh_mssql_authority_records import (
    mssql_artifact_retention_authorizes,
)
from dpone.ports.semantic_refresh_mssql_resources import (
    MSSQL_RESOURCE_KINDS,
    MssqlProtectedResourceAllocation,
    MssqlProtectedResourceAllocationClosure,
    mssql_resource_allocation_id,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE_FAILED_OUTCOMES = {"COMMITTED_WITH_IMAGES", "NOT_INVOKED", "ROLLED_BACK"}


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


class SemanticRefreshMssqlProtectedResourceError(RuntimeError):
    """Raised when protected allocation authority or exact closure is unproven."""


class MssqlSemanticRefreshProtectedResourceLedger:
    """Derive and mutate the exact five allocations from locked canonical state."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
        clock: Callable[[], datetime],
    ) -> None:
        if not isinstance(control_schema, str) or _IDENTIFIER.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._control_schema = control_schema
        self._clock = clock
        self._ledger = MssqlSemanticRefreshResourceLedger(
            cast(Any, connection_factory),
            control_schema=control_schema,
        )

    def reserve_operation(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> None:
        """Reserve all canonical operation maxima before publication mutation."""

        self._mutate(
            workflow_execution_binding_sha256,
            operation_id,
            action="reserve",
        )

    def release_operation(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> None:
        """Release only after exact COMPLETE or safe FAILED_PRE_COMMIT terminal state."""

        self._mutate(
            workflow_execution_binding_sha256,
            operation_id,
            action="release",
        )

    def assert_released(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlProtectedResourceAllocationClosure:
        """Prove every canonical allocation exists exactly and is RELEASED."""

        return self._mutate(
            workflow_execution_binding_sha256,
            operation_id,
            action="assert_released",
        )

    def _mutate(self, binding_sha256: str, operation_id: str, *, action: str):
        _require_digest(binding_sha256, "workflow_execution_binding_sha256")
        _require_digest(operation_id, "operation_id")
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            bundle, request, resource = self._authority(cursor, binding_sha256, operation_id)
            if action == "reserve" and not mssql_artifact_retention_authorizes(
                resource.artifact_authority,
                as_of=self._clock(),
            ):
                raise SemanticRefreshMssqlProtectedResourceError(
                    "artifact retention authority is expired or not yet effective"
                )
            self._state(cursor, bundle, request, operation_id, action)
            closure = _allocation_closure(bundle, request.reservation_id, resource, operation_id, action)
            if action == "reserve":
                for item in closure.allocations:
                    self._ledger.reserve_in_transaction(
                        cast(Any, cursor),
                        allocation_id=item.allocation_id,
                        reservation_id=item.reservation_id,
                        resource_kind=item.resource_kind,
                        amount=item.amount,
                    )
            elif action == "release":
                for item in closure.allocations:
                    self._ledger.release_in_transaction(cast(Any, cursor), allocation_id=item.allocation_id)
            self._assert_rows(cursor, closure, bundle)
            connection.commit()
            return closure
        except (SemanticRefreshMssqlProtectedResourceError, SemanticRefreshResourceReservationError):
            _rollback(connection)
            raise
        except Exception as exc:
            _rollback(connection)
            raise SemanticRefreshMssqlProtectedResourceError("protected MSSQL resource allocation failed") from exc
        finally:
            _close(cursor)
            _close(connection)

    def _authority(self, cursor: _Cursor, binding_sha256: str, operation_id: str):
        cursor.execute(
            f"""
SELECT workflow_execution_id, authority_sha256, authority_json, status
FROM {self._table("semantic_refresh_canonical_authorities")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ?;
""".strip(),
            binding_sha256,
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        if len(rows) != 1 or rows[0][3] != "ACTIVE":
            raise SemanticRefreshMssqlProtectedResourceError("canonical resource authority is unavailable")
        record = canonical_authority_record(
            workflow_execution_binding_sha256=binding_sha256,
            workflow_execution_id=str(rows[0][0]),
            authority_sha256=str(rows[0][1]),
            authority_json=str(rows[0][2]),
            status=str(rows[0][3]),
        )
        bundle = authority_from_record(record)
        operation = next((item for item in bundle.operation_plans if item.operation_id == operation_id), None)
        if operation is None:
            raise SemanticRefreshMssqlProtectedResourceError("operation is absent from canonical authority")
        resource = next(item for item in bundle.model_resources if item.model_unique_id == operation.model_unique_id)
        return bundle, compose_admission(bundle), resource

    def _state(self, cursor: _Cursor, bundle, request, operation_id: str, action: str) -> None:
        cursor.execute(
            f"""
SELECT execution.status, journal.status, journal.operation_plan_sha256,
       journal.attempt_binding_sha256, journal.fencing_epoch,
       journal.owner_id, journal.terminal_receipt_sha256, journal.mssql_outcome,
       guard.fencing_epoch, guard.owner_id, guard.workflow_id,
       guard.operation_id, guard.operation_plan_sha256,
       guard.attempt_binding_sha256, guard.status,
       reservation.reservation_id, reservation.workflow_id,
       reservation.workflow_execution_binding_sha256, reservation.status,
       reservation.max_prepared_models, reservation.max_sealed_extract_bytes,
       reservation.max_clickhouse_staging_bytes, reservation.max_shadow_bytes,
       reservation.max_peak_bytes
FROM {self._table("semantic_refresh_workflow_executions")} AS execution WITH (UPDLOCK, HOLDLOCK)
JOIN {self._table("semantic_refresh_journals")} AS journal WITH (UPDLOCK, HOLDLOCK)
  ON journal.workflow_id = execution.workflow_id
JOIN {self._table("semantic_refresh_guards")} AS guard WITH (UPDLOCK, HOLDLOCK)
  ON guard.resource_id = journal.target_resource_id
JOIN {self._table("semantic_refresh_reservations")} AS reservation WITH (UPDLOCK, HOLDLOCK)
  ON reservation.workflow_id = execution.workflow_id
WHERE execution.workflow_id = ? AND journal.operation_id = ?;
""".strip(),
            bundle.workflow_execution_id,
            operation_id,
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        operation = next(item for item in bundle.operation_plans if item.operation_id == operation_id)
        attempt = next(item for item in bundle.attempt_bindings if item.operation_id == operation_id)
        budget = request.resource_budget
        if len(rows) != 1 or rows[0][2:6] != (
            operation.operation_plan_sha256,
            attempt.attempt_binding_sha256,
            attempt.fencing_epoch,
            bundle.owner_id,
        ):
            raise SemanticRefreshMssqlProtectedResourceError("resource journal identity differs")
        row = rows[0]
        execution_status, journal_status = row[0:2]
        terminal_receipt, outcome = row[6:8]
        if row[8:15] != (
            attempt.fencing_epoch,
            bundle.owner_id,
            bundle.workflow_execution_id,
            operation.operation_id,
            operation.operation_plan_sha256,
            attempt.attempt_binding_sha256,
            "HELD" if journal_status != "FAILED_PRE_COMMIT" else "RELEASED",
        ):
            raise SemanticRefreshMssqlProtectedResourceError("resource target guard identity differs")
        if row[15:] != (
            request.reservation_id,
            request.workflow_id,
            request.workflow_execution_binding_sha256,
            "FAILED_PRE_COMMIT" if journal_status == "FAILED_PRE_COMMIT" else "PREPARING",
            budget.max_workflow_prepared_models,
            budget.max_workflow_sealed_extract_bytes,
            budget.max_workflow_clickhouse_staging_bytes,
            budget.max_workflow_shadow_bytes,
            budget.max_workflow_peak_bytes,
        ):
            raise SemanticRefreshMssqlProtectedResourceError("workflow reservation authority differs")
        if action == "reserve":
            if execution_status != "PREPARING" or journal_status not in {"PREPARED", "PREPARING"}:
                raise SemanticRefreshMssqlProtectedResourceError("resource reservation is not pre-mutation")
        elif not (
            (
                journal_status == "COMPLETE"
                and execution_status in {"COMPLETE", "PREPARING"}
                and _is_digest(terminal_receipt)
            )
            or (
                journal_status == "FAILED_PRE_COMMIT"
                and execution_status == "FAILED_PRE_COMMIT"
                and outcome in _SAFE_FAILED_OUTCOMES
            )
        ):
            raise SemanticRefreshMssqlProtectedResourceError("resource release terminal authority is unsafe")
        if request.workflow_execution_binding_sha256 != bundle.execution_binding.workflow_execution_binding_sha256:
            raise SemanticRefreshMssqlProtectedResourceError("resource reservation binding differs")

    def _assert_rows(self, cursor: _Cursor, closure: MssqlProtectedResourceAllocationClosure, bundle) -> None:
        rows = self._ledger.allocations_for_reservation_in_transaction(
            cast(Any, cursor),
            closure.reservation_id,
        )
        row_by_id = {str(row[0]): row for row in rows}
        if len(row_by_id) != len(rows):
            raise SemanticRefreshMssqlProtectedResourceError("resource allocation inventory is ambiguous")
        expected = {
            (item.allocation_id, item.reservation_id, item.resource_kind, item.amount, item.status)
            for item in closure.allocations
        }
        if {row_by_id.get(item.allocation_id) for item in closure.allocations} != expected:
            raise SemanticRefreshMssqlProtectedResourceError(
                "canonical resource allocation closure is missing, extra, or conflicting"
            )
        resources = {item.model_unique_id: item for item in bundle.model_resources}
        allowed: dict[str, tuple[str, str, int]] = {}
        for operation in bundle.operation_plans:
            resource = resources[operation.model_unique_id]
            candidate = _allocation_closure(
                bundle,
                closure.reservation_id,
                resource,
                operation.operation_id,
                "reserve",
            )
            allowed.update(
                {
                    item.allocation_id: (item.reservation_id, item.resource_kind, item.amount)
                    for item in candidate.allocations
                }
            )
        if any(
            str(row[0]) not in allowed
            or tuple(row[1:4]) != allowed[str(row[0])]
            or row[4] not in {"RESERVED", "RELEASED"}
            for row in rows
        ):
            raise SemanticRefreshMssqlProtectedResourceError(
                "reservation contains allocation outside the canonical operation closure"
            )

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


def _allocation_closure(bundle, reservation_id: str, resource, operation_id: str, action: str):
    binding = bundle.execution_binding.workflow_execution_binding_sha256
    policy = resource.resource_policy
    amounts = {
        "clickhouse_staging_bytes": policy.max_clickhouse_staging_bytes,
        "prepared_models": 1,
        "retained_generation_bytes": policy.max_clickhouse_retained_backup_bytes,
        "sealed_extract_bytes": resource.artifact_authority.max_artifact_bytes,
        "shadow_bytes": policy.max_clickhouse_shadow_bytes,
    }
    status = "RESERVED" if action == "reserve" else "RELEASED"
    allocations = tuple(
        MssqlProtectedResourceAllocation(
            allocation_id=mssql_resource_allocation_id(reservation_id, operation_id, kind),
            reservation_id=reservation_id,
            workflow_execution_binding_sha256=binding,
            operation_id=operation_id,
            resource_kind=kind,
            amount=amounts[kind],
            status=status,
        )
        for kind in MSSQL_RESOURCE_KINDS
    )
    return MssqlProtectedResourceAllocationClosure(binding, operation_id, reservation_id, allocations)


def _require_digest(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase sha256 digest")


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and _DIGEST.fullmatch(value) is not None


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
    "MssqlSemanticRefreshProtectedResourceLedger",
    "SemanticRefreshMssqlProtectedResourceError",
]

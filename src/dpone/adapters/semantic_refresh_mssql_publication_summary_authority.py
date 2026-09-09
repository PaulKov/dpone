"""Authenticate workflow guard and resource closure for terminal publication."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.ports.semantic_refresh_clickhouse_resources import (
    SemanticRefreshPublicationResourceAllocation,
    semantic_refresh_publication_resource_allocations,
)
from dpone.ports.semantic_refresh_mssql_authority_codec import (
    authority_from_record,
    canonical_authority_record,
)
from dpone.ports.semantic_refresh_mssql_primitives import (
    MssqlWorkflowResourceBudget,
    mssql_guard_set_sha256,
)


class SemanticRefreshWorkflowSummaryError(RuntimeError):
    """Raised when a workflow summary cannot be proven and persisted."""


class _SummaryCursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _SummaryCursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...


@dataclass(frozen=True, slots=True)
class CanonicalWorkflowSummaryAuthority:
    """Authenticated guard and allocation closure for one workflow execution."""

    guard_resources: tuple[str, ...]
    workflow_id: str
    workflow_execution_binding_sha256: str
    reservation_id: str
    resource_budget: MssqlWorkflowResourceBudget
    allocations: tuple[SemanticRefreshPublicationResourceAllocation, ...]


def load_canonical_workflow_summary_authority(
    cursor: _SummaryCursor,
    *,
    table: Callable[[str], str],
    summary: Mapping[str, object],
    execution: tuple[Any, ...],
) -> CanonicalWorkflowSummaryAuthority:
    """Load and authenticate the locked canonical run authority."""

    cursor.execute(
        f"""
SELECT workflow_execution_binding_sha256, workflow_execution_id,
       authority_sha256, authority_json, status
FROM {table("semantic_refresh_canonical_authorities")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ?;
""".strip(),
        summary["workflow_execution_binding_sha256"],
    )
    row = cursor.fetchone()
    if row is None:
        raise SemanticRefreshWorkflowSummaryError("canonical workflow guard authority is absent")
    try:
        bundle = authority_from_record(
            canonical_authority_record(
                workflow_execution_binding_sha256=str(row[0]),
                workflow_execution_id=str(row[1]),
                authority_sha256=str(row[2]),
                authority_json=str(row[3]),
                status=str(row[4]),
            )
        )
    except (TypeError, ValueError) as exc:
        raise SemanticRefreshWorkflowSummaryError("canonical workflow guard authority is invalid") from exc
    guard_resources = tuple(
        sorted(
            (
                bundle.workflow_guard.resource_id,
                *(item.resource_id for item in bundle.resource_guards),
            )
        )
    )
    if (
        bundle.workflow_execution_id != summary["workflow_execution_id"]
        or bundle.workflow_plan.workflow_plan_sha256 != summary["workflow_plan_sha256"]
        or bundle.execution_binding.workflow_execution_binding_sha256 != summary["workflow_execution_binding_sha256"]
        or execution[6] != bundle.workflow_guard.resource_id
        or execution[7] != len(guard_resources)
        or execution[9] != bundle.authority_sha256
        or execution[10] != mssql_guard_set_sha256(bundle.workflow_guard, bundle.resource_guards)
    ):
        raise SemanticRefreshWorkflowSummaryError("canonical workflow guard closure differs")

    resources_by_model = {item.model_unique_id: item for item in bundle.model_resources}
    allocations: list[SemanticRefreshPublicationResourceAllocation] = []
    for operation in bundle.operation_plans:
        resource = resources_by_model[operation.model_unique_id]
        allocations.extend(
            semantic_refresh_publication_resource_allocations(
                reservation_id=bundle.reservation_id,
                operation_id=operation.operation_id,
                sealed_extract_bytes=resource.artifact_authority.max_artifact_bytes,
                clickhouse_staging_bytes=resource.resource_policy.max_clickhouse_staging_bytes,
                shadow_bytes=resource.resource_policy.max_clickhouse_shadow_bytes,
                retained_generation_bytes=resource.resource_policy.max_clickhouse_retained_backup_bytes,
            )
        )
    return CanonicalWorkflowSummaryAuthority(
        guard_resources=guard_resources,
        workflow_id=str(execution[0]),
        workflow_execution_binding_sha256=bundle.execution_binding.workflow_execution_binding_sha256,
        reservation_id=bundle.reservation_id,
        resource_budget=bundle.resource_budget,
        allocations=tuple(sorted(allocations)),
    )


def assert_workflow_resources_released(
    cursor: _SummaryCursor,
    *,
    table: Callable[[str], str],
    workflow_id: str,
    execution_status: object,
    authority: CanonicalWorkflowSummaryAuthority,
) -> None:
    """Require zero counters and the exact authority-derived released allocations."""

    cursor.execute(
        f"""
SELECT reservation_id, workflow_id, workflow_execution_binding_sha256,
       max_prepared_models, max_sealed_extract_bytes,
       max_clickhouse_staging_bytes, max_shadow_bytes, max_peak_bytes,
       reserved_prepared_models, reserved_sealed_extract_bytes,
       reserved_clickhouse_staging_bytes, reserved_shadow_bytes,
       reserved_retained_generation_bytes, reserved_peak_bytes, status
FROM {table("semantic_refresh_reservations")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_id = ?;
""".strip(),
        workflow_id,
    )
    reservation_row = cursor.fetchone()
    reservation = None if reservation_row is None else tuple(reservation_row)
    expected_status = "COMPLETE" if execution_status == "COMPLETE" else "PREPARING"
    budget = authority.resource_budget
    expected_reservation = (
        authority.reservation_id,
        authority.workflow_id,
        authority.workflow_execution_binding_sha256,
        budget.max_workflow_prepared_models,
        budget.max_workflow_sealed_extract_bytes,
        budget.max_workflow_clickhouse_staging_bytes,
        budget.max_workflow_shadow_bytes,
        budget.max_workflow_peak_bytes,
        0,
        0,
        0,
        0,
        0,
        0,
        expected_status,
    )
    if reservation != expected_reservation:
        raise SemanticRefreshWorkflowSummaryError("workflow resource reservation is not fully released")
    cursor.execute(
        f"""
SELECT allocation.allocation_id, allocation.reservation_id,
       allocation.resource_kind, allocation.amount, allocation.status
FROM {table("semantic_refresh_resource_allocations")} AS allocation WITH (UPDLOCK, HOLDLOCK)
JOIN {table("semantic_refresh_reservations")} AS reservation WITH (UPDLOCK, HOLDLOCK)
  ON reservation.reservation_id = allocation.reservation_id
WHERE reservation.workflow_id = ? ORDER BY allocation.allocation_id;
""".strip(),
        workflow_id,
    )
    observed = tuple(tuple(row) for row in cursor.fetchall())
    expected = tuple(
        (
            item.allocation_id,
            item.reservation_id,
            item.resource_kind,
            item.amount,
            "RELEASED",
        )
        for item in authority.allocations
    )
    if observed != expected:
        raise SemanticRefreshWorkflowSummaryError("workflow resource allocation closure differs")


__all__ = [
    "CanonicalWorkflowSummaryAuthority",
    "SemanticRefreshWorkflowSummaryError",
    "assert_workflow_resources_released",
    "load_canonical_workflow_summary_authority",
]

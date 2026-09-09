"""Protected per-operation resource-allocation authority."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Protocol

from dpone.ports.semantic_refresh_clickhouse_resources import (
    semantic_refresh_publication_resource_allocations,
)
from dpone.ports.semantic_refresh_mssql_primitives import _require_digest, _require_text

MSSQL_RESOURCE_KINDS = (
    "clickhouse_staging_bytes",
    "prepared_models",
    "retained_generation_bytes",
    "sealed_extract_bytes",
    "shadow_bytes",
)


@dataclass(frozen=True, order=True, slots=True)
class MssqlProtectedResourceAllocation:
    """One deterministic allocation derived from canonical operation policy."""

    allocation_id: str
    reservation_id: str
    workflow_execution_binding_sha256: str
    operation_id: str
    resource_kind: str
    amount: int
    status: str

    def __post_init__(self) -> None:
        _require_digest(self.allocation_id, "allocation_id")
        _require_text(self.reservation_id, "reservation_id")
        _require_digest(self.workflow_execution_binding_sha256, "workflow_execution_binding_sha256")
        _require_digest(self.operation_id, "operation_id")
        if self.resource_kind not in MSSQL_RESOURCE_KINDS:
            raise ValueError("resource_kind is unsupported")
        if isinstance(self.amount, bool) or not isinstance(self.amount, int) or self.amount <= 0:
            raise ValueError("resource amount must be positive")
        if self.status not in {"RESERVED", "RELEASED"}:
            raise ValueError("resource allocation status is unsupported")


@dataclass(frozen=True, slots=True)
class MssqlProtectedResourceAllocationClosure:
    """Exact five-allocation closure required for one planned operation."""

    workflow_execution_binding_sha256: str
    operation_id: str
    reservation_id: str
    allocations: tuple[MssqlProtectedResourceAllocation, ...]

    def __post_init__(self) -> None:
        _require_digest(self.workflow_execution_binding_sha256, "workflow_execution_binding_sha256")
        _require_digest(self.operation_id, "operation_id")
        _require_text(self.reservation_id, "reservation_id")
        if tuple(item.resource_kind for item in self.allocations) != MSSQL_RESOURCE_KINDS:
            raise ValueError("resource allocations must contain the exact canonical five-kind closure")
        if any(
            item.workflow_execution_binding_sha256 != self.workflow_execution_binding_sha256
            or item.operation_id != self.operation_id
            or item.reservation_id != self.reservation_id
            for item in self.allocations
        ):
            raise ValueError("resource allocation identity closure differs")

    def to_mapping(self) -> dict[str, object]:
        """Return the exact canonical allocation-history projection."""

        return {
            "allocations": [
                {
                    "allocation_id": item.allocation_id,
                    "amount": item.amount,
                    "operation_id": item.operation_id,
                    "reservation_id": item.reservation_id,
                    "resource_kind": item.resource_kind,
                    "status": item.status,
                    "workflow_execution_binding_sha256": item.workflow_execution_binding_sha256,
                }
                for item in self.allocations
            ],
            "operation_id": self.operation_id,
            "reservation_id": self.reservation_id,
            "schema": "dpone.semantic-refresh-mssql-resource-allocation-closure.v1",
            "workflow_execution_binding_sha256": self.workflow_execution_binding_sha256,
        }

    @property
    def resource_allocation_closure_sha256(self) -> str:
        """Digest the full exact five-row allocation history."""

        raw = json.dumps(
            self.to_mapping(),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return "sha256:" + hashlib.sha256(raw).hexdigest()


class SemanticRefreshMssqlReleasedResourceClosurePort(Protocol):
    """Prove the complete canonical resource history before workflow summary."""

    def assert_released(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlProtectedResourceAllocationClosure: ...


def mssql_resource_allocation_id(
    reservation_id: str,
    operation_id: str,
    resource_kind: str,
) -> str:
    """Return the publication port's canonical allocation identity."""

    _require_text(reservation_id, "reservation_id")
    _require_digest(operation_id, "operation_id")
    if resource_kind not in MSSQL_RESOURCE_KINDS:
        raise ValueError("resource_kind is unsupported")
    allocations = semantic_refresh_publication_resource_allocations(
        reservation_id=reservation_id,
        operation_id=operation_id,
        sealed_extract_bytes=1,
        clickhouse_staging_bytes=1,
        shadow_bytes=1,
        retained_generation_bytes=1,
    )
    return next(item.allocation_id for item in allocations if item.resource_kind == resource_kind)


__all__ = [
    "MSSQL_RESOURCE_KINDS",
    "MssqlProtectedResourceAllocation",
    "MssqlProtectedResourceAllocationClosure",
    "SemanticRefreshMssqlReleasedResourceClosurePort",
    "mssql_resource_allocation_id",
]

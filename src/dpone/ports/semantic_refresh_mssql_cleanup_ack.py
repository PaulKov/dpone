"""Durable FAILED_PRE_COMMIT cleanup acknowledgement contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Protocol

from dpone.ports.semantic_refresh_clickhouse_resources import (
    ClickHouseFailedScratchCleanupReceipt,
)
from dpone.ports.semantic_refresh_mssql_primitives import _require_digest, _require_text
from dpone.ports.semantic_refresh_mssql_resources import (
    MssqlProtectedResourceAllocationClosure,
)


@dataclass(frozen=True, slots=True)
class MssqlFailedPrecommitCleanupAck:
    """Create-only proof that scratch is absent and all five allocations are released."""

    scratch: ClickHouseFailedScratchCleanupReceipt
    resources: MssqlProtectedResourceAllocationClosure
    cleanup_receipt_sha256: str
    status: str = "COMPLETE"

    def __post_init__(self) -> None:
        if not isinstance(self.scratch, ClickHouseFailedScratchCleanupReceipt):
            raise TypeError("scratch must be a typed ClickHouse cleanup receipt")
        if not isinstance(self.resources, MssqlProtectedResourceAllocationClosure):
            raise TypeError("resources must be a typed MSSQL allocation closure")
        if (
            self.resources.workflow_execution_binding_sha256 != self.scratch.workflow_execution_binding_sha256
            or self.resources.operation_id != self.scratch.operation_id
            or any(item.status != "RELEASED" for item in self.resources.allocations)
        ):
            raise ValueError("cleanup acknowledgement identity or RELEASED closure differs")
        if self.status != "COMPLETE":
            raise ValueError("cleanup acknowledgement status must equal COMPLETE")
        _require_digest(self.cleanup_receipt_sha256, "cleanup_receipt_sha256")
        if self.cleanup_receipt_sha256 != _cleanup_receipt_sha256(self.scratch, self.resources):
            raise ValueError("cleanup acknowledgement digest differs")

    @classmethod
    def build(
        cls,
        *,
        scratch: ClickHouseFailedScratchCleanupReceipt,
        resources: MssqlProtectedResourceAllocationClosure,
    ) -> MssqlFailedPrecommitCleanupAck:
        """Build the sole canonical acknowledgement from both typed proofs."""

        return cls(
            scratch=scratch,
            resources=resources,
            cleanup_receipt_sha256=_cleanup_receipt_sha256(scratch, resources),
        )

    def to_mapping(self) -> dict[str, object]:
        """Return the closed canonical acknowledgement document."""

        return {
            **_cleanup_receipt_payload(self.scratch, self.resources),
            "cleanup_receipt_sha256": self.cleanup_receipt_sha256,
        }


class SemanticRefreshMssqlFailedPrecommitCleanupAckPort(Protocol):
    """Persist and reload exact failed cleanup proof create-only."""

    def persist_exact(
        self,
        *,
        scratch: ClickHouseFailedScratchCleanupReceipt,
        resources: MssqlProtectedResourceAllocationClosure,
    ) -> MssqlFailedPrecommitCleanupAck: ...

    def load_exact(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlFailedPrecommitCleanupAck: ...


def _cleanup_receipt_sha256(
    scratch: ClickHouseFailedScratchCleanupReceipt,
    resources: MssqlProtectedResourceAllocationClosure,
) -> str:
    payload = _cleanup_receipt_payload(scratch, resources)
    raw = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _cleanup_receipt_payload(
    scratch: ClickHouseFailedScratchCleanupReceipt,
    resources: MssqlProtectedResourceAllocationClosure,
) -> dict[str, object]:
    _require_text(scratch.workflow_execution_id, "workflow_execution_id")
    return {
        "resource_allocation_closure": resources.to_mapping(),
        "resource_allocation_closure_sha256": resources.resource_allocation_closure_sha256,
        "schema": "dpone.semantic-refresh-mssql-failed-precommit-cleanup-ack.v1",
        "scratch_absence_evidence": scratch.to_mapping(),
        "scratch_absence_evidence_sha256": scratch.scratch_absence_evidence_sha256,
        "status": "COMPLETE",
    }


__all__ = [
    "MssqlFailedPrecommitCleanupAck",
    "SemanticRefreshMssqlFailedPrecommitCleanupAckPort",
]

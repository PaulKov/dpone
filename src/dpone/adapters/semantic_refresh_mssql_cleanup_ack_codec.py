"""Canonical JSON codec for failed-precommit cleanup acknowledgements."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, cast


class SemanticRefreshMssqlCleanupAckError(RuntimeError):
    """Raised when failed cleanup acknowledgement is absent or conflicts."""


def ack_row(ack: Any) -> tuple[object, ...]:
    """Project one canonical acknowledgement persistence row."""

    return (
        ack.scratch.workflow_execution_id,
        ack.scratch.operation_plan_sha256,
        ack.scratch.attempt_binding_sha256,
        ack.scratch.fencing_epoch,
        ack.scratch.target_uuid,
        ack.scratch.scratch_absence_evidence_sha256,
        canonical_json(ack.scratch.to_mapping()),
        ack.resources.resource_allocation_closure_sha256,
        canonical_json(ack.resources.to_mapping()),
        ack.cleanup_receipt_sha256,
        ack.status,
    )


def scratch_from_json(
    raw: str,
    *,
    receipt_factory: Callable[..., Any],
    relation_factory: Callable[..., Any],
) -> Any:
    """Parse and authenticate one scratch absence receipt."""

    value = mapping(raw)
    relations = value.get("relations")
    if not isinstance(relations, list):
        raise SemanticRefreshMssqlCleanupAckError("scratch cleanup relations are invalid")
    receipt = receipt_factory(
        workflow_execution_id=str(value.get("workflow_execution_id")),
        workflow_execution_binding_sha256=str(value.get("workflow_execution_binding_sha256")),
        operation_id=str(value.get("operation_id")),
        operation_plan_sha256=str(value.get("operation_plan_sha256")),
        attempt_binding_sha256=str(value.get("attempt_binding_sha256")),
        fencing_epoch=cast(int, value.get("fencing_epoch")),
        target_uuid=str(value.get("target_uuid")),
        relations=tuple(
            relation_factory(
                kind=str(item.get("kind")),
                name=str(item.get("name")),
                expected_uuid=cast(str | None, item.get("expected_uuid")),
                observed_uuid=cast(None, item.get("observed_uuid")),
            )
            for item in relations
            if isinstance(item, dict)
        ),
    )
    if value != receipt.to_mapping():
        raise SemanticRefreshMssqlCleanupAckError("scratch cleanup receipt fields or digest differ")
    return receipt


def resources_from_json(
    raw: str,
    *,
    closure_factory: Callable[..., Any],
    allocation_factory: Callable[..., Any],
) -> Any:
    """Parse and authenticate one released resource closure."""

    value = mapping(raw)
    allocations = value.get("allocations")
    if not isinstance(allocations, list):
        raise SemanticRefreshMssqlCleanupAckError("resource allocation closure is invalid")
    binding = str(value.get("workflow_execution_binding_sha256"))
    operation_id = str(value.get("operation_id"))
    reservation_id = str(value.get("reservation_id"))
    closure = closure_factory(
        workflow_execution_binding_sha256=binding,
        operation_id=operation_id,
        reservation_id=reservation_id,
        allocations=tuple(
            allocation_factory(
                allocation_id=str(item.get("allocation_id")),
                reservation_id=str(item.get("reservation_id")),
                workflow_execution_binding_sha256=str(item.get("workflow_execution_binding_sha256")),
                operation_id=str(item.get("operation_id")),
                resource_kind=str(item.get("resource_kind")),
                amount=cast(int, item.get("amount")),
                status=str(item.get("status")),
            )
            for item in allocations
            if isinstance(item, dict)
        ),
    )
    if value != closure.to_mapping():
        raise SemanticRefreshMssqlCleanupAckError("resource allocation closure fields differ")
    return closure


def mapping(raw: str) -> dict[str, object]:
    """Decode one acknowledgement JSON object."""

    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SemanticRefreshMssqlCleanupAckError("cleanup acknowledgement JSON is invalid") from exc
    if not isinstance(value, dict):
        raise SemanticRefreshMssqlCleanupAckError("cleanup acknowledgement JSON must be an object")
    return cast(dict[str, object], value)


def canonical_json(value: object) -> str:
    """Encode canonical acknowledgement JSON."""

    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)

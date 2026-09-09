"""Closed contract for replayable deployment-cache retention receipts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.deployment_cache_retention_state import (
    canonical_digest,
    canonical_uuid4,
    retention_operation_id,
)

RETENTION_APPLY_RECEIPT_SCHEMA = "dpone.deployment-cache-retention-apply-receipt.v1"
RETENTION_APPLY_RECEIPT_SCHEMA_V2 = "dpone.deployment-cache-retention-apply-receipt.v2"
MAX_RETENTION_APPLY_RECEIPT_BYTES = 8 * 1024 * 1024
MAX_RETENTION_APPLY_RECEIPTS = 10_000
MAX_RETENTION_APPLY_ITEMS = 10_000


class DeploymentCacheRetentionReceiptError(ValueError):
    """One retention receipt violates its closed state contract."""


def build_retention_apply_receipt(
    *,
    operation_id: str,
    status: str,
    environment: str,
    promoted_by: str,
    current_deployment_id: str | None,
    reviewed_plan_sha256: str,
    activation_history_revision: str,
    items: Sequence[Mapping[str, object]],
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "schema": RETENTION_APPLY_RECEIPT_SCHEMA,
        "operation_id": operation_id,
        "status": status,
        "environment": environment,
        "promoted_by": promoted_by,
        "current_deployment_id": current_deployment_id,
        "reviewed_plan_sha256": reviewed_plan_sha256,
        "activation_history_revision": activation_history_revision,
        "items": [dict(item) for item in items],
    }
    return parse_retention_apply_receipt({**body, "receipt_revision": canonical_digest(body)})


def build_retention_apply_receipt_v2(
    *,
    operation_id: str,
    status: str,
    environment: str,
    promoted_by: str,
    current_deployment_id: str | None,
    reviewed_plan_sha256: str,
    review_id: str,
    reviewed_protected_deployment_ids: Sequence[str],
    reviewed_recovery_revision: str | None,
    activation_history_revision: str,
    items: Sequence[Mapping[str, object]],
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "schema": RETENTION_APPLY_RECEIPT_SCHEMA_V2,
        "operation_id": operation_id,
        "status": status,
        "environment": environment,
        "promoted_by": promoted_by,
        "current_deployment_id": current_deployment_id,
        "reviewed_plan_sha256": reviewed_plan_sha256,
        "review_id": review_id,
        "reviewed_protected_deployment_ids": sorted(reviewed_protected_deployment_ids),
        "reviewed_recovery_revision": reviewed_recovery_revision,
        "activation_history_revision": activation_history_revision,
        "items": [dict(item) for item in items],
    }
    return parse_retention_apply_receipt({**body, "receipt_revision": canonical_digest(body)})


def parse_retention_apply_receipt(value: Mapping[str, object]) -> dict[str, Any]:
    if value.get("schema") == RETENTION_APPLY_RECEIPT_SCHEMA_V2:
        return _parse_retention_apply_receipt_v2(value)
    fields = {
        "schema",
        "operation_id",
        "receipt_revision",
        "status",
        "environment",
        "promoted_by",
        "current_deployment_id",
        "reviewed_plan_sha256",
        "activation_history_revision",
        "items",
    }
    if set(value) != fields or value.get("schema") != RETENTION_APPLY_RECEIPT_SCHEMA:
        raise DeploymentCacheRetentionReceiptError("retention apply receipt fields are invalid")
    operation_id = _digest(value.get("operation_id"), "operation id")
    environment = _bounded_text(value.get("environment"), "environment", maximum=128)
    reviewed_plan_sha256 = _digest(value.get("reviewed_plan_sha256"), "reviewed plan sha256")
    if operation_id != retention_operation_id(
        environment=environment,
        reviewed_plan_sha256=reviewed_plan_sha256,
    ):
        raise DeploymentCacheRetentionReceiptError("retention operation identity does not match its inputs")
    status = _enum(value.get("status"), {"applying", "committed", "aborted"}, "receipt status")
    current = value.get("current_deployment_id")
    if current is not None:
        current = _digest(current, "current deployment id")
    raw_items = value.get("items")
    if not isinstance(raw_items, list) or not raw_items or len(raw_items) > MAX_RETENTION_APPLY_ITEMS:
        raise DeploymentCacheRetentionReceiptError("retention receipt items must be a bounded non-empty array")
    items = [_parse_item(item) for item in raw_items]
    _require_unique_items(items)
    if status in {"committed", "aborted"} and any(item["action"] == "pending" for item in items):
        raise DeploymentCacheRetentionReceiptError("terminal retention receipt contains pending items")
    body: dict[str, Any] = {
        "schema": RETENTION_APPLY_RECEIPT_SCHEMA,
        "operation_id": operation_id,
        "status": status,
        "environment": environment,
        "promoted_by": _bounded_text(value.get("promoted_by"), "promoted by", maximum=512),
        "current_deployment_id": current,
        "reviewed_plan_sha256": reviewed_plan_sha256,
        "activation_history_revision": _digest(
            value.get("activation_history_revision"),
            "activation history revision",
        ),
        "items": items,
    }
    revision = _digest(value.get("receipt_revision"), "receipt revision")
    if revision != canonical_digest(body):
        raise DeploymentCacheRetentionReceiptError("retention receipt revision does not match its body")
    return {**body, "receipt_revision": revision}


def _parse_retention_apply_receipt_v2(value: Mapping[str, object]) -> dict[str, Any]:
    fields = {
        "schema",
        "operation_id",
        "receipt_revision",
        "status",
        "environment",
        "promoted_by",
        "current_deployment_id",
        "reviewed_plan_sha256",
        "review_id",
        "reviewed_protected_deployment_ids",
        "reviewed_recovery_revision",
        "activation_history_revision",
        "items",
    }
    if set(value) != fields:
        raise DeploymentCacheRetentionReceiptError("retention apply receipt fields are invalid")
    environment = _bounded_text(value.get("environment"), "environment", maximum=128)
    reviewed_plan = _digest(value.get("reviewed_plan_sha256"), "reviewed plan sha256")
    review_id = _review_id(value.get("review_id"))
    operation_id = _digest(value.get("operation_id"), "operation id")
    if operation_id != retention_operation_id(
        environment=environment,
        reviewed_plan_sha256=reviewed_plan,
        review_id=review_id,
    ):
        raise DeploymentCacheRetentionReceiptError("retention operation identity does not match its inputs")
    protected_raw = value.get("reviewed_protected_deployment_ids")
    if not isinstance(protected_raw, list) or len(protected_raw) > MAX_RETENTION_APPLY_ITEMS:
        raise DeploymentCacheRetentionReceiptError("reviewed protected deployment ids are invalid")
    protected = tuple(_digest(item, "reviewed protected deployment id") for item in protected_raw)
    if tuple(sorted(set(protected))) != protected:
        raise DeploymentCacheRetentionReceiptError("reviewed protected deployment ids must be sorted and unique")
    recovery_revision = value.get("reviewed_recovery_revision")
    if recovery_revision is not None:
        recovery_revision = _digest(recovery_revision, "reviewed recovery revision")
    raw_items = value.get("items")
    if not isinstance(raw_items, list) or not raw_items or len(raw_items) > MAX_RETENTION_APPLY_ITEMS:
        raise DeploymentCacheRetentionReceiptError("retention receipt items must be a bounded non-empty array")
    items = [_parse_item(item) for item in raw_items]
    _require_unique_items(items)
    status = _enum(value.get("status"), {"applying", "committed", "aborted"}, "receipt status")
    if status in {"committed", "aborted"} and any(item["action"] == "pending" for item in items):
        raise DeploymentCacheRetentionReceiptError("terminal retention receipt contains pending items")
    current = value.get("current_deployment_id")
    if current is not None:
        current = _digest(current, "current deployment id")
    body: dict[str, Any] = {
        "schema": RETENTION_APPLY_RECEIPT_SCHEMA_V2,
        "operation_id": operation_id,
        "status": status,
        "environment": environment,
        "promoted_by": _bounded_text(value.get("promoted_by"), "promoted by", maximum=512),
        "current_deployment_id": current,
        "reviewed_plan_sha256": reviewed_plan,
        "review_id": review_id,
        "reviewed_protected_deployment_ids": list(protected),
        "reviewed_recovery_revision": recovery_revision,
        "activation_history_revision": _digest(value.get("activation_history_revision"), "activation history revision"),
        "items": items,
    }
    revision = _digest(value.get("receipt_revision"), "receipt revision")
    if revision != canonical_digest(body):
        raise DeploymentCacheRetentionReceiptError("retention receipt revision does not match its body")
    return {**body, "receipt_revision": revision}


def _parse_item(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise DeploymentCacheRetentionReceiptError("retention receipt item must be an object")
    required = {"deployment_id", "action", "reason", "path"}
    allowed = required | {"error_code"}
    if not required <= set(value) <= allowed:
        raise DeploymentCacheRetentionReceiptError("retention receipt item fields are invalid")
    deployment_id = value.get("deployment_id")
    if deployment_id is not None:
        deployment_id = _digest(deployment_id, "item deployment id")
    action = _enum(value.get("action"), {"pending", "deleted", "skipped"}, "item action")
    if action in {"pending", "deleted"} and deployment_id is None:
        raise DeploymentCacheRetentionReceiptError("destructive receipt item requires a deployment id")
    item: dict[str, Any] = {
        "deployment_id": deployment_id,
        "action": action,
        "reason": _bounded_text(value.get("reason"), "item reason", maximum=512),
        "path": _bounded_text(value.get("path"), "item path", maximum=4_096),
    }
    if "error_code" in value:
        item["error_code"] = _bounded_text(value.get("error_code"), "item error code", maximum=256)
    return item


def _require_unique_items(items: Sequence[Mapping[str, object]]) -> None:
    deployment_ids = tuple(str(item["deployment_id"]) for item in items if item["deployment_id"] is not None)
    if len(set(deployment_ids)) != len(deployment_ids):
        raise DeploymentCacheRetentionReceiptError("retention receipt deployment ids must be unique")


def _digest(value: object, label: str) -> str:
    if not is_canonical_sha256_digest(value):
        raise DeploymentCacheRetentionReceiptError(f"{label} must be a canonical sha256 digest")
    return str(value)


def _bounded_text(value: object, label: str, *, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise DeploymentCacheRetentionReceiptError(f"{label} must be bounded non-empty text")
    return value


def _review_id(value: object) -> str:
    try:
        return canonical_uuid4(value)
    except ValueError as exc:
        raise DeploymentCacheRetentionReceiptError("review id must be a canonical UUIDv4") from exc


def _enum(value: object, allowed: set[str], label: str) -> str:
    text = _bounded_text(value, label, maximum=128)
    if text not in allowed:
        raise DeploymentCacheRetentionReceiptError(f"{label} is invalid")
    return text


__all__ = [
    "MAX_RETENTION_APPLY_RECEIPT_BYTES",
    "MAX_RETENTION_APPLY_RECEIPTS",
    "RETENTION_APPLY_RECEIPT_SCHEMA",
    "RETENTION_APPLY_RECEIPT_SCHEMA_V2",
    "DeploymentCacheRetentionReceiptError",
    "build_retention_apply_receipt",
    "build_retention_apply_receipt_v2",
    "parse_retention_apply_receipt",
    "retention_operation_id",
]

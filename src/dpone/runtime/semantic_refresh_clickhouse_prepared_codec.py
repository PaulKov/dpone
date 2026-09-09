"""Canonical JSON codec for durable ClickHouse PREPARE task handoff."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.runtime.semantic_refresh_clickhouse_models import (
    ClickHousePreparedReceipt,
    ClickHousePreparePlan,
    GroupedMultisetConformance,
    ShadowEquation,
    semantic_refresh_fingerprint,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_clickhouse_prepared import DurableClickHousePreparedPublication


@dataclass(frozen=True, slots=True)
class LoadedClickHousePreparedPublication:
    """Authenticated typed PREPARE input reconstructed in a distinct COMMIT task."""

    plan: ClickHousePreparePlan
    receipt: ClickHousePreparedReceipt


def prepared_publication_documents(
    plan: ClickHousePreparePlan,
    receipt: ClickHousePreparedReceipt,
) -> dict[str, object]:
    """Return fields persisted create-once by the PREPARED transition."""

    plan_mapping = plan.to_mapping()
    receipt_mapping = _receipt_mapping(receipt)
    return {
        "prepare_plan_sha256": plan.sha256,
        "prepare_plan_json": _canonical_json(plan_mapping),
        "prepared_receipt_sha256": receipt.receipt_sha256,
        "prepared_receipt_json": _canonical_json(receipt_mapping),
        "artifact_manifest_key": plan.artifact_manifest_key,
        "artifact_manifest_version": plan.artifact_manifest_version,
        "artifact_manifest_sha256": plan.artifact_manifest_sha256,
    }


def load_prepared_publication(
    value: DurableClickHousePreparedPublication,
) -> LoadedClickHousePreparedPublication:
    """Parse closed JSON and recompute every plan, receipt, and manifest identity."""

    try:
        plan_raw = json.loads(value.prepare_plan_json)
        receipt_raw = json.loads(value.prepared_receipt_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("durable PREPARED JSON is invalid") from exc
    plan = _plan_from_mapping(plan_raw)
    receipt = _receipt_from_mapping(receipt_raw)
    if (
        plan.workflow_execution_binding_sha256 != value.workflow_execution_binding_sha256
        or plan.operation_id != value.operation_id
        or plan.sha256 != value.prepare_plan_sha256
        or receipt.operation_id != value.operation_id
        or receipt.prepare_plan_sha256 != plan.sha256
        or receipt.attempt_binding_sha256 != plan.attempt_binding_sha256
        or receipt.target_uuid != plan.expected_target_uuid
        or receipt.receipt_sha256 != value.prepared_receipt_sha256
        or (
            plan.artifact_manifest_key,
            plan.artifact_manifest_version,
            plan.artifact_manifest_sha256,
        )
        != (
            value.artifact_manifest_key,
            value.artifact_manifest_version,
            value.artifact_manifest_sha256,
        )
    ):
        raise ValueError("durable PREPARED identity differs")
    unsigned = {key: item for key, item in receipt_raw.items() if key != "receipt_sha256"}
    if semantic_refresh_fingerprint(unsigned) != receipt.receipt_sha256:
        raise ValueError("durable PREPARED receipt digest differs")
    if receipt.status != "PREPARED" or ((receipt.publication_mode == "EMPTY_SCOPE") != plan.is_empty_scope):
        raise ValueError("durable PREPARED mode differs from plan")
    if receipt.publication_mode not in {"EXCHANGE", "EMPTY_SCOPE"}:
        raise ValueError("durable PREPARED mode is unsupported")
    return LoadedClickHousePreparedPublication(plan, receipt)


def _plan_from_mapping(value: Any) -> ClickHousePreparePlan:
    if not isinstance(value, dict) or set(value) != set(ClickHousePreparePlan.__dataclass_fields__):
        raise ValueError("durable PREPARE plan fields are not closed")
    raw = dict(value)
    equation = raw.pop("shadow_equation")
    conformance = raw.pop("conformance")
    if not isinstance(equation, dict) or set(equation) != {"retained_target_rule", "append_rule"}:
        raise ValueError("durable PREPARE shadow equation is invalid")
    if not isinstance(conformance, dict) or set(conformance) != {"mode"}:
        raise ValueError("durable PREPARE conformance is invalid")
    for field_name in ("business_columns", "effective_key_columns"):
        if not isinstance(raw[field_name], list):
            raise ValueError(f"durable PREPARE {field_name} is invalid")
        raw[field_name] = tuple(raw[field_name])
    return ClickHousePreparePlan(
        **raw,
        shadow_equation=ShadowEquation(**equation),
        conformance=GroupedMultisetConformance(**conformance),
    )


def _receipt_from_mapping(value: Any) -> ClickHousePreparedReceipt:
    if not isinstance(value, dict) or set(value) != set(ClickHousePreparedReceipt.__dataclass_fields__):
        raise ValueError("durable PREPARED receipt fields are not closed")
    return ClickHousePreparedReceipt(**value)


def _receipt_mapping(receipt: ClickHousePreparedReceipt) -> dict[str, object]:
    return {field_name: getattr(receipt, field_name) for field_name in receipt.__dataclass_fields__}


def _canonical_json(value: object) -> str:
    return json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


__all__ = [
    "LoadedClickHousePreparedPublication",
    "load_prepared_publication",
    "prepared_publication_documents",
]

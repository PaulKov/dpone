"""Authenticate create-once ClickHouse PREPARE documents for scratch cleanup."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from uuid import UUID

_PLAN_FIELDS = {
    "artifact_chunk_count",
    "artifact_manifest_key",
    "artifact_manifest_sha256",
    "artifact_manifest_version",
    "artifact_total_rows",
    "attempt_binding_sha256",
    "business_columns",
    "clickhouse_cluster_authority_id",
    "conformance",
    "database",
    "database_engine",
    "effective_key_columns",
    "event_time_column",
    "expected_physical_sha256",
    "expected_schema_sha256",
    "expected_target_uuid",
    "fence_epoch",
    "max_retained_backup_bytes",
    "max_shadow_bytes",
    "max_staging_bytes",
    "max_staging_rows",
    "max_target_scope_rows",
    "max_total_transient_bytes",
    "operation_id",
    "operation_plan_sha256",
    "replica_count",
    "scope_end",
    "scope_id",
    "scope_revision",
    "scope_start",
    "shadow_equation",
    "shadow_table",
    "shard_count",
    "staging_table",
    "table_engine",
    "target_authority_id",
    "target_resource_id",
    "target_table",
    "workflow_execution_binding_sha256",
    "workflow_execution_id",
    "workflow_plan_sha256",
}
_RECEIPT_FIELDS = {
    "attempt_binding_sha256",
    "conformance_mode",
    "desired_rows",
    "forward_difference_groups",
    "operation_id",
    "prepare_plan_sha256",
    "publication_mode",
    "receipt_sha256",
    "reverse_difference_groups",
    "shadow_equation",
    "shadow_rows",
    "shadow_uuid",
    "staging_rows",
    "staging_uuid",
    "status",
    "target_uuid",
}


@dataclass(frozen=True, slots=True)
class ScratchDocumentExpectation:
    """Protected identity that persisted PREPARE documents must reproduce."""

    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    operation_id: str
    operation_plan_sha256: str
    attempt_binding_sha256: str
    fencing_epoch: int
    target_authority_id: str
    database_name: str
    target_table: str
    target_uuid: str
    staging_table: str
    shadow_table: str


def authenticate_scratch_documents(
    *,
    expectation: ScratchDocumentExpectation,
    prepare_plan_sha256: str | None,
    prepare_plan_json: str | None,
    prepared_receipt_sha256: str | None,
    prepared_receipt_json: str | None,
) -> tuple[str | None, str | None]:
    """Return authenticated staging/shadow UUIDs, or an exact absent closure."""

    documents = (
        prepare_plan_sha256,
        prepare_plan_json,
        prepared_receipt_sha256,
        prepared_receipt_json,
    )
    if all(value is None for value in documents):
        return None, None
    if any(value is None for value in documents):
        raise ValueError("failed cleanup PREPARE documents are partial")
    assert prepare_plan_sha256 is not None
    assert prepare_plan_json is not None
    assert prepared_receipt_sha256 is not None
    assert prepared_receipt_json is not None
    plan = _mapping(prepare_plan_json, _PLAN_FIELDS, "PREPARE plan")
    receipt = _mapping(prepared_receipt_json, _RECEIPT_FIELDS, "PREPARE receipt")
    if _sha256(plan) != prepare_plan_sha256:
        raise ValueError("failed cleanup PREPARE plan digest differs")
    expected_plan = {
        "workflow_execution_id": expectation.workflow_execution_id,
        "workflow_execution_binding_sha256": expectation.workflow_execution_binding_sha256,
        "operation_id": expectation.operation_id,
        "operation_plan_sha256": expectation.operation_plan_sha256,
        "attempt_binding_sha256": expectation.attempt_binding_sha256,
        "fence_epoch": expectation.fencing_epoch,
        "target_authority_id": expectation.target_authority_id,
        "database": expectation.database_name,
        "target_table": expectation.target_table,
        "expected_target_uuid": expectation.target_uuid,
        "staging_table": expectation.staging_table,
        "shadow_table": expectation.shadow_table,
    }
    if any(plan.get(field_name) != value for field_name, value in expected_plan.items()):
        raise ValueError("failed cleanup PREPARE plan identity differs")
    unsigned_receipt = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    if (
        receipt["receipt_sha256"] != prepared_receipt_sha256
        or _sha256(unsigned_receipt) != prepared_receipt_sha256
        or receipt["operation_id"] != expectation.operation_id
        or receipt["attempt_binding_sha256"] != expectation.attempt_binding_sha256
        or receipt["prepare_plan_sha256"] != prepare_plan_sha256
        or receipt["target_uuid"] != expectation.target_uuid
        or receipt["status"] != "PREPARED"
    ):
        raise ValueError("failed cleanup PREPARE receipt identity differs")
    staging_uuid = _optional_uuid(receipt["staging_uuid"])
    shadow_uuid = _optional_uuid(receipt["shadow_uuid"])
    if receipt["publication_mode"] == "EXCHANGE":
        if staging_uuid is None or shadow_uuid is None:
            raise ValueError("failed cleanup EXCHANGE scratch UUIDs are absent")
    elif receipt["publication_mode"] == "EMPTY_SCOPE":
        if staging_uuid is not None or shadow_uuid is not None:
            raise ValueError("failed cleanup EMPTY_SCOPE scratch UUIDs must be absent")
    else:
        raise ValueError("failed cleanup PREPARE publication mode is unsupported")
    return staging_uuid, shadow_uuid


def _mapping(raw: str, fields: set[str], label: str) -> Mapping[str, object]:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"failed cleanup {label} JSON is invalid") from exc
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError(f"failed cleanup {label} fields are not closed")
    return value


def _sha256(value: Mapping[str, object]) -> str:
    raw = json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _optional_uuid(value: object) -> str | None:
    if value is None:
        return None
    try:
        parsed = UUID(str(value))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("failed cleanup scratch UUID is invalid") from exc
    canonical = str(parsed)
    if canonical != value:
        raise ValueError("failed cleanup scratch UUID is not canonical")
    return canonical


__all__ = ["ScratchDocumentExpectation", "authenticate_scratch_documents"]

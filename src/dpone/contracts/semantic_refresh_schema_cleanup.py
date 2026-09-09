"""Deterministic schemas for durable semantic-refresh cleanup evidence."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.semantic_refresh_schema_common import (
    DIGEST_SCHEMA,
    POSITIVE_INTEGER_SCHEMA,
    TEXT_SCHEMA,
    closed_schema,
    schema_discriminator,
)

CLICKHOUSE_FAILED_SCRATCH_CLEANUP_RECEIPT_SCHEMA = "dpone.semantic-refresh-clickhouse-failed-scratch-cleanup-receipt.v1"
MSSQL_FAILED_PRECOMMIT_CLEANUP_ACK_SCHEMA = "dpone.semantic-refresh-mssql-failed-precommit-cleanup-ack.v1"
MSSQL_RESOURCE_ALLOCATION_CLOSURE_SCHEMA = "dpone.semantic-refresh-mssql-resource-allocation-closure.v1"

_MSSQL_RESOURCE_KINDS = (
    "clickhouse_staging_bytes",
    "prepared_models",
    "retained_generation_bytes",
    "sealed_extract_bytes",
    "shadow_bytes",
)
_UUID: dict[str, object] = {
    "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    "type": "string",
}


def cleanup_contract_schemas() -> dict[str, dict[str, Any]]:
    """Return fresh closed schemas for both durable cleanup documents."""

    return {
        CLICKHOUSE_FAILED_SCRATCH_CLEANUP_RECEIPT_SCHEMA: _scratch_cleanup_receipt_schema(),
        MSSQL_FAILED_PRECOMMIT_CLEANUP_ACK_SCHEMA: _mssql_cleanup_ack_schema(),
    }


def _closed_object(properties: Mapping[str, object]) -> dict[str, object]:
    return {
        "additionalProperties": False,
        "properties": dict(properties),
        "required": sorted(properties),
        "type": "object",
    }


def _scratch_relation_schema(kind: str) -> dict[str, object]:
    return _closed_object(
        {
            "expected_uuid": {"anyOf": [_UUID, {"type": "null"}]},
            "kind": {"const": kind},
            "name": TEXT_SCHEMA,
            "observed_uuid": {"const": None},
        }
    )


def _scratch_cleanup_receipt_properties() -> dict[str, object]:
    return {
        "attempt_binding_sha256": DIGEST_SCHEMA,
        "fencing_epoch": POSITIVE_INTEGER_SCHEMA,
        "operation_id": DIGEST_SCHEMA,
        "operation_plan_sha256": DIGEST_SCHEMA,
        "relations": {
            "items": False,
            "maxItems": 2,
            "minItems": 2,
            "prefixItems": [
                _scratch_relation_schema("shadow"),
                _scratch_relation_schema("staging"),
            ],
            "type": "array",
        },
        "schema": schema_discriminator(CLICKHOUSE_FAILED_SCRATCH_CLEANUP_RECEIPT_SCHEMA),
        "scratch_absence_evidence_sha256": DIGEST_SCHEMA,
        "target_uuid": _UUID,
        "workflow_execution_binding_sha256": DIGEST_SCHEMA,
        "workflow_execution_id": TEXT_SCHEMA,
    }


def _scratch_cleanup_receipt_schema() -> dict[str, Any]:
    properties = _scratch_cleanup_receipt_properties()
    return closed_schema(
        CLICKHOUSE_FAILED_SCRATCH_CLEANUP_RECEIPT_SCHEMA,
        properties,
        sorted(properties),
        comment=(
            "The digest is recomputed from every field except scratch_absence_evidence_sha256. "
            "The typed producer requires the exact ordered shadow/staging absence closure."
        ),
    )


def _released_allocation_schema(resource_kind: str) -> dict[str, object]:
    return _closed_object(
        {
            "allocation_id": DIGEST_SCHEMA,
            "amount": POSITIVE_INTEGER_SCHEMA,
            "operation_id": DIGEST_SCHEMA,
            "reservation_id": TEXT_SCHEMA,
            "resource_kind": {"const": resource_kind},
            "status": {"const": "RELEASED"},
            "workflow_execution_binding_sha256": DIGEST_SCHEMA,
        }
    )


def _released_resource_closure_schema() -> dict[str, object]:
    properties: dict[str, object] = {
        "allocations": {
            "items": False,
            "maxItems": len(_MSSQL_RESOURCE_KINDS),
            "minItems": len(_MSSQL_RESOURCE_KINDS),
            "prefixItems": [_released_allocation_schema(kind) for kind in _MSSQL_RESOURCE_KINDS],
            "type": "array",
        },
        "operation_id": DIGEST_SCHEMA,
        "reservation_id": TEXT_SCHEMA,
        "schema": schema_discriminator(MSSQL_RESOURCE_ALLOCATION_CLOSURE_SCHEMA),
        "workflow_execution_binding_sha256": DIGEST_SCHEMA,
    }
    return _closed_object(properties)


def _mssql_cleanup_ack_schema() -> dict[str, Any]:
    properties: dict[str, object] = {
        "cleanup_receipt_sha256": DIGEST_SCHEMA,
        "resource_allocation_closure": _released_resource_closure_schema(),
        "resource_allocation_closure_sha256": DIGEST_SCHEMA,
        "schema": schema_discriminator(MSSQL_FAILED_PRECOMMIT_CLEANUP_ACK_SCHEMA),
        "scratch_absence_evidence": _closed_object(_scratch_cleanup_receipt_properties()),
        "scratch_absence_evidence_sha256": DIGEST_SCHEMA,
        "status": {"const": "COMPLETE"},
    }
    return closed_schema(
        MSSQL_FAILED_PRECOMMIT_CLEANUP_ACK_SCHEMA,
        properties,
        sorted(properties),
        comment=(
            "The acknowledgement digest excludes only cleanup_receipt_sha256. The typed producer verifies exact "
            "scratch/resource identity correlation, both nested digests, and the complete RELEASED allocation closure."
        ),
    )


__all__ = [
    "CLICKHOUSE_FAILED_SCRATCH_CLEANUP_RECEIPT_SCHEMA",
    "MSSQL_FAILED_PRECOMMIT_CLEANUP_ACK_SCHEMA",
    "MSSQL_RESOURCE_ALLOCATION_CLOSURE_SCHEMA",
    "cleanup_contract_schemas",
]

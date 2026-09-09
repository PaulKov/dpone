"""JSON Schema projections for canonical deployment-cache retention state."""

from __future__ import annotations

from typing import Any

from dpone.contracts.deployment_cache_retention_state import (
    ACTIVATION_HISTORY_SCHEMA,
    MAX_ACTIVATION_HISTORY_ENTRIES,
    MAX_CONTROL_PATH,
    MAX_DAG_IDS,
    MAX_RECOVERY_TRANSACTIONS,
    RETENTION_RECOVERY_SCHEMA,
    RETENTION_RECOVERY_SCHEMA_V1,
    RETENTION_TRANSACTION_PHASES,
)

_DIGEST_PATTERN = "^sha256:[0-9a-f]{64}$"
_UUID4_PATTERN = "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"


def activation_history_schema_properties() -> dict[str, Any]:
    return {
        "schema": {"const": ACTIVATION_HISTORY_SCHEMA},
        "revision": _digest_schema(),
        "entries": {
            "type": "object",
            "maxProperties": MAX_ACTIVATION_HISTORY_ENTRIES,
            "propertyNames": _uuid4_schema(),
            "additionalProperties": _activation_entry_schema(),
        },
        "legacy_v1_diagnostics": {
            "type": "array",
            "maxItems": MAX_ACTIVATION_HISTORY_ENTRIES,
            "items": {
                "type": "object",
                "required": ["entry_sha256"],
                "additionalProperties": False,
                "properties": {"entry_sha256": _digest_schema()},
            },
        },
    }


def retention_recovery_schema_properties(*, legacy_v1: bool = False) -> dict[str, Any]:
    return {
        "schema": {"const": RETENTION_RECOVERY_SCHEMA_V1 if legacy_v1 else RETENTION_RECOVERY_SCHEMA},
        "revision": _digest_schema(),
        "status": {"enum": ["recovering", "blocked", "recovered"]},
        "restored_deployment_ids": _deployment_ids_schema(),
        "pending_deployment_ids": _deployment_ids_schema(),
        "quarantined_paths": {
            "type": "array",
            "maxItems": MAX_RECOVERY_TRANSACTIONS,
            "uniqueItems": True,
            "items": {"type": "string", "minLength": 1, "maxLength": MAX_CONTROL_PATH},
        },
        "transactions": {
            "type": "object",
            "maxProperties": MAX_RECOVERY_TRANSACTIONS,
            "propertyNames": _digest_schema(),
            "additionalProperties": _retention_transaction_schema(legacy_v1=legacy_v1),
        },
    }


def _activation_entry_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "activation_id",
            "release_id",
            "deployment_id",
            "airflow_index_sha256",
            "loaded_dag_ids",
            "verified_at",
        ],
        "additionalProperties": False,
        "properties": {
            "activation_id": _uuid4_schema(),
            "release_id": _digest_schema(),
            "deployment_id": _digest_schema(),
            "airflow_index_sha256": _digest_schema(),
            "loaded_dag_ids": {
                "type": "array",
                "minItems": 1,
                "maxItems": MAX_DAG_IDS,
                "uniqueItems": True,
                "items": {"type": "string", "minLength": 1, "maxLength": 250},
            },
            "verified_at": {"type": "string", "format": "date-time"},
        },
    }


def _retention_transaction_schema(*, legacy_v1: bool) -> dict[str, Any]:
    required = [
        "deployment_id",
        "environment",
        "phase",
        "original_path",
        "detached_path",
        "activation_path",
        "expected_device",
        "expected_inode",
    ]
    properties = {
        "deployment_id": _digest_schema(),
        "environment": {"type": "string", "minLength": 1, "maxLength": 128},
        "phase": {"enum": sorted(RETENTION_TRANSACTION_PHASES)},
        "original_path": {"type": "string", "minLength": 1, "maxLength": MAX_CONTROL_PATH},
        "detached_path": {"type": "string", "minLength": 1, "maxLength": MAX_CONTROL_PATH},
        "activation_path": {"type": "string", "minLength": 1, "maxLength": MAX_CONTROL_PATH},
        "expected_device": {"type": "integer", "minimum": 0},
        "expected_inode": {"type": "integer", "minimum": 1},
    }
    if not legacy_v1:
        required.extend(["operation_id", "transaction_id"])
        properties["operation_id"] = {"oneOf": [_digest_schema(), {"type": "null"}]}
        properties["transaction_id"] = _digest_schema()
    return {
        "type": "object",
        "required": required,
        "additionalProperties": False,
        "properties": properties,
    }


def _deployment_ids_schema() -> dict[str, Any]:
    return {
        "type": "array",
        "maxItems": MAX_RECOVERY_TRANSACTIONS,
        "uniqueItems": True,
        "items": _digest_schema(),
    }


def _digest_schema() -> dict[str, object]:
    return {"type": "string", "pattern": _DIGEST_PATTERN}


def _uuid4_schema() -> dict[str, object]:
    return {"type": "string", "format": "uuid", "pattern": _UUID4_PATTERN}


__all__ = ["activation_history_schema_properties", "retention_recovery_schema_properties"]

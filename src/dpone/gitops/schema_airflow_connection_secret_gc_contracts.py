"""Public evidence contracts for Airflow Connection Secret garbage collection."""

from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract


def airflow_connection_secret_gc_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        airflow_connection_secret_gc_plan_contract(),
        airflow_connection_secret_gc_apply_contract(),
    )


def airflow_connection_secret_gc_plan_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-connection-secret-gc-plan",
        kind="dpone.airflow-connection-secret-gc-plan.v1",
        title="dpone GitOps Airflow Connection Secret GC plan",
        required=(
            "schema",
            "status",
            "namespace",
            "observed_at",
            "minimum_age_seconds",
            "page_size",
            "inventory",
            "items",
            "delete_candidates",
        ),
        properties={
            "schema": {"const": "dpone.airflow-connection-secret-gc-plan.v1"},
            "status": {"enum": ["ok", "needs_cleanup", "needs_attention"]},
            "namespace": _non_empty_string(),
            "observed_at": _date_time(),
            "minimum_age_seconds": {"type": "integer", "minimum": 300, "maximum": 2_592_000},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 1_000},
            "inventory": {"$ref": "#/$defs/inventory"},
            "items": {"type": "array", "items": {"$ref": "#/$defs/plan_item"}},
            "delete_candidates": _sha256_array(),
        },
        defs={
            "sha256": _sha256(),
            "inventory": _inventory(),
            "plan_item": _plan_item(),
        },
        additional_properties=False,
    )


def airflow_connection_secret_gc_apply_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-connection-secret-gc-apply",
        kind="dpone.airflow-connection-secret-gc-apply.v1",
        title="dpone GitOps Airflow Connection Secret GC apply report",
        required=(
            "schema",
            "status",
            "namespace",
            "actor",
            "observed_at",
            "minimum_age_seconds",
            "max_delete_count",
            "items",
            "deleted_secret_refs",
            "skipped_secret_refs",
            "failed_secret_refs",
        ),
        properties={
            "schema": {"const": "dpone.airflow-connection-secret-gc-apply.v1"},
            "status": {"enum": ["ok", "partial", "failed"]},
            "namespace": _non_empty_string(),
            "actor": _non_empty_string(),
            "observed_at": _date_time(),
            "minimum_age_seconds": {"type": "integer", "minimum": 300, "maximum": 2_592_000},
            "max_delete_count": {"type": "integer", "minimum": 1, "maximum": 1_000},
            "items": {"type": "array", "items": {"$ref": "#/$defs/apply_item"}},
            "deleted_secret_refs": _sha256_array(),
            "skipped_secret_refs": _sha256_array(),
            "failed_secret_refs": _sha256_array(),
        },
        defs={
            "sha256": _sha256(),
            "apply_item": _apply_item(),
        },
        additional_properties=False,
    )


def _inventory() -> dict[str, Any]:
    fields = (
        "managed_secrets",
        "managed_pods",
        "active_references",
        "orphan_pod_references",
        "quarantined",
    )
    return {
        "type": "object",
        "required": list(fields),
        "additionalProperties": False,
        "properties": {field: {"type": "integer", "minimum": 0} for field in fields},
    }


def _plan_item() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["secret_ref", "attempt_ref", "action", "reason", "cleanup_policy", "age_seconds"],
        "additionalProperties": False,
        "properties": {
            "secret_ref": {"$ref": "#/$defs/sha256"},
            "attempt_ref": _nullable_sha256(),
            "action": {"enum": ["protect", "delete", "quarantine"]},
            "reason": {
                "enum": [
                    "active_pod",
                    "minimum_age",
                    "retained_expired",
                    "synchronous_cleanup_orphaned",
                    "invalid_metadata",
                    "future_timestamp",
                ]
            },
            "cleanup_policy": {"type": ["string", "null"], "enum": ["retain", "after_execute", None]},
            "age_seconds": {"type": ["integer", "null"]},
        },
    }


def _apply_item() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["secret_ref", "attempt_ref", "action", "reason"],
        "additionalProperties": False,
        "properties": {
            "secret_ref": {"$ref": "#/$defs/sha256"},
            "attempt_ref": _nullable_sha256(),
            "action": {"enum": ["deleted", "skipped", "failed"]},
            "reason": {
                "enum": [
                    "retained_expired",
                    "synchronous_cleanup_orphaned",
                    "already_absent",
                    "changed_since_plan",
                    "delete_failed",
                    "batch_limit",
                    "not_attempted_after_failure",
                    "invalid_metadata",
                    "future_timestamp",
                ]
            },
            "error_code": {
                "enum": [
                    "DPONE_AIRFLOW_SECRET_GC_ACCESS_DENIED",
                    "DPONE_AIRFLOW_SECRET_GC_DELETE_FAILED",
                ]
            },
            "http_status": {"type": "integer", "minimum": 100, "maximum": 599},
        },
    }


def _sha256_array() -> dict[str, Any]:
    return {"type": "array", "items": {"$ref": "#/$defs/sha256"}, "uniqueItems": True}


def _nullable_sha256() -> dict[str, Any]:
    return {"anyOf": [{"$ref": "#/$defs/sha256"}, {"type": "null"}]}


def _sha256() -> dict[str, str]:
    return {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}


def _date_time() -> dict[str, str]:
    return {"type": "string", "format": "date-time"}


def _non_empty_string() -> dict[str, Any]:
    return {"type": "string", "minLength": 1}


__all__ = ["airflow_connection_secret_gc_schema_contracts"]

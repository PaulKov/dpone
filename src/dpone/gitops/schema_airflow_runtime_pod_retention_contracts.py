"""Public plan/render schemas for executable Airflow runtime Pod retention."""

from __future__ import annotations

from typing import Any

from dpone.gitops.schema_airflow_runtime_pod_retention_mutation import (
    apply_contract,
    date_time_schema,
    event_contract,
    minimum_age_schema,
    page_size_schema,
    sha256_schema,
    text_schema,
)
from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract


def airflow_runtime_pod_retention_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        _plan_contract(),
        apply_contract(),
        event_contract(),
        _render_contract(),
    )


def _plan_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-runtime-pod-retention-plan",
        kind="dpone.airflow-runtime-pod-retention-plan.v1",
        title="dpone GitOps Airflow runtime Pod retention plan",
        required=(
            "schema",
            "status",
            "namespace",
            "observed_at",
            "minimum_age_seconds",
            "page_size",
            "age_basis",
            "terminal_age_exact",
            "inventory",
            "warnings",
            "items",
            "delete_candidates",
        ),
        properties={
            "schema": {"const": "dpone.airflow-runtime-pod-retention-plan.v1"},
            "status": {"enum": ["ok", "needs_cleanup", "needs_attention"]},
            "namespace": text_schema(),
            "observed_at": date_time_schema(),
            "minimum_age_seconds": minimum_age_schema(),
            "page_size": page_size_schema(),
            "age_basis": {"const": "creation_timestamp_fallback"},
            "terminal_age_exact": {"const": False},
            "inventory": {"$ref": "#/$defs/inventory"},
            "warnings": {"type": "array", "items": text_schema(), "uniqueItems": True, "maxItems": 10_000},
            "items": {"type": "array", "items": {"$ref": "#/$defs/plan_item"}, "maxItems": 10_000},
            "delete_candidates": {
                "type": "array",
                "items": text_schema(),
                "uniqueItems": True,
                "maxItems": 10_000,
            },
        },
        defs={"inventory": _inventory_schema(), "plan_item": _plan_item_schema()},
        additional_properties=False,
    )


def _render_contract() -> GitOpsSchemaContract:
    contract = documented_contract(
        name="airflow-runtime-pod-retention-render",
        kind="dpone.airflow-runtime-pod-retention-render.v1",
        title="dpone GitOps Airflow runtime Pod retention manifest render",
        required=(
            "schema",
            "passed",
            "status",
            "namespace",
            "mode",
            "image",
            "schedule",
            "minimum_age_seconds",
            "page_size",
            "max_delete_count",
            "alerts",
            "manifest_sha256",
            "resources",
            "manifests",
        ),
        properties={
            "schema": {"const": "dpone.airflow-runtime-pod-retention-render.v1"},
            "passed": {"const": True},
            "status": {"const": "rendered"},
            "namespace": text_schema(),
            "mode": {"enum": ["plan", "apply"]},
            "image": {"type": "string", "pattern": "^[^\\s@]+@sha256:[0-9a-f]{64}$"},
            "schedule": text_schema(),
            "minimum_age_seconds": minimum_age_schema(),
            "page_size": page_size_schema(),
            "max_delete_count": {"type": "integer", "minimum": 1, "maximum": 1_000},
            "alerts": {"enum": ["off", "prometheus"]},
            "stale_after_seconds": {
                "type": "integer",
                "minimum": 600,
                "maximum": 63_244_800,
            },
            "manifest_sha256": sha256_schema(),
            "resources": {"type": "array", "items": {"$ref": "#/$defs/resource"}},
            "manifests": {"type": "array", "items": {"type": "object"}, "minItems": 4, "maxItems": 5},
        },
        defs={
            "resource": {
                "type": "object",
                "required": ["kind", "name"],
                "additionalProperties": False,
                "properties": {"kind": text_schema(), "name": text_schema()},
            }
        },
        additional_properties=False,
    )
    contract.schema["allOf"] = [
        {
            "if": {"properties": {"alerts": {"const": "prometheus"}}, "required": ["alerts"]},
            "then": {"required": ["stale_after_seconds"]},
        },
        {
            "if": {"properties": {"alerts": {"const": "off"}}, "required": ["alerts"]},
            "then": {"not": {"required": ["stale_after_seconds"]}},
        },
    ]
    return contract


def _inventory_schema() -> dict[str, Any]:
    fields = ("terminal_pods", "succeeded_pods", "failed_pods", "quarantined")
    return {
        "type": "object",
        "required": list(fields),
        "additionalProperties": False,
        "properties": {field: {"type": "integer", "minimum": 0} for field in fields},
    }


def _plan_item_schema() -> dict[str, Any]:
    nullable_text = {"type": ["string", "null"]}
    return {
        "type": "object",
        "required": [
            "pod_ref",
            "precondition_ref",
            "pod_name",
            "phase",
            "action",
            "reason",
            "age_seconds",
            "age_basis",
            "terminal_age_exact",
            "created_at",
            "workload_id",
            "dag_id",
            "task_id",
            "run_id",
        ],
        "additionalProperties": False,
        "properties": {
            "pod_ref": sha256_schema(),
            "precondition_ref": sha256_schema(),
            "pod_name": text_schema(),
            "phase": text_schema(),
            "action": {"enum": ["protect", "delete", "quarantine"]},
            "reason": {
                "enum": [
                    "inventory_phase_conflict",
                    "invalid_phase",
                    "invalid_identity",
                    "terminating",
                    "invalid_ownership",
                    "missing_correlation",
                    "timestamp_missing",
                    "future_timestamp",
                    "minimum_age",
                    "stale_terminal",
                ]
            },
            "age_seconds": {"type": ["integer", "null"]},
            "age_basis": {"const": "creation_timestamp_fallback"},
            "terminal_age_exact": {"const": False},
            "created_at": nullable_text,
            "workload_id": nullable_text,
            "dag_id": nullable_text,
            "task_id": nullable_text,
            "run_id": nullable_text,
        },
    }


__all__ = ["airflow_runtime_pod_retention_schema_contracts"]

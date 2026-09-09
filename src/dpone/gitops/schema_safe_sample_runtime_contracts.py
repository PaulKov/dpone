from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract


from typing import Any

from dpone.gitops.schema_contract_primitives import documented_contract, pinned_cache_ref_schema
from dpone.gitops.schema_runtime_artifact_delivery import runtime_artifact_delivery_schema
from dpone.gitops.schema_safe_sample_deployment_context_contracts import airflow_deployment_context_properties
from dpone.gitops.schema_safe_sample_route_execution_contracts import (
    data_copy_properties_schema,
    data_copy_required_fields,
    data_copy_status_guards,
    json_value_schema,
    table_schema,
)


def safe_sample_runtime_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        safe_sample_execution_plan_contract(),
        safe_sample_runtime_readiness_contract(),
        safe_sample_runtime_handoff_contract(),
        safe_sample_runtime_execution_contract(),
        safe_sample_runtime_evidence_write_contract(),
        safe_sample_runtime_run_contract(),
    )


def safe_sample_execution_plan_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="safe-sample-execution-plan",
        kind="dpone.safe-sample-execution-plan.v1",
        title="dpone GitOps safe sample execution plan",
        required=(
            "schema",
            "sample_rows",
            "environment",
            "runnable",
            "policy_result",
            "artifact_pinning",
            "blockers",
        ),
        properties={
            "schema": {"const": "dpone.safe-sample-execution-plan.v1"},
            "sample_rows": {"type": "integer", "minimum": 0},
            "environment": non_empty_string_schema(),
            "runnable": {"type": "boolean"},
            "source_snapshot": nullable_source_snapshot_schema(),
            "policy_result": policy_result_schema(),
            "temporary_target_plan": nullable_object_schema(),
            "deployment_context": deployment_context_schema(),
            "artifact_pinning": artifact_pinning_schema(),
            "blockers": {"type": "array", "items": {"$ref": "#/$defs/error"}},
        },
        defs={
            "sha256": sha256_schema(),
            "artifact": artifact_schema(),
            "error": error_schema(),
        },
    )


def safe_sample_runtime_readiness_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="safe-sample-runtime-readiness",
        kind="dpone.safe-sample-runtime-readiness.v1",
        title="dpone GitOps safe sample runtime readiness",
        required=("schema", "ready", "available_contracts", "blockers", "errors"),
        properties={
            "schema": {"const": "dpone.safe-sample-runtime-readiness.v1"},
            "ready": {"type": "boolean"},
            "available_contracts": unique_non_empty_string_array_schema(),
            "blockers": unique_non_empty_string_array_schema(),
            "errors": {"type": "array", "items": {"$ref": "#/$defs/error"}},
        },
        defs={"error": error_schema()},
    )


def safe_sample_runtime_handoff_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="safe-sample-runtime-handoff",
        kind="dpone.safe-sample-runtime-handoff.v1",
        title="dpone GitOps safe sample runtime handoff",
        required=(
            "schema",
            "plan_path",
            "plan_sha256",
            "plan_bytes",
            "command",
            "live_copy_command",
            "live_copy_requires",
        ),
        properties={
            "schema": {"const": "dpone.safe-sample-runtime-handoff.v1"},
            "plan_path": non_empty_string_schema(),
            "plan_sha256": sha256_schema(),
            "plan_bytes": {"type": "integer", "minimum": 1},
            "command": non_empty_string_schema(),
            "live_copy_command": non_empty_string_schema(),
            "live_copy_requires": unique_non_empty_string_array_schema(),
        },
        defs={"sha256": sha256_schema()},
        additional_properties=False,
    )


def safe_sample_runtime_execution_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="safe-sample-runtime-execution",
        kind="dpone.safe-sample-runtime-execution.v1",
        title="dpone GitOps safe sample runtime execution",
        required=("schema", "execution_status", "data_outcome", "release_id", "deployment_id", "errors"),
        properties={
            "schema": {"const": "dpone.safe-sample-runtime-execution.v1"},
            "execution_mode": {"type": "string", "enum": ["local_handoff", "live_copy"]},
            "execution_status": execution_status_schema(),
            "data_outcome": data_outcome_schema(),
            "release_id": {"$ref": "#/$defs/nullable_sha256"},
            "deployment_id": {"$ref": "#/$defs/nullable_sha256"},
            "deployment_identity": deployment_identity_schema(),
            "source_snapshot": nullable_source_snapshot_schema(),
            "route_attestation_verification": nullable_object_schema(),
            "init_fetch": nullable_object_schema(),
            "temporary_target_prepare": nullable_object_schema(),
            "data_copy": data_copy_schema(),
            "temporary_target_cleanup": nullable_object_schema(),
            "errors": {"type": "array", "items": {"$ref": "#/$defs/error"}},
        },
        defs={
            "sha256": sha256_schema(),
            "nullable_sha256": nullable_sha256_schema(),
            "artifact": artifact_schema(),
            "error": error_schema(),
            "json_value": json_value_schema(),
            "table": table_schema(),
        },
    )


def safe_sample_runtime_evidence_write_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="safe-sample-runtime-evidence-write",
        kind="dpone.safe-sample-runtime-evidence-write.v1",
        title="dpone GitOps safe sample runtime evidence write report",
        required=("schema", "path", "sha256", "bytes", "release_id", "deployment_id"),
        properties={
            "schema": {"const": "dpone.safe-sample-runtime-evidence-write.v1"},
            "path": non_empty_string_schema(),
            "sha256": sha256_schema(),
            "bytes": {"type": "integer", "minimum": 1},
            "release_id": nullable_sha256_schema(),
            "deployment_id": nullable_sha256_schema(),
        },
        defs={"sha256": sha256_schema()},
        additional_properties=False,
    )


def safe_sample_runtime_run_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="safe-sample-runtime-run",
        kind="dpone.safe-sample-runtime-run.v1",
        title="dpone GitOps safe sample runtime run report",
        required=(
            "schema",
            "release_id",
            "deployment_id",
            "execution_status",
            "data_outcome",
            "runtime_execution",
            "evidence_write",
            "errors",
        ),
        properties={
            "schema": {"const": "dpone.safe-sample-runtime-run.v1"},
            "release_id": nullable_sha256_schema(),
            "deployment_id": nullable_sha256_schema(),
            "execution_status": execution_status_schema(),
            "data_outcome": data_outcome_schema(),
            "runtime_execution": runtime_execution_summary_schema(),
            "evidence_write": evidence_write_summary_schema(),
            "errors": {"type": "array", "items": {"$ref": "#/$defs/error"}},
        },
        defs={
            "sha256": sha256_schema(),
            "error": error_schema(),
        },
        additional_properties=False,
    )


def policy_result_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["passed", "request", "policy", "capabilities", "errors"],
        "additionalProperties": True,
        "properties": {
            "passed": {"type": "boolean"},
            "request": {"type": "object"},
            "policy": {"type": "object"},
            "capabilities": {"type": "object"},
            "errors": {"type": "array", "items": {"$ref": "#/$defs/error"}},
        },
    }


def deployment_context_schema() -> dict[str, Any]:
    return {
        "type": ["object", "null"],
        "additionalProperties": True,
        "properties": airflow_deployment_context_properties(),
    }


def artifact_pinning_schema() -> dict[str, Any]:
    return {
        "type": ["object", "null"],
        "additionalProperties": True,
        "properties": {
            "release_id": {"$ref": "#/$defs/sha256"},
            "deployment_id": {"$ref": "#/$defs/sha256"},
            "pinned_workload_uri": {
                "type": "string",
                "pattern": "^cached://deployments/sha256:[0-9a-f]{64}/workloads/[^\\s/?]+$",
            },
            "workload_packs": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["id", "sha256"],
                    "additionalProperties": True,
                    "properties": {
                        "id": {"type": "string"},
                        "sha256": {"type": "string"},
                    },
                },
            },
        },
    }


def deployment_identity_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["release_id", "deployment_id", "workload_packs", "runtime_artifact_delivery"],
        "additionalProperties": True,
        "properties": {
            "release_id": {"$ref": "#/$defs/nullable_sha256"},
            "deployment_id": {"$ref": "#/$defs/nullable_sha256"},
            "binding_set_ref": {"$ref": "#/$defs/nullable_sha256"},
            "connection_registry_ref": {"$ref": "#/$defs/nullable_sha256"},
            "credential_runtime_ref": {"$ref": "#/$defs/nullable_sha256"},
            "runtime_image_digest": {"$ref": "#/$defs/nullable_sha256"},
            "airflow_bundle_ref": {"type": ["string", "null"]},
            "workload_packs": {"type": "array", "items": {"$ref": "#/$defs/artifact"}},
            "runtime_artifact_delivery": runtime_artifact_delivery_schema(require_mode=False),
        },
    }


def data_copy_schema() -> dict[str, Any]:
    return {
        "type": ["object", "null"],
        "required": ["schema", "status", "errors"],
        "additionalProperties": True,
        "properties": data_copy_properties_schema(),
        "allOf": [
            {
                "if": {
                    "required": ["status"],
                    "properties": {"status": {"enum": ["copied", "copied_with_quarantine"]}},
                },
                "then": {"required": list(data_copy_required_fields())},
            },
            *data_copy_status_guards(),
        ],
    }


def runtime_execution_summary_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["schema", "execution_status", "data_outcome", "release_id", "deployment_id", "errors"],
        "additionalProperties": True,
        "properties": {
            "schema": {"const": "dpone.safe-sample-runtime-execution.v1"},
        },
    }


def evidence_write_summary_schema() -> dict[str, Any]:
    return {
        "type": ["object", "null"],
        "required": ["schema", "path", "sha256", "bytes"],
        "additionalProperties": True,
        "properties": {
            "schema": {"const": "dpone.safe-sample-runtime-evidence-write.v1"},
            "path": non_empty_string_schema(),
            "sha256": {"$ref": "#/$defs/sha256"},
        },
    }


def nullable_object_schema() -> dict[str, Any]:
    return {
        "type": ["object", "null"],
        "additionalProperties": True,
    }


def nullable_source_snapshot_schema() -> dict[str, Any]:
    return {
        "type": ["object", "null"],
        "required": ["pipeline_id", "path", "sha256"],
        "additionalProperties": False,
        "properties": {
            "pipeline_id": {"type": "string", "pattern": "^[a-z0-9][a-z0-9_-]{1,127}$"},
            "path": {
                "type": "string",
                "pattern": r"^(?!/)(?!.*(?:^|/)\.\.(?:/|$))(?!.*\\)\S+$",
            },
            "sha256": sha256_schema(),
        },
    }


def artifact_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["id", "artifact_ref", "sha256", "bytes"],
        "additionalProperties": True,
        "properties": {
            "id": non_empty_string_schema(),
            "artifact_ref": pinned_cache_ref_schema(),
            "sha256": {"$ref": "#/$defs/sha256"},
            "bytes": {"type": "integer", "minimum": 1},
        },
    }


def error_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["schema", "code", "stage", "severity", "message"],
        "additionalProperties": True,
        "properties": {
            "schema": {"const": "dpone.error.v1"},
            "code": {"type": "string", "pattern": "^DPONE_[A-Z0-9_]+$"},
            "stage": non_empty_string_schema(),
            "severity": {"enum": ["info", "warning", "error"]},
            "message": non_empty_string_schema(),
            "fixes": {"type": "array"},
        },
    }


def unique_non_empty_string_array_schema() -> dict[str, Any]:
    return {
        "type": "array",
        "items": non_empty_string_schema(),
        "uniqueItems": True,
    }


def nullable_sha256_schema() -> dict[str, Any]:
    return {
        "anyOf": [{"$ref": "#/$defs/sha256"}, {"type": "null"}],
    }


def sha256_schema() -> dict[str, str]:
    return {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}


def execution_status_schema() -> dict[str, list[str]]:
    return {"enum": ["succeeded", "failed", "cancelled", "skipped"]}


def data_outcome_schema() -> dict[str, list[str]]:
    return {"enum": ["passed", "passed_with_quarantine", "failed_quality_gate", "no_data", "unknown"]}


def non_empty_string_schema() -> dict[str, Any]:
    return {"type": "string", "minLength": 1}


__all__ = ["safe_sample_runtime_schema_contracts"]

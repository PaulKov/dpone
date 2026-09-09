from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract


from dpone.gitops.schema_contract_primitives import (
    array_schema,
    boolean_schema,
    const_schema,
    contract,
    documented_contract,
    integer_schema,
    issue_ref,
    object_schema,
    pinned_cache_ref_schema,
    string_schema,
)
from dpone.gitops.schema_runtime_artifact_delivery import (
    runtime_artifact_delivery_schema,
)


def airflow_deployment_index_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-deployment-index",
        kind="dpone.airflow-deployment-index.v1",
        title="dpone GitOps Airflow deployment index",
        required=("schema", "release_id", "deployment_id", "dag_specs", "workload_packs", "runtime_artifact_delivery"),
        properties={
            "schema": {"const": "dpone.airflow-deployment-index.v1"},
            "release_id": {"$ref": "#/$defs/identity"},
            "deployment_id": {"$ref": "#/$defs/identity"},
            "environment": string_schema(),
            "dag_specs": {"$ref": "#/$defs/artifacts"},
            "workload_packs": {"$ref": "#/$defs/artifacts"},
            "binding_set_ref": nullable_sha256_schema(),
            "connection_registry_ref": nullable_sha256_schema(),
            "credential_runtime_ref": nullable_sha256_schema(),
            "runtime_image_digest": nullable_sha256_schema(),
            "airflow_bundle_ref": {"type": ["string", "null"]},
            "runtime_artifact_delivery": runtime_artifact_delivery_schema(),
        },
        defs=airflow_deployment_index_defs(),
    )


def airflow_explain_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-explain",
        kind="dpone.airflow-explain.v1",
        title="dpone GitOps Airflow explain report",
        required=(
            "kind",
            "passed",
            "changes",
            "errors",
            "pipeline_ref",
            "source_path",
            "artifact_state",
            "operator_diagnostics",
            "hint",
        ),
        properties={
            "kind": {"const": "dpone.airflow-explain.v1"},
            "passed": boolean_schema(),
            "changes": array_schema(object_schema()),
            "errors": array_schema(object_schema()),
            "pipeline_ref": string_schema(),
            "source_path": string_schema(),
            "artifact_state": airflow_explain_artifact_state_schema(),
            "operator_diagnostics": object_schema(),
            "hint": string_schema(),
        },
    )


def airflow_operator_diagnostics_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-operator-diagnostics",
        kind="dpone.airflow-operator-diagnostics.v1",
        title="dpone GitOps Airflow operator diagnostics",
        required=(
            "kind",
            "status",
            "index_path",
            "parse_side_effects",
            "operator_pinning",
            "checks",
            "summary",
            "next_actions",
        ),
        properties={
            "kind": {"const": "dpone.airflow-operator-diagnostics.v1"},
            "status": {
                "enum": [
                    "planned",
                    "invalid",
                    "materialized",
                    "operator_issues",
                    "operator_warnings",
                ]
            },
            "index_path": string_schema(),
            "release_id": nullable_sha256_schema(),
            "deployment_id": nullable_sha256_schema(),
            "runtime_image_digest": nullable_sha256_schema(),
            "binding_set_ref": nullable_sha256_schema(),
            "connection_registry_ref": nullable_sha256_schema(),
            "credential_runtime_ref": nullable_sha256_schema(),
            "airflow_bundle_ref": {"type": ["string", "null"]},
            "airflow_bundle": airflow_bundle_identity_schema(),
            "runtime_artifact_delivery": runtime_artifact_delivery_schema(require_mode=False),
            "parse_side_effects": airflow_operator_parse_side_effects_schema(),
            "operator_pinning": {"enum": ["planned", "pinned", "incomplete"]},
            "workload_operators": array_schema(object_schema()),
            "checks": array_schema(airflow_operator_check_schema()),
            "summary": airflow_operator_summary_schema(),
            "next_actions": array_schema(airflow_operator_next_action_schema()),
        },
        defs={"identity": identity_schema()},
    )


def airflow_bundle_identity_schema() -> dict[str, object]:
    return {
        "type": ["object", "null"],
        "additionalProperties": False,
        "properties": {
            "backend": {"type": "string", "minLength": 1},
            "ref": {"type": "string", "minLength": 1},
            "versioned": {"type": "boolean"},
            "version": {"type": ["string", "null"]},
        },
    }


def airflow_artifact_index_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-artifact-index",
        kind="gitops.airflow_artifact_index",
        title="dpone GitOps Airflow artifact index contract",
        required=(
            "kind",
            "schema_version",
            "producer",
            "artifact_dir",
            "output_path",
            "created_at",
            "entries",
        ),
        properties={
            "kind": const_schema("gitops.airflow_artifact_index"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "artifact_dir": string_schema(),
            "output_path": string_schema(),
            "created_at": string_schema(),
            "entries": array_schema(airflow_artifact_index_entry_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def airflow_preflight_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-preflight",
        kind="gitops.airflow_preflight",
        title="dpone GitOps Airflow preflight contract",
        required=(
            "kind",
            "schema_version",
            "producer",
            "artifact_dir",
            "artifact_index_path",
            "runner_policy",
            "artifact_index",
            "pod_doctor",
            "checks",
            "next_actions",
        ),
        properties={
            "kind": const_schema("gitops.airflow_preflight"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "artifact_dir": string_schema(),
            "artifact_index_path": string_schema(),
            "runner_policy": string_schema(),
            "artifact_index": object_schema(),
            "pod_doctor": object_schema(),
            "checks": array_schema(airflow_preflight_check_schema()),
            "next_actions": array_schema(string_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def airflow_artifact_index_entry_schema() -> dict[str, object]:
    return object_schema(
        required=(
            "name",
            "path",
            "format",
            "expected_kind",
            "required",
            "exists",
            "passed",
            "reason",
        ),
        properties={
            "name": string_schema(),
            "path": string_schema(),
            "format": string_schema(),
            "expected_kind": string_schema(),
            "actual_kind": {"type": ["string", "null"]},
            "schema_version": {"type": ["string", "null"]},
            "producer": {"type": ["string", "null"]},
            "required": boolean_schema(),
            "exists": boolean_schema(),
            "sha256": {"type": ["string", "null"]},
            "bytes": {"type": ["integer", "null"]},
            "passed": boolean_schema(),
            "reason": string_schema(),
        },
    )


def airflow_preflight_check_schema() -> dict[str, object]:
    return object_schema(
        required=("name", "passed", "severity", "message", "path", "source"),
        properties={
            "name": string_schema(),
            "passed": boolean_schema(),
            "severity": string_schema(),
            "message": string_schema(),
            "path": string_schema(),
            "source": string_schema(),
        },
    )


def airflow_explain_artifact_state_schema() -> dict[str, object]:
    return object_schema(
        required=("manifest", "dag_spec", "airflow_pack", "published_generation"),
        properties={
            "manifest": {"enum": ["source", "missing", "invalid"]},
            "dag_spec": {"enum": ["planned", "materialized", "stale"]},
            "airflow_pack": {"enum": ["planned", "materialized", "stale"]},
            "published_deployment_id": {"type": ["string", "null"]},
            "published_generation": {
                "type": ["string", "null"],
                "deprecated": True,
                "description": (
                    "Compatibility alias for published_deployment_id. Use published_deployment_id in new integrations."
                ),
            },
        },
    )


def airflow_operator_parse_side_effects_schema() -> dict[str, object]:
    return object_schema(
        required=(
            "network",
            "metadata_db",
            "airflow_variables",
            "airflow_connections",
            "vault",
            "kubernetes",
            "cache_refresh",
        ),
        properties={
            "network": {"const": False},
            "metadata_db": {"const": False},
            "airflow_variables": {"const": False},
            "airflow_connections": {"const": False},
            "vault": {"const": False},
            "kubernetes": {"const": False},
            "cache_refresh": {"const": False},
        },
    )


def airflow_operator_check_schema() -> dict[str, object]:
    return object_schema(
        required=("code", "status", "message"),
        properties={
            "code": {"type": "string", "pattern": "^DPONE_[A-Z0-9_]+$"},
            "status": {"enum": ["passed", "warning", "failed"]},
            "message": string_schema(),
        },
    )


def airflow_operator_summary_schema() -> dict[str, object]:
    return object_schema(
        required=("passed", "warning", "failed"),
        properties={
            "passed": integer_schema(),
            "warning": integer_schema(),
            "failed": integer_schema(),
        },
    )


def airflow_operator_next_action_schema() -> dict[str, object]:
    return object_schema(
        required=("code", "action", "safety"),
        properties={
            "code": {"type": "string", "pattern": "^DPONE_[A-Z0-9_]+$"},
            "action": string_schema(),
            "safety": {"enum": ["safe", "manual", "destructive"]},
        },
    )


def nullable_sha256_schema() -> dict[str, object]:
    return {"anyOf": [{"$ref": "#/$defs/identity"}, {"type": "null"}]}


def identity_schema() -> dict[str, str]:
    return {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}


def checksum_schema() -> dict[str, str]:
    return {"type": "string", "pattern": "^sha256:[A-Fa-f0-9]{64}$"}


def airflow_deployment_index_defs() -> dict[str, object]:
    return {
        "identity": identity_schema(),
        "sha256": checksum_schema(),
        "artifact": {
            "type": "object",
            "required": ["id", "artifact_ref", "sha256"],
            "additionalProperties": True,
            "properties": {
                "id": {"type": "string", "minLength": 1},
                "artifact_ref": pinned_cache_ref_schema(),
                "sha256": {"$ref": "#/$defs/sha256"},
                "bytes": {"type": "integer", "minimum": 0},
            },
        },
        "artifacts": {
            "type": "array",
            "items": {"$ref": "#/$defs/artifact"},
        },
    }


__all__ = [
    "airflow_artifact_index_contract",
    "airflow_artifact_index_entry_schema",
    "airflow_deployment_index_contract",
    "airflow_explain_artifact_state_schema",
    "airflow_explain_contract",
    "airflow_operator_check_schema",
    "airflow_operator_diagnostics_contract",
    "airflow_operator_next_action_schema",
    "airflow_operator_parse_side_effects_schema",
    "airflow_operator_summary_schema",
    "airflow_preflight_contract",
    "airflow_preflight_check_schema",
]

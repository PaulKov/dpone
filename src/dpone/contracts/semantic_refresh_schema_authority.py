"""Deterministic schemas for semantic-refresh terminal and compiler authority."""

from __future__ import annotations

from typing import Any

from dpone.contracts.semantic_refresh_authority_bundle import COMPILER_RUNTIME_AUTHORITY_BUNDLE_SCHEMA
from dpone.contracts.semantic_refresh_schema_common import (
    DIGEST_SCHEMA,
    POSITIVE_INTEGER_SCHEMA,
    TEXT_SCHEMA,
    WORKFLOW_MODE_SCHEMA,
    closed_schema,
    schema_discriminator,
    string_set_schema,
)
from dpone.contracts.semantic_refresh_termination_receipt import TRUSTED_ATTEMPT_TERMINATION_RECEIPT_SCHEMA
from dpone.contracts.semantic_refresh_types import WorkflowMode
from dpone.contracts.semantic_refresh_workflow_summary import DURABLE_WORKFLOW_SUMMARY_SCHEMA

_UTC_TIMESTAMP: dict[str, object] = {"format": "date-time", "pattern": "Z$", "type": "string"}
_NONNEGATIVE_INTEGER: dict[str, object] = {"minimum": 0, "type": "integer"}
_MAP_INDEX: dict[str, object] = {"minimum": -1, "type": "integer"}
_UUID: dict[str, object] = {
    "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    "type": "string",
}
SEMANTIC_REFRESH_DAG_PROJECTION_SCHEMA = "dpone.semantic-refresh-v2-dag-projection.v1"


def authority_contract_schemas() -> dict[str, dict[str, Any]]:
    """Return fresh durable-summary, termination and authority-bundle schemas."""

    return {
        DURABLE_WORKFLOW_SUMMARY_SCHEMA: _workflow_summary_schema(),
        TRUSTED_ATTEMPT_TERMINATION_RECEIPT_SCHEMA: _termination_receipt_schema(),
        COMPILER_RUNTIME_AUTHORITY_BUNDLE_SCHEMA: _authority_bundle_schema(),
        SEMANTIC_REFRESH_DAG_PROJECTION_SCHEMA: _dag_projection_schema(),
    }


def _dag_projection_schema() -> dict[str, Any]:
    nonempty_object: dict[str, object] = {"minProperties": 1, "type": "object"}
    properties: dict[str, object] = {
        "dag_projection_sha256": DIGEST_SCHEMA,
        "deployment_id": DIGEST_SCHEMA,
        "operation_ids_by_model": {
            "additionalProperties": DIGEST_SCHEMA,
            "minProperties": 1,
            "type": "object",
        },
        "package_artifacts_sha256": DIGEST_SCHEMA,
        "plan_bundle": nonempty_object,
        "plan_bundle_sha256": DIGEST_SCHEMA,
        "pre_release_bundle_sha256": DIGEST_SCHEMA,
        "release_id": DIGEST_SCHEMA,
        "schema": schema_discriminator(SEMANTIC_REFRESH_DAG_PROJECTION_SCHEMA),
        "task_projection": nonempty_object,
        "template_pack": nonempty_object,
        "template_pack_fingerprint": DIGEST_SCHEMA,
        "topology": nonempty_object,
        "topology_sha256": DIGEST_SCHEMA,
        "workflow_plan_sha256": DIGEST_SCHEMA,
    }
    return closed_schema(
        SEMANTIC_REFRESH_DAG_PROJECTION_SCHEMA,
        properties,
        sorted(properties),
        comment=(
            "The optional Airflow pack recomputes dag_projection_sha256 and validates the exact nested template, "
            "plan, topology, operation and task closures before DAG materialization."
        ),
    )


def _workflow_summary_schema() -> dict[str, Any]:
    publication_properties: dict[str, object] = {
        "artifact_manifest_sha256": DIGEST_SCHEMA,
        "attempt_binding_sha256": DIGEST_SCHEMA,
        "clickhouse_terminal_receipt_sha256": DIGEST_SCHEMA,
        "operation_id": DIGEST_SCHEMA,
        "operation_plan_sha256": DIGEST_SCHEMA,
        "scope_revision": POSITIVE_INTEGER_SCHEMA,
        "status": {"const": "COMPLETE"},
        "target_generation": POSITIVE_INTEGER_SCHEMA,
        "terminal_receipt_sha256": DIGEST_SCHEMA,
        "workflow_execution_binding_sha256": DIGEST_SCHEMA,
    }
    publication = {
        "additionalProperties": False,
        "properties": publication_properties,
        "required": sorted(publication_properties),
        "type": "object",
    }
    properties: dict[str, object] = {
        "expected_operation_ids": _digest_set_schema(),
        "publications": {"items": publication, "minItems": 1, "type": "array"},
        "schema": schema_discriminator(DURABLE_WORKFLOW_SUMMARY_SCHEMA),
        "status": {"const": "FULLY_COMPLETE"},
        "terminal_summary_sha256": DIGEST_SCHEMA,
        "workflow_execution_binding_sha256": DIGEST_SCHEMA,
        "workflow_execution_id": TEXT_SCHEMA,
        "workflow_plan_sha256": DIGEST_SCHEMA,
    }
    return closed_schema(
        DURABLE_WORKFLOW_SUMMARY_SCHEMA,
        properties,
        sorted(properties),
        comment="Only exact complete model receipt closure can produce this summary or authorize success Assets.",
    )


def _termination_receipt_schema() -> dict[str, Any]:
    container_properties: dict[str, object] = {
        "container_id": TEXT_SCHEMA,
        "exit_code": {"type": "integer"},
        "finished_at": _UTC_TIMESTAMP,
        "name": TEXT_SCHEMA,
        "reason": TEXT_SCHEMA,
    }
    container = {
        "additionalProperties": False,
        "properties": container_properties,
        "required": ["container_id", "finished_at", "name", "reason"],
        "type": "object",
    }
    properties: dict[str, object] = {
        "attempt_binding_sha256": DIGEST_SCHEMA,
        "cluster_id": TEXT_SCHEMA,
        "container_terminations": {"items": container, "minItems": 1, "type": "array"},
        "dag_id": TEXT_SCHEMA,
        "map_index": _MAP_INDEX,
        "namespace": TEXT_SCHEMA,
        "observed_at": _UTC_TIMESTAMP,
        "observer_attestation_sha256": DIGEST_SCHEMA,
        "observer_authority": TEXT_SCHEMA,
        "observer_policy_sha256": DIGEST_SCHEMA,
        "observer_signature_sha256": DIGEST_SCHEMA,
        "operation_ids": _digest_set_schema(),
        "operation_set_sha256": DIGEST_SCHEMA,
        "pod_name": TEXT_SCHEMA,
        "pod_resource_version": TEXT_SCHEMA,
        "pod_uid": _UUID,
        "run_id": TEXT_SCHEMA,
        "schema": schema_discriminator(TRUSTED_ATTEMPT_TERMINATION_RECEIPT_SCHEMA),
        "task_id": TEXT_SCHEMA,
        "terminal_phase": {"enum": ["Failed", "Succeeded"], "type": "string"},
        "termination_receipt_sha256": DIGEST_SCHEMA,
        "try_number": POSITIVE_INTEGER_SCHEMA,
        "verification_status": {"const": "VERIFIED"},
        "workflow_execution_binding_sha256": DIGEST_SCHEMA,
        "workflow_execution_id": TEXT_SCHEMA,
    }
    return closed_schema(
        TRUSTED_ATTEMPT_TERMINATION_RECEIPT_SCHEMA,
        properties,
        sorted(properties),
        comment="Airflow state alone is not evidence; the trusted observer, pod UID and full container closure are mandatory.",
    )


def _authority_bundle_schema() -> dict[str, Any]:
    model_properties: dict[str, object] = {
        "baseline_adoption_receipt_sha256": DIGEST_SCHEMA,
        "model_definition_proof_sha256": DIGEST_SCHEMA,
        "model_unique_id": TEXT_SCHEMA,
        "mutation_closure_sha256": DIGEST_SCHEMA,
        "operation_id": DIGEST_SCHEMA,
        "operation_plan_sha256": DIGEST_SCHEMA,
        "read_dependency_proof_sha256": DIGEST_SCHEMA,
        "sqlserver_lifecycle_policy_sha256": DIGEST_SCHEMA,
    }
    model = {
        "additionalProperties": False,
        "properties": model_properties,
        "required": sorted(model_properties),
        "type": "object",
    }
    properties: dict[str, object] = {
        "authority_bundle_sha256": DIGEST_SCHEMA,
        "compiler_policy_sha256": DIGEST_SCHEMA,
        "deployment_id": DIGEST_SCHEMA,
        "environment": TEXT_SCHEMA,
        "models": {"items": model, "minItems": 1, "type": "array"},
        "release_id": DIGEST_SCHEMA,
        "route_certification_receipt_sha256": DIGEST_SCHEMA,
        "runtime_policy_sha256": DIGEST_SCHEMA,
        "schema": schema_discriminator(COMPILER_RUNTIME_AUTHORITY_BUNDLE_SCHEMA),
        "workflow_execution_binding_sha256": DIGEST_SCHEMA,
        "workflow_execution_id": TEXT_SCHEMA,
        "workflow_mode": WORKFLOW_MODE_SCHEMA,
        "workflow_plan_sha256": DIGEST_SCHEMA,
        "workflow_replacement_plan_sha256": DIGEST_SCHEMA,
    }
    return closed_schema(
        COMPILER_RUNTIME_AUTHORITY_BUNDLE_SCHEMA,
        properties,
        sorted(set(properties) - {"workflow_replacement_plan_sha256"}),
        comment="This authority ends at execution binding. Attempts, orchestration facts, receipts and outcomes are forbidden.",
        one_of=[
            {
                "not": {"required": ["workflow_replacement_plan_sha256"]},
                "properties": {
                    "workflow_mode": {"enum": [WorkflowMode.NORMAL.value, WorkflowMode.COMPLETE_SCOPE_REPLAY.value]}
                },
            },
            {
                "properties": {"workflow_mode": {"const": WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT.value}},
                "required": ["workflow_replacement_plan_sha256"],
            },
        ],
    )


def _digest_set_schema() -> dict[str, object]:
    result = string_set_schema()
    result["items"] = DIGEST_SCHEMA
    return result


__all__ = ["SEMANTIC_REFRESH_DAG_PROJECTION_SCHEMA", "authority_contract_schemas"]

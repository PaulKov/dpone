"""Generated schemas for semantic-refresh execution and attempt bindings."""

from __future__ import annotations

from typing import Any

from dpone.contracts.semantic_refresh_attempt_binding import ATTEMPT_BINDING_SCHEMA
from dpone.contracts.semantic_refresh_attempt_continuation import (
    ATTEMPT_CONTINUATION_RECEIPT_SCHEMA,
)
from dpone.contracts.semantic_refresh_execution_binding import WORKFLOW_EXECUTION_BINDING_SCHEMA
from dpone.contracts.semantic_refresh_schema_common import (
    DIGEST_SCHEMA,
    POSITIVE_INTEGER_SCHEMA,
    TEXT_SCHEMA,
    WORKFLOW_MODE_SCHEMA,
    closed_schema,
    schema_discriminator,
    string_set_schema,
)
from dpone.contracts.semantic_refresh_types import WorkflowMode


def binding_contract_schemas() -> dict[str, dict[str, Any]]:
    """Return fresh deterministic schemas for the two binding documents."""

    return {
        WORKFLOW_EXECUTION_BINDING_SCHEMA: _execution_binding_schema(),
        ATTEMPT_BINDING_SCHEMA: _attempt_binding_schema(),
        ATTEMPT_CONTINUATION_RECEIPT_SCHEMA: _attempt_continuation_receipt_schema(),
    }


def _execution_binding_schema() -> dict[str, Any]:
    properties: dict[str, object] = {
        "binding_set_ref": TEXT_SCHEMA,
        "connection_registry_ref": TEXT_SCHEMA,
        "credential_runtime_ref": TEXT_SCHEMA,
        "deployment_id": DIGEST_SCHEMA,
        "expected_model_outcome_ids": string_set_schema(),
        "model_operation_plan_ids": string_set_schema(),
        "recovery_plan_digest": DIGEST_SCHEMA,
        "replacement_action_ids": string_set_schema(allow_empty=True),
        "schema": schema_discriminator(WORKFLOW_EXECUTION_BINDING_SCHEMA),
        "selected_mutating_node_ids": string_set_schema(),
        "workflow_execution_binding_sha256": DIGEST_SCHEMA,
        "workflow_execution_id": TEXT_SCHEMA,
        "workflow_mode": WORKFLOW_MODE_SCHEMA,
        "workflow_plan_sha256": DIGEST_SCHEMA,
        "workflow_replacement_plan_sha256": DIGEST_SCHEMA,
    }
    required = sorted(set(properties) - {"recovery_plan_digest", "workflow_replacement_plan_sha256"})
    no_replacement_fields: dict[str, object] = {
        "not": {
            "anyOf": [
                {"required": ["recovery_plan_digest"]},
                {"required": ["workflow_replacement_plan_sha256"]},
            ]
        },
    }
    return closed_schema(
        WORKFLOW_EXECUTION_BINDING_SCHEMA,
        properties,
        required,
        comment=(
            "workflow_execution_id is the trusted logical DagRun identity and participates in the binding digest. "
            "The first three model ID arrays are equal; replacement also equals action IDs."
        ),
        one_of=[
            {
                **no_replacement_fields,
                "properties": {
                    "replacement_action_ids": {"maxItems": 0},
                    "workflow_mode": {"const": WorkflowMode.NORMAL.value},
                },
            },
            {
                **no_replacement_fields,
                "properties": {
                    "replacement_action_ids": {"maxItems": 0},
                    "workflow_mode": {"const": WorkflowMode.COMPLETE_SCOPE_REPLAY.value},
                },
            },
            {
                "properties": {
                    "replacement_action_ids": string_set_schema(),
                    "workflow_mode": {"const": WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT.value},
                },
                "required": ["recovery_plan_digest", "workflow_replacement_plan_sha256"],
            },
        ],
    )


def _attempt_binding_schema() -> dict[str, Any]:
    properties: dict[str, object] = {
        "attempt_binding_sha256": DIGEST_SCHEMA,
        "dag_run_id": TEXT_SCHEMA,
        "fencing_epoch": POSITIVE_INTEGER_SCHEMA,
        "operation_id": DIGEST_SCHEMA,
        "operation_plan_sha256": DIGEST_SCHEMA,
        "owner_id": TEXT_SCHEMA,
        "pod_uid": TEXT_SCHEMA,
        "schema": schema_discriminator(ATTEMPT_BINDING_SCHEMA),
        "task_id": TEXT_SCHEMA,
        "try_number": POSITIVE_INTEGER_SCHEMA,
        "workflow_execution_id": TEXT_SCHEMA,
        "workflow_execution_binding_sha256": DIGEST_SCHEMA,
    }
    return closed_schema(
        ATTEMPT_BINDING_SCHEMA,
        properties,
        sorted(properties),
        comment="Python validation requires workflow_execution_id to equal dag_run_id exactly.",
    )


def _attempt_continuation_receipt_schema() -> dict[str, Any]:
    properties: dict[str, object] = {
        "after_image_sha256": DIGEST_SCHEMA,
        "build_receipt_sha256": DIGEST_SCHEMA,
        "clickhouse_cluster_authority_id": TEXT_SCHEMA,
        "clickhouse_query_id_prefix": TEXT_SCHEMA,
        "clickhouse_quiescence_observation_sha256": DIGEST_SCHEMA,
        "continuation_receipt_sha256": DIGEST_SCHEMA,
        "engine_quiescence_receipt_sha256": DIGEST_SCHEMA,
        "fencing_epoch": POSITIVE_INTEGER_SCHEMA,
        "guard_resource_id": TEXT_SCHEMA,
        "mssql_active_session_count": {"const": 0, "type": "integer"},
        "mssql_guard_lock_status": {"const": "EXCLUSIVE_ACQUIRED"},
        "operation_id": DIGEST_SCHEMA,
        "operation_plan_sha256": DIGEST_SCHEMA,
        "original_attempt_binding_sha256": DIGEST_SCHEMA,
        "original_attempt_termination_receipt_sha256": DIGEST_SCHEMA,
        "pod_uid": TEXT_SCHEMA,
        "schema": schema_discriminator(ATTEMPT_CONTINUATION_RECEIPT_SCHEMA),
        "status": {"const": "VERIFIED"},
        "task_id": TEXT_SCHEMA,
        "try_number": POSITIVE_INTEGER_SCHEMA,
        "verified_at": {"format": "date-time", "type": "string"},
        "workflow_execution_binding_sha256": DIGEST_SCHEMA,
    }
    return closed_schema(
        ATTEMPT_CONTINUATION_RECEIPT_SCHEMA,
        properties,
        sorted(properties),
        comment=(
            "The receipt preserves the original attempt/fence and binds a later task try only after "
            "a trusted original-pod termination, an exclusive MSSQL guard with zero original-attempt "
            "sessions, zero matching ClickHouse query IDs, and exact current-target reconciliation."
        ),
    )


__all__ = ["binding_contract_schemas"]

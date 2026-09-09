"""Generated schema definitions for semantic-refresh operation/workflow plans."""

from __future__ import annotations

from typing import Any

from dpone.contracts.semantic_refresh_failure_summary import FAILED_WORKFLOW_SUMMARY_SCHEMA
from dpone.contracts.semantic_refresh_operation_plan import (
    IGNORE_MISSING,
    OPERATION_PLAN_SCHEMA,
    SCOPE_STABLE_EVENT_FACT,
)
from dpone.contracts.semantic_refresh_schema_common import (
    DIGEST_SCHEMA,
    POSITIVE_INTEGER_SCHEMA,
    TEXT_SCHEMA,
    UTC_DAY_SCHEMA,
    closed_schema,
    schema_discriminator,
    string_set_schema,
)
from dpone.contracts.semantic_refresh_schema_effective_key import effective_key_schema
from dpone.contracts.semantic_refresh_types import (
    ReplacementAction,
    SqlServerModelOutcome,
    WorkflowMode,
)
from dpone.contracts.semantic_refresh_workflow_plan import WORKFLOW_PLAN_SCHEMA
from dpone.contracts.semantic_refresh_workflow_replacement import WORKFLOW_REPLACEMENT_PLAN_SCHEMA


def plan_contract_schemas() -> dict[str, dict[str, Any]]:
    """Return fresh deterministic schemas for semantic plans and terminal summary."""

    return {
        OPERATION_PLAN_SCHEMA: _operation_plan_schema(),
        WORKFLOW_PLAN_SCHEMA: _workflow_plan_schema(),
        WORKFLOW_REPLACEMENT_PLAN_SCHEMA: _replacement_plan_schema(),
        FAILED_WORKFLOW_SUMMARY_SCHEMA: _failed_workflow_summary_schema(),
    }


def _failed_workflow_summary_schema() -> dict[str, Any]:
    model_properties: dict[str, object] = {
        "attempt_binding_sha256": DIGEST_SCHEMA,
        "mssql_evidence_sha256": DIGEST_SCHEMA,
        "mssql_outcome": {
            "enum": [
                SqlServerModelOutcome.NOT_INVOKED.value,
                SqlServerModelOutcome.ROLLED_BACK.value,
                SqlServerModelOutcome.COMMITTED_WITH_IMAGES.value,
            ],
            "type": "string",
        },
        "operation_id": DIGEST_SCHEMA,
    }
    model_schema = {
        "additionalProperties": False,
        "properties": model_properties,
        "required": sorted(model_properties),
        "type": "object",
    }
    properties: dict[str, object] = {
        "expected_operation_ids": string_set_schema(),
        "models": {"items": model_schema, "minItems": 1, "type": "array"},
        "schema": schema_discriminator(FAILED_WORKFLOW_SUMMARY_SCHEMA),
        "status": {"const": "FAILED_PRE_COMMIT"},
        "terminal_summary_sha256": DIGEST_SCHEMA,
        "workflow_execution_binding_sha256": DIGEST_SCHEMA,
        "workflow_id": TEXT_SCHEMA,
        "workflow_plan_sha256": DIGEST_SCHEMA,
    }
    return closed_schema(
        FAILED_WORKFLOW_SUMMARY_SCHEMA,
        properties,
        sorted(properties),
        comment=(
            "Python validation additionally requires expected_operation_ids and ordered model operation IDs "
            "to be the same canonical set. COMMIT_UNKNOWN is intentionally excluded."
        ),
    )


def _operation_plan_schema() -> dict[str, Any]:
    properties: dict[str, object] = {
        "archetype": {"const": SCOPE_STABLE_EVENT_FACT},
        "deployment_id": DIGEST_SCHEMA,
        "effective_key_columns": {
            "contains": {
                "oneOf": [
                    {
                        "properties": {
                            "source_type": {"const": "date"},
                            "target_type": {"const": "Date"},
                        },
                        "required": ["source_type", "target_type"],
                    },
                    {
                        "properties": {
                            "source_type": {"const": "datetime2(6)"},
                            "target_type": {"const": "DateTime64(6,'UTC')"},
                        },
                        "required": ["source_type", "target_type"],
                    },
                ]
            },
            "items": effective_key_schema(),
            "minContains": 1,
            "minItems": 1,
            "type": "array",
        },
        "effective_key_mapping_sha256": DIGEST_SCHEMA,
        "effective_key_template_sha256": DIGEST_SCHEMA,
        "event_time_column": TEXT_SCHEMA,
        "environment": TEXT_SCHEMA,
        "missing_key_policy": {"const": IGNORE_MISSING},
        "model_definition_proof_sha256": DIGEST_SCHEMA,
        "model_unique_id": TEXT_SCHEMA,
        "mutation_closure_sha256": DIGEST_SCHEMA,
        "mutation_order": {
            "items": False,
            "maxItems": 2,
            "minItems": 2,
            "prefixItems": [{"const": "UPDATE"}, {"const": "INSERT"}],
            "type": "array",
        },
        "operation_id": DIGEST_SCHEMA,
        "operation_kind": {"enum": [item.value for item in WorkflowMode], "type": "string"},
        "operation_plan_sha256": DIGEST_SCHEMA,
        "owner_generation": POSITIVE_INTEGER_SCHEMA,
        "platform_policy_digest": DIGEST_SCHEMA,
        "read_dependency_proof_sha256": DIGEST_SCHEMA,
        "release_id": DIGEST_SCHEMA,
        "replaces_failed_operation_id": DIGEST_SCHEMA,
        "replacement_ordinal": POSITIVE_INTEGER_SCHEMA,
        "replacement_reason": TEXT_SCHEMA,
        "resource_policy_digest": DIGEST_SCHEMA,
        "schema": schema_discriminator(OPERATION_PLAN_SCHEMA),
        "scope_end": UTC_DAY_SCHEMA,
        "scope_family_id": DIGEST_SCHEMA,
        "scope_predecessor_operation_id": DIGEST_SCHEMA,
        "scope_revision": POSITIVE_INTEGER_SCHEMA,
        "scope_start": UTC_DAY_SCHEMA,
        "sqlserver_lifecycle_policy_sha256": DIGEST_SCHEMA,
        "target_predecessor_generation_id": DIGEST_SCHEMA,
        "workflow_id": TEXT_SCHEMA,
        "writer_assurance_digest": DIGEST_SCHEMA,
    }
    required = sorted(
        set(properties)
        - {
            "scope_predecessor_operation_id",
            "replaces_failed_operation_id",
            "replacement_reason",
            "replacement_ordinal",
        }
    )
    return closed_schema(
        OPERATION_PLAN_SCHEMA,
        properties,
        required,
        comment=(
            "operation_id excludes its own field and all attempt/runtime facts; operation_plan_sha256 excludes "
            "itself. Python validation requires the named event_time_column itself to be the temporal key pair."
        ),
        one_of=[
            {
                "not": {
                    "anyOf": [
                        {"required": ["scope_predecessor_operation_id"]},
                        {"required": ["replaces_failed_operation_id"]},
                        {"required": ["replacement_reason"]},
                        {"required": ["replacement_ordinal"]},
                    ]
                },
                "properties": {
                    "operation_kind": {"const": WorkflowMode.NORMAL.value},
                    "scope_revision": {"const": 1},
                },
            },
            {
                "not": {
                    "anyOf": [
                        {"required": ["replaces_failed_operation_id"]},
                        {"required": ["replacement_reason"]},
                        {"required": ["replacement_ordinal"]},
                    ]
                },
                "properties": {
                    "operation_kind": {"const": WorkflowMode.COMPLETE_SCOPE_REPLAY.value},
                    "scope_revision": {"minimum": 2},
                },
                "required": ["scope_predecessor_operation_id"],
            },
            {
                "not": {"required": ["scope_predecessor_operation_id"]},
                "properties": {"operation_kind": {"const": WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT.value}},
                "required": [
                    "replaces_failed_operation_id",
                    "replacement_reason",
                    "replacement_ordinal",
                ],
            },
        ],
    )


def _operation_ref_schema() -> dict[str, object]:
    return {
        "additionalProperties": False,
        "properties": {
            "model_unique_id": TEXT_SCHEMA,
            "operation_id": DIGEST_SCHEMA,
            "operation_plan_sha256": DIGEST_SCHEMA,
        },
        "required": ["model_unique_id", "operation_id", "operation_plan_sha256"],
        "type": "object",
    }


def _workflow_plan_schema() -> dict[str, Any]:
    properties: dict[str, object] = {
        "expected_model_outcome_ids": string_set_schema(),
        "model_operation_plan_ids": string_set_schema(),
        "mutation_closure_sha256": DIGEST_SCHEMA,
        "operation_plan_refs": {"items": _operation_ref_schema(), "minItems": 1, "type": "array"},
        "replacement_action_ids": string_set_schema(allow_empty=True),
        "schema": schema_discriminator(WORKFLOW_PLAN_SCHEMA),
        "scope_end": UTC_DAY_SCHEMA,
        "scope_revision": POSITIVE_INTEGER_SCHEMA,
        "scope_start": UTC_DAY_SCHEMA,
        "selected_mutating_node_ids": string_set_schema(),
        "workflow_mode": {"enum": [item.value for item in WorkflowMode], "type": "string"},
        "workflow_name": TEXT_SCHEMA,
        "workflow_plan_sha256": DIGEST_SCHEMA,
    }
    return closed_schema(
        WORKFLOW_PLAN_SCHEMA,
        properties,
        sorted(properties),
        comment="The three model ID arrays are equal; replacement also equals action IDs. This plan never contains recovery_plan_digest.",
        one_of=[
            {
                "properties": {
                    "replacement_action_ids": {"maxItems": 0},
                    "scope_revision": {"const": 1},
                    "workflow_mode": {"const": WorkflowMode.NORMAL.value},
                }
            },
            {
                "properties": {
                    "replacement_action_ids": {"maxItems": 0},
                    "scope_revision": {"minimum": 2},
                    "workflow_mode": {"const": WorkflowMode.COMPLETE_SCOPE_REPLAY.value},
                }
            },
            {
                "properties": {
                    "replacement_action_ids": string_set_schema(),
                    "workflow_mode": {"const": WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT.value},
                }
            },
        ],
    )


def _replacement_plan_schema() -> dict[str, Any]:
    action = {
        "additionalProperties": False,
        "properties": {
            "action": {"enum": [item.value for item in ReplacementAction], "type": "string"},
            "action_id": TEXT_SCHEMA,
            "outcome": {"enum": [item.value for item in SqlServerModelOutcome], "type": "string"},
        },
        "required": ["action", "action_id", "outcome"],
        "type": "object",
    }
    properties: dict[str, object] = {
        "expected_model_outcome_ids": string_set_schema(),
        "model_operation_plan_ids": string_set_schema(),
        "predecessor_workflow_execution_id": TEXT_SCHEMA,
        "predecessor_workflow_execution_binding_sha256": DIGEST_SCHEMA,
        "predecessor_workflow_summary_sha256": DIGEST_SCHEMA,
        "recovery_plan_digest": DIGEST_SCHEMA,
        "replacement_action_ids": string_set_schema(),
        "replacement_actions": {"items": action, "minItems": 1, "type": "array", "uniqueItems": True},
        "schema": schema_discriminator(WORKFLOW_REPLACEMENT_PLAN_SCHEMA),
        "selected_mutating_node_ids": string_set_schema(),
        "workflow_mode": {"const": WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT.value},
        "workflow_plan_sha256": DIGEST_SCHEMA,
        "workflow_replacement_plan_sha256": DIGEST_SCHEMA,
    }
    return closed_schema(
        WORKFLOW_REPLACEMENT_PLAN_SCHEMA,
        properties,
        sorted(properties),
        comment="All four model ID arrays are equal and each action is derived from its durable MSSQL outcome.",
    )


__all__ = ["plan_contract_schemas"]

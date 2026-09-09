from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract
from dpone.gitops.schema_deployment_cache_primitives import (
    non_empty_string_schema,
    nullable_sha256_schema,
    sha256_schema,
    uuid4_schema,
)


def deployment_cache_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        deployment_cache_retention_plan_contract(),
        deployment_cache_retention_apply_contract(),
        deployment_cache_retention_apply_v2_contract(),
        deployment_cache_retention_apply_v3_contract(),
        deployment_cache_recovery_plan_contract(),
        deployment_cache_recovery_apply_contract(),
    )


def deployment_cache_retention_plan_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="deployment-cache-retention-plan",
        kind="dpone.deployment-cache-retention-plan.v1",
        title="dpone GitOps deployment cache retention plan",
        required=(
            "schema",
            "environment",
            "current_deployment_id",
            "protected_deployment_ids",
            "items",
            "delete_candidates",
        ),
        properties={
            "schema": {"const": "dpone.deployment-cache-retention-plan.v1"},
            "environment": non_empty_string_schema(),
            "current_deployment_id": nullable_sha256_schema(),
            "plan_sha256": sha256_schema(),
            "recovery_revision": sha256_schema(),
            "protected_deployment_ids": {
                "type": "array",
                "items": {"$ref": "#/$defs/sha256"},
                "uniqueItems": True,
            },
            "items": {"type": "array", "items": {"$ref": "#/$defs/item"}},
            "delete_candidates": {"type": "array", "items": {"$ref": "#/$defs/sha256"}},
        },
        defs={
            "sha256": sha256_schema(),
            "item": retention_plan_item_schema(),
        },
    )


def deployment_cache_retention_apply_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="deployment-cache-retention-apply",
        kind="dpone.deployment-cache-retention-apply.v1",
        title="dpone GitOps deployment cache retention apply report",
        required=(
            "schema",
            "environment",
            "promoted_by",
            "current_deployment_id",
            "items",
            "deleted_deployment_ids",
            "skipped_deployment_ids",
        ),
        properties={
            "schema": {"const": "dpone.deployment-cache-retention-apply.v1"},
            "environment": non_empty_string_schema(),
            "promoted_by": non_empty_string_schema(),
            "current_deployment_id": nullable_sha256_schema(),
            "items": {"type": "array", "items": {"$ref": "#/$defs/item"}},
            "deleted_deployment_ids": {"type": "array", "items": {"$ref": "#/$defs/sha256"}},
            "skipped_deployment_ids": {"type": "array", "items": {"$ref": "#/$defs/sha256"}},
        },
        defs={
            "sha256": sha256_schema(),
            "item": retention_apply_v1_item_schema(),
        },
    )


def deployment_cache_retention_apply_v2_contract() -> GitOpsSchemaContract:
    contract = documented_contract(
        name="deployment-cache-retention-apply-v2",
        kind="dpone.deployment-cache-retention-apply.v2",
        title="dpone GitOps deployment cache retention apply report with durable activation history",
        required=(
            "schema",
            "environment",
            "promoted_by",
            "current_deployment_id",
            "items",
            "deleted_deployment_ids",
            "skipped_deployment_ids",
        ),
        properties={
            "schema": {"const": "dpone.deployment-cache-retention-apply.v2"},
            "environment": non_empty_string_schema(),
            "promoted_by": non_empty_string_schema(),
            "current_deployment_id": nullable_sha256_schema(),
            "reviewed_plan_sha256": sha256_schema(),
            "activation_history_revision": sha256_schema(),
            "items": {"type": "array", "items": {"$ref": "#/$defs/item"}},
            "deleted_deployment_ids": {
                "type": "array",
                "items": {"$ref": "#/$defs/sha256"},
                "uniqueItems": True,
            },
            "skipped_deployment_ids": {
                "type": "array",
                "items": {"$ref": "#/$defs/sha256"},
                "uniqueItems": True,
            },
        },
        defs={
            "sha256": sha256_schema(),
            "item": retention_apply_item_schema(additional_properties=False),
        },
        additional_properties=False,
    )
    contract.schema["allOf"] = [
        {
            "if": {
                "properties": {"deleted_deployment_ids": {"minItems": 1}},
                "required": ["deleted_deployment_ids"],
            },
            "then": {
                "required": [
                    "reviewed_plan_sha256",
                    "activation_history_revision",
                ]
            },
        }
    ]
    contract.schema["x-dpone-authoritative-field"] = "items"
    return contract


def deployment_cache_retention_apply_v3_contract() -> GitOpsSchemaContract:
    contract = documented_contract(
        name="deployment-cache-retention-apply-v3",
        kind="dpone.deployment-cache-retention-apply.v3",
        title="dpone GitOps deployment cache retention apply report with durable transaction receipt",
        required=(
            "schema",
            "environment",
            "promoted_by",
            "current_deployment_id",
            "items",
            "deleted_deployment_ids",
            "skipped_deployment_ids",
        ),
        properties={
            "schema": {"const": "dpone.deployment-cache-retention-apply.v3"},
            "environment": non_empty_string_schema(),
            "promoted_by": non_empty_string_schema(),
            "current_deployment_id": nullable_sha256_schema(),
            "reviewed_plan_sha256": sha256_schema(),
            "activation_history_revision": sha256_schema(),
            "operation_id": sha256_schema(),
            "review_id": uuid4_schema(),
            "receipt_revision": sha256_schema(),
            "transaction_status": {"const": "committed"},
            "items": {"type": "array", "items": {"$ref": "#/$defs/item"}},
            "deleted_deployment_ids": {
                "type": "array",
                "items": {"$ref": "#/$defs/sha256"},
                "uniqueItems": True,
            },
            "skipped_deployment_ids": {
                "type": "array",
                "items": {"$ref": "#/$defs/sha256"},
                "uniqueItems": True,
            },
        },
        defs={
            "sha256": sha256_schema(),
            "item": retention_apply_item_schema(additional_properties=False),
        },
        additional_properties=False,
    )
    contract.schema["allOf"] = [
        {
            "if": {
                "properties": {"deleted_deployment_ids": {"minItems": 1}},
                "required": ["deleted_deployment_ids"],
            },
            "then": {
                "required": [
                    "reviewed_plan_sha256",
                    "activation_history_revision",
                    "operation_id",
                    "review_id",
                    "receipt_revision",
                    "transaction_status",
                ]
            },
        }
    ]
    contract.schema["x-dpone-authoritative-field"] = "items"
    return contract


def deployment_cache_recovery_plan_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="deployment-cache-recovery-plan",
        kind="dpone.deployment-cache-recovery-plan.v1",
        title="dpone GitOps deployment cache recovery plan",
        required=(
            "schema",
            "environment",
            "status",
            "current_deployment_id",
            "current_path_deployment_id",
            "preferred_repair_deployment_id",
            "issues",
            "repair_candidates",
        ),
        properties={
            "schema": {"const": "dpone.deployment-cache-recovery-plan.v1"},
            "environment": non_empty_string_schema(),
            "status": {"enum": ["ok", "repairable", "blocked"]},
            "current_deployment_id": nullable_sha256_schema(),
            "current_path_deployment_id": nullable_sha256_schema(),
            "preferred_repair_deployment_id": nullable_sha256_schema(),
            "issues": {"type": "array", "items": {"$ref": "#/$defs/issue"}},
            "repair_candidates": {"type": "array", "items": {"$ref": "#/$defs/candidate"}},
        },
        defs={
            "sha256": sha256_schema(),
            "issue": recovery_issue_schema(),
            "candidate": recovery_candidate_schema(),
        },
    )


def deployment_cache_recovery_apply_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="deployment-cache-recovery-apply",
        kind="dpone.deployment-cache-recovery-apply.v1",
        title="dpone GitOps deployment cache recovery apply report",
        required=("schema", "environment", "recovered_deployment_id", "release_id", "current_path", "pointer_path"),
        properties={
            "schema": {"const": "dpone.deployment-cache-recovery-apply.v1"},
            "environment": non_empty_string_schema(),
            "recovered_deployment_id": {"$ref": "#/$defs/sha256"},
            "release_id": {"$ref": "#/$defs/sha256"},
            "current_path": non_empty_string_schema(),
            "pointer_path": non_empty_string_schema(),
        },
        defs={"sha256": sha256_schema()},
    )


def retention_plan_item_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["deployment_id", "action", "reason", "path"],
        "additionalProperties": True,
        "properties": {
            "deployment_id": nullable_sha256_schema(),
            "action": {"enum": ["protect", "delete", "quarantine"]},
            "reason": {"enum": ["current", "retention_evidence", "unreferenced", "incomplete", "invalid"]},
            "path": non_empty_string_schema(),
            "error_code": {"type": "string", "pattern": "^DPONE_(CACHE|RELEASE|DEPLOYMENT|AIRFLOW)_[A-Z0-9_]+$"},
        },
        "allOf": [
            {
                "if": {"properties": {"deployment_id": {"type": "null"}}, "required": ["deployment_id"]},
                "then": {
                    "properties": {
                        "action": {"const": "quarantine"},
                        "reason": {"enum": ["incomplete", "invalid"]},
                    }
                },
            },
            {
                "if": {"properties": {"action": {"enum": ["protect", "delete"]}}, "required": ["action"]},
                "then": {"properties": {"deployment_id": {"$ref": "#/$defs/sha256"}}},
            },
        ],
    }


def retention_apply_item_schema(*, additional_properties: bool = True) -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["deployment_id", "action", "reason", "path"],
        "additionalProperties": additional_properties,
        "properties": {
            "deployment_id": nullable_sha256_schema(),
            "action": {"enum": ["deleted", "skipped"]},
            "reason": {"enum": ["current", "retention_evidence", "unreferenced", "incomplete", "invalid"]},
            "path": non_empty_string_schema(),
            "error_code": {"type": "string", "pattern": "^DPONE_(CACHE|RELEASE|DEPLOYMENT|AIRFLOW)_[A-Z0-9_]+$"},
        },
        "allOf": [
            {
                "if": {"properties": {"deployment_id": {"type": "null"}}, "required": ["deployment_id"]},
                "then": {
                    "properties": {
                        "action": {"const": "skipped"},
                        "reason": {"enum": ["incomplete", "invalid"]},
                    }
                },
            },
            {
                "if": {"properties": {"action": {"const": "deleted"}}, "required": ["action"]},
                "then": {
                    "properties": {
                        "deployment_id": {"$ref": "#/$defs/sha256"},
                        "reason": {"const": "unreferenced"},
                    }
                },
            },
            {
                "if": {
                    "properties": {"reason": {"enum": ["current", "retention_evidence"]}},
                    "required": ["reason"],
                },
                "then": {"properties": {"action": {"const": "skipped"}}},
            },
        ],
    }


def retention_apply_v1_item_schema() -> dict[str, Any]:
    """Preserve the published v1 item grammar exactly."""

    return {
        "type": "object",
        "required": ["deployment_id", "action", "reason", "path"],
        "additionalProperties": True,
        "properties": {
            "deployment_id": nullable_sha256_schema(),
            "action": {"enum": ["deleted", "skipped"]},
            "reason": {"enum": ["current", "retention_evidence", "unreferenced", "incomplete", "invalid"]},
            "path": non_empty_string_schema(),
            "error_code": {"type": "string", "pattern": "^DPONE_(CACHE|RELEASE|DEPLOYMENT|AIRFLOW)_[A-Z0-9_]+$"},
        },
        "allOf": [
            {
                "if": {"properties": {"deployment_id": {"type": "null"}}, "required": ["deployment_id"]},
                "then": {
                    "properties": {
                        "action": {"const": "skipped"},
                        "reason": {"enum": ["incomplete", "invalid"]},
                    }
                },
            },
            {
                "if": {"properties": {"action": {"const": "deleted"}}, "required": ["action"]},
                "then": {"properties": {"deployment_id": {"$ref": "#/$defs/sha256"}}},
            },
        ],
    }


def recovery_issue_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["code", "severity", "message", "path"],
        "additionalProperties": True,
        "properties": {
            "code": {
                "type": "string",
                "pattern": "^DPONE_(CACHE|CURRENT|DEPLOYMENT|AIRFLOW|RELEASE)_[A-Z0-9_]+$",
            },
            "severity": {"enum": ["error", "warning"]},
            "message": non_empty_string_schema(),
            "path": non_empty_string_schema(),
        },
    }


def recovery_candidate_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["deployment_id", "path", "reason"],
        "additionalProperties": True,
        "properties": {
            "deployment_id": {"$ref": "#/$defs/sha256"},
            "path": non_empty_string_schema(),
            "reason": {"enum": ["current_state", "complete"]},
        },
    }


__all__ = ["deployment_cache_schema_contracts"]

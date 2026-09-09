"""Public schemas for project layout, ownership, and discovery projections."""

from __future__ import annotations

from typing import Any

from dpone.contracts.workload_index import (
    MAX_PROJECT_WORKLOADS,
    WORKLOAD_INDEX_AIRFLOW_FIELDS,
    WORKLOAD_INDEX_DEPENDENCY_FIELDS,
    WORKLOAD_INDEX_FIELDS,
    WORKLOAD_INDEX_ITEM_FIELDS,
    WORKLOAD_INDEX_SCHEMA,
)
from dpone.gitops.schema_contract_primitives import (
    GitOpsSchemaContract,
    array_schema,
    boolean_schema,
    const_schema,
    documented_contract,
    string_schema,
)

_DIGEST = {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}
_ID = {"type": "string", "pattern": "^[a-z0-9][a-z0-9_-]{1,127}$"}
_NON_EMPTY_TEXT = {"type": "string", "minLength": 1}
_PROJECT_PATH = {
    "type": "string",
    "minLength": 1,
    "allOf": [
        {"not": {"pattern": r"(^/|\\|\x00|(^|/)\.{1,2}(/|$))"}},
        {"not": {"pattern": r"//|/$"}},
    ],
}
_LAYOUT_ROOT = {
    **_PROJECT_PATH,
    "allOf": [
        {"pattern": r"^\S(?:.*\S)?$"},
        *_PROJECT_PATH["allOf"],
    ],
}


def self_service_authoring_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        project_config_contract(),
        domain_ownership_contract(),
        domain_dag_contract(),
        workload_index_contract(),
        workload_change_impact_contract(),
        workload_index_promotion_contract(),
    )


def project_config_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="project",
        kind="dpone.project.v1",
        title="dpone GitOps project authoring policy",
        required=("schema",),
        properties={
            "schema": const_schema("dpone.project.v1"),
            "authoring": {
                "type": "object",
                "additionalProperties": True,
                "properties": {"primary_source_policy": string_schema()},
            },
            "airflow": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "enabled": boolean_schema(),
                    "index_path": string_schema(),
                },
            },
            "layout": {
                "type": "object",
                "additionalProperties": False,
                "required": ["mode"],
                "properties": {
                    "mode": {"enum": ["flat", "domain_first"]},
                    "root": _LAYOUT_ROOT,
                    "pipeline_id_scope": {"const": "project"},
                    "system_root": _LAYOUT_ROOT,
                    "dual_read_legacy_catalogs": boolean_schema(),
                },
            },
        },
        additional_properties=True,
    )


def domain_dag_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="domain-dag",
        kind="dpone.domain-dag.v1",
        title="dpone GitOps domain-colocated Airflow DAG authoring",
        required=("schema", "dag_id", "domain", "start_date", "pipelines"),
        properties={
            "schema": const_schema("dpone.domain-dag.v1"),
            "dag_id": {
                "type": "string",
                "minLength": 1,
                "pattern": r"^DAG__[A-Za-z0-9_]+$",
            },
            "domain": _ID,
            "description": {"type": ["string", "null"]},
            "start_date": _NON_EMPTY_TEXT,
            "timezone": {"type": ["string", "null"]},
            "schedule": {
                "oneOf": [
                    {"type": "null"},
                    {"type": "string"},
                    {
                        "type": "object",
                        "required": ["assets"],
                        "additionalProperties": True,
                        "properties": {"assets": {**array_schema({"type": "object"}), "minItems": 1}},
                    },
                ]
            },
            "catchup": boolean_schema(),
            "max_active_runs": {"type": ["integer", "null"], "minimum": 1},
            "tags": array_schema(_NON_EMPTY_TEXT),
            "default_args": {"type": "object", "additionalProperties": True},
            "operator_overrides": {"type": "object", "additionalProperties": True},
            "pipelines": {**array_schema(_ID), "minItems": 1},
            "wiring": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "mode": {"enum": ["waves", "explicit", "assets"]},
                    "max_parallel_workloads": {"type": "integer", "minimum": 1},
                    "dependencies": {
                        "type": "object",
                        "additionalProperties": array_schema(_NON_EMPTY_TEXT),
                    },
                    "visible_task_budget": {"type": "object", "additionalProperties": True},
                },
            },
        },
        additional_properties=False,
    )


def domain_ownership_contract() -> GitOpsSchemaContract:
    text = {
        "type": "string",
        "minLength": 1,
        "maxLength": 256,
        "pattern": r"\S",
        "not": {"pattern": r"^\s*TODO\s*$"},
    }
    return documented_contract(
        name="domain-ownership",
        kind="dpone.domain-ownership.v1",
        title="dpone GitOps domain ownership authority",
        required=("schema", "domain", "owner", "approvers"),
        properties={
            "schema": const_schema("dpone.domain-ownership.v1"),
            "domain": _ID,
            "owner": _closed_object(
                required=("team", "contact"),
                properties={"team": text, "contact": text},
            ),
            "approvers": _closed_object(
                required=("github_team",),
                properties={"github_team": text},
            ),
        },
        additional_properties=False,
    )


def workload_index_contract() -> GitOpsSchemaContract:
    dependency = _closed_object(
        required=WORKLOAD_INDEX_DEPENDENCY_FIELDS,
        properties={"kind": _NON_EMPTY_TEXT, "path": _PROJECT_PATH, "sha256": _DIGEST},
    )
    workload = _closed_object(
        required=WORKLOAD_INDEX_ITEM_FIELDS,
        properties={
            "pipeline_id": _ID,
            "domain": _ID,
            "owner": {"type": ["string", "null"], "minLength": 1, "maxLength": 256},
            "ownership_fingerprint": _DIGEST,
            "authoring_source": _PROJECT_PATH,
            "source_sha256": _DIGEST,
            "semantic_fingerprint": _DIGEST,
            "workload_fingerprint": _DIGEST,
            "connection_refs": {
                "type": "array",
                "items": _NON_EMPTY_TEXT,
                "uniqueItems": True,
            },
            "dependencies": array_schema(dependency),
            "airflow": _closed_object(
                required=WORKLOAD_INDEX_AIRFLOW_FIELDS,
                properties={
                    "enabled": boolean_schema(),
                    "dag_id": _NON_EMPTY_TEXT,
                    "schedule": {"type": ["string", "null"]},
                },
            ),
        },
    )
    return documented_contract(
        name="workload-index",
        kind=WORKLOAD_INDEX_SCHEMA,
        title="dpone GitOps ephemeral workload index",
        required=WORKLOAD_INDEX_FIELDS,
        properties={
            "schema": const_schema(WORKLOAD_INDEX_SCHEMA),
            "project_fingerprint": _DIGEST,
            "layout_mode": {"const": "domain_first"},
            "layout_root": _LAYOUT_ROOT,
            "pipeline_id_scope": {"const": "project"},
            "workloads": {
                "type": "array",
                "items": workload,
                "uniqueItems": True,
                "maxItems": MAX_PROJECT_WORKLOADS,
            },
        },
        additional_properties=False,
    )


def workload_change_impact_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="workload-change-impact",
        kind="dpone.workload-change-impact.v1",
        title="dpone GitOps workload discovery change impact",
        required=(
            "schema",
            "baseline_fingerprint",
            "current_fingerprint",
            "baseline_content_sha256",
            "current_content_sha256",
            "current_source",
            "validation_status",
            "discovery_status",
            "issues",
            "added",
            "modified",
            "removed",
        ),
        properties={
            "schema": const_schema("dpone.workload-change-impact.v1"),
            "baseline_fingerprint": {"type": ["string", "null"], "pattern": "^sha256:[0-9a-f]{64}$"},
            "current_fingerprint": _DIGEST,
            "baseline_content_sha256": {"type": ["string", "null"], "pattern": "^sha256:[0-9a-f]{64}$"},
            "current_content_sha256": {"type": ["string", "null"], "pattern": "^sha256:[0-9a-f]{64}$"},
            "current_source": {"enum": ["candidate", "discovery"]},
            "validation_status": {"const": "passed"},
            "discovery_status": {"enum": ["passed", "failed", "not_run"]},
            "issues": array_schema(
                _closed_object(
                    required=("code", "message", "path"),
                    properties={
                        "code": string_schema(),
                        "message": string_schema(),
                        "path": {"type": ["string", "null"]},
                    },
                )
            ),
            "added": array_schema(_ID),
            "modified": array_schema(_ID),
            "removed": array_schema(_ID),
        },
        additional_properties=False,
    )


def workload_index_promotion_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="workload-index-promotion",
        kind="dpone.workload-index-promotion.v1",
        title="dpone GitOps approval-bound workload-index promotion receipt",
        required=(
            "schema",
            "status",
            "mode",
            "baseline_path",
            "baseline_before_sha256",
            "baseline_before_fingerprint",
            "promoted_sha256",
            "promoted_fingerprint",
        ),
        properties={
            "schema": const_schema("dpone.workload-index-promotion.v1"),
            "status": {"const": "promoted"},
            "mode": {"enum": ["bootstrap", "change"]},
            "baseline_path": _PROJECT_PATH,
            "baseline_before_sha256": {"type": ["string", "null"], "pattern": "^sha256:[0-9a-f]{64}$"},
            "baseline_before_fingerprint": {
                "type": ["string", "null"],
                "pattern": "^sha256:[0-9a-f]{64}$",
            },
            "promoted_sha256": _DIGEST,
            "promoted_fingerprint": _DIGEST,
        },
        additional_properties=False,
    )


def _closed_object(
    *,
    required: tuple[str, ...],
    properties: dict[str, Any],
) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": list(required),
        "properties": properties,
    }


__all__ = ["self_service_authoring_schema_contracts"]

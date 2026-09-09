from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import (
    GitOpsSchemaContract,
    array_schema,
    boolean_schema,
    documented_contract,
    integer_schema,
    object_schema,
    string_schema,
)

_SHA256 = {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}
_SELECTION_TEXT = {"type": "string", "minLength": 1, "maxLength": 256}
_SELECTION_TEXT_LIST = {"type": "array", "maxItems": 1000, "items": _SELECTION_TEXT}


def airflow_authoring_migration_plan_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-authoring-migration-plan",
        kind="dpone.airflow-authoring-migration-plan.v1",
        title="dpone GitOps Airflow authoring migration plan",
        required=(
            "kind",
            "schema",
            "mode",
            "apply",
            "source_path",
            "summary",
            "changes",
        ),
        properties={
            "kind": {"const": "dpone.airflow-authoring-migration-plan.v1"},
            "schema": {"const": "dpone.airflow-authoring-migration-plan.v1"},
            "mode": {"const": "plan"},
            "apply": {"const": False},
            "source_path": string_schema(),
            "summary": airflow_authoring_migration_summary_schema(),
            "changes": array_schema(airflow_authoring_migration_change_schema()),
        },
    )


def airflow_authoring_fix_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-authoring-fix",
        kind="dpone.airflow-authoring-fix.v1",
        title="dpone GitOps Airflow authoring fix report",
        required=(
            "kind",
            "schema",
            "passed",
            "changes",
            "errors",
            "mode",
            "apply",
            "applied",
            "target",
            "summary",
        ),
        properties={
            "kind": {"const": "dpone.airflow-authoring-fix.v1"},
            "schema": {"const": "dpone.airflow-authoring-fix.v1"},
            "passed": boolean_schema(),
            "changes": array_schema(airflow_authoring_fix_change_schema()),
            "errors": array_schema(object_schema()),
            "mode": {"enum": ["plan", "apply"]},
            "apply": boolean_schema(),
            "applied": boolean_schema(),
            "target": string_schema(),
            "summary": airflow_authoring_fix_summary_schema(),
            "migration_plan": airflow_authoring_migration_plan_contract().schema,
        },
    )


def airflow_authoring_migration_summary_schema() -> dict[str, object]:
    return object_schema(
        required=("legacy_sections", "manual_review_required", "apply_supported"),
        properties={
            "legacy_sections": integer_schema(),
            "manual_review_required": boolean_schema(),
            "apply_supported": boolean_schema(),
        },
    )


def airflow_authoring_migration_change_schema() -> dict[str, object]:
    return object_schema(
        required=(
            "action",
            "path",
            "suggested_connection_ref",
            "detected",
            "proposed_section",
            "manual_review_required",
            "unified_diff",
        ),
        properties={
            "action": {"const": "replace_legacy_connection_config"},
            "path": string_schema(),
            "suggested_connection_ref": string_schema(),
            "detected": object_schema(),
            "proposed_section": object_schema(),
            "manual_review_required": boolean_schema(),
            "unified_diff": string_schema(),
        },
    )


def airflow_authoring_fix_summary_schema() -> dict[str, object]:
    return object_schema(
        required=("legacy_sections", "applied_sections", "no_op"),
        properties={
            "legacy_sections": integer_schema(),
            "applied_sections": integer_schema(),
            "no_op": boolean_schema(),
        },
    )


def airflow_authoring_fix_change_schema() -> dict[str, object]:
    return object_schema(
        required=("action", "path", "message", "diff"),
        properties={
            "action": {"enum": ["plan_modify", "modify"]},
            "path": string_schema(),
            "message": string_schema(),
            "diff": string_schema(),
        },
    )


def selection_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    """Return public build-plane workload selection state/report schemas."""

    return (_selection_state_contract(), _selection_report_contract())


def _selection_state_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="selection-state",
        kind="dpone.selection-state.v1",
        title="dpone GitOps workload selection state",
        required=("schema", "state_fingerprint", "nodes"),
        properties={
            "schema": {"const": "dpone.selection-state.v1"},
            "state_fingerprint": _SHA256,
            "nodes": {"type": "array", "maxItems": 1000, "items": {"$ref": "#/$defs/state_node"}},
        },
        defs={"sha256": _SHA256, "state_node": _selection_state_node_schema()},
        additional_properties=False,
    )


def _selection_report_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="selection-report",
        kind="dpone.selection-report.v1",
        title="dpone GitOps explainable workload selection report",
        required=(
            "schema",
            "selection_fingerprint",
            "catalog_fingerprint",
            "state_fingerprint",
            "expressions",
            "selected",
            "excluded",
            "unmatched",
            "removed",
        ),
        properties={
            "schema": {"const": "dpone.selection-report.v1"},
            "selection_fingerprint": _SHA256,
            "catalog_fingerprint": _SHA256,
            "state_fingerprint": {"oneOf": [_SHA256, {"type": "null"}]},
            "expressions": {
                "type": "object",
                "required": ["select", "exclude"],
                "additionalProperties": False,
                "properties": {
                    "select": {"$ref": "#/$defs/expression_list"},
                    "exclude": {"$ref": "#/$defs/expression_list"},
                },
            },
            "selected": {"$ref": "#/$defs/entry_list"},
            "excluded": {"$ref": "#/$defs/entry_list"},
            "unmatched": {"$ref": "#/$defs/expression_list"},
            "removed": _SELECTION_TEXT_LIST,
        },
        defs={
            "sha256": _SHA256,
            "expression_list": {
                "type": "array",
                "maxItems": 1000,
                "items": {"type": "string", "minLength": 1, "maxLength": 256},
            },
            "entry_list": {
                "type": "array",
                "maxItems": 1000,
                "items": {"$ref": "#/$defs/entry"},
            },
            "entry": _selection_entry_schema(),
            "reason": _selection_reason_schema(),
        },
        additional_properties=False,
    )


def _selection_state_node_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["id", "semantic_fingerprint", "graph_fingerprint"],
        "additionalProperties": False,
        "properties": {
            "id": _SELECTION_TEXT,
            "semantic_fingerprint": _SHA256,
            "graph_fingerprint": _SHA256,
        },
    }


def _selection_entry_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "id",
            "source",
            "semantic_fingerprint",
            "graph_fingerprint",
            "domain",
            "owner",
            "tags",
            "sources",
            "sinks",
            "groups",
            "reasons",
        ],
        "additionalProperties": False,
        "properties": {
            "id": _SELECTION_TEXT,
            "source": _SELECTION_TEXT,
            "semantic_fingerprint": _SHA256,
            "graph_fingerprint": _SHA256,
            "domain": {"oneOf": [_SELECTION_TEXT, {"type": "null"}]},
            "owner": {"oneOf": [_SELECTION_TEXT, {"type": "null"}]},
            "tags": _SELECTION_TEXT_LIST,
            "sources": _SELECTION_TEXT_LIST,
            "sinks": _SELECTION_TEXT_LIST,
            "groups": _SELECTION_TEXT_LIST,
            "reasons": {
                "type": "array",
                "minItems": 1,
                "maxItems": 1000,
                "items": {"$ref": "#/$defs/reason"},
            },
        },
    }


def _selection_reason_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["kind", "expression", "root", "path"],
        "additionalProperties": False,
        "properties": {
            "kind": {"enum": ["all_by_default", "direct", "named", "state", "ancestor", "descendant"]},
            "expression": {"type": "string", "minLength": 1, "maxLength": 256},
            "root": _SELECTION_TEXT,
            "path": {
                "type": "array",
                "minItems": 1,
                "maxItems": 1000,
                "items": _SELECTION_TEXT,
            },
        },
    }


__all__ = [
    "airflow_authoring_fix_change_schema",
    "airflow_authoring_fix_contract",
    "airflow_authoring_fix_summary_schema",
    "airflow_authoring_migration_change_schema",
    "airflow_authoring_migration_plan_contract",
    "airflow_authoring_migration_summary_schema",
    "selection_schema_contracts",
]

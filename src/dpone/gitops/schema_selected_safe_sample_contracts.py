from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract


from typing import Any

from dpone.gitops.schema_contract_primitives import documented_contract


def selected_safe_sample_schema_contract() -> GitOpsSchemaContract:
    """Return the public aggregate report contract for selected safe samples."""

    return documented_contract(
        name="selected-safe-sample-report",
        kind="dpone.selected-safe-sample-report.v1",
        title="dpone GitOps selected safe sample report",
        required=("schema", "passed", "exit_code", "selection", "results", "unscheduled", "errors"),
        properties={
            "schema": {"const": "dpone.selected-safe-sample-report.v1"},
            "passed": {"type": "boolean"},
            "exit_code": {"type": "integer", "minimum": 0, "maximum": 5},
            "sample": {"type": ["integer", "null"], "minimum": 1},
            "target": {"type": ["string", "null"], "enum": ["temporary", None]},
            "environment": {"type": "string", "minLength": 1},
            "selection": selection_schema(),
            "results": {
                "type": "array",
                "items": {"$ref": "#/$defs/result"},
            },
            "unscheduled": {
                "type": "array",
                "items": non_empty_string_schema(),
                "uniqueItems": True,
            },
            "errors": {
                "type": "array",
                "items": {"$ref": "#/$defs/error"},
            },
        },
        defs={
            "result": selected_workload_result_schema(),
            "error": error_schema(),
        },
        additional_properties=False,
    )


def selection_schema() -> dict[str, Any]:
    return {
        "anyOf": [
            {"type": "null"},
            {
                "type": "object",
                "required": ["schema", "selection_fingerprint"],
                "properties": {
                    "schema": {"const": "dpone.selection-report.v1"},
                    "selection_fingerprint": sha256_schema(),
                },
                "additionalProperties": True,
            },
        ]
    }


def selected_workload_result_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "workload_id",
            "source",
            "run_id",
            "passed",
            "status",
            "exit_code",
            "errors",
            "evidence_path",
        ],
        "properties": {
            "workload_id": non_empty_string_schema(),
            "source": non_empty_string_schema(),
            "run_id": {"type": "string"},
            "passed": {"type": "boolean"},
            "status": non_empty_string_schema(),
            "exit_code": {"type": "integer", "minimum": 0, "maximum": 5},
            "errors": {
                "type": "array",
                "items": {"$ref": "#/$defs/error"},
            },
            "evidence_path": {
                "anyOf": [
                    {"type": "null"},
                    non_empty_string_schema(),
                ]
            },
        },
        "additionalProperties": False,
    }


def error_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["code", "message"],
        "properties": {
            "schema": {"const": "dpone.error.v1"},
            "code": {"type": "string", "pattern": "^DPONE_[A-Z0-9_]+$"},
            "stage": non_empty_string_schema(),
            "severity": {"enum": ["info", "warning", "error"]},
            "message": non_empty_string_schema(),
            "entity": {"type": "object"},
            "fixes": {"type": "array"},
            "docs_url": non_empty_string_schema(),
            "trace_id": non_empty_string_schema(),
        },
        "additionalProperties": True,
    }


def non_empty_string_schema() -> dict[str, Any]:
    return {"type": "string", "minLength": 1}


def sha256_schema() -> dict[str, str]:
    return {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}


__all__ = ["selected_safe_sample_schema_contract"]

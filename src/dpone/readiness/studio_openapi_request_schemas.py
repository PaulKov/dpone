"""Strict Studio request schemas shared by generated OpenAPI."""

from __future__ import annotations

from typing import Any


def studio_openapi_request_schemas(
    object_schema: dict[str, Any],
) -> dict[str, Any]:
    return {
        "ManifestDraftRequest": {
            "type": "object",
            "additionalProperties": False,
            "required": ["source_type", "sink_type", "strategy"],
            "properties": {
                "source_type": {"type": "string", "minLength": 1},
                "sink_type": {"type": "string", "minLength": 1},
                "strategy": {"type": "string", "minLength": 1},
                "source_connection": {"type": "string", "minLength": 1},
                "sink_connection": {"type": "string", "minLength": 1},
                "source_schema": {"type": "string", "minLength": 1},
                "source_table": {"type": "string", "minLength": 1},
                "target_schema": {"type": "string", "minLength": 1},
                "target_table": {"type": "string", "minLength": 1},
                "unique_key": {"type": "string", "minLength": 1},
                "manifest_path": {"type": "string", "minLength": 1},
                "quality_checks": {
                    "type": "array",
                    "minItems": 1,
                    "items": {"type": "string"},
                    "uniqueItems": True,
                },
            },
        },
        "PlanRequest": {
            "oneOf": [
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["manifest_path"],
                    "properties": {
                        "manifest_path": {"type": "string", "minLength": 1},
                        "selector": {"type": "string", "minLength": 1},
                    },
                },
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["manifest_yaml"],
                    "properties": {
                        "manifest_yaml": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 1048576,
                        },
                        "selector": {"type": "string", "minLength": 1},
                    },
                },
            ]
        },
        "StaticCheckRequest": {
            "oneOf": [
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["pipeline_ref"],
                    "properties": {
                        "pipeline_ref": {"type": "string", "minLength": 1},
                    },
                },
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["manifest_yaml"],
                    "properties": {
                        "manifest_yaml": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 1048576,
                        },
                    },
                },
            ]
        },
        "QualityCheckRequest": {
            "type": "object",
            "additionalProperties": False,
            "required": ["rows", "checks"],
            "properties": {
                "rows": {"type": "array", "items": object_schema},
                "checks": {"type": "array", "items": object_schema},
            },
        },
        "GitOpsPrepareRequest": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "manifest_path": {"type": "string", "minLength": 1},
                "artifact_path": {"type": "string", "minLength": 1},
                "branch": {"type": "string", "minLength": 1},
            },
        },
        "ReconciliationPreviewRequest": {
            "type": "object",
            "additionalProperties": False,
            "required": ["source_type", "sink_type"],
            "properties": {
                "source_type": {"type": "string"},
                "sink_type": {"type": "string"},
                "unique_key": {"type": "string"},
                "apply_deletes": {"type": "boolean"},
            },
        },
    }


__all__ = ["studio_openapi_request_schemas"]

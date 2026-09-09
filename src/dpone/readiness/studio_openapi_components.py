"""Reusable OpenAPI component definitions for the Studio adapter."""

from __future__ import annotations

import json
from importlib import resources
from typing import Any

from dpone.readiness.studio_openapi_request_schemas import (
    studio_openapi_request_schemas,
)
from dpone.readiness.studio_openapi_response_schemas import (
    studio_openapi_response_schemas,
)


def studio_openapi_schemas() -> dict[str, Any]:
    """Return the canonical Studio OpenAPI component schemas."""
    object_schema = {"type": "object", "additionalProperties": True}
    return {
        "ObjectResponse": object_schema,
        "OpenApiDocument": {
            "type": "object",
            "required": ["openapi", "info", "paths", "components"],
            "properties": {
                "openapi": {"const": "3.1.0"},
                "info": {"type": "object"},
                "paths": {"type": "object"},
                "components": {"type": "object"},
            },
        },
        "HealthResponse": {
            "type": "object",
            "additionalProperties": False,
            "required": ["status", "service", "version"],
            "properties": {
                "status": {"const": "ok"},
                "service": {"const": "dpone-studio-api"},
                "version": {"const": "v1"},
            },
        },
        "StudioMetaResponse": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "schema",
                "service",
                "version",
                "mode",
                "read_only",
                "ui_status",
                "ui_version",
                "ui_reason_code",
                "usability_status",
                "release_verdict",
                "release_reason_code",
                "snapshot_id",
            ],
            "properties": {
                "schema": {"const": "dpone.studio-meta.v1"},
                "service": {"const": "dpone-studio-api"},
                "version": {"const": "v1"},
                "mode": {"const": "local_development_adapter"},
                "read_only": {"const": True},
                "ui_status": {"enum": ["installed", "not_installed", "incompatible"]},
                "ui_version": {"type": ["string", "null"]},
                "ui_reason_code": {"type": "string"},
                "usability_status": {"const": "UNVERIFIED"},
                "release_verdict": {"const": "NO-GO"},
                "release_reason_code": {"const": "studio_usability_evidence_missing"},
                "snapshot_id": {"type": "string", "pattern": "^sha256:"},
            },
        },
        "CapabilityDiscoveryResponse": _canonical_schema_component(
            "capability-discovery.schema.json",
            component_name="CapabilityDiscoveryResponse",
        ),
        "RecipeListResponse": {
            "type": "object",
            "additionalProperties": False,
            "required": ["schema", "passed", "snapshot_id", "recipes", "issues"],
            "properties": {
                "schema": {"const": "dpone.recipe-discovery-list.v1"},
                "passed": {"type": "boolean"},
                "snapshot_id": {"type": "string", "pattern": "^sha256:"},
                "recipes": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/CapabilityDiscoveryResponse/$defs/recipe"},
                },
                "issues": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/CapabilityDiscoveryResponse/$defs/issue"},
                },
            },
        },
        "PipelineSummaryListResponse": {
            "type": "object",
            "required": ["schema", "items", "count", "total"],
            "properties": {
                "schema": {"const": "dpone.pipeline-summary-list.v1"},
                "items": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/PipelineSummary"},
                },
                "count": {"type": "integer", "minimum": 0},
                "total": {"type": "integer", "minimum": 0},
            },
        },
        "PipelineSummary": _canonical_schema_component(
            "pipeline-summary.schema.json",
            component_name="PipelineSummary",
        ),
        **studio_openapi_response_schemas(object_schema),
        "ManifestDraftResponse": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "schema",
                "valid",
                "status",
                "manifest_path",
                "manifest_yaml",
                "quality_checks",
                "quality_gates",
                "commands",
                "warnings",
            ],
            "properties": {
                "schema": {"const": "dpone.manifest-draft.v1"},
                "valid": {"type": "boolean"},
                "status": {"type": "string"},
                "manifest_path": {"type": "string"},
                "manifest_yaml": {"type": "string"},
                "quality_checks": {"type": "array", "items": object_schema},
                "quality_gates": {"type": "array", "items": object_schema},
                "commands": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/StructuredCommand"},
                },
                "warnings": {"type": "array", "items": {"type": "string"}},
            },
        },
        "NextAction": {
            "type": "object",
            "additionalProperties": False,
            "required": ["id", "label", "argv"],
            "properties": {
                "id": {"type": "string", "minLength": 1},
                "label": {"type": "string", "minLength": 1},
                "argv": {"type": "array", "items": {"type": "string"}},
            },
        },
        "ArtifactState": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "manifest",
                "dag_spec",
                "airflow_pack",
                "published_deployment_id",
                "published_generation",
            ],
            "properties": {
                "manifest": {"enum": ["source", "invalid", "missing"]},
                "dag_spec": {"enum": ["planned", "materialized", "stale", "unavailable"]},
                "airflow_pack": {"enum": ["planned", "materialized", "stale", "unavailable"]},
                "published_deployment_id": {"type": ["string", "null"]},
                "published_generation": {"type": ["string", "null"]},
            },
        },
        "StructuredCommand": {
            "type": "object",
            "additionalProperties": False,
            "required": ["argv"],
            "properties": {
                "argv": {"type": "array", "minItems": 1, "items": {"type": "string"}},
                "stdout_path": {"type": "string"},
            },
        },
        "ErrorFix": {
            "type": "object",
            "additionalProperties": False,
            "required": ["id", "safety"],
            "properties": {
                "id": {"type": "string"},
                "safety": {"enum": ["safe", "manual", "destructive"]},
                "argv": {
                    "type": "array",
                    "minItems": 1,
                    "items": {"type": "string"},
                },
            },
        },
        "DponeError": {
            "type": "object",
            "additionalProperties": True,
            "required": [
                "schema",
                "code",
                "stage",
                "severity",
                "message",
                "fixes",
            ],
            "properties": {
                "schema": {"const": "dpone.error.v1"},
                "code": {"type": "string"},
                "stage": {"type": "string"},
                "severity": {"enum": ["info", "warning", "error", "critical"]},
                "message": {"type": "string"},
                "fixes": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/ErrorFix"},
                },
                "docs_url": {"type": "string"},
                "trace_id": {"type": "string"},
            },
        },
        "ErrorEnvelope": {
            "type": "object",
            "additionalProperties": False,
            "required": ["passed", "errors"],
            "properties": {
                "passed": {"const": False},
                "errors": {
                    "type": "array",
                    "minItems": 1,
                    "items": {"$ref": "#/components/schemas/DponeError"},
                },
            },
        },
        **studio_openapi_request_schemas(object_schema),
    }


def _canonical_schema_component(
    filename: str,
    *,
    component_name: str,
) -> dict[str, Any]:
    payload = json.loads(resources.files("dpone").joinpath("schema").joinpath(filename).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"Studio schema resource is not a JSON object: {filename}")
    payload.pop("$schema", None)
    payload.pop("$id", None)
    return _rewrite_local_refs(payload, component_name=component_name)


def _rewrite_local_refs(value: Any, *, component_name: str) -> Any:
    if isinstance(value, dict):
        return {
            key: (
                item.replace(
                    "#/$defs/",
                    f"#/components/schemas/{component_name}/$defs/",
                    1,
                )
                if key == "$ref" and isinstance(item, str) and item.startswith("#/$defs/")
                else _rewrite_local_refs(item, component_name=component_name)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_rewrite_local_refs(item, component_name=component_name) for item in value]
    return value


__all__ = ["studio_openapi_schemas"]

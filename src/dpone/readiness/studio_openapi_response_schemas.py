"""Typed canonical response schemas for the Studio OpenAPI contract."""

from __future__ import annotations

from typing import Any


def studio_openapi_response_schemas(
    object_schema: dict[str, Any],
) -> dict[str, Any]:
    plan_sections: dict[str, Any] = {
        name: {"$ref": "#/components/schemas/PlanSection"}
        for name in (
            "staging",
            "schema_evolution",
            "type_fidelity",
            "type_matrix",
            "type_inference",
            "physical_design",
            "reconciliation",
            "state",
            "partitioning",
            "runtime_storage",
            "native_transfer_execution",
            "native_transfer_transport",
            "native_transfer_bulk_wire",
            "native_transfer_snapshot_optimization",
            "native_transfer_route_decision",
            "postgres_mssql_wire",
            "columnar_fast_path",
            "quality",
            "strategy_intelligence",
        )
    }
    plan_sections["postgres_mssql_correctness"] = {
        "oneOf": [{"type": "null"}, {"$ref": "#/components/schemas/PlanSection"}]
    }
    return {
        "PipelineExplainResponse": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "kind",
                "pipeline_ref",
                "passed",
                "changes",
                "errors",
                "source_path",
                "artifact_state",
                "operator_diagnostics",
                "hint",
                "next_actions",
            ],
            "properties": {
                "kind": {"const": "dpone.airflow-explain.v1"},
                "pipeline_ref": {"type": "string"},
                "passed": {"type": "boolean"},
                "changes": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/Change"},
                },
                "errors": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/DponeError"},
                },
                "source_path": {"type": ["string", "null"]},
                "artifact_state": {"$ref": "#/components/schemas/ArtifactState"},
                "operator_diagnostics": {"$ref": "#/components/schemas/OperatorDiagnostics"},
                "hint": {"type": "string"},
                "next_actions": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/NextAction"},
                },
                "exit_code": {"type": ["integer", "null"]},
            },
        },
        "OperatorDiagnostics": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "kind",
                "status",
                "operator_pinning",
                "parse_side_effects",
                "checks",
                "summary",
                "next_actions",
            ],
            "properties": {
                "kind": {"const": "dpone.airflow-operator-diagnostics.v1"},
                "status": {
                    "enum": [
                        "planned",
                        "invalid",
                        "materialized",
                        "operator_issues",
                        "operator_warnings",
                        "not_applicable",
                    ]
                },
                "index_path": {"type": "string"},
                "operator_pinning": {"enum": ["planned", "incomplete", "pinned", "not_applicable"]},
                "parse_side_effects": {"$ref": "#/components/schemas/ParseSideEffects"},
                "checks": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/DiagnosticCheck"},
                },
                "summary": {"$ref": "#/components/schemas/DiagnosticSummary"},
                "next_actions": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/DiagnosticAction"},
                },
                "release_id": {"type": ["string", "null"]},
                "deployment_id": {"type": ["string", "null"]},
                "runtime_image_digest": {"type": ["string", "null"]},
                "binding_set_ref": {"type": ["string", "null"]},
                "connection_registry_ref": {"type": ["string", "null"]},
                "credential_runtime_ref": {"type": ["string", "null"]},
                "airflow_bundle_ref": {"type": ["string", "null"]},
                "airflow_bundle": {
                    "oneOf": [{"type": "null"}, object_schema],
                },
                "runtime_artifact_delivery": object_schema,
                "workload_operators": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/WorkloadOperatorSummary"},
                },
            },
        },
        "ParseSideEffects": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "network",
                "metadata_db",
                "airflow_variables",
                "airflow_connections",
                "vault",
                "kubernetes",
                "cache_refresh",
            ],
            "properties": {
                name: {"type": "boolean"}
                for name in (
                    "network",
                    "metadata_db",
                    "airflow_variables",
                    "airflow_connections",
                    "vault",
                    "kubernetes",
                    "cache_refresh",
                )
            },
        },
        "DiagnosticCheck": {
            "type": "object",
            "additionalProperties": False,
            "required": ["code", "status", "message"],
            "properties": {
                "code": {"type": "string"},
                "status": {"enum": ["passed", "warning", "failed"]},
                "message": {"type": "string"},
            },
        },
        "DiagnosticSummary": {
            "type": "object",
            "additionalProperties": False,
            "required": ["passed", "warning", "failed"],
            "properties": {
                "passed": {"type": "integer", "minimum": 0},
                "warning": {"type": "integer", "minimum": 0},
                "failed": {"type": "integer", "minimum": 0},
            },
        },
        "DiagnosticAction": {
            "type": "object",
            "additionalProperties": False,
            "required": ["code", "action", "safety"],
            "properties": {
                "code": {"type": "string"},
                "action": {"type": "string"},
                "safety": {"const": "manual"},
            },
        },
        "WorkloadOperatorSummary": {
            "type": "object",
            "additionalProperties": False,
            "required": ["workload_id", "task_id", "operator"],
            "properties": {
                "workload_id": {"type": ["string", "null"]},
                "task_id": {"type": ["string", "null"]},
                "operator": {"type": "string"},
                "airflow_connection_bridge": object_schema,
            },
        },
        "PlanResponse": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "dry_run",
                "process",
                "selector",
                "source",
                "sink",
                "strategy",
                "quality",
                "warnings",
            ],
            "properties": {
                "dry_run": {"const": True},
                "process": {"type": "string"},
                "selector": {"type": "string"},
                "source": {"$ref": "#/components/schemas/PlanEndpoint"},
                "sink": {"$ref": "#/components/schemas/PlanEndpoint"},
                "strategy": {"$ref": "#/components/schemas/PlanStrategy"},
                "bulk_path": {"type": "string"},
                "estimated_rows": {"type": ["integer", "null"]},
                "warnings": {"type": "array", "items": {"type": "string"}},
                "source_impact": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/PlanIssue"},
                },
                **plan_sections,
            },
        },
        "PlanEndpoint": {
            "type": "object",
            "additionalProperties": False,
            "required": ["type", "connection_id", "table"],
            "properties": {
                "type": {"type": "string"},
                "connection_id": {"type": "string"},
                "table": {"type": "string"},
                "columns": {"type": "array", "items": {"type": "string"}},
            },
        },
        "PlanStrategy": {
            "type": "object",
            "additionalProperties": False,
            "required": ["mode", "unique_key", "merge_policy"],
            "properties": {
                "mode": {"type": "string"},
                "merge_policy": {"type": "string"},
                "unique_key": {
                    "oneOf": [
                        {"type": "null"},
                        {"type": "string"},
                        {"type": "array", "items": {"type": "string"}},
                    ]
                },
                "mssql_contract": {"$ref": "#/components/schemas/PlanSection"},
            },
        },
        "PlanSection": object_schema,
        "PlanIssue": {
            "type": "object",
            "additionalProperties": False,
            "required": ["code", "severity", "message", "action"],
            "properties": {
                "code": {"type": "string"},
                "severity": {"enum": ["info", "warning", "error", "critical"]},
                "message": {"type": "string"},
                "action": {"type": "string"},
            },
        },
        "StaticCheckResponse": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "passed",
                "changes",
                "errors",
                "network",
                "secrets",
                "source_queries",
            ],
            "properties": {
                "passed": {"type": "boolean"},
                "changes": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/Change"},
                },
                "errors": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/DponeError"},
                },
                "network": {"type": "boolean"},
                "secrets": {"type": "boolean"},
                "source_queries": {"type": "boolean"},
                "status": {"type": "string"},
                "mode": {"type": "string"},
                "authoring_mode": {"type": "string"},
                "source_kind": {"type": "string"},
                "canonical_kind": {"type": "string"},
                "source_fingerprint": {
                    "type": "string",
                    "pattern": "^sha256:",
                },
                "semantic_fingerprint": {
                    "type": "string",
                    "pattern": "^sha256:",
                },
                "deprecated_aliases": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "source_files": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/SourceFile"},
                },
                "recipe_resolution": object_schema,
                "airflow_authoring_migration_plan": object_schema,
            },
        },
        "Change": {
            "type": "object",
            "additionalProperties": False,
            "required": ["action", "path", "message", "diff"],
            "properties": {
                "action": {"type": "string"},
                "path": {"type": "string"},
                "message": {"type": "string"},
                "diff": {"type": "string"},
            },
        },
        "SourceFile": {
            "type": "object",
            "additionalProperties": False,
            "required": ["kind", "path", "fingerprint"],
            "properties": {
                "kind": {"type": "string"},
                "path": {"type": "string"},
                "fingerprint": {"type": "string", "pattern": "^sha256:"},
            },
        },
    }


__all__ = ["studio_openapi_response_schemas"]

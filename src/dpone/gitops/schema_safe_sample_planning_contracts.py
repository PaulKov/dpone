from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, connection_ref_schema, documented_contract
from dpone.gitops.schema_safe_sample_common import estimated_read_bytes_schema, full_scan_positive_estimate_guard


def safe_sample_planning_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        safe_sample_cli_argument_validation_contract(),
        safe_sample_source_request_contract(),
        safe_sample_data_copier_registry_contract(),
        safe_sample_certified_copy_request_contract(),
    )


def safe_sample_cli_argument_validation_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="safe-sample-cli-argument-validation",
        kind="dpone.safe-sample-cli-argument-validation.v1",
        title="dpone GitOps safe sample CLI argument validation",
        required=("schema", "errors"),
        properties={
            "schema": {"const": "dpone.safe-sample-cli-argument-validation.v1"},
            "errors": {
                "type": "array",
                "minItems": 1,
                "items": {"$ref": "#/$defs/error"},
            },
        },
        defs={"error": cli_argument_error_schema()},
        additional_properties=False,
    )


def safe_sample_source_request_contract() -> GitOpsSchemaContract:
    contract = documented_contract(
        name="safe-sample-source-request",
        kind="dpone.safe-sample-source-request.v1",
        title="dpone GitOps safe sample source request",
        required=(
            "schema",
            "status",
            "mode",
            "sample_rows",
            "max_bytes",
            "timeout_seconds",
            "source_read_only",
            "full_scan_allowed",
            "pii_policy",
            "target",
            "errors",
        ),
        properties={
            "schema": {"const": "dpone.safe-sample-source-request.v1"},
            "status": {"enum": ["planned", "blocked"]},
            "mode": {"enum": ["pushdown", "full_scan", "blocked"]},
            "sample_rows": positive_integer_schema(),
            "max_bytes": positive_integer_schema(),
            "timeout_seconds": positive_integer_schema(),
            "source_read_only": {"const": True},
            "full_scan_allowed": {"type": "boolean"},
            "estimated_read_bytes": estimated_read_bytes_schema(),
            "proof": {"type": ["string", "null"]},
            "pii_policy": pii_policy_schema(),
            "target": source_request_target_schema(),
            "errors": {"type": "array", "items": {"$ref": "#/$defs/error"}},
        },
        defs={"error": source_request_error_schema()},
        additional_properties=False,
    )
    contract.schema["allOf"] = [full_scan_positive_estimate_guard()]
    return contract


def safe_sample_data_copier_registry_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="safe-sample-data-copier-registry",
        kind="dpone.safe-sample-data-copier-registry.v1",
        title="dpone GitOps safe sample data copier registry",
        required=("schema", "registered_copiers"),
        properties={
            "schema": {"const": "dpone.safe-sample-data-copier-registry.v1"},
            "registered_copiers": {
                "type": "array",
                "items": {"$ref": "#/$defs/registration"},
            },
        },
        defs={"registration": data_copier_registration_schema()},
    )


def safe_sample_certified_copy_request_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="safe-sample-certified-copy-request",
        kind="dpone.safe-sample-certified-copy-request.v1",
        title="dpone GitOps safe sample certified copy request",
        required=(
            "schema",
            "certification_id",
            "source",
            "sink",
            "strategy",
            "sample_rows",
            "max_bytes",
            "timeout_seconds",
            "source_read_only",
            "pii_policy",
            "proof",
        ),
        properties={
            "schema": {"const": "dpone.safe-sample-certified-copy-request.v1"},
            "certification_id": non_empty_string_schema(),
            "source": certified_copy_endpoint_schema(
                connection_type="mssql",
                table_key="table",
            ),
            "sink": certified_copy_endpoint_schema(
                connection_type="clickhouse",
                table_key="temporary_table",
            ),
            "strategy": {"const": "incremental_merge"},
            "sample_rows": positive_integer_schema(),
            "max_bytes": positive_integer_schema(),
            "timeout_seconds": positive_integer_schema(),
            "source_read_only": {"const": True},
            "pii_policy": pii_policy_schema(),
            "proof": {
                "type": "string",
                "pattern": "^route_certification:[A-Za-z0-9_\\-]+$",
            },
        },
        defs={"table": table_schema()},
    )


def cli_argument_error_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["schema", "code", "stage", "severity", "message"],
        "additionalProperties": True,
        "properties": {
            "schema": {"const": "dpone.error.v1"},
            "code": dpone_error_code_schema(),
            "stage": {"const": "safe_sample_cli_arguments"},
            "severity": error_severity_schema(),
            "message": non_empty_string_schema(),
            "entity": {"type": "object", "additionalProperties": True},
            "fixes": {"type": "array", "items": cli_argument_fix_schema()},
        },
    }


def cli_argument_fix_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["id", "safety", "description"],
        "additionalProperties": True,
        "properties": {
            "id": non_empty_string_schema(),
            "safety": fix_safety_schema(),
            "description": non_empty_string_schema(),
            "command": non_empty_string_schema(),
        },
    }


def source_request_error_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["schema", "code", "stage", "severity", "message"],
        "additionalProperties": True,
        "properties": {
            "schema": {"const": "dpone.error.v1"},
            "code": dpone_error_code_schema(),
            "stage": non_empty_string_schema(),
            "severity": error_severity_schema(),
            "message": non_empty_string_schema(),
            "fixes": {"type": "array"},
        },
    }


def source_request_target_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["mode", "connection_ref", "temporary_table", "ttl_seconds"],
        "additionalProperties": False,
        "properties": {
            "mode": {"const": "temporary"},
            "connection_ref": connection_ref_schema(),
            "temporary_table": table_schema(),
            "ttl_seconds": positive_integer_schema(),
        },
    }


def data_copier_registration_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "certification_id",
            "status",
            "source",
            "sink",
            "strategy",
            "transport",
            "schema_evolution",
            "airflow_runtime_mode",
            "sampling_mode",
        ],
        "additionalProperties": True,
        "properties": {
            "certification_id": non_empty_string_schema(),
            "status": {
                "enum": [
                    "experimental",
                    "route-certified",
                    "production-certified",
                    "enterprise-certified",
                ]
            },
            "source": non_empty_string_schema(),
            "sink": non_empty_string_schema(),
            "strategy": non_empty_string_schema(),
            "transport": non_empty_string_schema(),
            "schema_evolution": non_empty_string_schema(),
            "airflow_runtime_mode": non_empty_string_schema(),
            "sampling_mode": non_empty_string_schema(),
        },
    }


def certified_copy_endpoint_schema(*, connection_type: str, table_key: str) -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["type", "connection_ref", table_key],
        "additionalProperties": False,
        "properties": {
            "type": {"const": connection_type},
            "connection_ref": connection_ref_schema(),
            table_key: {"$ref": "#/$defs/table"},
        },
    }


def table_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["schema", "name"],
        "additionalProperties": False,
        "properties": {
            "schema": {"type": "string"},
            "name": non_empty_string_schema(),
        },
    }


def dpone_error_code_schema() -> dict[str, str]:
    return {"type": "string", "pattern": "^DPONE_[A-Z0-9_]+$"}


def error_severity_schema() -> dict[str, list[str]]:
    return {"enum": ["info", "warning", "error"]}


def fix_safety_schema() -> dict[str, list[str]]:
    return {"enum": ["safe", "manual", "destructive"]}


def pii_policy_schema() -> dict[str, list[str]]:
    return {"enum": ["masked", "not_logged"]}


def positive_integer_schema() -> dict[str, Any]:
    return {"type": "integer", "minimum": 1}


def non_empty_string_schema() -> dict[str, Any]:
    return {"type": "string", "minLength": 1}


__all__ = ["safe_sample_planning_schema_contracts"]

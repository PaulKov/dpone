from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract


from typing import Any

from dpone.gitops.schema_contract_primitives import connection_ref_schema, documented_contract
from dpone.gitops.schema_safe_sample_common import estimated_read_bytes_schema, full_scan_positive_estimate_guard


def safe_sample_route_execution_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        safe_sample_data_copy_contract(),
        mssql_clickhouse_safe_sample_copy_config_contract(),
        mssql_safe_sample_read_plan_contract(),
        clickhouse_safe_sample_insert_plan_contract(),
    )


def safe_sample_data_copy_contract() -> GitOpsSchemaContract:
    schema_contract = documented_contract(
        name="safe-sample-data-copy",
        kind="dpone.safe-sample-data-copy.v1",
        title="dpone GitOps safe sample data copy",
        required=data_copy_required_fields(),
        properties=data_copy_properties_schema(),
        defs={
            "error": error_schema(),
            "json_value": json_value_schema(),
            "table": table_schema(),
        },
        additional_properties=False,
    )
    schema_contract.schema["allOf"] = data_copy_status_guards()
    return schema_contract


def data_copy_required_fields() -> tuple[str, ...]:
    return (
        "schema",
        "status",
        "source_request",
        "copy_request",
        "rows_read",
        "rows_written",
        "bytes_read",
        "pii_policy",
        "diagnostics",
        "errors",
    )


def data_copy_properties_schema() -> dict[str, Any]:
    return {
        "schema": {"const": "dpone.safe-sample-data-copy.v1"},
        "status": data_copy_status_schema(),
        "source_request": source_request_schema(),
        "copy_request": certified_copy_request_schema(),
        "rows_read": non_negative_integer_schema(),
        "rows_written": non_negative_integer_schema(),
        "quarantined_rows": non_negative_integer_schema(),
        "bytes_read": non_negative_integer_schema(),
        "pii_policy": pii_policy_schema(),
        "diagnostics": {
            "type": "object",
            "additionalProperties": {"$ref": "#/$defs/json_value"},
        },
        "errors": {"type": "array", "items": {"$ref": "#/$defs/error"}},
    }


def data_copy_status_guards() -> list[dict[str, Any]]:
    return [
        {
            "if": {"required": ["status"], "properties": {"status": {"const": "copied"}}},
            "then": {"properties": {"quarantined_rows": {"const": 0}}},
        },
        {
            "if": {
                "required": ["status"],
                "properties": {"status": {"const": "copied_with_quarantine"}},
            },
            "then": {
                "required": ["quarantined_rows"],
                "properties": {"quarantined_rows": {"type": "integer", "minimum": 1}},
            },
        },
        {
            "if": {
                "required": ["status"],
                "properties": {"status": {"enum": ["copied", "copied_with_quarantine"]}},
            },
            "then": {"properties": {"pii_policy": {"const": "masked"}}},
        },
    ]


def json_value_schema() -> dict[str, Any]:
    return {
        "anyOf": [
            {"type": ["string", "number", "boolean", "null"]},
            {"type": "array", "items": {"$ref": "#/$defs/json_value"}},
            {
                "type": "object",
                "additionalProperties": {"$ref": "#/$defs/json_value"},
            },
        ]
    }


def mssql_clickhouse_safe_sample_copy_config_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="mssql-clickhouse-safe-sample-copy-config",
        kind="dpone.mssql-clickhouse-safe-sample-copy-config.v1",
        title="dpone GitOps MSSQL ClickHouse safe sample copy config",
        required=("schema", "source_connection_ref", "source_table"),
        properties={
            "schema": {"const": "dpone.mssql-clickhouse-safe-sample-copy-config.v1"},
            "source_connection_ref": connection_ref_schema(),
            "source_table": {"$ref": "#/$defs/table"},
        },
        defs={"table": table_schema()},
        additional_properties=False,
    )


def mssql_safe_sample_read_plan_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="mssql-safe-sample-read-plan",
        kind="dpone.mssql-safe-sample-read-plan.v1",
        title="dpone GitOps MSSQL safe sample read plan",
        required=(
            "schema",
            "sql",
            "parameters",
            "sample_rows",
            "max_bytes",
            "timeout_seconds",
            "source_read_only",
            "source",
        ),
        properties={
            "schema": {"const": "dpone.mssql-safe-sample-read-plan.v1"},
            "sql": {
                "type": "string",
                "pattern": "^SELECT TOP \\(@sample_rows\\) \\* FROM .+$",
            },
            "parameters": {
                "type": "object",
                "required": ["sample_rows"],
                "additionalProperties": True,
                "properties": {
                    "sample_rows": positive_integer_schema(),
                },
            },
            "sample_rows": positive_integer_schema(),
            "max_bytes": positive_integer_schema(),
            "timeout_seconds": positive_integer_schema(),
            "source_read_only": {"const": True},
            "source": mssql_source_endpoint_schema(),
        },
        defs={"table": table_schema()},
        additional_properties=False,
    )


def clickhouse_safe_sample_insert_plan_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="clickhouse-safe-sample-insert-plan",
        kind="dpone.clickhouse-safe-sample-insert-plan.v1",
        title="dpone GitOps ClickHouse safe sample insert plan",
        required=("schema", "sql", "row_count", "temporary_table", "sink"),
        properties={
            "schema": {"const": "dpone.clickhouse-safe-sample-insert-plan.v1"},
            "sql": {
                "type": "string",
                "pattern": "^INSERT INTO .+ VALUES$",
            },
            "row_count": non_negative_integer_schema(),
            "temporary_table": {"$ref": "#/$defs/table"},
            "sink": clickhouse_sink_endpoint_schema(),
        },
        defs={"table": table_schema()},
        additional_properties=False,
    )


def source_request_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
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
        ],
        "additionalProperties": True,
        "properties": {
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
            "target": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "connection_ref": connection_ref_schema(),
                },
            },
            "errors": {"type": "array", "items": {"$ref": "#/$defs/error"}},
        },
        "allOf": [full_scan_positive_estimate_guard()],
    }


def certified_copy_request_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
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
        ],
        "additionalProperties": True,
        "properties": {
            "schema": {"const": "dpone.safe-sample-certified-copy-request.v1"},
            "certification_id": non_empty_string_schema(),
            "source": mssql_source_endpoint_schema(),
            "sink": clickhouse_sink_endpoint_schema(),
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
    }


def mssql_source_endpoint_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["type", "connection_ref", "table"],
        "additionalProperties": False,
        "properties": {
            "type": {"const": "mssql"},
            "connection_ref": connection_ref_schema(),
            "table": {"$ref": "#/$defs/table"},
        },
    }


def clickhouse_sink_endpoint_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["type", "connection_ref", "temporary_table"],
        "additionalProperties": False,
        "properties": {
            "type": {"const": "clickhouse"},
            "connection_ref": connection_ref_schema(),
            "temporary_table": {"$ref": "#/$defs/table"},
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


def error_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["schema", "code", "stage", "severity", "message"],
        "additionalProperties": True,
        "properties": {
            "schema": {"const": "dpone.error.v1"},
            "code": {"type": "string", "pattern": "^DPONE_[A-Z0-9_]+$"},
            "stage": non_empty_string_schema(),
            "severity": {"enum": ["info", "warning", "error"]},
            "message": non_empty_string_schema(),
            "fixes": {"type": "array"},
        },
    }


def pii_policy_schema() -> dict[str, list[str]]:
    return {"enum": ["masked", "not_logged"]}


def data_copy_status_schema() -> dict[str, list[str]]:
    return {"enum": ["blocked", "copied", "copied_with_quarantine", "failed"]}


def positive_integer_schema() -> dict[str, Any]:
    return {"type": "integer", "minimum": 1}


def non_negative_integer_schema() -> dict[str, Any]:
    return {"type": "integer", "minimum": 0}


def non_empty_string_schema() -> dict[str, Any]:
    return {"type": "string", "minLength": 1}


__all__ = ["safe_sample_route_execution_schema_contracts"]

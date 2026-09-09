from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import (
    JSON_SCHEMA_DRAFT,
    SCHEMA_BASE_ID,
    GitOpsSchemaContract,
    airflow_connection_id_schema,
    connection_ref_schema,
    safe_volume_mount_path_schema,
)
from dpone.gitops.schema_kubernetes_contracts import kubernetes_secret_name_schema


def connection_check_contract() -> GitOpsSchemaContract:
    return GitOpsSchemaContract(
        name="connection-check",
        kind="dpone.connection-check.v1",
        schema={
            "$schema": JSON_SCHEMA_DRAFT,
            "$id": f"{SCHEMA_BASE_ID}/connection-check.schema.json",
            "title": "dpone GitOps connection check report",
            "type": "object",
            "required": [
                "schema",
                "passed",
                "changes",
                "errors",
                "mode",
                "network",
                "secrets",
                "source_queries",
                "handshake",
                "environment",
                "connection_refs",
                "resolved_connection_refs",
                "airflow_connection_bridge",
                "binding_set_path",
                "connection_registry_path",
                "credential_runtime_path",
            ],
            "additionalProperties": True,
            "properties": _properties(),
            "$defs": _defs(),
        },
    )


def live_preflight_contract() -> GitOpsSchemaContract:
    return GitOpsSchemaContract(
        name="live-preflight",
        kind="dpone.live-preflight.v1",
        schema={
            "$schema": JSON_SCHEMA_DRAFT,
            "$id": f"{SCHEMA_BASE_ID}/live-preflight.schema.json",
            "title": "dpone GitOps live preflight report",
            "type": "object",
            "required": [
                "schema",
                "passed",
                "runner",
                "network",
                "secrets",
                "source_queries",
                "planned_network",
                "planned_secrets",
                "planned_source_queries",
                "environment",
                "connection_refs",
                "resolved_connection_refs",
                "probes",
                "errors",
            ],
            "additionalProperties": True,
            "properties": _live_preflight_properties(),
            "$defs": {"error": _error_schema(), "probe": _live_preflight_probe_schema()},
        },
    )


def _properties() -> dict[str, Any]:
    return {
        "schema": {"const": "dpone.connection-check.v1"},
        "passed": {"type": "boolean"},
        "changes": {"type": "array", "items": _change_schema()},
        "errors": {"type": "array", "items": {"$ref": "#/$defs/error"}},
        "mode": {"enum": ["connections", "live"]},
        "network": {"type": "boolean"},
        "secrets": {"type": "boolean"},
        "source_queries": {"type": "boolean"},
        "planned_network": {"type": "boolean"},
        "planned_secrets": {"type": "boolean"},
        "planned_source_queries": {"type": "string"},
        "live_preflight": {"type": "string"},
        "live_preflight_report": {"type": "object"},
        "handshake": {"const": "configuration_only"},
        "environment": {"type": "string", "minLength": 1},
        "connection_refs": _connection_ref_array(),
        "resolved_connection_refs": _connection_ref_array(),
        "airflow_connection_bridge": {"$ref": "#/$defs/airflow_connection_bridge"},
        "binding_set_path": {"type": "string", "minLength": 1},
        "connection_registry_path": {"type": "string", "minLength": 1},
        "credential_runtime_path": {"type": "string", "minLength": 1},
    }


def _live_preflight_properties() -> dict[str, Any]:
    return {
        "schema": {"const": "dpone.live-preflight.v1"},
        "passed": {"type": "boolean"},
        "runner": {"enum": ["not_configured", "configured"]},
        "network": {"type": "boolean"},
        "secrets": {"type": "boolean"},
        "source_queries": {"type": "boolean"},
        "planned_network": {"const": True},
        "planned_secrets": {"const": True},
        "planned_source_queries": {"const": "bounded_probes"},
        "environment": {"type": "string", "minLength": 1},
        "connection_refs": _connection_ref_array(),
        "resolved_connection_refs": _connection_ref_array(),
        "probes": {"type": "array", "items": {"$ref": "#/$defs/probe"}},
        "errors": {"type": "array", "items": {"$ref": "#/$defs/error"}},
    }


def _live_preflight_probe_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "connection_ref",
            "probe",
            "status",
            "runner",
            "network",
            "secrets",
            "source_queries",
        ],
        "additionalProperties": False,
        "properties": {
            "connection_ref": connection_ref_schema(),
            "probe": {"enum": ["credential_resolution", "bounded_source_probe", "bounded_sink_probe"]},
            "status": {"enum": ["blocked", "passed", "failed"]},
            "runner": {"enum": ["not_configured", "configured"]},
            "network": {"type": "boolean"},
            "secrets": {"type": "boolean"},
            "source_queries": {"type": "boolean"},
        },
    }


def _change_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["action", "path", "message", "diff"],
        "additionalProperties": False,
        "properties": {
            "action": {"type": "string"},
            "path": {"type": "string"},
            "message": {"type": "string"},
            "diff": {"type": "string"},
        },
    }


def _defs() -> dict[str, Any]:
    return {
        "airflow_connection_bridge": _airflow_connection_bridge_schema(),
        "airflow_connection_bridge_entry": _airflow_connection_bridge_entry_schema(),
        "airflow_connection_bridge_projection": _airflow_connection_bridge_projection_schema(),
        "airflow_connection_bridge_projection_entry": _airflow_connection_bridge_projection_entry_schema(),
        "error": _error_schema(),
    }


def _airflow_connection_bridge_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "required",
            "execution_mode",
            "resolver_location",
            "parse_safe",
            "secrets",
            "required_connection_ids",
            "connections",
            "projection",
            "next_actions",
        ],
        "additionalProperties": False,
        "properties": {
            "required": {"type": "boolean"},
            "execution_mode": {"type": ["string", "null"], "enum": ["operator_bridge", None]},
            "resolver_location": {"type": ["string", "null"], "enum": ["operator_execution", None]},
            "parse_safe": {"const": True},
            "secrets": {"const": False},
            "required_connection_ids": {"type": "array", "items": airflow_connection_id_schema()},
            "connections": {"type": "array", "items": {"$ref": "#/$defs/airflow_connection_bridge_entry"}},
            "projection": {
                "anyOf": [
                    {"type": "null"},
                    {"$ref": "#/$defs/airflow_connection_bridge_projection"},
                ]
            },
            "next_actions": _string_array(),
        },
    }


def _airflow_connection_bridge_entry_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["connection_ref", "registry_connection_ref", "connection_id", "env_name"],
        "additionalProperties": False,
        "properties": {
            "connection_ref": connection_ref_schema(),
            "registry_connection_ref": connection_ref_schema(),
            "connection_id": airflow_connection_id_schema(),
            "env_name": {"type": "string", "pattern": "^AIRFLOW_CONN_[A-Z0-9_]+$"},
        },
    }


def _airflow_connection_bridge_projection_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "mode",
            "secret_name",
            "mount_path",
            "payload_format",
            "secret_values",
            "connections",
        ],
        "additionalProperties": False,
        "properties": {
            "mode": {"const": "kubernetes_secret_volume"},
            "secret_name": kubernetes_secret_name_schema(),
            "mount_path": _public_volume_mount_path_schema(),
            "payload_format": {"const": "airflow_connection_uri"},
            "secret_values": {"const": False},
            "connections": {
                "type": "array",
                "items": {"$ref": "#/$defs/airflow_connection_bridge_projection_entry"},
            },
        },
    }


def _airflow_connection_bridge_projection_entry_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "connection_ref",
            "registry_connection_ref",
            "connection_id",
            "secret_key",
            "mount_path",
            "fields",
        ],
        "additionalProperties": False,
        "properties": {
            "connection_ref": connection_ref_schema(),
            "registry_connection_ref": connection_ref_schema(),
            "connection_id": airflow_connection_id_schema(),
            "secret_key": {"type": "string", "pattern": "^AIRFLOW_CONN_[A-Z0-9_]+$"},
            "mount_path": _public_volume_mount_path_schema(),
            "fields": {
                "type": "object",
                "required": ["uri"],
                "additionalProperties": False,
                "properties": {"uri": {"const": "uri"}},
            },
        },
    }


def _error_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["schema", "code", "stage", "severity", "message"],
        "additionalProperties": True,
        "properties": {
            "schema": {"const": "dpone.error.v1"},
            "code": {"type": "string", "pattern": "^DPONE_[A-Z0-9_]+$"},
            "stage": {"type": "string", "minLength": 1},
            "severity": {"enum": ["info", "warning", "error"]},
            "message": {"type": "string", "minLength": 1},
            "entity": {"type": "object", "additionalProperties": True},
            "fixes": {"type": "array", "items": _fix_schema()},
        },
    }


def _public_volume_mount_path_schema() -> dict[str, Any]:
    """Allow the stable placeholder emitted by public JSON redaction."""

    return {
        "oneOf": [
            safe_volume_mount_path_schema(),
            {"const": "$ABSOLUTE_PATH"},
        ]
    }


def _fix_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["id", "safety"],
        "additionalProperties": True,
        "properties": {
            "id": {"type": "string", "minLength": 1},
            "safety": {"enum": ["safe", "manual", "destructive"]},
            "description": {"type": "string"},
            "command": {"type": "string"},
        },
    }


def _string_array() -> dict[str, Any]:
    return {"type": "array", "items": {"type": "string", "minLength": 1}}


def _connection_ref_array() -> dict[str, Any]:
    return {"type": "array", "items": connection_ref_schema()}


__all__ = ["connection_check_contract", "live_preflight_contract"]

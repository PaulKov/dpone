"""Credential resolver JSON-schema fragments for environment contracts."""

from __future__ import annotations

from typing import Any

from dpone.contracts.credential_env import ENV_VAR_NAME_PATTERN
from dpone.contracts.credential_security import FORBIDDEN_SECRET_KEYS
from dpone.gitops.schema_contract_primitives import non_empty_string_schema, string_schema
from dpone.gitops.schema_kubernetes_contracts import kubernetes_secret_name_schema
from dpone.gitops.schema_vault_contracts import vault_logical_path_schema, vault_mount_schema


def fields_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "minProperties": 1,
        "propertyNames": {"type": "string", "minLength": 1, "pattern": "\\S"},
        "additionalProperties": {"type": "string", "minLength": 1, "pattern": "\\S"},
    }


def safe_volume_fields_schema() -> dict[str, Any]:
    field_path_schema = {
        "type": "string",
        "minLength": 1,
        "pattern": "\\S",
        "not": {"anyOf": [{"pattern": "^/"}, {"pattern": "(^|/)\\.\\.(/|$)"}]},
    }
    return {
        "type": "object",
        "minProperties": 1,
        "propertyNames": {"type": "string", "minLength": 1, "pattern": "\\S"},
        "additionalProperties": field_path_schema,
    }


def env_var_fields_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "minProperties": 1,
        "propertyNames": {"type": "string", "minLength": 1, "pattern": "\\S"},
        "additionalProperties": {"type": "string", "pattern": ENV_VAR_NAME_PATTERN},
    }


def vault_kv_schema() -> dict[str, Any]:
    return _credential_resolver_schema(
        {
            "type": "object",
            "required": ["resolver", "mount", "path", "fields", "version_policy", "resolution_scope"],
            "additionalProperties": True,
            "properties": {
                "resolver": {"const": "vault_kv"},
                "mount": vault_mount_schema(),
                "kv_version": {"enum": [1, 2]},
                "path": vault_logical_path_schema(),
                "fields": {"$ref": "#/$defs/fields"},
                "version_policy": {"enum": ["latest", "pinned"]},
                "resolution_scope": {"enum": ["workload_start", "dag_run_start"]},
            },
        }
    )


def kubernetes_secret_volume_schema() -> dict[str, Any]:
    return _credential_resolver_schema(
        {
            "type": "object",
            "required": ["resolver", "secret_name", "mount_path", "fields"],
            "additionalProperties": True,
            "properties": {
                "resolver": {"const": "kubernetes_secret_volume"},
                "secret_name": kubernetes_secret_name_schema(),
                "mount_path": {"$ref": "#/$defs/safeVolumeMountPath"},
                "payload_format": {"enum": ["field_mapping", "airflow_connection_uri"]},
                "fields": {"$ref": "#/$defs/safeVolumeFields"},
            },
        }
    )


def kubernetes_secret_api_schema() -> dict[str, Any]:
    return _credential_resolver_schema(
        {
            "type": "object",
            "required": ["resolver", "namespace", "name", "fields"],
            "additionalProperties": True,
            "properties": {
                "resolver": {"const": "kubernetes_secret_api"},
                "namespace": kubernetes_secret_name_schema(),
                "name": kubernetes_secret_name_schema(),
                "fields": {"$ref": "#/$defs/fields"},
            },
        }
    )


def airflow_connection_schema() -> dict[str, Any]:
    return _credential_resolver_schema(
        {
            "type": "object",
            "required": ["resolver", "connection_id", "execution_mode"],
            "additionalProperties": True,
            "properties": {
                "resolver": {"const": "airflow_connection"},
                "connection_id": {"$ref": "#/$defs/airflowConnectionId"},
                "execution_mode": {"const": "operator_bridge"},
            },
        }
    )


def env_var_schema() -> dict[str, Any]:
    return _credential_resolver_schema(
        {
            "type": "object",
            "required": ["resolver", "support", "fields"],
            "additionalProperties": True,
            "properties": {
                "resolver": {"const": "env_var"},
                "support": {"const": "development_only"},
                "fields": {"$ref": "#/$defs/envVarFields"},
            },
        }
    )


def _credential_resolver_schema(schema: dict[str, Any]) -> dict[str, Any]:
    schema["not"] = {"anyOf": [{"required": [key]} for key in sorted(FORBIDDEN_SECRET_KEYS)]}
    return schema


def vault_runtime_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["address", "auth"],
        "additionalProperties": True,
        "properties": {
            "address": {"type": "string", "format": "uri"},
            "namespace": string_schema(),
            "auth": vault_auth_schema(),
        },
    }


def vault_auth_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["method", "role"],
        "additionalProperties": True,
        "properties": {
            "method": {"enum": ["kubernetes", "jwt", "approle"]},
            "role": non_empty_string_schema(),
        },
        "not": {"anyOf": [{"required": [key]} for key in sorted(FORBIDDEN_SECRET_KEYS)]},
    }


def sha256_schema() -> dict[str, str]:
    return {"type": "string", "pattern": "^sha256:[A-Fa-f0-9]{64}$"}


__all__ = [
    "airflow_connection_schema",
    "env_var_fields_schema",
    "env_var_schema",
    "fields_schema",
    "kubernetes_secret_api_schema",
    "kubernetes_secret_volume_schema",
    "safe_volume_fields_schema",
    "sha256_schema",
    "vault_kv_schema",
    "vault_runtime_schema",
]

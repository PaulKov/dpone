from __future__ import annotations

from typing import Any

from dpone.contracts.credential_security import FORBIDDEN_SECRET_KEYS
from dpone.gitops.schema_contract_primitives import (
    GitOpsSchemaContract,
    airflow_connection_id_schema,
    array_schema,
    boolean_schema,
    connection_ref_schema,
    documented_contract,
    integer_schema,
    non_empty_string_schema,
    object_schema,
    safe_volume_mount_path_schema,
    string_schema,
)
from dpone.gitops.schema_credential_contracts import (
    airflow_connection_schema,
    env_var_fields_schema,
    env_var_schema,
    fields_schema,
    kubernetes_secret_api_schema,
    kubernetes_secret_volume_schema,
    safe_volume_fields_schema,
    sha256_schema,
    vault_kv_schema,
    vault_runtime_schema,
)
from dpone.gitops.schema_postgres_authority_contracts import (
    postgres_source_authority_schema,
)


def environment_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        binding_set_contract(),
        connection_registry_contract(),
        connection_registry_migration_plan_contract(),
        credential_runtime_contract(),
    )


def binding_set_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="binding-set",
        kind="dpone.binding-set.v1",
        title="dpone GitOps binding-set",
        required=("schema", "environment", "bindings", "runtime"),
        properties={
            "schema": {"const": "dpone.binding-set.v1"},
            "environment": non_empty_string_schema(),
            "bindings": {
                "type": "object",
                "propertyNames": {"$ref": "#/$defs/connectionRef"},
                "additionalProperties": {
                    "type": "object",
                    "required": ["connection_ref"],
                    "additionalProperties": False,
                    "properties": {"connection_ref": {"$ref": "#/$defs/connectionRef"}},
                },
            },
            "runtime": binding_runtime_schema(),
            "connection_registry_ref": string_schema(),
            "fingerprint": {"$ref": "#/$defs/sha256"},
        },
        defs={"connectionRef": connection_ref_schema(), "sha256": sha256_schema()},
    )


def connection_registry_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="connection-registry",
        kind="dpone.connection-registry.v1",
        title="dpone GitOps connection registry",
        required=("schema", "connections"),
        properties={
            "schema": {"const": "dpone.connection-registry.v1"},
            "environment": string_schema(),
            "connections": {
                "type": "object",
                "propertyNames": {"$ref": "#/$defs/connectionRef"},
                "additionalProperties": {
                    "type": "object",
                    "required": ["type", "credentials"],
                    "additionalProperties": True,
                    "properties": {
                        "type": non_empty_string_schema(),
                        "connection": connection_metadata_schema(),
                        "credentials": {
                            "oneOf": [
                                {"$ref": "#/$defs/vaultKv"},
                                {"$ref": "#/$defs/kubernetesSecretVolume"},
                                {"$ref": "#/$defs/kubernetesSecretApi"},
                                {"$ref": "#/$defs/airflowConnection"},
                                {"$ref": "#/$defs/envVar"},
                            ]
                        },
                    },
                },
            },
            "fingerprint": {"$ref": "#/$defs/sha256"},
        },
        defs=connection_registry_defs(),
    )


def connection_registry_migration_plan_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="connection-registry-migration-plan",
        kind="dpone.connection-registry-migration-plan.v1",
        title="dpone GitOps connection registry migration plan",
        required=(
            "kind",
            "schema",
            "mode",
            "apply",
            "environment",
            "registry_path",
            "summary",
            "changes",
        ),
        properties={
            "kind": {"const": "dpone.connection-registry-migration-plan.v1"},
            "schema": {"const": "dpone.connection-registry-migration-plan.v1"},
            "mode": {"const": "plan"},
            "apply": {"const": False},
            "environment": string_schema(),
            "registry_path": string_schema(),
            "summary": connection_registry_migration_summary_schema(),
            "changes": array_schema(connection_registry_migration_change_schema()),
        },
    )


def credential_runtime_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="credential-runtime",
        kind="dpone.credential-runtime.v1",
        title="dpone GitOps credential runtime",
        required=("schema", "environment"),
        properties={
            "schema": {"const": "dpone.credential-runtime.v1"},
            "environment": non_empty_string_schema(),
            "vault": vault_runtime_schema(),
            "fingerprint": {"$ref": "#/$defs/sha256"},
        },
        defs={"sha256": sha256_schema()},
    )


def connection_registry_migration_summary_schema() -> dict[str, Any]:
    return object_schema(
        required=("legacy_entries", "manual_review_required", "apply_supported"),
        properties={
            "legacy_entries": integer_schema(),
            "manual_review_required": boolean_schema(),
            "apply_supported": boolean_schema(),
        },
    )


def connection_registry_migration_change_schema() -> dict[str, Any]:
    return object_schema(
        required=(
            "action",
            "connection_ref",
            "path",
            "detected",
            "proposed_entry",
            "manual_review_required",
            "unified_diff",
        ),
        properties={
            "action": {"const": "replace_legacy_connection_entry"},
            "connection_ref": string_schema(),
            "path": string_schema(),
            "detected": object_schema(),
            "proposed_entry": object_schema(),
            "manual_review_required": boolean_schema(),
            "unified_diff": string_schema(),
        },
    )


def binding_runtime_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "kubernetes_namespace": string_schema(),
            "service_account": string_schema(),
            "pool": string_schema(),
            "resource_profile": string_schema(),
        },
    }


def connection_registry_defs() -> dict[str, Any]:
    return {
        "sha256": sha256_schema(),
        "connectionRef": connection_ref_schema(),
        "fields": fields_schema(),
        "envVarFields": env_var_fields_schema(),
        "airflowConnectionId": airflow_connection_id_schema(),
        "safeVolumeMountPath": safe_volume_mount_path_schema(),
        "safeVolumeFields": safe_volume_fields_schema(),
        "vaultKv": vault_kv_schema(),
        "kubernetesSecretVolume": kubernetes_secret_volume_schema(),
        "kubernetesSecretApi": kubernetes_secret_api_schema(),
        "airflowConnection": airflow_connection_schema(),
        "envVar": env_var_schema(),
    }


def connection_metadata_schema() -> dict[str, Any]:
    """Connector-neutral non-secret endpoint metadata."""

    non_secret_names = {"not": {"enum": sorted(FORBIDDEN_SECRET_KEYS)}}
    return {
        "type": "object",
        "propertyNames": non_secret_names,
        "additionalProperties": True,
        "properties": {
            "host": non_empty_string_schema(),
            "instance": non_empty_string_schema(),
            "port": {"type": "integer", "minimum": 1, "maximum": 65535},
            "database": non_empty_string_schema(),
            "schema": non_empty_string_schema(),
            "secure": {"type": "boolean"},
            "asset_authority": mssql_asset_authority_schema(),
            "database_authorities": mssql_database_authorities_schema(),
            "postgres_source_authority": postgres_source_authority_schema(),
            "parameters": {
                "type": "object",
                "propertyNames": non_secret_names,
                "additionalProperties": True,
            },
        },
    }


def mssql_asset_authority_schema() -> dict[str, Any]:
    """Deployment-owned MSSQL Airflow Asset URI authority (never connection_ref)."""

    return object_schema(
        required=("host",),
        properties={
            "host": non_empty_string_schema(),
            "port": {"type": "integer", "minimum": 1, "maximum": 65535},
            "instance": {"type": ["string", "null"], "minLength": 1},
        },
        additional_properties=False,
    )


def mssql_database_authorities_schema() -> dict[str, Any]:
    """Signed physical identities for every SQL Server database a route touches."""

    identity = object_schema(
        required=("database_id", "create_token", "database_guid"),
        properties={
            "database_id": {"type": "integer", "minimum": 1},
            "create_token": {
                "type": "string",
                "pattern": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,7})?$",
            },
            "database_guid": {
                "type": "string",
                "pattern": ("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"),
            },
        },
        additional_properties=False,
    )
    return {
        "type": "object",
        "minProperties": 1,
        "propertyNames": {"type": "string", "minLength": 1, "maxLength": 128},
        "additionalProperties": identity,
    }


__all__ = [
    "connection_registry_migration_plan_contract",
    "environment_schema_contracts",
]

"""Credential resolver validation for Airflow self-service connection checks."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.airflow_connection_names import is_valid_airflow_connection_id
from dpone.contracts.credential_resolution import (
    DAG_RUN_SCOPE_UNSUPPORTED,
    PINNED_VERSION_UNSUPPORTED,
    RESOLUTION_SCOPE_DAG_RUN_START,
    VERSION_POLICY_PINNED,
    is_valid_env_var_name,
)
from dpone.kubernetes_names import is_valid_kubernetes_dns_label
from dpone.readiness.airflow_connection_check_support import (
    FORBIDDEN_SECRET_KEYS,
    PRODUCTION_ENV_NAMES,
    SUPPORTED_CREDENTIAL_RESOLVERS,
    fixes_for_code,
    is_non_empty_string_mapping,
    is_safe_volume_field_path,
    is_safe_volume_mount_path,
)
from dpone.readiness.error_contract import dpone_error, manual_fix
from dpone.vault_references import is_valid_vault_logical_path, is_valid_vault_mount


def validate_credentials(ref: str, credentials: dict[str, Any], environment: str, path: Path) -> list[dict[str, str]]:
    resolver = credentials.get("resolver")
    errors: list[dict[str, str]] = []
    if resolver not in SUPPORTED_CREDENTIAL_RESOLVERS:
        errors.append(
            dpone_error(
                "DPONE_CREDENTIAL_RESOLVER_UNSUPPORTED",
                f"credentials.resolver is unsupported: {ref}",
                stage="check_connections",
                path=path.as_posix(),
                entity={"kind": "connection_ref", "id": ref},
                fixes=[manual_fix("declare_supported_credential_resolver")],
                extra={
                    "resolver": str(resolver or "<missing>"),
                    "supported_resolvers": sorted(SUPPORTED_CREDENTIAL_RESOLVERS),
                },
            )
        )
    if resolver == "env_var":
        errors.extend(_validate_env_var(ref, credentials, environment, path))
    if resolver == "vault_kv":
        errors.extend(_validate_vault_kv(ref, credentials, path))
    if resolver == "kubernetes_secret_volume":
        errors.extend(_validate_kubernetes_secret_volume(ref, credentials, path))
    if resolver == "kubernetes_secret_api":
        errors.extend(_validate_kubernetes_secret_api(ref, credentials, path))
    if resolver == "airflow_connection":
        errors.extend(_validate_airflow_connection(ref, credentials, path))
    errors.extend(_secret_key_errors(ref, credentials, path))
    return errors


def _validate_env_var(
    ref: str,
    credentials: dict[str, Any],
    environment: str,
    path: Path,
) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    if credentials.get("support") != "development_only":
        errors.append(
            _error(
                "DPONE_ENV_VAR_SUPPORT_REQUIRED",
                f"env_var resolver requires support=development_only: {ref}",
                path,
            )
        )
    fields = credentials.get("fields")
    if not is_non_empty_string_mapping(fields):
        errors.append(
            _error(
                "DPONE_ENV_VAR_FIELDS_INVALID",
                f"env_var resolver fields must be a non-empty mapping: {ref}",
                path,
            )
        )
    else:
        for field_name, env_name in fields.items():
            if not is_valid_env_var_name(env_name):
                errors.append(
                    _error(
                        "DPONE_ENV_VAR_NAME_INVALID",
                        f"env_var resolver field must be a valid environment variable name: {ref}.{field_name}",
                        path,
                    )
                )
    if environment.lower() in PRODUCTION_ENV_NAMES:
        errors.append(
            _error(
                "DPONE_ENV_VAR_RESOLVER_FORBIDDEN_IN_PROD",
                f"env_var resolver is development/legacy only: {ref}",
                path,
            )
        )
    return errors


def _validate_vault_kv(ref: str, credentials: dict[str, Any], path: Path) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    if not is_valid_vault_mount(credentials.get("mount")):
        errors.append(
            _error(
                "DPONE_VAULT_MOUNT_NOT_LOGICAL",
                f"vault_kv resolver mount must be a logical Vault mount name: {ref}",
                path,
            )
        )
    vault_path = str(credentials.get("path") or "")
    if not vault_path:
        errors.append(_error("DPONE_VAULT_PATH_REQUIRED", f"vault_kv resolver requires path: {ref}", path))
    elif not is_valid_vault_logical_path(vault_path):
        errors.append(_error("DPONE_VAULT_PATH_NOT_LOGICAL", f"Vault path must be logical: {ref}", path))
    fields = credentials.get("fields")
    if not is_non_empty_string_mapping(fields):
        errors.append(
            _error(
                "DPONE_VAULT_FIELDS_INVALID",
                f"vault_kv fields must be a non-empty mapping: {ref}",
                path,
            )
        )
    if credentials.get("version_policy") not in {"latest", "pinned"}:
        errors.append(
            _error(
                "DPONE_VAULT_VERSION_POLICY_REQUIRED",
                f"vault_kv resolver requires version_policy latest or pinned: {ref}",
                path,
            )
        )
    elif credentials.get("version_policy") == VERSION_POLICY_PINNED:
        errors.append(
            _error(
                PINNED_VERSION_UNSUPPORTED,
                f"vault_kv pinned data versions are not supported by the runtime client: {ref}",
                path,
            )
        )
    if credentials.get("resolution_scope") not in {"workload_start", "dag_run_start"}:
        errors.append(
            _error(
                "DPONE_VAULT_RESOLUTION_SCOPE_REQUIRED",
                f"vault_kv resolver requires resolution_scope workload_start or dag_run_start: {ref}",
                path,
            )
        )
    elif credentials.get("resolution_scope") == RESOLUTION_SCOPE_DAG_RUN_START:
        errors.append(
            _error(
                DAG_RUN_SCOPE_UNSUPPORTED,
                f"vault_kv dag_run_start snapshots are not implemented; use workload_start: {ref}",
                path,
            )
        )
    return errors


def _validate_kubernetes_secret_volume(ref: str, credentials: dict[str, Any], path: Path) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    if not is_valid_kubernetes_dns_label(credentials.get("secret_name")):
        errors.append(
            _error(
                "DPONE_KUBERNETES_SECRET_VOLUME_SECRET_NAME_INVALID",
                f"kubernetes_secret_volume secret_name must be a safe Kubernetes Secret name: {ref}",
                path,
            )
        )
    mount_path = str(credentials.get("mount_path") or "")
    if not mount_path:
        errors.append(
            _error(
                "DPONE_KUBERNETES_SECRET_VOLUME_MOUNT_PATH_REQUIRED",
                f"kubernetes_secret_volume requires mount_path: {ref}",
                path,
            )
        )
    elif not is_safe_volume_mount_path(mount_path):
        errors.append(
            _error(
                "DPONE_KUBERNETES_SECRET_VOLUME_MOUNT_PATH_INVALID",
                f"kubernetes_secret_volume mount_path must stay under /run/secrets/dpone: {ref}",
                path,
            )
        )
    fields = credentials.get("fields")
    if not is_non_empty_string_mapping(fields):
        errors.append(
            _error(
                "DPONE_KUBERNETES_SECRET_VOLUME_FIELDS_INVALID",
                f"kubernetes_secret_volume fields must be a non-empty mapping: {ref}",
                path,
            )
        )
    else:
        for field, file_name in fields.items():
            if not is_safe_volume_field_path(file_name):
                errors.append(
                    _error(
                        "DPONE_KUBERNETES_SECRET_VOLUME_FIELD_PATH_INVALID",
                        f"kubernetes_secret_volume field path must stay inside mount_path: {ref}.{field}",
                        path,
                    )
                )
    return errors


def _validate_kubernetes_secret_api(ref: str, credentials: dict[str, Any], path: Path) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    if not is_valid_kubernetes_dns_label(credentials.get("namespace")):
        errors.append(
            _error(
                "DPONE_KUBERNETES_SECRET_API_NAMESPACE_INVALID",
                f"kubernetes_secret_api namespace must be a safe Kubernetes namespace: {ref}",
                path,
            )
        )
    if not is_valid_kubernetes_dns_label(credentials.get("name")):
        errors.append(
            _error(
                "DPONE_KUBERNETES_SECRET_API_NAME_INVALID",
                f"kubernetes_secret_api name must be a safe Kubernetes Secret name: {ref}",
                path,
            )
        )
    fields = credentials.get("fields")
    if not is_non_empty_string_mapping(fields):
        errors.append(
            _error(
                "DPONE_KUBERNETES_SECRET_API_FIELDS_INVALID",
                f"kubernetes_secret_api fields must be a non-empty mapping: {ref}",
                path,
            )
        )
    return errors


def _validate_airflow_connection(ref: str, credentials: dict[str, Any], path: Path) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    connection_id = str(credentials.get("connection_id") or "")
    if not connection_id:
        errors.append(
            _error(
                "DPONE_AIRFLOW_CONNECTION_ID_REQUIRED",
                f"airflow_connection resolver requires connection_id: {ref}",
                path,
            )
        )
    elif not is_valid_airflow_connection_id(connection_id):
        errors.append(
            _error(
                "DPONE_AIRFLOW_CONNECTION_ID_INVALID",
                f"airflow_connection resolver connection_id must be a logical id: {ref}",
                path,
            )
        )
    if credentials.get("execution_mode") != "operator_bridge":
        errors.append(
            _error(
                "DPONE_AIRFLOW_CONNECTION_EXECUTION_MODE_INVALID",
                f"airflow_connection resolver requires execution_mode=operator_bridge: {ref}",
                path,
            )
        )
    return errors


def _secret_key_errors(ref: str, credentials: dict[str, Any], path: Path) -> list[dict[str, str]]:
    return [
        _error("DPONE_SECRET_VALUE_IN_REGISTRY", f"secret-like field is forbidden: {ref}.{key}", path)
        for key in credentials
        if key.lower() in FORBIDDEN_SECRET_KEYS
    ]


def _error(code: str, message: str, path: Path) -> dict[str, Any]:
    return dpone_error(
        code,
        message,
        stage="check_connections",
        path=path.as_posix(),
        fixes=fixes_for_code(code),
    )


__all__ = ["validate_credentials"]

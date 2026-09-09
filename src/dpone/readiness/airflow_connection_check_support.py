"""Shared helpers for credential-free Airflow connection checks."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any, TypeGuard

from dpone.contracts.credential_security import FORBIDDEN_SECRET_KEYS
from dpone.readiness.error_contract import dpone_error, manual_fix

PRODUCTION_ENV_NAMES = frozenset({"prod", "production"})
SUPPORTED_CREDENTIAL_RESOLVERS = frozenset(
    {
        "airflow_connection",
        "env_var",
        "kubernetes_secret_api",
        "kubernetes_secret_volume",
        "vault_kv",
    }
)


def is_safe_volume_field_path(value: object) -> bool:
    if not isinstance(value, str) or not value:
        return False
    path = PurePosixPath(value)
    return not path.is_absolute() and ".." not in path.parts


def is_safe_volume_mount_path(value: object) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    path = PurePosixPath(value)
    text = str(path)
    return (
        path.is_absolute()
        and text.startswith("/run/secrets/dpone/")
        and bool(text.removeprefix("/run/secrets/dpone/"))
        and ".." not in path.parts
        and not any(char.isspace() for char in value)
    )


def is_non_empty_string_mapping(value: object) -> TypeGuard[dict[str, str]]:
    if not isinstance(value, dict) or not value:
        return False
    return all(
        isinstance(key, str) and bool(key.strip()) and isinstance(item, str) and bool(item.strip())
        for key, item in value.items()
    )


def environment_mismatch_errors(
    payload: dict[str, Any],
    *,
    expected_environment: str,
    code: str,
    message: str,
    path: Path,
) -> list[dict[str, Any]]:
    actual_environment = payload.get("environment")
    if actual_environment in (None, expected_environment):
        return []
    return [
        dpone_error(
            code,
            message,
            stage="check_connections",
            path=path.as_posix(),
            fixes=[manual_fix("fix_environment_config_file")],
            extra={
                "expected_environment": expected_environment,
                "actual_environment": str(actual_environment),
            },
        )
    ]


def fixes_for_code(code: str) -> list[dict[str, str]]:
    if code == "DPONE_CONNECTION_REF_INVALID":
        return [manual_fix("replace_connection_ref_alias")]
    if code == "DPONE_CREDENTIAL_PINNED_VERSION_UNSUPPORTED":
        return [manual_fix("use_latest_version_policy")]
    if code == "DPONE_CREDENTIAL_DAG_RUN_SCOPE_UNSUPPORTED":
        return [manual_fix("use_workload_start_resolution_scope")]
    if code == "DPONE_ENV_VAR_RESOLVER_FORBIDDEN_IN_PROD":
        return [manual_fix("replace_env_var_resolver")]
    if code in {"DPONE_ENV_VAR_SUPPORT_REQUIRED", "DPONE_ENV_VAR_FIELDS_INVALID", "DPONE_ENV_VAR_NAME_INVALID"}:
        return [manual_fix("declare_development_only_env_var_mapping")]
    if code.startswith("DPONE_VAULT_"):
        return [manual_fix("declare_vault_kv_resolver_contract")]
    if code.startswith("DPONE_KUBERNETES_SECRET_VOLUME_"):
        return [manual_fix("fix_kubernetes_secret_volume_projection")]
    if code.startswith("DPONE_KUBERNETES_SECRET_API_"):
        return [manual_fix("fix_kubernetes_secret_api_resolver")]
    if code.startswith("DPONE_AIRFLOW_CONNECTION_"):
        return [manual_fix("declare_airflow_operator_bridge")]
    if code.startswith("DPONE_CREDENTIAL_RUNTIME_"):
        return [manual_fix("fix_credential_runtime")]
    return []


__all__ = [
    "FORBIDDEN_SECRET_KEYS",
    "PRODUCTION_ENV_NAMES",
    "SUPPORTED_CREDENTIAL_RESOLVERS",
    "environment_mismatch_errors",
    "fixes_for_code",
    "is_non_empty_string_mapping",
    "is_safe_volume_field_path",
    "is_safe_volume_mount_path",
]

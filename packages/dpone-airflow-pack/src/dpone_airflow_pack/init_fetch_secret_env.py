"""Fail-closed Kubernetes Secret projection for the init-fetch container."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from dpone_airflow_pack.init_fetch_contract import RegistryCredentialSource
from dpone_airflow_pack.init_fetch_pod_guard import provider_env, reserved_collision


def split_registry_credential_env(
    credentials: RegistryCredentialSource | None,
    raw_env_vars: object,
) -> tuple[object, Any | None]:
    """Remove one env-projected credential before ordinary pack validation."""

    if credentials is None or credentials.projection_mode != "env" or not isinstance(raw_env_vars, Mapping):
        return raw_env_vars, None
    credential_value = raw_env_vars.get(credentials.env_name)
    clean = {key: value for key, value in raw_env_vars.items() if str(key) != credentials.env_name}
    return clean, credential_value


def restore_registry_credential_env(
    credentials: RegistryCredentialSource | None,
    env_vars: dict[str, Any],
    credential_value: Any | None,
) -> None:
    """Restore one selected credential after ordinary pack validation."""

    if credentials is not None and credential_value is not None:
        env_vars[credentials.env_name] = deepcopy(credential_value)


def prepare_registry_credential_env(
    credentials: RegistryCredentialSource | None,
    raw_env_vars: object,
) -> dict[str, Any]:
    """Validate ordinary pack env while preserving one init-only credential."""

    clean, credential_value = split_registry_credential_env(credentials, raw_env_vars)
    env_vars = provider_env(clean)
    restore_registry_credential_env(credentials, env_vars, credential_value)
    return env_vars


def ensure_registry_credential_env_available(
    credentials: RegistryCredentialSource | None,
    env_vars: Mapping[str, Any],
) -> None:
    """Reject pack-owned values that could shadow the trusted Secret reference."""

    if credentials is None:
        return
    if credentials.projection_mode == "k8s_secret" and credentials.env_name in env_vars:
        raise reserved_collision("pack cannot shadow the runtime artifact registry credential variable")
    if credentials.projection_mode == "env" and credentials.env_name not in env_vars:
        raise reserved_collision("pack must provide the selected runtime artifact registry environment variable")


def without_registry_credential(
    credentials: RegistryCredentialSource | None,
    env_vars: Mapping[str, Any],
) -> dict[str, Any]:
    """Keep an env-projected registry credential out of the base container."""

    values = dict(env_vars)
    if credentials is not None and credentials.projection_mode == "env":
        values.pop(credentials.env_name, None)
    return values


def project_registry_credential(
    credentials: RegistryCredentialSource | None,
    env_vars: Mapping[str, Any],
) -> dict[str, Any]:
    """Return init-only environment values with an exact non-optional Secret key."""

    values = dict(env_vars)
    if credentials is None:
        return values
    if credentials.projection_mode == "env":
        return values
    if credentials.secret_name is None or credentials.secret_key is None:
        raise reserved_collision("k8s_secret projection requires one complete Secret reference")
    values[credentials.env_name] = {
        "valueFrom": {
            "secretKeyRef": {
                "name": credentials.secret_name,
                "key": credentials.secret_key,
                "optional": False,
            }
        }
    }
    return values


__all__ = [
    "ensure_registry_credential_env_available",
    "prepare_registry_credential_env",
    "project_registry_credential",
    "restore_registry_credential_env",
    "split_registry_credential_env",
    "without_registry_credential",
]

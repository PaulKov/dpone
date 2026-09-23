"""Fail-closed Kubernetes Secret projection for the init-fetch container."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.init_fetch_contract import RegistryCredentialSource
from dpone_airflow_pack.init_fetch_pod_guard import reserved_collision


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
    "project_registry_credential",
    "without_registry_credential",
]

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

    if credentials is not None and credentials.secret_key in env_vars:
        raise reserved_collision("pack cannot provide the runtime artifact registry credential variable")


def project_registry_credential(
    credentials: RegistryCredentialSource | None,
    env_vars: Mapping[str, Any],
) -> dict[str, Any]:
    """Return init-only environment values with an exact non-optional Secret key."""

    values = dict(env_vars)
    if credentials is None:
        return values
    values[credentials.secret_key] = {
        "valueFrom": {
            "secretKeyRef": {
                "name": credentials.secret_name,
                "key": credentials.secret_key,
                "optional": False,
            }
        }
    }
    return values


__all__ = ["ensure_registry_credential_env_available", "project_registry_credential"]

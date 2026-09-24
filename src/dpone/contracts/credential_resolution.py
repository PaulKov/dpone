"""Stable, secret-free credential resolution policy and error contract."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.credential_env import is_valid_connection_ref, is_valid_env_var_name

VERSION_POLICY_LATEST = "latest"
VERSION_POLICY_PINNED = "pinned"
RESOLUTION_SCOPE_WORKLOAD_START = "workload_start"
RESOLUTION_SCOPE_DAG_RUN_START = "dag_run_start"

SUPPORTED_VERSION_POLICIES = frozenset({VERSION_POLICY_LATEST, VERSION_POLICY_PINNED})
SUPPORTED_RESOLUTION_SCOPES = frozenset({RESOLUTION_SCOPE_WORKLOAD_START, RESOLUTION_SCOPE_DAG_RUN_START})

PINNED_VERSION_UNSUPPORTED = "DPONE_CREDENTIAL_PINNED_VERSION_UNSUPPORTED"
DAG_RUN_SCOPE_UNSUPPORTED = "DPONE_CREDENTIAL_DAG_RUN_SCOPE_UNSUPPORTED"
VERSION_METADATA_MISSING = "DPONE_CREDENTIAL_VERSION_METADATA_MISSING"
FIELD_MISSING = "DPONE_CREDENTIAL_FIELD_MISSING"
BACKEND_UNAVAILABLE = "DPONE_CREDENTIAL_BACKEND_UNAVAILABLE"


class CredentialResolutionError(ValueError):
    """Runtime credential failure with a stable code and no secret details."""

    def __init__(self, code: str, message: str, *, resolver: str) -> None:
        super().__init__(message)
        self.code = code
        self.resolver = resolver


def require_supported_snapshot_policy(credentials: Mapping[str, Any], *, resolver: str) -> None:
    """Reject guarantees unavailable to current non-snapshot runtime readers."""
    if credentials.get("version_policy") == VERSION_POLICY_PINNED:
        raise CredentialResolutionError(
            PINNED_VERSION_UNSUPPORTED,
            "Pinned credential versions are not supported by this runtime resolver; use latest.",
            resolver=resolver,
        )
    if credentials.get("resolution_scope") == RESOLUTION_SCOPE_DAG_RUN_START:
        raise CredentialResolutionError(
            DAG_RUN_SCOPE_UNSUPPORTED,
            "DAG-run credential snapshots are not supported; use workload_start.",
            resolver=resolver,
        )


__all__ = [
    "require_supported_snapshot_policy",
    "BACKEND_UNAVAILABLE",
    "DAG_RUN_SCOPE_UNSUPPORTED",
    "FIELD_MISSING",
    "PINNED_VERSION_UNSUPPORTED",
    "RESOLUTION_SCOPE_DAG_RUN_START",
    "RESOLUTION_SCOPE_WORKLOAD_START",
    "SUPPORTED_RESOLUTION_SCOPES",
    "SUPPORTED_VERSION_POLICIES",
    "VERSION_METADATA_MISSING",
    "VERSION_POLICY_LATEST",
    "VERSION_POLICY_PINNED",
    "CredentialResolutionError",
    "is_valid_connection_ref",
    "is_valid_env_var_name",
]

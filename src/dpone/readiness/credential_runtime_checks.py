"""Credential-runtime validation for Airflow self-service readiness checks."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.contracts.credential_security import FORBIDDEN_SECRET_KEYS
from dpone.readiness.airflow_connection_check_support import environment_mismatch_errors
from dpone.readiness.error_contract import dpone_error, error_docs_url

SUPPORTED_VAULT_AUTH_METHODS = frozenset({"kubernetes", "jwt", "approle"})


def validate_credential_runtime(payload: dict[str, Any], environment: str, path: Path) -> list[dict[str, Any]]:
    """Validate non-secret credential runtime configuration without contacting secret backends."""

    if payload.get("schema") != "dpone.credential-runtime.v1":
        return [_error("DPONE_CREDENTIAL_RUNTIME_SCHEMA_INVALID", "credential-runtime schema is invalid", path)]
    actual_environment = payload.get("environment")
    if actual_environment in (None, ""):
        return [
            _error(
                "DPONE_CREDENTIAL_RUNTIME_ENVIRONMENT_REQUIRED",
                "credential-runtime environment is required",
                path,
                extra={"expected_environment": environment},
            )
        ]
    errors = environment_mismatch_errors(
        payload,
        expected_environment=environment,
        code="DPONE_CREDENTIAL_RUNTIME_ENVIRONMENT_MISMATCH",
        message="credential-runtime environment does not match requested environment",
        path=path,
    )
    vault = payload.get("vault")
    if isinstance(vault, dict):
        errors.extend(_validate_vault_runtime(vault, path))
    return errors


def _validate_vault_runtime(vault: dict[str, Any], path: Path) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    if not str(vault.get("address") or ""):
        errors.append(
            _error(
                "DPONE_CREDENTIAL_RUNTIME_VAULT_ADDRESS_REQUIRED",
                "credential-runtime vault.address is required",
                path,
            )
        )
    auth = vault.get("auth")
    if not isinstance(auth, dict):
        return [
            *errors,
            _error(
                "DPONE_CREDENTIAL_RUNTIME_VAULT_AUTH_REQUIRED",
                "credential-runtime vault.auth is required",
                path,
            ),
        ]
    if str(auth.get("method") or "") not in SUPPORTED_VAULT_AUTH_METHODS:
        errors.append(
            _error(
                "DPONE_CREDENTIAL_RUNTIME_VAULT_AUTH_METHOD_INVALID",
                "credential-runtime vault.auth.method must be kubernetes, jwt, or approle",
                path,
            )
        )
    if not str(auth.get("role") or ""):
        errors.append(
            _error(
                "DPONE_CREDENTIAL_RUNTIME_VAULT_AUTH_ROLE_REQUIRED",
                "credential-runtime vault.auth.role is required",
                path,
            )
        )
    if any(key.lower() in FORBIDDEN_SECRET_KEYS for key in auth):
        errors.append(
            _error(
                "DPONE_CREDENTIAL_RUNTIME_SECRET_MATERIAL_FORBIDDEN",
                "credential-runtime must reference workload identity and must not contain "
                f"inline secret material fields: {_forbidden_secret_key_list()}",
                path,
            )
        )
    return errors


def _forbidden_secret_key_list() -> str:
    return ", ".join(sorted(FORBIDDEN_SECRET_KEYS))


def _error(code: str, message: str, path: Path, *, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    return dpone_error(
        code=code,
        message=message,
        stage="check_connections",
        severity="error",
        entity={"kind": "connection_config", "id": path.name},
        path=str(path),
        fixes=[],
        docs_url=error_docs_url(code),
        extra=extra,
    )

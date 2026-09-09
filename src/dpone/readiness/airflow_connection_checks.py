"""Credential-free binding and connection registry validation for Airflow UX."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml

from dpone.contracts.credential_env import is_valid_connection_ref
from dpone.contracts.credential_security import forbidden_inline_secret_paths
from dpone.readiness.airflow_connection_bridge_report import airflow_connection_bridge_report
from dpone.readiness.airflow_connection_check_support import (
    environment_mismatch_errors,
    fixes_for_code,
)
from dpone.readiness.airflow_connection_credential_checks import validate_credentials
from dpone.readiness.airflow_connection_migration_plan import connection_registry_migration_plan
from dpone.readiness.airflow_mssql_database_authority_checks import (
    validate_governed_mssql_database_authorities,
)
from dpone.readiness.credential_runtime_checks import validate_credential_runtime
from dpone.readiness.error_contract import dpone_error, error_docs_url, manual_fix


def validate_connection_configuration(
    *,
    root: Path,
    pipeline_source: dict[str, Any],
    environment: str,
) -> dict[str, Any]:
    """Validate non-secret connection references without network or secret reads."""

    source_connection_refs = _connection_refs_from_pipeline(pipeline_source)
    connection_refs = {ref for ref in source_connection_refs if is_valid_connection_ref(ref)}
    binding_path = root / "environments" / environment / "binding-set.yaml"
    runtime_path = root / "environments" / environment / "credential-runtime.yaml"
    registry_path = root / "platform" / "connection-registries" / f"{environment}.yaml"
    errors: list[dict[str, Any]] = []
    errors.extend(_invalid_connection_ref_errors(source_connection_refs, root))
    binding_set = _read_mapping(binding_path, errors, code="DPONE_BINDING_SET_NOT_FOUND")
    registry = _read_mapping(registry_path, errors, code="DPONE_CONNECTION_REGISTRY_NOT_FOUND")
    credential_runtime = _read_mapping(runtime_path, errors, code="DPONE_CREDENTIAL_RUNTIME_NOT_FOUND")
    connection_ref_map = {ref: ref for ref in connection_refs}
    if binding_set:
        errors.extend(_validate_binding_set(binding_set, connection_refs, environment, binding_path))
        connection_ref_map = _connection_ref_resolution_map(binding_set, connection_refs)
    resolved_connection_refs = set(connection_ref_map.values())
    migration_plan = None
    if registry:
        errors.extend(_validate_registry(registry, resolved_connection_refs, environment, registry_path))
        errors.extend(
            validate_governed_mssql_database_authorities(
                pipeline_source=pipeline_source,
                connection_ref_map=connection_ref_map,
                registry=registry,
                path=registry_path,
            )
        )
        migration_plan = connection_registry_migration_plan(
            registry=_registry_with_safe_connection_refs(registry),
            registry_path=registry_path,
            root=root,
            environment=environment,
        )
    if credential_runtime:
        errors.extend(validate_credential_runtime(credential_runtime, environment, runtime_path))
    result = {
        "schema": "dpone.connection-check.v1",
        "mode": "connections",
        "network": False,
        "secrets": False,
        "source_queries": False,
        "handshake": "configuration_only",
        "environment": environment,
        "connection_refs": sorted(connection_refs),
        "resolved_connection_refs": sorted(resolved_connection_refs),
        "airflow_connection_bridge": airflow_connection_bridge_report(registry, connection_ref_map),
        "binding_set_path": _relative(binding_path, root),
        "connection_registry_path": _relative(registry_path, root),
        "credential_runtime_path": _relative(runtime_path, root),
        "errors": errors,
    }
    if migration_plan is not None:
        result["connection_registry_migration_plan"] = migration_plan
    return result


def _connection_refs_from_pipeline(source: dict[str, Any]) -> set[str]:
    refs: set[str] = set()
    processes = source.get("processes")
    if not isinstance(processes, list):
        return refs
    for process in processes:
        if not isinstance(process, dict):
            continue
        for side in ("source", "sink", "state"):
            section = process.get(side)
            if isinstance(section, dict) and isinstance(section.get("connection_ref"), str):
                refs.add(section["connection_ref"])
    return refs


def _validate_binding_set(
    payload: dict[str, Any],
    refs: set[str],
    environment: str,
    path: Path,
) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    if payload.get("schema") != "dpone.binding-set.v1":
        errors.append(_error("DPONE_BINDING_SET_SCHEMA_INVALID", "binding-set schema is invalid", path))
    errors.extend(
        environment_mismatch_errors(
            payload,
            expected_environment=environment,
            code="DPONE_BINDING_SET_ENVIRONMENT_MISMATCH",
            message="binding-set environment does not match requested environment",
            path=path,
        )
    )
    bindings = payload.get("bindings")
    if not isinstance(bindings, dict):
        return [*errors, _error("DPONE_BINDING_SET_BINDINGS_INVALID", "bindings must be a mapping", path)]
    errors.extend(_invalid_connection_ref_errors(bindings.keys(), path))
    for binding in bindings.values():
        if not isinstance(binding, dict):
            continue
        target_ref = binding.get("connection_ref")
        if isinstance(target_ref, str) and target_ref and not is_valid_connection_ref(target_ref):
            errors.append(_connection_ref_invalid_error(path))
    for ref in refs:
        binding = bindings.get(ref)
        if (
            not isinstance(binding, dict)
            or not isinstance(binding.get("connection_ref"), str)
            or not binding.get("connection_ref")
        ):
            errors.append(_error("DPONE_CREDENTIAL_REF_NOT_BOUND", f"connection_ref is not bound: {ref}", path))
    return errors


def _connection_ref_resolution_map(payload: dict[str, Any], refs: set[str]) -> dict[str, str]:
    bindings = payload.get("bindings")
    if not isinstance(bindings, dict):
        return {ref: ref for ref in refs}
    resolved: dict[str, str] = {}
    for ref in refs:
        binding = bindings.get(ref)
        if (
            isinstance(binding, dict)
            and isinstance(binding.get("connection_ref"), str)
            and is_valid_connection_ref(binding["connection_ref"])
        ):
            resolved[ref] = binding["connection_ref"]
        else:
            resolved[ref] = ref
    return resolved


def _validate_registry(
    payload: dict[str, Any],
    refs: set[str],
    environment: str,
    path: Path,
) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    if payload.get("schema") != "dpone.connection-registry.v1":
        errors.append(_error("DPONE_CONNECTION_REGISTRY_SCHEMA_INVALID", "connection-registry schema is invalid", path))
    errors.extend(
        environment_mismatch_errors(
            payload,
            expected_environment=environment,
            code="DPONE_CONNECTION_REGISTRY_ENVIRONMENT_MISMATCH",
            message="connection-registry environment does not match requested environment",
            path=path,
        )
    )
    connections = payload.get("connections")
    if not isinstance(connections, dict):
        return [*errors, _error("DPONE_CONNECTION_REGISTRY_CONNECTIONS_INVALID", "connections must be a mapping", path)]
    secret_paths = forbidden_inline_secret_paths(_without_legacy_vault_paths(payload))
    if secret_paths:
        errors.append(
            _error(
                "DPONE_SECRET_VALUE_IN_REGISTRY",
                "connection-registry contains forbidden inline secret fields: " + ", ".join(secret_paths),
                path,
            )
        )
    errors.extend(_invalid_connection_ref_errors(connections.keys(), path))
    for ref in refs:
        connection = connections.get(ref)
        if not isinstance(connection, dict):
            errors.append(
                _error("DPONE_CREDENTIAL_REF_NOT_FOUND", f"connection_ref is missing from registry: {ref}", path)
            )
            continue
        legacy_error = _legacy_registry_entry_error(ref, connection, path)
        if legacy_error is not None:
            errors.append(legacy_error)
            continue
        credentials = connection.get("credentials")
        if not isinstance(credentials, dict):
            errors.append(_error("DPONE_CREDENTIAL_RESOLVER_MISSING", f"credentials resolver is missing: {ref}", path))
            continue
        errors.extend(validate_credentials(ref, credentials, environment, path))
    return errors


def _without_legacy_vault_paths(payload: dict[str, Any]) -> dict[str, Any]:
    """Avoid reporting one legacy reference as both migration and secret data."""

    connections = payload.get("connections")
    if not isinstance(connections, dict):
        return payload
    sanitized = {
        ref: (
            {key: value for key, value in entry.items() if key != "vault_path"}
            if isinstance(entry, dict) and ("connection_type" in entry or "vault_path" in entry)
            else entry
        )
        for ref, entry in connections.items()
    }
    return {**payload, "connections": sanitized}


def _registry_with_safe_connection_refs(payload: dict[str, Any]) -> dict[str, Any]:
    connections = payload.get("connections")
    if not isinstance(connections, dict):
        return payload
    safe_connections = {
        ref: entry for ref, entry in connections.items() if is_valid_connection_ref(ref) and isinstance(entry, dict)
    }
    return {**payload, "connections": safe_connections}


def _invalid_connection_ref_errors(refs: object, path: Path) -> list[dict[str, Any]]:
    if not isinstance(refs, Iterable) or isinstance(refs, (str, bytes)):
        return []
    return [_connection_ref_invalid_error(path) for ref in refs if not is_valid_connection_ref(ref)]


def _connection_ref_invalid_error(path: Path) -> dict[str, Any]:
    return _error(
        "DPONE_CONNECTION_REF_INVALID",
        "connection_ref must be a logical alias using letters, digits, dot, underscore or hyphen",
        path,
    )


def _legacy_registry_entry_error(ref: str, connection: dict[str, Any], path: Path) -> dict[str, Any] | None:
    if "connection_type" not in connection and "vault_path" not in connection:
        return None
    resolver = "vault_kv" if connection.get("connection_type") == "vault" or connection.get("vault_path") else ""
    return dpone_error(
        "DPONE_LEGACY_CONNECTION_REGISTRY_ENTRY_FOUND",
        (
            "Legacy connection_type/vault_path registry entries must be migrated to credentials.resolver "
            "with non-secret connection metadata."
        ),
        stage="check_connections",
        path=path.as_posix(),
        entity={"kind": "connection_ref", "id": ref},
        docs_url=error_docs_url("DPONE_LEGACY_CONNECTION_REGISTRY_ENTRY_FOUND"),
        fixes=[manual_fix("migrate_legacy_connection_entry")],
        extra={"resolver": resolver or "backend_neutral"},
    )


def _read_mapping(path: Path, errors: list[dict[str, Any]], *, code: str) -> dict[str, Any] | None:
    if not path.exists():
        errors.append(_error(code, "required connection configuration file is missing", path))
        return None
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        errors.append(_error("DPONE_CONNECTION_CONFIG_INVALID", "configuration file must be a YAML object", path))
        return None
    return payload


def _relative(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def _error(code: str, message: str, path: Path) -> dict[str, Any]:
    return dpone_error(
        code,
        message,
        stage="check_connections",
        path=path.as_posix(),
        fixes=fixes_for_code(code),
    )


__all__ = ["validate_connection_configuration"]

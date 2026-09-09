"""Strict MSSQL database authority composition for runtime hydration."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from dpone.runtime.errors import RuntimeConfigurationError


def bind_target_atomic_state(
    *,
    state_bindings: Any,
    sink_obj: Any,
    connections: Any,
    database_authority_verifier: Any | None,
) -> None:
    """Bind every route state store to target DML and signed DB authority."""

    location = getattr(state_bindings, "mssql_state_location", None)
    if location is None or location.atomicity != "target_atomic":
        return
    state_connector = state_bindings.shared_mssql_state_connector
    target_connector = getattr(sink_obj, "connector", None)
    if state_connector is None or target_connector is None:
        raise RuntimeConfigurationError("state.atomicity=target_atomic requires MSSQL state and target connectors")
    storages = tuple(
        storage
        for storage in (
            state_bindings.xmin_state_storage,
            getattr(state_bindings, "xmin_handoff_state_storage", None),
        )
        if storage is not None
    )
    for storage in storages:
        binder = getattr(storage, "bind_transaction_connector", None)
        if not callable(binder):
            raise RuntimeConfigurationError(
                "state.atomicity=target_atomic requires a transaction-aware MSSQL source-state storage"
            )
        binder(target_connector)
    if not bool(getattr(connections, "strict", False)):
        return
    if database_authority_verifier is None:
        raise RuntimeConfigurationError(
            "Strict target-atomic MSSQL governance requires target and state connection authority"
        )
    for storage in storages:
        authority_binder = getattr(storage, "bind_database_authority", None)
        if not callable(authority_binder):
            raise RuntimeConfigurationError(
                "Strict target-atomic MSSQL governance requires database-authority-aware state storage"
            )
        authority_binder(database_authority_verifier)


def preflight_target_atomic_database_authority(
    *,
    connections: Any,
    state_config: Mapping[str, Any],
    load_config: Any,
    verifier_factory: Callable[..., Any] | None,
) -> Any | None:
    """Verify deployment-signed database pins before endpoint construction."""

    if not bool(getattr(connections, "strict", False)):
        return None
    target_connection = getattr(connections, "sink", None)
    state_connection = getattr(connections, "state", None)
    target_descriptor = getattr(target_connection, "descriptor", None)
    state_descriptor = getattr(state_connection, "descriptor", None)
    target_type = str(getattr(target_descriptor, "connection_type", "") or "").strip().lower()
    state_type = str(state_config.get("type") or getattr(state_descriptor, "connection_type", "") or "").strip().lower()
    atomicity = str(state_config.get("atomicity") or "").strip().lower()
    if target_type != "mssql" or state_type != "mssql" or atomicity != "target_atomic":
        return None
    if target_connection is None or state_connection is None:
        raise RuntimeConfigurationError(
            "Strict target-atomic MSSQL governance requires target and state connection authority"
        )
    if verifier_factory is None:
        from dpone.runtime.state.mssql_database_authority import MssqlDatabaseAuthorityVerifier

        verifier_factory = MssqlDatabaseAuthorityVerifier.from_connections
    verifier = verifier_factory(
        target_connection=target_connection,
        state_connection=state_connection,
        target_database=str(load_config.target_database or target_connection.credentials.database or ""),
        staging_database=str(
            load_config.staging_database or load_config.target_database or target_connection.credentials.database or ""
        ),
        state_database=str(state_connection.credentials.database or ""),
    )
    preflight = getattr(verifier, "verify_pins", None)
    if not callable(preflight):
        raise RuntimeConfigurationError("Strict target-atomic MSSQL governance requires a pre-source database verifier")
    preflight()
    from dpone.runtime.state.mssql_database_authority import MSSQL_DATABASE_AUTHORITY_SHA256_OPTION

    options = getattr(load_config, "options", None)
    if not isinstance(options, dict):
        raise RuntimeConfigurationError("Strict target-atomic MSSQL governance requires mutable load options")
    authored = options.get(MSSQL_DATABASE_AUTHORITY_SHA256_OPTION)
    if authored not in (None, verifier.authority_sha256):
        raise RuntimeConfigurationError("MSSQL database authority digest is runtime-owned")
    options[MSSQL_DATABASE_AUTHORITY_SHA256_OPTION] = verifier.authority_sha256
    return verifier


def apply_connection_database_defaults(*, load_config: Any, connections: Any) -> None:
    """Project registry-owned databases into the runtime relation."""

    if not connections.strict:
        return
    for field_name, connection in (
        ("source_database", connections.source),
        ("target_database", connections.sink),
    ):
        if connection is None:
            continue
        resolved = str(connection.credentials.database or "").strip() or None
        declared = str(getattr(load_config, field_name, None) or "").strip() or None
        if declared and resolved and declared != resolved:
            raise RuntimeConfigurationError(f"{field_name} conflicts with the deployment-owned connection database")
        if resolved:
            setattr(load_config, field_name, resolved)


__all__ = [
    "apply_connection_database_defaults",
    "bind_target_atomic_state",
    "preflight_target_atomic_database_authority",
]

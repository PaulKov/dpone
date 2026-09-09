"""Credential-free checks for governed SQL Server database pins."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.contracts.mssql_database_authority import (
    MssqlDatabaseAuthorityContractError,
    MssqlDatabaseAuthoritySet,
)
from dpone.readiness.error_contract import dpone_error, error_docs_url, manual_fix

_ROLE_CODES = {
    "target": "DPONE_MSSQL_TARGET_DATABASE_AUTHORITY_REQUIRED",
    "staging": "DPONE_MSSQL_STAGING_DATABASE_AUTHORITY_REQUIRED",
    "state": "DPONE_MSSQL_STATE_DATABASE_AUTHORITY_REQUIRED",
}


def validate_governed_mssql_database_authorities(
    *,
    pipeline_source: Mapping[str, Any],
    connection_ref_map: Mapping[str, str],
    registry: Mapping[str, Any],
    path: Path,
) -> list[dict[str, Any]]:
    """Require a finite signed pin for every database one route can touch."""

    connections = _mapping(registry.get("connections"))
    errors: list[dict[str, Any]] = []
    processes = pipeline_source.get("processes")
    if not isinstance(processes, list):
        return errors
    for process in processes:
        if not isinstance(process, Mapping):
            continue
        sink = _mapping(process.get("sink"))
        state = _mapping(process.get("state"))
        if not _is_governed_mssql(sink, state):
            continue
        sink_entry = _bound_entry(sink, connection_ref_map, connections)
        state_entry = _bound_entry(state, connection_ref_map, connections)
        if not sink_entry or not state_entry:
            # The generic registry checker owns missing refs/entries.
            continue
        sink_connection = _mapping(sink_entry.get("connection"))
        state_connection = _mapping(state_entry.get("connection"))
        target_database = _text(sink_connection.get("database"))
        staging_database = _text(_mapping(sink.get("staging")).get("database")) or target_database
        state_database = _text(state_connection.get("database"))
        target_authorities = _parse_authorities(
            sink_connection,
            capability="target",
            role="target",
            path=path,
            process=process,
            errors=errors,
        )
        if target_authorities is not None:
            _require_pin(
                target_authorities,
                target_database,
                capability="target",
                role="target",
                path=path,
                process=process,
                errors=errors,
            )
            _require_pin(
                target_authorities,
                staging_database,
                capability="staging",
                role="staging",
                path=path,
                process=process,
                errors=errors,
            )
        state_authorities = _parse_authorities(
            state_connection,
            capability="state",
            role="state",
            path=path,
            process=process,
            errors=errors,
        )
        if state_authorities is not None:
            _require_pin(
                state_authorities,
                state_database,
                capability="state",
                role="state",
                path=path,
                process=process,
                errors=errors,
            )
    return errors


def _is_governed_mssql(sink: Mapping[str, Any], state: Mapping[str, Any]) -> bool:
    return (
        canonical_endpoint_type(_text(sink.get("type"))) == "mssql"
        and canonical_endpoint_type(_text(state.get("type"))) == "mssql"
        and _text(state.get("atomicity")).lower() == "target_atomic"
    )


def _bound_entry(
    endpoint: Mapping[str, Any],
    connection_ref_map: Mapping[str, str],
    connections: Mapping[str, Any],
) -> Mapping[str, Any]:
    logical_ref = _text(endpoint.get("connection_ref"))
    registry_ref = _text(connection_ref_map.get(logical_ref))
    return _mapping(connections.get(registry_ref))


def _parse_authorities(
    connection: Mapping[str, Any],
    *,
    capability: str,
    role: str,
    path: Path,
    process: Mapping[str, Any],
    errors: list[dict[str, Any]],
) -> MssqlDatabaseAuthoritySet | None:
    try:
        return MssqlDatabaseAuthoritySet.from_connection_properties(
            connection,
            capability=capability,
        )
    except MssqlDatabaseAuthorityContractError as exc:
        required = exc.code.endswith(("database_authority_required", "database_authorities_required"))
        code = _ROLE_CODES[role] if required else "DPONE_MSSQL_DATABASE_AUTHORITY_INVALID"
        errors.append(_error(code, role=role, path=path, process=process))
        return None


def _require_pin(
    authorities: MssqlDatabaseAuthoritySet,
    database: str,
    *,
    capability: str,
    role: str,
    path: Path,
    process: Mapping[str, Any],
    errors: list[dict[str, Any]],
) -> None:
    try:
        authorities.require(database, capability=capability)
    except MssqlDatabaseAuthorityContractError:
        errors.append(_error(_ROLE_CODES[role], role=role, path=path, process=process))


def _error(
    code: str,
    *,
    role: str,
    path: Path,
    process: Mapping[str, Any],
) -> dict[str, Any]:
    process_id = _text(process.get("name")) or "unnamed"
    if code == "DPONE_MSSQL_DATABASE_AUTHORITY_INVALID":
        message = "SQL Server database authority must be a closed canonical identity document."
    else:
        message = f"Governed MSSQL route requires a signed {role} database authority pin."
    return dpone_error(
        code,
        message,
        stage="check_connections",
        path=path.as_posix(),
        entity={"kind": "process", "id": process_id},
        docs_url=error_docs_url(code),
        fixes=[manual_fix("declare_mssql_database_authorities")],
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: object) -> str:
    return str(value or "").strip()


__all__ = ["validate_governed_mssql_database_authorities"]

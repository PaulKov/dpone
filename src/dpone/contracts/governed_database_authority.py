"""Signed database authority requirements for strict governed routes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.config.load_config import LoadConfig
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthoritySet
from dpone.contracts.postgres_source_authority import (
    PostgresSourceAuthority,
    PostgresSourceAuthorityContractError,
)
from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    RuntimeConnectionAuthorityError,
    canonical_runtime_endpoint_type,
)


def require_governed_mssql_database_authority(
    *,
    sink: ResolvedBindingConnection,
    state: ResolvedBindingConnection | None,
    state_config: Mapping[str, Any],
    target_database: str,
    staging_database: str,
) -> None:
    """Require signed physical pins for strict target-atomic MSSQL routes."""

    sink_descriptor = sink.descriptor
    sink_type = canonical_runtime_endpoint_type(getattr(sink_descriptor, "connection_type", ""))
    atomicity = str(state_config.get("atomicity") or "").strip().lower()
    state_type = canonical_runtime_endpoint_type(state_config.get("type"))
    if sink_type != "mssql" or state_type != "mssql" or atomicity != "target_atomic":
        return
    if state is None:
        raise RuntimeConnectionAuthorityError(
            "DPONE_MSSQL_STATE_DATABASE_AUTHORITY_REQUIRED",
            "Strict target-atomic MSSQL routes require a resolved state connection.",
        )
    state_descriptor = state.descriptor
    if sink_descriptor is None or state_descriptor is None:
        raise RuntimeConnectionAuthorityError(
            "DPONE_MSSQL_DATABASE_AUTHORITY_REQUIRED",
            "Strict target-atomic MSSQL routes require resolved connection descriptors.",
        )
    target_authorities = MssqlDatabaseAuthoritySet.from_connection_properties(
        sink_descriptor.properties,
        capability="target",
    )
    target_authorities.require(target_database, capability="target")
    target_authorities.require(staging_database, capability="staging")
    state_authorities = MssqlDatabaseAuthoritySet.from_connection_properties(
        state_descriptor.properties,
        capability="state",
    )
    state_database = str(state.credentials.database or "").strip()
    state_authorities.require(state_database, capability="state")


def require_governed_postgres_source_authority(
    *,
    source: ResolvedBindingConnection | None,
    sink: ResolvedBindingConnection,
    state_config: Mapping[str, Any],
    load_config: LoadConfig,
) -> None:
    """Require a complete signed source selection before connector creation."""

    source_descriptor = getattr(source, "descriptor", None)
    sink_descriptor = sink.descriptor
    source_type = canonical_runtime_endpoint_type(getattr(source_descriptor, "connection_type", ""))
    sink_type = canonical_runtime_endpoint_type(getattr(sink_descriptor, "connection_type", ""))
    state_type = canonical_runtime_endpoint_type(state_config.get("type"))
    atomicity = str(state_config.get("atomicity") or "").strip().lower()
    if source_type != "postgres" or sink_type != "mssql" or state_type != "mssql" or atomicity != "target_atomic":
        return
    if source is None or source_descriptor is None:
        raise RuntimeConnectionAuthorityError(
            "DPONE_POSTGRES_SOURCE_AUTHORITY_REQUIRED",
            "Strict PostgreSQL→MSSQL governance requires a signed source descriptor.",
        )
    try:
        authority = PostgresSourceAuthority.from_connection_properties(
            source_descriptor.properties,
        )
        selected = authority.select(
            authored_schema=str(load_config.source_schema or ""),
            authored_relation=str(load_config.source_table or ""),
        )
    except PostgresSourceAuthorityContractError as exc:
        raise RuntimeConnectionAuthorityError(
            "DPONE_POSTGRES_SOURCE_AUTHORITY_INVALID",
            str(exc),
        ) from exc
    configured_database = str(source.credentials.database or "")
    authored_database = str(load_config.source_database or configured_database)
    if configured_database != selected.database.canonical_name or authored_database != selected.database.canonical_name:
        raise RuntimeConnectionAuthorityError(
            "DPONE_POSTGRES_SOURCE_DATABASE_AUTHORITY_MISMATCH",
            "The source connection and route database must equal the signed canonical database.",
        )


__all__ = [
    "require_governed_mssql_database_authority",
    "require_governed_postgres_source_authority",
]

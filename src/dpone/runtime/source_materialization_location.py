"""Bind source snapshot work locations from verified runtime connections."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from dpone.contracts.runtime_connection import RuntimeConnectionAuthorityError
from dpone.runtime.source_materialization import SourceMaterializationPolicy

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.runtime.credentials.authority import RuntimeResolvedConnections


_ERROR_CODE = "DPONE_MSSQL_SOURCE_MATERIALIZATION_AUTHORITY_MISMATCH"


def bind_source_materialization_location(
    *,
    load_config: LoadConfig,
    connections: RuntimeResolvedConnections,
) -> None:
    """Bind one same-server registry location before source connector I/O."""

    policy = SourceMaterializationPolicy.from_source_options(load_config.options)
    connection_ref = policy.work_connection_ref
    if connection_ref is None:
        return
    source = connections.source
    work = connections.source_materialization
    if source is None or work is None:
        raise _authority_error("verified source and work connections are required")
    if not _same_mssql_endpoint(source.credentials, work.credentials):
        raise _authority_error("source and work connections must resolve to the same MSSQL endpoint")
    database = _coordinate(work.credentials.database, "database")
    schema = _coordinate(work.credentials.schema, "schema")
    load_config.source_materialization_work_connection_ref = connection_ref
    load_config.source_materialization_work_database = database
    load_config.source_materialization_work_schema = schema


def effective_source_materialization_policy(
    policy: SourceMaterializationPolicy,
    *,
    load_config: LoadConfig,
) -> SourceMaterializationPolicy:
    """Apply only the composition-root-bound location to a logical policy."""

    if policy.work_connection_ref is None:
        return policy
    if load_config.source_materialization_work_connection_ref != policy.work_connection_ref:
        raise _authority_error("source materialization work location was not bound")
    return replace(
        policy,
        work_database=_coordinate(load_config.source_materialization_work_database, "database"),
        work_schema=_coordinate(load_config.source_materialization_work_schema, "schema"),
    )


def _same_mssql_endpoint(source: object, work: object) -> bool:
    source_host = str(getattr(source, "host", "") or "").strip().casefold()
    work_host = str(getattr(work, "host", "") or "").strip().casefold()
    source_port = int(getattr(source, "port", None) or 1433)
    work_port = int(getattr(work, "port", None) or 1433)
    return bool(source_host) and source_host == work_host and source_port == work_port


def _coordinate(value: object, field: str) -> str:
    coordinate = str(value or "").strip()
    if not coordinate or "." in coordinate or "[" in coordinate or "]" in coordinate:
        raise _authority_error(f"work {field} must be one non-empty identifier")
    return coordinate


def _authority_error(message: str) -> RuntimeConnectionAuthorityError:
    return RuntimeConnectionAuthorityError(_ERROR_CODE, message)


__all__ = ["bind_source_materialization_location", "effective_source_materialization_policy"]

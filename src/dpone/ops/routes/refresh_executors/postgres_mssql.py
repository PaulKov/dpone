# ruff: noqa: F822
"""Lazy public facade for the Postgres -> MSSQL refresh executor."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "CHUNK_SCHEMA_VERSION",
    "ConfigurationBlockedPostgresMssqlRouteRefreshExecutor",
    "MssqlBcpChunkLoader",
    "PostgresCopyChunkExporter",
    "PostgresMssqlRefreshConfig",
    "PostgresMssqlRouteRefreshExecutor",
]

_EXPORTS: dict[str, str] = {
    "CHUNK_SCHEMA_VERSION": "dpone.ops.routes.refresh_executors.postgres_mssql_artifacts:CHUNK_SCHEMA_VERSION",
    "ConfigurationBlockedPostgresMssqlRouteRefreshExecutor": (
        "dpone.ops.routes.refresh_executors.postgres_mssql_executor:"
        "ConfigurationBlockedPostgresMssqlRouteRefreshExecutor"
    ),
    "MssqlBcpChunkLoader": "dpone.ops.routes.refresh_executors.postgres_mssql_adapters:MssqlBcpChunkLoader",
    "PostgresCopyChunkExporter": (
        "dpone.ops.routes.refresh_executors.postgres_mssql_adapters:PostgresCopyChunkExporter"
    ),
    "PostgresMssqlRefreshConfig": (
        "dpone.ops.routes.refresh_executors.postgres_mssql_config:PostgresMssqlRefreshConfig"
    ),
    "PostgresMssqlRouteRefreshExecutor": (
        "dpone.ops.routes.refresh_executors.postgres_mssql_executor:PostgresMssqlRouteRefreshExecutor"
    ),
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(name)
    module_name, attr = target.split(":", 1)
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value

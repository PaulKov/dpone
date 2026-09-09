# ruff: noqa: F822
"""Public facade for the MSSQL -> ClickHouse route refresh executor."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "CHUNK_SCHEMA_VERSION",
    "ClickHouseChunkLoadResult",
    "ClickHouseChunkLoader",
    "ClickHouseChunkPrepareResult",
    "ClickHouseClientChunkLoader",
    "ConfigurationBlockedRouteRefreshExecutor",
    "MssqlBcpChunkExporter",
    "MssqlChunkExportResult",
    "MssqlChunkExporter",
    "MssqlClickHouseRefreshConfig",
    "MssqlClickHouseRouteRefreshExecutor",
]

_EXPORTS: dict[str, str] = {
    "CHUNK_SCHEMA_VERSION": "dpone.ops.routes.refresh_executors.mssql_clickhouse_artifacts:CHUNK_SCHEMA_VERSION",
    "ClickHouseChunkLoadResult": "dpone.ops.routes.refresh_executors.mssql_clickhouse_adapters:ClickHouseChunkLoadResult",
    "ClickHouseChunkLoader": "dpone.ops.routes.refresh_executors.mssql_clickhouse_adapters:ClickHouseChunkLoader",
    "ClickHouseChunkPrepareResult": (
        "dpone.ops.routes.refresh_executors.mssql_clickhouse_adapters:ClickHouseChunkPrepareResult"
    ),
    "ClickHouseClientChunkLoader": "dpone.ops.routes.refresh_executors.mssql_clickhouse_adapters:ClickHouseClientChunkLoader",
    "ConfigurationBlockedRouteRefreshExecutor": (
        "dpone.ops.routes.refresh_executors.mssql_clickhouse_executor:ConfigurationBlockedRouteRefreshExecutor"
    ),
    "MssqlBcpChunkExporter": "dpone.ops.routes.refresh_executors.mssql_clickhouse_adapters:MssqlBcpChunkExporter",
    "MssqlChunkExportResult": "dpone.ops.routes.refresh_executors.mssql_clickhouse_adapters:MssqlChunkExportResult",
    "MssqlChunkExporter": "dpone.ops.routes.refresh_executors.mssql_clickhouse_adapters:MssqlChunkExporter",
    "MssqlClickHouseRefreshConfig": (
        "dpone.ops.routes.refresh_executors.mssql_clickhouse_config:MssqlClickHouseRefreshConfig"
    ),
    "MssqlClickHouseRouteRefreshExecutor": (
        "dpone.ops.routes.refresh_executors.mssql_clickhouse_executor:MssqlClickHouseRouteRefreshExecutor"
    ),
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value

# ruff: noqa: F822
"""Route refresh executor registry and concrete backend adapters."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ClickHouseChunkLoadResult",
    "ClickHouseChunkLoader",
    "ClickHouseChunkPrepareResult",
    "ClickHouseClientChunkLoader",
    "ConfigurationBlockedRouteRefreshExecutor",
    "MssqlBcpChunkExporter",
    "MssqlChunkExportResult",
    "MssqlChunkExporter",
    "MssqlBcpChunkLoader",
    "NativeChunkExportResult",
    "NativeChunkExporter",
    "NativeChunkLoadResult",
    "NativeChunkLoader",
    "NativeChunkPrepareResult",
    "PostgresCopyChunkExporter",
    "PostgresMssqlRefreshConfig",
    "PostgresMssqlRouteRefreshExecutor",
    "MssqlClickHouseRefreshConfig",
    "MssqlClickHouseRouteRefreshExecutor",
    "RouteRefreshExecutorRegistry",
]

_EXPORTS: dict[str, str] = {
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
    "MssqlBcpChunkLoader": "dpone.ops.routes.refresh_executors.postgres_mssql_adapters:MssqlBcpChunkLoader",
    "NativeChunkExportResult": "dpone.ops.routes.refresh_executors.native_pipeline:NativeChunkExportResult",
    "NativeChunkExporter": "dpone.ops.routes.refresh_executors.native_pipeline:NativeChunkExporter",
    "NativeChunkLoadResult": "dpone.ops.routes.refresh_executors.native_pipeline:NativeChunkLoadResult",
    "NativeChunkLoader": "dpone.ops.routes.refresh_executors.native_pipeline:NativeChunkLoader",
    "NativeChunkPrepareResult": "dpone.ops.routes.refresh_executors.native_pipeline:NativeChunkPrepareResult",
    "PostgresCopyChunkExporter": (
        "dpone.ops.routes.refresh_executors.postgres_mssql_adapters:PostgresCopyChunkExporter"
    ),
    "PostgresMssqlRefreshConfig": (
        "dpone.ops.routes.refresh_executors.postgres_mssql_config:PostgresMssqlRefreshConfig"
    ),
    "PostgresMssqlRouteRefreshExecutor": (
        "dpone.ops.routes.refresh_executors.postgres_mssql_executor:PostgresMssqlRouteRefreshExecutor"
    ),
    "MssqlClickHouseRefreshConfig": (
        "dpone.ops.routes.refresh_executors.mssql_clickhouse_config:MssqlClickHouseRefreshConfig"
    ),
    "MssqlClickHouseRouteRefreshExecutor": (
        "dpone.ops.routes.refresh_executors.mssql_clickhouse_executor:MssqlClickHouseRouteRefreshExecutor"
    ),
    "RouteRefreshExecutorRegistry": "dpone.ops.routes.refresh_executors.registry:RouteRefreshExecutorRegistry",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value

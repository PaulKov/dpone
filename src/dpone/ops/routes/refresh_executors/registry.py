"""Generic route refresh executor registry."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from dpone.ops.routes.refresh_execution_executor import RouteRefreshExecutor
from dpone.ops.routes.refresh_executors.blocked_executor import ConfigurationBlockedRouteRefreshExecutor
from dpone.ops.routes.refresh_executors.mssql_clickhouse_config import MssqlClickHouseRefreshConfig
from dpone.ops.routes.refresh_executors.postgres_mssql_config import PostgresMssqlRefreshConfig

ExecutorBuilder = Callable[[str | Path | None], RouteRefreshExecutor | None]


class RouteRefreshExecutorRegistry:
    """Build optional route refresh executor backends from CLI/API selectors."""

    def __init__(self, builders: dict[str, ExecutorBuilder] | None = None) -> None:
        self._builders = dict(builders or {})

    @classmethod
    def default(cls) -> RouteRefreshExecutorRegistry:
        return cls(
            {
                "mssql_clickhouse": _build_mssql_clickhouse,
                "mssql-clickhouse": _build_mssql_clickhouse,
                "postgres_mssql": _build_postgres_mssql,
                "postgres-mssql": _build_postgres_mssql,
            }
        )

    def build(self, backend: str | None, *, config_json: str | Path | None = None) -> RouteRefreshExecutor | None:
        normalized = _normalize_backend(backend)
        if normalized in {"", "none"}:
            return None
        builder = self._builders.get(normalized)
        if builder is None:
            return ConfigurationBlockedRouteRefreshExecutor(
                blockers=(f"route_refresh_execution.executor_unknown:{normalized}",),
                summary=f"unknown route refresh executor backend `{normalized}`",
            )
        return builder(config_json)


def _build_mssql_clickhouse(config_json: str | Path | None) -> RouteRefreshExecutor:
    if config_json is None:
        return ConfigurationBlockedRouteRefreshExecutor(
            blockers=("mssql_clickhouse_refresh_executor.config_missing",),
            summary="mssql_clickhouse executor config is required",
            default_blocker="mssql_clickhouse_refresh_executor.config_invalid",
        )
    try:
        config = MssqlClickHouseRefreshConfig.from_json(config_json)
    except Exception as exc:
        return ConfigurationBlockedRouteRefreshExecutor(
            blockers=("mssql_clickhouse_refresh_executor.config_invalid",),
            summary=f"mssql_clickhouse executor config is invalid: {exc}",
            default_blocker="mssql_clickhouse_refresh_executor.config_invalid",
        )
    blockers = config.blockers()
    if blockers:
        return ConfigurationBlockedRouteRefreshExecutor(
            blockers=blockers,
            summary="mssql_clickhouse executor config failed validation",
            default_blocker="mssql_clickhouse_refresh_executor.config_invalid",
        )
    from dpone.ops.routes.refresh_executors.mssql_clickhouse_executor import MssqlClickHouseRouteRefreshExecutor

    return MssqlClickHouseRouteRefreshExecutor.from_config(config)


def _build_postgres_mssql(config_json: str | Path | None) -> RouteRefreshExecutor:
    if config_json is None:
        return ConfigurationBlockedRouteRefreshExecutor(
            blockers=("postgres_mssql_refresh_executor.config_missing",),
            summary="postgres_mssql executor config is required",
            default_blocker="postgres_mssql_refresh_executor.config_invalid",
        )
    try:
        config = PostgresMssqlRefreshConfig.from_json(config_json)
    except Exception as exc:
        return ConfigurationBlockedRouteRefreshExecutor(
            blockers=("postgres_mssql_refresh_executor.config_invalid",),
            summary=f"postgres_mssql executor config is invalid: {exc}",
            default_blocker="postgres_mssql_refresh_executor.config_invalid",
        )
    blockers = config.blockers()
    if blockers:
        return ConfigurationBlockedRouteRefreshExecutor(
            blockers=blockers,
            summary="postgres_mssql executor config failed validation",
            default_blocker="postgres_mssql_refresh_executor.config_invalid",
        )
    from dpone.ops.routes.refresh_executors.postgres_mssql_executor import PostgresMssqlRouteRefreshExecutor

    return PostgresMssqlRouteRefreshExecutor.from_config(config)


def _normalize_backend(value: str | None) -> str:
    return str(value or "").strip().lower().replace("-", "_")


__all__ = ["RouteRefreshExecutorRegistry"]

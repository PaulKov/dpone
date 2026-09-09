"""Generic registry for route refresh snapshot capture row readers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from dpone.ops.routes.refresh_executors.mssql_clickhouse_config import MssqlClickHouseRefreshConfig, int_value
from dpone.ops.routes.refresh_executors.postgres_mssql_adapters import (
    build_mssql_connector,
    build_postgres_connector,
)
from dpone.ops.routes.refresh_executors.postgres_mssql_config import PostgresMssqlRefreshConfig
from dpone.ops.routes.refresh_snapshot_capture_adapters import (
    ClickHouseRouteRefreshRowsReader,
    MssqlRouteRefreshRowsReader,
    PostgresRouteRefreshRowsReader,
)
from dpone.ops.routes.refresh_snapshot_capture_reader import (
    RouteRefreshRowsReader,
    UnavailableRouteRefreshRowsReader,
)
from dpone.runtime.connectors.clickhouse import ClickHouseConnector


@dataclass(frozen=True, slots=True)
class RouteRefreshSnapshotReaderPair:
    """Source and sink row readers for one route refresh snapshot capture."""

    source_reader: RouteRefreshRowsReader
    sink_reader: RouteRefreshRowsReader
    blockers: tuple[str, ...] = ()


ReaderBuilder = Callable[[str | Path | None], RouteRefreshSnapshotReaderPair]


class RouteRefreshSnapshotCaptureReaderRegistry:
    """Build optional route refresh snapshot capture readers from backend selectors."""

    def __init__(self, builders: dict[str, ReaderBuilder] | None = None) -> None:
        self._builders = dict(builders or {})

    @classmethod
    def default(cls) -> RouteRefreshSnapshotCaptureReaderRegistry:
        return cls(
            {
                "mssql_clickhouse": _build_mssql_clickhouse,
                "mssql-clickhouse": _build_mssql_clickhouse,
                "postgres_mssql": _build_postgres_mssql,
                "postgres-mssql": _build_postgres_mssql,
            }
        )

    def build(self, backend: str | None, *, config_json: str | Path | None = None) -> RouteRefreshSnapshotReaderPair:
        normalized = _normalize_backend(backend)
        builder = self._builders.get(normalized)
        if builder is None:
            return _unavailable(f"route_refresh_snapshot_capture.reader_backend_unknown:{normalized}")
        return builder(config_json)


def _build_postgres_mssql(config_json: str | Path | None) -> RouteRefreshSnapshotReaderPair:
    if config_json is None:
        return _unavailable("postgres_mssql_snapshot_capture.config_missing")
    try:
        config = PostgresMssqlRefreshConfig.from_json(config_json)
    except Exception:
        return _unavailable("postgres_mssql_snapshot_capture.config_invalid")
    blockers = config.blockers()
    if blockers:
        return _unavailable(*blockers)
    return RouteRefreshSnapshotReaderPair(
        source_reader=PostgresRouteRefreshRowsReader(
            connector=build_postgres_connector(config.postgres),
            dataset=config.source_dataset,
            columns=config.columns,
            boundary_column=config.boundary_column,
            query_template=config.query_template,
        ),
        sink_reader=MssqlRouteRefreshRowsReader(
            connector=build_mssql_connector(config.mssql),
            dataset=config.target_dataset,
            columns=config.columns,
            boundary_column=config.boundary_column,
        ),
    )


def _build_mssql_clickhouse(config_json: str | Path | None) -> RouteRefreshSnapshotReaderPair:
    if config_json is None:
        return _unavailable("mssql_clickhouse_snapshot_capture.config_missing")
    try:
        config = MssqlClickHouseRefreshConfig.from_json(config_json)
    except Exception:
        return _unavailable("mssql_clickhouse_snapshot_capture.config_invalid")
    blockers = config.blockers()
    if blockers:
        return _unavailable(*blockers)
    return RouteRefreshSnapshotReaderPair(
        source_reader=MssqlRouteRefreshRowsReader(
            connector=build_mssql_connector(config.mssql),
            dataset=config.source_dataset,
            columns=config.columns,
            boundary_column=config.boundary_column,
            query_template=config.query_template,
        ),
        sink_reader=ClickHouseRouteRefreshRowsReader(
            connector=_clickhouse_connector(config.clickhouse, config.target_dataset),
            dataset=config.target_dataset,
            columns=config.columns,
            boundary_column=config.boundary_column,
        ),
    )


def _clickhouse_connector(payload: object, target_dataset: str) -> ClickHouseConnector:
    database = target_dataset.split(".", 1)[0] if "." in target_dataset else "default"
    values = payload if isinstance(payload, dict) else {}
    return ClickHouseConnector(
        host=str(values.get("host", "")),
        port=int_value(values.get("port"), default=9000),
        database=str(values.get("database", database)),
        user=str(values.get("user", "default")),
        password=str(values.get("password", "")),
        secure=str(values.get("secure", "")).strip().lower() in {"1", "true", "yes", "y"},
    )


def _unavailable(*blockers: str) -> RouteRefreshSnapshotReaderPair:
    reader = UnavailableRouteRefreshRowsReader()
    return RouteRefreshSnapshotReaderPair(source_reader=reader, sink_reader=reader, blockers=tuple(blockers))


def _normalize_backend(value: str | None) -> str:
    return str(value or "").strip().lower().replace("-", "_")


__all__ = ["RouteRefreshSnapshotCaptureReaderRegistry", "RouteRefreshSnapshotReaderPair"]

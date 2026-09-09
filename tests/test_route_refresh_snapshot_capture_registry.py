from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.routes.models import RouteKey
from dpone.ops.routes.refresh_snapshot_capture_models import RouteRefreshSnapshotCaptureRequest


class _RecordingConnector:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def get_records(self, query: str, params: object | None = None, as_dict: bool = False) -> list[dict[str, object]]:
        del params
        self.queries.append(query)
        assert as_dict is True
        return [{"id": 1, "name": "alpha"}]


def test_snapshot_capture_registry_builds_postgres_mssql_readers_from_executor_config(tmp_path: Path) -> None:
    from dpone.ops.routes.refresh_snapshot_capture_registry import RouteRefreshSnapshotCaptureReaderRegistry

    config_json = _write_json(
        tmp_path / "postgres_mssql.json",
        {
            "source_dataset": "public.orders",
            "target_dataset": "dbo.orders",
            "boundary_column": "id",
            "columns": ["id", "name"],
            "postgres": {
                "host": "127.0.0.1",
                "port": 5432,
                "database": "src",
                "user": "postgres",
                "password": "secret",
            },
            "mssql": {"host": "127.0.0.1", "port": 1433, "database": "dst", "user": "sa", "password": "secret"},
        },
    )

    readers = RouteRefreshSnapshotCaptureReaderRegistry.default().build("postgres_mssql", config_json=config_json)

    assert readers.source_reader.__class__.__name__ == "PostgresRouteRefreshRowsReader"
    assert readers.sink_reader.__class__.__name__ == "MssqlRouteRefreshRowsReader"


def test_snapshot_capture_registry_builds_mssql_clickhouse_readers_from_executor_config(tmp_path: Path) -> None:
    from dpone.ops.routes.refresh_snapshot_capture_registry import RouteRefreshSnapshotCaptureReaderRegistry

    config_json = _write_json(
        tmp_path / "mssql_clickhouse.json",
        {
            "source_dataset": "dbo.orders",
            "target_dataset": "analytics.orders",
            "boundary_column": "id",
            "columns": ["id", "name"],
            "mssql": {"host": "127.0.0.1", "port": 1433, "database": "src", "user": "sa", "password": "secret"},
            "clickhouse": {
                "host": "127.0.0.1",
                "port": 9000,
                "database": "analytics",
                "user": "default",
                "password": "secret",
            },
        },
    )

    readers = RouteRefreshSnapshotCaptureReaderRegistry.default().build("mssql_clickhouse", config_json=config_json)

    assert readers.source_reader.__class__.__name__ == "MssqlRouteRefreshRowsReader"
    assert readers.sink_reader.__class__.__name__ == "ClickHouseRouteRefreshRowsReader"


def test_sql_row_readers_generate_bounded_queries_with_safe_quoting() -> None:
    from dpone.ops.routes.refresh_snapshot_capture_adapters import (
        ClickHouseRouteRefreshRowsReader,
        MssqlRouteRefreshRowsReader,
        PostgresRouteRefreshRowsReader,
    )

    request = _request()
    postgres = _RecordingConnector()
    mssql = _RecordingConnector()
    clickhouse = _RecordingConnector()

    assert PostgresRouteRefreshRowsReader(
        connector=postgres,
        dataset="public.orders",
        columns=("id", "name"),
        boundary_column="id",
    ).read_rows(request) == [{"id": 1, "name": "alpha"}]
    assert MssqlRouteRefreshRowsReader(
        connector=mssql,
        dataset="dbo.orders",
        columns=("id", "name"),
        boundary_column="id",
    ).read_rows(request) == [{"id": 1, "name": "alpha"}]
    assert ClickHouseRouteRefreshRowsReader(
        connector=clickhouse,
        dataset="analytics.orders",
        columns=("id", "name"),
        boundary_column="id",
    ).read_rows(request) == [{"id": 1, "name": "alpha"}]

    assert 'SELECT "id", "name"' in postgres.queries[0]
    assert 'FROM "public"."orders"' in postgres.queries[0]
    assert '"id" BETWEEN 1 AND 10' in postgres.queries[0]
    assert 'ORDER BY "id"' in postgres.queries[0]

    assert "SELECT [id], [name]" in mssql.queries[0]
    assert "FROM [dbo].[orders]" in mssql.queries[0]
    assert "[id] BETWEEN 1 AND 10" in mssql.queries[0]
    assert "ORDER BY [id]" in mssql.queries[0]

    assert "SELECT `id`, `name`" in clickhouse.queries[0]
    assert "FROM `analytics`.`orders`" in clickhouse.queries[0]
    assert "`id` BETWEEN 1 AND 10" in clickhouse.queries[0]
    assert "ORDER BY `id`" in clickhouse.queries[0]


def test_sql_row_reader_uses_query_template_for_physical_projection() -> None:
    from dpone.ops.routes.refresh_snapshot_capture_adapters import MssqlRouteRefreshRowsReader

    connector = _RecordingConnector()

    MssqlRouteRefreshRowsReader(
        connector=connector,
        dataset="dbo.orders",
        columns=("id", "payload_hex"),
        boundary_column="id",
        query_template=(
            "SELECT {columns}, CONVERT(varchar(max), src.[payload], 2) AS [payload_hex] "
            "FROM {source_table} AS src "
            "WHERE src.{boundary_column} BETWEEN {start} AND {end} "
            "ORDER BY src.{boundary_column}"
        ),
    ).read_rows(_request())

    assert "CONVERT(varchar(max), src.[payload], 2) AS [payload_hex]" in connector.queries[0]
    assert "FROM [dbo].[orders] AS src" in connector.queries[0]


def test_snapshot_capture_registry_unknown_backend_returns_unavailable_readers() -> None:
    from dpone.ops.routes.refresh_snapshot_capture_registry import RouteRefreshSnapshotCaptureReaderRegistry

    readers = RouteRefreshSnapshotCaptureReaderRegistry.default().build("not_a_backend", config_json=None)

    assert readers.source_reader.__class__.__name__ == "UnavailableRouteRefreshRowsReader"
    assert readers.sink_reader.__class__.__name__ == "UnavailableRouteRefreshRowsReader"


def _request() -> RouteRefreshSnapshotCaptureRequest:
    return RouteRefreshSnapshotCaptureRequest(
        route=RouteKey.of("postgres", "mssql", "incremental_merge"),
        dataset="dbo.orders",
        ordinal=1,
        start="1",
        end="10",
        partition="",
        source_boundary="1..10",
        sink_boundary="1..10",
        idempotency_key="route:dbo.orders:1:1..10",
        runner_id="capture-a",
        execution_path="route_refresh_execution.json",
        chunk_artifact_path="",
        columns=("id", "name"),
        key_columns=("id",),
        boundary_column="id",
        type_hints={"id": "int"},
    )


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path

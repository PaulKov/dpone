from __future__ import annotations

from dpone.config import LoadConfig
from dpone.runtime.clickhouse_bulk_path import resolve_clickhouse_bulk_path
from dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts import MSSQLQueryoutArtifactFactory


class ClickHouseConnector:
    """Name-only fake that matches the runtime connector class guard."""


def _load_config(options: dict) -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options=options,
    )


def test_clickhouse_bulk_path_auto_prefers_configured_http_endpoint() -> None:
    path = resolve_clickhouse_bulk_path(
        {"clickhouse_bulk": {"mode": "auto", "http": {"host": "clickhouse.local"}}},
        env={},
        executable_lookup=lambda _name: None,
    )

    assert path == "http"


def test_clickhouse_bulk_path_auto_uses_client_when_binary_is_available() -> None:
    path = resolve_clickhouse_bulk_path(
        {"clickhouse_bulk": {"mode": "auto"}},
        env={},
        executable_lookup=lambda name: "/usr/bin/clickhouse-client" if name == "clickhouse-client" else None,
    )

    assert path == "client"


def test_clickhouse_bulk_path_python_mode_wins_over_runtime_detection() -> None:
    path = resolve_clickhouse_bulk_path(
        {"clickhouse_bulk": {"mode": "python", "http": {"host": "clickhouse.local"}}},
        env={"DPONE_CLICKHOUSE_CLIENT": "clickhouse-client"},
        executable_lookup=lambda _name: "/usr/bin/clickhouse-client",
    )

    assert path == "python"


def test_mssql_source_does_not_emit_clickhouse_tsv_for_python_sink_path() -> None:
    factory = MSSQLQueryoutArtifactFactory(connector=object(), logger=object(), sink_connector=ClickHouseConnector())

    assert not factory.should_encode_for_clickhouse_direct(_load_config({"clickhouse_bulk": {"mode": "python"}}))


def test_mssql_source_does_not_emit_clickhouse_tsv_for_native_tcp_sink_path() -> None:
    factory = MSSQLQueryoutArtifactFactory(connector=object(), logger=object(), sink_connector=ClickHouseConnector())

    assert not factory.should_encode_for_clickhouse_direct(_load_config({"clickhouse_bulk": {"mode": "native_tcp"}}))


def test_mssql_source_uses_nested_clickhouse_bulk_mode_for_direct_tsv() -> None:
    factory = MSSQLQueryoutArtifactFactory(connector=object(), logger=object(), sink_connector=ClickHouseConnector())

    assert factory.should_encode_for_clickhouse_direct(_load_config({"clickhouse_bulk": {"mode": "client"}}))

from __future__ import annotations

import os
import uuid

import pytest
from psycopg import sql

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sinks.load_payload import LoadPayload

pytestmark = [pytest.mark.integration, pytest.mark.integration_postgres, pytest.mark.integration_clickhouse]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)


def test_postgres_copy_stream_to_clickhouse_http_full_refresh_wide_table(
    postgres_connector,
    postgres_schema: str,
    clickhouse_connector,
    clickhouse_settings,
    tmp_path,
) -> None:
    rows = int(os.getenv("DPONE_STREAM_IT_ROWS", "10000"))
    column_count = int(os.getenv("DPONE_STREAM_IT_COLUMNS", "200"))
    source_table = f"stream_src_{uuid.uuid4().hex[:8]}"
    target_table = f"stream_dst_{uuid.uuid4().hex[:8]}"
    columns = ["id", *[f"metric_{index:03d}" for index in range(1, column_count)]]

    _create_postgres_wide_table(postgres_connector, postgres_schema, source_table, columns, rows)
    clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{target_table}`")
    try:
        query = sql.SQL("SELECT {} FROM {}.{} ORDER BY {}").format(
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
            sql.Identifier(postgres_schema),
            sql.Identifier(source_table),
            sql.Identifier("id"),
        )
        stream = postgres_connector.copy_to_stream(
            query,
            columns=columns,
            format="MSSQL_DELIMITED",
            estimated_rows=rows,
        )
        config = LoadConfig(
            source_conn_id="postgres_it",
            target_conn_id="clickhouse_it",
            source_schema=postgres_schema,
            source_table=source_table,
            target_schema=clickhouse_settings.database,
            target_table=target_table,
            load_strategy=LoadStrategy.FULL_REFRESH,
            options={
                "source_type": "clickhouse",
                "clickhouse_bulk": {
                    "mode": "http",
                    "http": {
                        "host": clickhouse_settings.host,
                        "port": _clickhouse_http_port(),
                        "password": clickhouse_settings.password,
                    },
                },
                "physical_design": {
                    "storage": {
                        "clickhouse": {
                            "engine": "MergeTree",
                            "order_by": ["id"],
                        }
                    }
                },
            },
        )

        result = ClickHouseSink(clickhouse_connector).load(
            config,
            LoadPayload(artifact=stream, schema=[(column, "Int64") for column in columns]),
        )

        assert result.inserted_rows == rows
        assert stream.stats.size_bytes > 0
        assert stream.stats.chunks >= 1
        assert list(tmp_path.iterdir()) == []
        assert clickhouse_connector.get_records(
            f"SELECT count(), sum(`id`), sum(`metric_001`) FROM `{clickhouse_settings.database}`.`{target_table}`"
        )[0] == (rows, rows * (rows + 1) // 2, rows * (rows + 1) // 2 + rows)
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{target_table}`")


def _create_postgres_wide_table(connector, schema: str, table: str, columns: list[str], rows: int) -> None:
    column_defs = sql.SQL(", ").join(
        sql.SQL("{} integer NOT NULL").format(sql.Identifier(column)) for column in columns
    )
    connector.execute_query(
        sql.SQL("CREATE TABLE {}.{} ({})").format(sql.Identifier(schema), sql.Identifier(table), column_defs)
    )
    select_expressions = [sql.SQL("gs")]
    select_expressions.extend(sql.SQL("gs + {}").format(sql.Literal(index)) for index in range(1, len(columns)))
    connector.execute_query(
        sql.SQL("INSERT INTO {}.{} ({}) SELECT {} FROM generate_series(1, {}) AS gs").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
            sql.SQL(", ").join(select_expressions),
            sql.Literal(rows),
        )
    )


def _clickhouse_http_port() -> int:
    raw_port = os.getenv("DPONE_IT_CH_HTTP_PORT") or os.getenv("DPONE_IT_CH_HTTP_PORT_FORWARD") or "58123"
    return int(raw_port)

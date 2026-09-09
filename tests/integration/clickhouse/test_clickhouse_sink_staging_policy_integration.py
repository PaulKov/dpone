from __future__ import annotations

import os
import uuid

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink

pytestmark = [pytest.mark.integration, pytest.mark.integration_clickhouse]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)


def _load_config(database: str, table: str, strategy: LoadStrategy, **kwargs) -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="clickhouse-it",
        source_schema="dbo",
        source_table=table,
        target_schema=database,
        target_table=table,
        load_strategy=strategy,
        batch_size=2,
        **kwargs,
    )


def _rows(connector, database: str, table: str) -> list[dict]:
    return connector.get_records(
        f"SELECT id, name FROM `{database}`.`{table}` ORDER BY id",
        as_dict=True,
    )


def _leftover_tables(connector, database: str, table: str) -> list[str]:
    return [
        row["name"]
        for row in connector.get_records(
            f"""
            SELECT name
            FROM system.tables
            WHERE database = '{database}'
              AND startsWith(name, '{table}__dpone_')
            ORDER BY name
            """,
            as_dict=True,
        )
    ]


def test_clickhouse_sink_full_refresh_and_replace_use_staging_shadow_swap(
    clickhouse_connector,
    clickhouse_settings,
) -> None:
    table = f"it_stage_policy_{uuid.uuid4().hex[:10]}"
    sink = ClickHouseSink(clickhouse_connector)
    schema = [("id", "bigint"), ("name", "text")]

    try:
        first = sink.load(
            _load_config(clickhouse_settings.database, table, LoadStrategy.FULL_REFRESH),
            LoadPayload(
                artifact=InMemoryRowsArtifact([{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]),
                schema=schema,
            ),
        )
        assert first.inserted_rows == 2
        assert _rows(clickhouse_connector, clickhouse_settings.database, table) == [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
        ]

        second = sink.load(
            _load_config(clickhouse_settings.database, table, LoadStrategy.FULL_REFRESH),
            LoadPayload(artifact=InMemoryRowsArtifact([{"id": 3, "name": "gamma"}]), schema=schema),
        )
        assert second.inserted_rows == 1
        assert _rows(clickhouse_connector, clickhouse_settings.database, table) == [{"id": 3, "name": "gamma"}]

        replaced = sink.load(
            _load_config(
                clickhouse_settings.database,
                table,
                LoadStrategy.REPLACE,
                custom_predicate="id = 3",
            ),
            LoadPayload(artifact=InMemoryRowsArtifact([{"id": 4, "name": "delta"}]), schema=schema),
        )
        assert replaced.inserted_rows == 1
        assert replaced.replaced_rows == 1
        assert _rows(clickhouse_connector, clickhouse_settings.database, table) == [{"id": 4, "name": "delta"}]
        assert _leftover_tables(clickhouse_connector, clickhouse_settings.database, table) == []
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{table}`")

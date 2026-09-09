from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse_sql_mixin import ClickHouseSqlMixin
from dpone.runtime.sinks.clickhouse_staging_decoder import ClickHouseStagingDecoder


def test_clickhouse_staging_decoder_drops_decoded_table_when_population_fails() -> None:
    class FailingConnector:
        @staticmethod
        def execute_query(query: str) -> None:
            raise RuntimeError(f"decode insert failed: {query.split()[0]}")

    load_config, staging_config, decoded_config = _configs()
    dropped: list[str] = []
    decoder = ClickHouseStagingDecoder(
        connector=FailingConnector(),
        table_name=lambda cfg: f"`{cfg.target_schema}`.`{cfg.target_table}`",
        create_staging_table=lambda _cfg, _schema: decoded_config,
        map_type=ClickHouseSqlMixin._map_type_for_config,
        drop_staging_table=lambda config: dropped.append(config.target_table),
    )

    with pytest.raises(RuntimeError, match="decode insert failed"):
        decoder.prepare(load_config, staging_config, _payload())

    assert dropped == ["orders__decoded"]


def test_clickhouse_staging_decoder_drops_planned_table_when_create_result_is_ambiguous() -> None:
    class Connector:
        @staticmethod
        def execute_query(_query: str) -> None:
            raise AssertionError("population must not start after a create failure")

    load_config, staging_config, decoded_config = _configs()
    dropped: list[str] = []

    def create_then_timeout(config: LoadConfig, _schema) -> None:
        assert config == decoded_config
        raise TimeoutError("ambiguous create timeout")

    decoder = ClickHouseStagingDecoder(
        connector=Connector(),
        table_name=lambda cfg: f"`{cfg.target_schema}`.`{cfg.target_table}`",
        create_staging_table=lambda _cfg, _schema: decoded_config,
        map_type=ClickHouseSqlMixin._map_type_for_config,
        drop_staging_table=lambda config: dropped.append(config.target_table),
        plan_staging_table=lambda _config: decoded_config,
        create_planned_staging_table=create_then_timeout,
    )

    with pytest.raises(TimeoutError, match="ambiguous create timeout"):
        decoder.prepare(load_config, staging_config, _payload())

    assert dropped == ["orders__decoded"]


class FakeCodec:
    @staticmethod
    def clickhouse_decode_expression(value_sql: str) -> str:
        return f"decode({value_sql})"


def _configs() -> tuple[LoadConfig, LoadConfig, LoadConfig]:
    load_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
    )
    return (
        load_config,
        replace(load_config, target_table="orders__staging"),
        replace(load_config, target_table="orders__decoded"),
    )


def _payload() -> LoadPayload:
    return LoadPayload(
        artifact=SimpleNamespace(bulk_text_codec=FakeCodec()),
        schema=[("description", "nvarchar(max)")],
    )

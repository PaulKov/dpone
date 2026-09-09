"""Sink-side backfill contracts: ClickHouse delegation and Kafka semantics."""

from __future__ import annotations

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.sinks.clickhouse_staged_load import ClickHouseStagedLoadService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.backfill import BackfillStrategy, backfill_inner_strategy
from dpone.runtime.sinks.strategies.base import SinkStrategy


def _cfg(strategy: LoadStrategy = LoadStrategy.BACKFILL, **options) -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        load_strategy=strategy,
        options=options,
    )


def test_backfill_inner_strategy_resolution_contract() -> None:
    assert backfill_inner_strategy(_cfg()) == LoadStrategy.PARTITION_REPLACE
    assert backfill_inner_strategy(_cfg(backfill={"inner_mode": "full_refresh"})) == LoadStrategy.FULL_REFRESH
    with pytest.raises(ValueError, match="backfill.inner_mode"):
        backfill_inner_strategy(_cfg(backfill={"inner_mode": "scd2"}))


def test_backfill_strategy_supports_full_refresh_inner_mode() -> None:
    class _Recording(SinkStrategy):
        def __init__(self) -> None:
            self.seen = None

        def load(self, load_config, payload):
            self.seen = load_config.load_strategy
            return LoadResult(inserted_rows=1, updated_rows=0, total_rows=1)

    inner = _Recording()
    payload = LoadPayload(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "bigint")])

    BackfillStrategy({LoadStrategy.FULL_REFRESH: inner}).load(_cfg(backfill={"inner_mode": "full_refresh"}), payload)

    assert inner.seen == LoadStrategy.FULL_REFRESH


def test_clickhouse_staged_load_maps_backfill_to_inner_strategy() -> None:
    service = ClickHouseStagedLoadService(sink=object())

    effective = service._effective_config(_cfg(backfill={"inner_mode": "incremental_merge"}))
    assert effective.load_strategy == LoadStrategy.INCREMENTAL_MERGE

    default_effective = service._effective_config(_cfg())
    assert default_effective.load_strategy == LoadStrategy.PARTITION_REPLACE

    untouched = _cfg(LoadStrategy.REPLACE)
    assert service._effective_config(untouched) is untouched


def test_kafka_backfill_rejects_partitioned_inner_modes() -> None:
    from dpone.runtime.sinks.kafka import KafkaSink

    sink = KafkaSink(connector=object())
    payload = LoadPayload(artifact=InMemoryRowsArtifact([{"id": 1}]), schema=[("id", "bigint")])

    with pytest.raises(ValueError, match="keyed upsert replay"):
        sink.load(_cfg(backfill={"inner_mode": "partition_replace"}), payload)


def test_sources_register_backfill_extraction_as_bounded_full_scan() -> None:
    from dpone.runtime.sources.mssql import MSSQLSource
    from dpone.runtime.sources.postgres import PostgresSource

    mssql = MSSQLSource(connector=object(), logger=None)
    assert mssql._strategy_map[LoadStrategy.BACKFILL] is mssql._strategy_map[LoadStrategy.FULL_REFRESH]

    postgres = PostgresSource(connector=None, state_storage=None, logger=None)
    assert postgres._base_strategy_map[LoadStrategy.BACKFILL] is postgres._base_strategy_map[LoadStrategy.FULL_REFRESH]

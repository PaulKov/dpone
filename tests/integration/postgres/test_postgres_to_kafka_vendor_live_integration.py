"""Docker Postgres → Kafka vendor-live: wide types + KafkaSink-supported strategies.

``partition_replace`` / ``scd2`` and non-merge backfill inner modes are N/A.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from dpone.runtime.lineage.strategy_metadata import StrategyMetadataEnricher
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.kafka import KafkaSink
from dpone.runtime.sources.strategies.postgres.postgres_full_extract import PostgresFullExtractStrategy
from dpone.runtime.sources.strategies.postgres.postgres_incremental_extract import (
    PostgresIncrementalExtractStrategy,
)
from tests.integration.postgres import postgres_kafka_strategy_configs as cfg
from tests.integration.postgres.postgres_kafka_assertions import (
    assert_ids_present,
    assert_ops,
    assert_typed_spot_checks,
    consume_envelopes,
    latest_data_by_id,
)
from tests.integration.postgres.postgres_kafka_wide_fixtures import (
    SOURCE_SCHEMA,
    WIDE_TABLE,
    create_wide_postgres_table,
    insert_wide_watermark_row,
)
from tests.integration.postgres.postgres_live_support import (
    NoopLogger,
    ensure_postgres_schemas,
    kafka_connector,
    postgres_connector,
    postgres_kafka_enabled,
    wait_until_ready,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_kafka,
]


def _topic(suffix: str) -> str:
    return f"dpone_postgres_kafka_{suffix}_{uuid.uuid4().hex[:10]}"


def _watermark_state(postgres, *, table: str, schema: str = SOURCE_SCHEMA, column: str = "updated_at") -> dict:
    rows = postgres.get_records(
        f'SELECT MAX("{column}") AS m FROM "{schema}"."{table}"',
        as_dict=True,
    )
    return {"last_value": rows[0]["m"], "column": column}


def _ready(table: str):
    postgres = postgres_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    kafka = kafka_connector(client_id="dpone-postgres-kafka-it")
    wait_until_ready("kafka", lambda: kafka.create_producer().flush(1))
    postgres.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{table}" CASCADE')
    columns = create_wide_postgres_table(postgres, table=table, schema=SOURCE_SCHEMA)
    return postgres, kafka, columns


def _load(sink, config, artifact, schema):
    payload = StrategyMetadataEnricher().enrich_payload(
        LoadPayload(artifact=artifact, schema=schema),
        load_config=config,
    )
    return sink.load(config, payload)


def _consume(kafka, topic: str, *, expected: int) -> list[dict]:
    return consume_envelopes(
        kafka,
        topic,
        expected=expected,
        group_id=f"dpone-postgres-kafka-it-{uuid.uuid4().hex[:8]}",
    )


@pytest.mark.skipif(not postgres_kafka_enabled(), reason="Postgres/Kafka Docker IT not configured")
def test_postgres_to_kafka_full_refresh_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_fr"
    topic = _topic("fr")
    postgres, kafka, _columns = _ready(table)
    config = cfg.full_refresh(topic=topic, tmp_path=tmp_path, table=table)
    extract = PostgresFullExtractStrategy(postgres, logger=NoopLogger()).extract(config, None)
    assert getattr(extract.artifact, "format", None) == "csv"
    assert getattr(extract.artifact, "rows_exported", None) == 2
    result = _load(KafkaSink(kafka, state_storage=None, logger=NoopLogger()), config, extract.artifact, extract.schema)
    assert result.inserted_rows == 2
    envelopes = _consume(kafka, topic, expected=2)
    assert_ids_present(envelopes, {1, 2})
    assert_ops(envelopes, expected={"upsert"})
    assert_typed_spot_checks(envelopes)


@pytest.mark.skipif(not postgres_kafka_enabled(), reason="Postgres/Kafka Docker IT not configured")
def test_postgres_to_kafka_incremental_append_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_append"
    topic = _topic("append")
    postgres, kafka, _columns = _ready(table)
    config = cfg.incremental_append(topic=topic, tmp_path=tmp_path, table=table)
    strategy = PostgresIncrementalExtractStrategy(postgres, sink_connector=kafka, logger=NoopLogger())
    sink = KafkaSink(kafka, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    assert getattr(first.artifact, "rows_exported", None) == 2
    _load(sink, config, first.artifact, first.schema)
    state_after_first = _watermark_state(postgres, table=table)
    insert_wide_watermark_row(postgres, table=table)
    third = strategy.extract(config, state_after_first)
    assert getattr(third.artifact, "rows_exported", None) == 1
    _load(sink, config, third.artifact, third.schema)
    envelopes = _consume(kafka, topic, expected=3)
    assert_ids_present(envelopes, {1, 2, 3})
    assert_typed_spot_checks(envelopes)


@pytest.mark.skipif(not postgres_kafka_enabled(), reason="Postgres/Kafka Docker IT not configured")
def test_postgres_to_kafka_incremental_merge_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_merge"
    topic = _topic("merge")
    postgres, kafka, _columns = _ready(table)
    config = cfg.incremental_merge(topic=topic, tmp_path=tmp_path, table=table)
    strategy = PostgresIncrementalExtractStrategy(postgres, sink_connector=kafka, logger=NoopLogger())
    sink = KafkaSink(kafka, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    _load(sink, config, first.artifact, first.schema)
    state_after_first = _watermark_state(postgres, table=table)
    postgres.execute_query(
        f'UPDATE "{SOURCE_SCHEMA}"."{table}" '
        f"SET c_name = 'merged', updated_at = TIMESTAMP '2026-07-21 11:30:00' WHERE id = 1"
    )
    updated = strategy.extract(config, state_after_first)
    assert getattr(updated.artifact, "rows_exported", None) == 1
    _load(sink, config, updated.artifact, updated.schema)
    state_after_update = _watermark_state(postgres, table=table)
    insert_wide_watermark_row(postgres, table=table)
    third = strategy.extract(config, state_after_update)
    _load(sink, config, third.artifact, third.schema)
    envelopes = _consume(kafka, topic, expected=4)
    latest = latest_data_by_id(envelopes)
    assert latest[1]["c_name"] == "merged"
    assert latest[3]["c_name"] == "row-three"
    assert_typed_spot_checks(envelopes, expected_name="merged")


@pytest.mark.skipif(not postgres_kafka_enabled(), reason="Postgres/Kafka Docker IT not configured")
def test_postgres_to_kafka_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_replace"
    topic = _topic("replace")
    postgres, kafka, _columns = _ready(table)
    fr = cfg.full_refresh(topic=topic, tmp_path=tmp_path, table=table)
    extract = PostgresFullExtractStrategy(postgres, logger=NoopLogger()).extract(fr, None)
    sink = KafkaSink(kafka, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    postgres.execute_query(f'UPDATE "{SOURCE_SCHEMA}"."{table}" SET c_name = \'replaced\' WHERE id = 1')
    config = cfg.replace(topic=topic, tmp_path=tmp_path, table=table)
    replaced = PostgresFullExtractStrategy(postgres, logger=NoopLogger()).extract(config, None)
    _load(sink, config, replaced.artifact, replaced.schema)
    envelopes = _consume(kafka, topic, expected=4)
    replace_rows = [item for item in envelopes if str(item.get("op")).lower() == "replace"]
    assert replace_rows, "replace strategy must publish replacement-intent events"
    latest = latest_data_by_id(envelopes)
    assert latest[1]["c_name"] == "replaced"
    assert_typed_spot_checks(envelopes, expected_name="replaced")


@pytest.mark.skipif(not postgres_kafka_enabled(), reason="Postgres/Kafka Docker IT not configured")
def test_postgres_to_kafka_snapshot_diff_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_diff"
    topic = _topic("diff")
    postgres, kafka, _columns = _ready(table)
    config = cfg.snapshot_diff(topic=topic, tmp_path=tmp_path, table=table)
    strategy = PostgresFullExtractStrategy(postgres, logger=NoopLogger())
    sink = KafkaSink(kafka, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, None)
    _load(sink, config, first.artifact, first.schema)
    postgres.execute_query(f'DELETE FROM "{SOURCE_SCHEMA}"."{table}" WHERE id = 2')
    postgres.execute_query(f'UPDATE "{SOURCE_SCHEMA}"."{table}" SET c_name = \'diff-updated\' WHERE id = 1')
    insert_wide_watermark_row(postgres, table=table)
    second = strategy.extract(config, None)
    _load(sink, config, second.artifact, second.schema)
    envelopes = _consume(kafka, topic, expected=4)
    latest = latest_data_by_id(envelopes)
    assert latest[1]["c_name"] == "diff-updated"
    assert latest[3]["c_name"] == "row-three"
    assert_typed_spot_checks(envelopes, expected_name="diff-updated")


@pytest.mark.skipif(not postgres_kafka_enabled(), reason="Postgres/Kafka Docker IT not configured")
def test_postgres_to_kafka_backfill_merge_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_backfill"
    topic = _topic("backfill")
    postgres, kafka, _columns = _ready(table)
    fr = cfg.full_refresh(topic=topic, tmp_path=tmp_path, table=table)
    extract = PostgresFullExtractStrategy(postgres, logger=NoopLogger()).extract(fr, None)
    sink = KafkaSink(kafka, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    postgres.execute_query(f'UPDATE "{SOURCE_SCHEMA}"."{table}" SET c_name = \'backfilled\' WHERE id = 1')
    config = cfg.backfill_merge(topic=topic, tmp_path=tmp_path, table=table)
    batch = PostgresFullExtractStrategy(postgres, logger=NoopLogger()).extract(config, None)
    _load(sink, config, batch.artifact, batch.schema)
    envelopes = _consume(kafka, topic, expected=4)
    latest = latest_data_by_id(envelopes)
    assert latest[1]["c_name"] == "backfilled"
    assert_typed_spot_checks(envelopes, expected_name="backfilled")

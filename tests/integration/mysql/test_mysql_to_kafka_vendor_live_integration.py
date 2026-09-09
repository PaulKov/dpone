"""Docker MySQL → Kafka vendor-live: wide types + KafkaSink-supported strategies."""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from dpone.runtime.lineage.strategy_metadata import StrategyMetadataEnricher
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.kafka import KafkaSink
from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy
from dpone.runtime.sources.strategies.mysql.mysql_incremental import MySQLIncrementalExtractStrategy
from tests.integration.mysql import mysql_kafka_strategy_configs as cfg
from tests.integration.mysql.mysql_kafka_assertions import (
    assert_ids_present,
    assert_ops,
    assert_typed_spot_checks,
    consume_envelopes,
    latest_data_by_id,
)
from tests.integration.mysql.mysql_kafka_wide_fixtures import (
    WIDE_TABLE,
    create_wide_mysql_table,
    insert_wide_watermark_row,
)
from tests.integration.mysql.mysql_live_support import (
    NoopLogger,
    kafka_connector,
    mysql_connector,
    mysql_kafka_enabled,
    wait_until_ready,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_mysql,
    pytest.mark.integration_kafka,
]


def _topic(suffix: str) -> str:
    return f"dpone_mysql_kafka_{suffix}_{uuid.uuid4().hex[:10]}"


def _incremental_state(mysql, *, table: str, column: str = "updated_at") -> dict[str, str]:
    max_value = mysql.get_max_column_value(mysql.database, table, column, database=mysql.database)
    return {"last_value": max_value, "column": column}


def _ready(table: str):
    mysql = mysql_connector()
    wait_until_ready("mysql", lambda: mysql.get_records("SELECT 1"))
    kafka = kafka_connector()
    wait_until_ready("kafka", lambda: kafka.create_producer().flush(1))
    columns = create_wide_mysql_table(mysql, table=table)
    return mysql, kafka, columns


def _load(sink, config, artifact, schema):
    """Mirror ETLProcessor: enrich strategy metadata before sink.load."""

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
        group_id=f"dpone-mysql-kafka-it-{uuid.uuid4().hex[:8]}",
    )


@pytest.mark.skipif(not mysql_kafka_enabled(), reason="MySQL/Kafka Docker IT not configured")
def test_mysql_to_kafka_full_refresh_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_fr"
    topic = _topic("fr")
    mysql, kafka, _columns = _ready(table)
    config = cfg.full_refresh(mysql_database=mysql.database, topic=topic, tmp_path=tmp_path, table=table)
    extract = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=kafka).extract(config, None)
    assert getattr(extract.artifact, "format", None) == "csv"
    assert getattr(extract.artifact, "rows_exported", None) == 2
    result = _load(KafkaSink(kafka, state_storage=None, logger=NoopLogger()), config, extract.artifact, extract.schema)
    assert result.inserted_rows == 2
    envelopes = _consume(kafka, topic, expected=2)
    assert_ids_present(envelopes, {1, 2})
    assert_ops(envelopes, expected={"upsert"})
    assert_typed_spot_checks(envelopes)


@pytest.mark.skipif(not mysql_kafka_enabled(), reason="MySQL/Kafka Docker IT not configured")
def test_mysql_to_kafka_incremental_append_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_append"
    topic = _topic("append")
    mysql, kafka, _columns = _ready(table)
    config = cfg.incremental_append(mysql_database=mysql.database, topic=topic, tmp_path=tmp_path, table=table)
    strategy = MySQLIncrementalExtractStrategy(mysql, logger=NoopLogger(), sink_connector=kafka)
    sink = KafkaSink(kafka, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    assert getattr(first.artifact, "rows_exported", None) == 2
    _load(sink, config, first.artifact, first.schema)
    state_after_first = _incremental_state(mysql, table=table)
    insert_wide_watermark_row(mysql, table=table)
    third = strategy.extract(config, state_after_first)
    assert getattr(third.artifact, "rows_exported", None) == 1
    _load(sink, config, third.artifact, third.schema)
    envelopes = _consume(kafka, topic, expected=3)
    assert_ids_present(envelopes, {1, 2, 3})
    assert_typed_spot_checks(envelopes)


@pytest.mark.skipif(not mysql_kafka_enabled(), reason="MySQL/Kafka Docker IT not configured")
def test_mysql_to_kafka_incremental_merge_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_merge"
    topic = _topic("merge")
    mysql, kafka, _columns = _ready(table)
    config = cfg.incremental_merge(mysql_database=mysql.database, topic=topic, tmp_path=tmp_path, table=table)
    strategy = MySQLIncrementalExtractStrategy(mysql, logger=NoopLogger(), sink_connector=kafka)
    sink = KafkaSink(kafka, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    _load(sink, config, first.artifact, first.schema)
    state_after_first = _incremental_state(mysql, table=table)
    mysql.execute_query(
        f"UPDATE `{mysql.database}`.`{table}` SET c_name = 'merged', updated_at = '2026-07-21 11:30:00' WHERE id = 1"
    )
    updated = strategy.extract(config, state_after_first)
    assert getattr(updated.artifact, "rows_exported", None) == 1
    _load(sink, config, updated.artifact, updated.schema)
    state_after_update = _incremental_state(mysql, table=table)
    insert_wide_watermark_row(mysql, table=table)
    third = strategy.extract(config, state_after_update)
    _load(sink, config, third.artifact, third.schema)
    envelopes = _consume(kafka, topic, expected=4)
    latest = latest_data_by_id(envelopes)
    assert latest[1]["c_name"] == "merged"
    assert latest[3]["c_name"] == "row-three"
    assert_typed_spot_checks(envelopes, expected_name="merged")


@pytest.mark.skipif(not mysql_kafka_enabled(), reason="MySQL/Kafka Docker IT not configured")
def test_mysql_to_kafka_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_replace"
    topic = _topic("replace")
    mysql, kafka, _columns = _ready(table)
    fr = cfg.full_refresh(mysql_database=mysql.database, topic=topic, tmp_path=tmp_path, table=table)
    extract = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=kafka).extract(fr, None)
    sink = KafkaSink(kafka, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    mysql.execute_query(f"UPDATE `{mysql.database}`.`{table}` SET c_name = 'replaced' WHERE id = 1")
    config = cfg.replace(mysql_database=mysql.database, topic=topic, tmp_path=tmp_path, table=table)
    replaced = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=kafka).extract(config, None)
    _load(sink, config, replaced.artifact, replaced.schema)
    envelopes = _consume(kafka, topic, expected=4)
    replace_rows = [item for item in envelopes if str(item.get("op")).lower() == "replace"]
    assert replace_rows, "replace strategy must publish replacement-intent events"
    latest = latest_data_by_id(envelopes)
    assert latest[1]["c_name"] == "replaced"
    assert_typed_spot_checks(envelopes, expected_name="replaced")


@pytest.mark.skipif(not mysql_kafka_enabled(), reason="MySQL/Kafka Docker IT not configured")
def test_mysql_to_kafka_snapshot_diff_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_diff"
    topic = _topic("diff")
    mysql, kafka, _columns = _ready(table)
    config = cfg.snapshot_diff(mysql_database=mysql.database, topic=topic, tmp_path=tmp_path, table=table)
    strategy = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=kafka)
    sink = KafkaSink(kafka, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, None)
    _load(sink, config, first.artifact, first.schema)
    mysql.execute_query(f"DELETE FROM `{mysql.database}`.`{table}` WHERE id = 2")
    mysql.execute_query(f"UPDATE `{mysql.database}`.`{table}` SET c_name = 'diff-updated' WHERE id = 1")
    insert_wide_watermark_row(mysql, table=table)
    second = strategy.extract(config, None)
    _load(sink, config, second.artifact, second.schema)
    envelopes = _consume(kafka, topic, expected=4)
    latest = latest_data_by_id(envelopes)
    assert latest[1]["c_name"] == "diff-updated"
    assert latest[3]["c_name"] == "row-three"
    assert_typed_spot_checks(envelopes, expected_name="diff-updated")


@pytest.mark.skipif(not mysql_kafka_enabled(), reason="MySQL/Kafka Docker IT not configured")
def test_mysql_to_kafka_backfill_merge_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_backfill"
    topic = _topic("backfill")
    mysql, kafka, _columns = _ready(table)
    fr = cfg.full_refresh(mysql_database=mysql.database, topic=topic, tmp_path=tmp_path, table=table)
    extract = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=kafka).extract(fr, None)
    sink = KafkaSink(kafka, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    mysql.execute_query(f"UPDATE `{mysql.database}`.`{table}` SET c_name = 'backfilled' WHERE id = 1")
    config = cfg.backfill_merge(mysql_database=mysql.database, topic=topic, tmp_path=tmp_path, table=table)
    batch = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=kafka).extract(config, None)
    _load(sink, config, batch.artifact, batch.schema)
    envelopes = _consume(kafka, topic, expected=4)
    latest = latest_data_by_id(envelopes)
    assert latest[1]["c_name"] == "backfilled"
    assert_typed_spot_checks(envelopes, expected_name="backfilled")

"""Docker MySQL → ClickHouse vendor-live: wide types + supported ClickHouse load strategies."""

from __future__ import annotations

from pathlib import Path

import pytest

from dpone.runtime.lineage.strategy_metadata import StrategyMetadataEnricher
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy
from dpone.runtime.sources.strategies.mysql.mysql_incremental import MySQLIncrementalExtractStrategy
from tests.integration.mysql import mysql_clickhouse_strategy_configs as cfg
from tests.integration.mysql.mysql_clickhouse_assertions import (
    assert_row_count,
    assert_typed_spot_checks,
    assert_unique_ids,
    fq,
)
from tests.integration.mysql.mysql_clickhouse_wide_fixtures import (
    WIDE_TABLE,
    create_wide_mysql_table,
    insert_wide_watermark_row,
)
from tests.integration.mysql.mysql_live_support import (
    NoopLogger,
    clickhouse_connector,
    drop_clickhouse_table,
    ensure_clickhouse_database,
    mysql_clickhouse_enabled,
    mysql_connector,
    wait_until_ready,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_mysql,
    pytest.mark.integration_clickhouse,
]


def _ready(table: str):
    mysql = mysql_connector()
    wait_until_ready("mysql", lambda: mysql.get_records("SELECT 1"))
    ensure_clickhouse_database()
    clickhouse = clickhouse_connector()
    wait_until_ready("clickhouse", lambda: clickhouse.get_records("SELECT 1"))
    drop_clickhouse_table(clickhouse, table)
    columns = create_wide_mysql_table(mysql, table=table)
    return mysql, clickhouse, columns


def _load(sink, config, artifact, schema):
    """Mirror ETLProcessor: enrich strategy metadata before sink.load."""

    payload = StrategyMetadataEnricher().enrich_payload(
        LoadPayload(artifact=artifact, schema=schema),
        load_config=config,
    )
    return sink.load(config, payload)


@pytest.mark.skipif(not mysql_clickhouse_enabled(), reason="MySQL/ClickHouse Docker IT not configured")
def test_mysql_to_clickhouse_full_refresh_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_fr"
    mysql, clickhouse, columns = _ready(table)
    config = cfg.full_refresh(mysql_database=mysql.database, tmp_path=tmp_path, table=table)
    extract = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=clickhouse).extract(config, None)
    assert getattr(extract.artifact, "format", None) == "clickhouse-tsv"
    assert getattr(extract.artifact, "rows_exported", None) == 2
    result = _load(
        ClickHouseSink(clickhouse, state_storage=None, logger=NoopLogger()), config, extract.artifact, extract.schema
    )
    assert result.total_rows == 2
    assert_row_count(clickhouse, table=table, expected=2)
    assert_typed_spot_checks(clickhouse, table=table, columns=columns)


@pytest.mark.skipif(not mysql_clickhouse_enabled(), reason="MySQL/ClickHouse Docker IT not configured")
def test_mysql_to_clickhouse_incremental_append_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_append"
    mysql, clickhouse, columns = _ready(table)
    config = cfg.incremental_append(mysql_database=mysql.database, tmp_path=tmp_path, table=table)
    strategy = MySQLIncrementalExtractStrategy(mysql, logger=NoopLogger(), sink_connector=clickhouse)
    sink = ClickHouseSink(clickhouse, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    assert getattr(first.artifact, "rows_exported", None) == 2
    _load(sink, config, first.artifact, first.schema)
    assert_row_count(clickhouse, table=table, expected=2)
    insert_wide_watermark_row(mysql, table=table)
    third = strategy.extract(config, strategy.get_state(config))
    assert getattr(third.artifact, "rows_exported", None) == 1
    _load(sink, config, third.artifact, third.schema)
    assert_row_count(clickhouse, table=table, expected=3)
    assert_typed_spot_checks(clickhouse, table=table, columns=columns)


@pytest.mark.skipif(not mysql_clickhouse_enabled(), reason="MySQL/ClickHouse Docker IT not configured")
def test_mysql_to_clickhouse_incremental_merge_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_merge"
    mysql, clickhouse, columns = _ready(table)
    config = cfg.incremental_merge(mysql_database=mysql.database, tmp_path=tmp_path, table=table)
    strategy = MySQLIncrementalExtractStrategy(mysql, logger=NoopLogger(), sink_connector=clickhouse)
    sink = ClickHouseSink(clickhouse, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    assert getattr(first.artifact, "rows_exported", None) == 2
    _load(sink, config, first.artifact, first.schema)
    idle = strategy.extract(config, strategy.get_state(config))
    assert getattr(idle.artifact, "rows_exported", None) == 0
    mysql.execute_query(
        f"UPDATE `{mysql.database}`.`{table}` SET c_name = 'merged', updated_at = '2026-07-21 11:30:00' WHERE id = 1"
    )
    updated = strategy.extract(config, strategy.get_state(config))
    assert getattr(updated.artifact, "rows_exported", None) == 1
    _load(sink, config, updated.artifact, updated.schema)
    assert_row_count(clickhouse, table=table, expected=2)
    row = clickhouse.get_records(f"SELECT c_name FROM {fq(table)} WHERE id = 1", as_dict=True)[0]
    assert row["c_name"] == "merged"
    insert_wide_watermark_row(mysql, table=table)
    third = strategy.extract(config, strategy.get_state(config))
    assert getattr(third.artifact, "rows_exported", None) == 1
    _load(sink, config, third.artifact, third.schema)
    assert_row_count(clickhouse, table=table, expected=3)
    assert_typed_spot_checks(clickhouse, table=table, columns=columns, expected_name="merged")


@pytest.mark.skipif(not mysql_clickhouse_enabled(), reason="MySQL/ClickHouse Docker IT not configured")
def test_mysql_to_clickhouse_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_replace"
    mysql, clickhouse, columns = _ready(table)
    fr = cfg.full_refresh(mysql_database=mysql.database, tmp_path=tmp_path, table=table)
    extract = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=clickhouse).extract(fr, None)
    sink = ClickHouseSink(clickhouse, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    clickhouse.execute_query(
        f"INSERT INTO {fq(table)} (id, business_date, updated_at, c_name) "
        f"VALUES (99, '2020-01-01', '2020-01-01 00:00:00', 'outside')"
    )
    mysql.execute_query(f"UPDATE `{mysql.database}`.`{table}` SET c_name = 'replaced' WHERE id = 1")
    config = cfg.replace(mysql_database=mysql.database, tmp_path=tmp_path, table=table)
    replaced = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=clickhouse).extract(config, None)
    _load(sink, config, replaced.artifact, replaced.schema)
    rows = clickhouse.get_records(f"SELECT id, c_name FROM {fq(table)} ORDER BY id", as_dict=True)
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert by_id[1] == "replaced"
    assert by_id[99] == "outside"
    assert_unique_ids(clickhouse, table=table)
    assert_typed_spot_checks(clickhouse, table=table, columns=columns, expected_name="replaced")


@pytest.mark.skipif(not mysql_clickhouse_enabled(), reason="MySQL/ClickHouse Docker IT not configured")
def test_mysql_to_clickhouse_partition_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_part"
    mysql, clickhouse, columns = _ready(table)
    fr = cfg.full_refresh(mysql_database=mysql.database, tmp_path=tmp_path, table=table)
    extract = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=clickhouse).extract(fr, None)
    sink = ClickHouseSink(clickhouse, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    clickhouse.execute_query(
        f"INSERT INTO {fq(table)} (id, business_date, updated_at, c_name) "
        f"VALUES (99, '2020-01-01', '2020-01-01 00:00:00', 'other-partition')"
    )
    mysql.execute_query(f"UPDATE `{mysql.database}`.`{table}` SET c_name = 'part-replaced' WHERE id = 1")
    config = cfg.partition_replace(mysql_database=mysql.database, tmp_path=tmp_path, table=table)
    batch = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=clickhouse).extract(config, None)
    _load(sink, config, batch.artifact, batch.schema)
    rows = clickhouse.get_records(f"SELECT id, c_name FROM {fq(table)} ORDER BY id", as_dict=True)
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert by_id[1] == "part-replaced"
    assert by_id[99] == "other-partition"
    assert_unique_ids(clickhouse, table=table)
    assert_typed_spot_checks(clickhouse, table=table, columns=columns, expected_name="part-replaced")


@pytest.mark.skipif(not mysql_clickhouse_enabled(), reason="MySQL/ClickHouse Docker IT not configured")
def test_mysql_to_clickhouse_backfill_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_backfill"
    mysql, clickhouse, columns = _ready(table)
    fr = cfg.full_refresh(mysql_database=mysql.database, tmp_path=tmp_path, table=table)
    extract = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=clickhouse).extract(fr, None)
    sink = ClickHouseSink(clickhouse, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    mysql.execute_query(f"UPDATE `{mysql.database}`.`{table}` SET c_name = 'backfilled' WHERE id = 1")
    config = cfg.backfill_replace(mysql_database=mysql.database, tmp_path=tmp_path, table=table)
    batch = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=clickhouse).extract(config, None)
    _load(sink, config, batch.artifact, batch.schema)
    row = clickhouse.get_records(f"SELECT c_name FROM {fq(table)} WHERE id = 1", as_dict=True)[0]
    assert row["c_name"] == "backfilled"
    assert_typed_spot_checks(clickhouse, table=table, columns=columns, expected_name="backfilled")


@pytest.mark.skipif(not mysql_clickhouse_enabled(), reason="MySQL/ClickHouse Docker IT not configured")
def test_mysql_to_clickhouse_snapshot_diff_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_diff"
    mysql, clickhouse, columns = _ready(table)
    config = cfg.snapshot_diff(mysql_database=mysql.database, tmp_path=tmp_path, table=table)
    strategy = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=clickhouse)
    sink = ClickHouseSink(clickhouse, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, None)
    _load(sink, config, first.artifact, first.schema)
    assert_row_count(clickhouse, table=table, expected=2)
    mysql.execute_query(f"DELETE FROM `{mysql.database}`.`{table}` WHERE id = 2")
    mysql.execute_query(f"UPDATE `{mysql.database}`.`{table}` SET c_name = 'diff-updated' WHERE id = 1")
    insert_wide_watermark_row(mysql, table=table)
    second = strategy.extract(config, None)
    _load(sink, config, second.artifact, second.schema)
    rows = clickhouse.get_records(f"SELECT id, c_name FROM {fq(table)} ORDER BY id", as_dict=True)
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert 2 not in by_id
    assert by_id[1] == "diff-updated"
    assert by_id[3] == "row-three"
    assert_typed_spot_checks(clickhouse, table=table, columns=columns, expected_name="diff-updated")


@pytest.mark.skipif(not mysql_clickhouse_enabled(), reason="MySQL/ClickHouse Docker IT not configured")
def test_mysql_to_clickhouse_scd2_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_scd2"
    mysql, clickhouse, columns = _ready(table)
    config = cfg.scd2(mysql_database=mysql.database, tmp_path=tmp_path, table=table)
    strategy = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=clickhouse)
    sink = ClickHouseSink(clickhouse, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, None)
    _load(sink, config, first.artifact, first.schema)
    cols = clickhouse.get_records(f"DESCRIBE TABLE {fq(table)}", as_dict=True)
    names = {str(row.get("name") or row.get("Name")) for row in cols}
    assert "__dpone__is_current" in names
    assert "__dpone__row_hash" in names
    mysql.execute_query(
        f"UPDATE `{mysql.database}`.`{table}` SET c_name = 'scd2-v2', updated_at = '2026-07-21 13:00:00' WHERE id = 1"
    )
    second = strategy.extract(config, None)
    _load(sink, config, second.artifact, second.schema)
    currents = clickhouse.get_records(
        f"SELECT id, c_name, `__dpone__is_current` AS is_current FROM {fq(table)} "
        f"WHERE id = 1 ORDER BY `__dpone__is_current` DESC",
        as_dict=True,
    )
    assert any(bool(r["is_current"]) and r["c_name"] == "scd2-v2" for r in currents)
    assert any(not bool(r["is_current"]) for r in currents)
    assert_typed_spot_checks(
        clickhouse, table=table, columns=columns, expected_name="scd2-v2", check_seed_scalars=False
    )

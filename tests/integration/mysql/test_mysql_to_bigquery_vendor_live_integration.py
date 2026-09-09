"""Docker MySQL → BigQuery vendor-live: wide types + all BQ load strategies.

Credentials (never commit, never log values):
  BIGQUERY_DWH_PROJECT_ID
  BIGQUERY_DWH_SERVICE_ACCOUNT_KEY_FILE

Optional:
  DPONE_IT_BQ_DATASET  (default: dpone_it_mysql)
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dpone.runtime.lineage.strategy_metadata import StrategyMetadataEnricher
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.bigquery import BigQuerySink
from dpone.runtime.sources.strategies.mysql.mysql_full import MySQLFullExtractStrategy
from dpone.runtime.sources.strategies.mysql.mysql_incremental import MySQLIncrementalExtractStrategy
from tests.integration.mysql import mysql_bigquery_strategy_configs as cfg
from tests.integration.mysql.mysql_bigquery_assertions import (
    assert_row_count,
    assert_typed_spot_checks,
    assert_unique_ids,
    fq,
)
from tests.integration.mysql.mysql_live_support import (
    NoopLogger,
    bigquery_connector,
    drop_bq_table,
    ensure_bq_dataset,
    mysql_bigquery_enabled,
    mysql_connector,
    skip_if_billing_blocked,
    wait_until_ready,
)
from tests.integration.mysql.mysql_wide_type_fixtures import (
    WIDE_TABLE,
    create_wide_mysql_table,
    insert_wide_watermark_row,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_mysql,
]


def _ready(table: str):
    mysql = mysql_connector()
    wait_until_ready("mysql", lambda: mysql.get_records("SELECT 1"))
    bq = bigquery_connector()
    wait_until_ready("bigquery", lambda: bq.get_records("SELECT 1"))
    dataset = ensure_bq_dataset(bq)
    drop_bq_table(bq, table, dataset=dataset)
    columns = create_wide_mysql_table(mysql, table=table)
    return mysql, bq, dataset, columns


def _load(sink, config, artifact, schema):
    """Mirror ETLProcessor: enrich strategy metadata before sink.load."""

    payload = StrategyMetadataEnricher().enrich_payload(
        LoadPayload(artifact=artifact, schema=schema),
        load_config=config,
    )
    try:
        return sink.load(config, payload)
    except Exception as exc:  # noqa: BLE001 - map billing gate to SKIP
        skip_if_billing_blocked(exc)
        raise


@pytest.mark.skipif(not mysql_bigquery_enabled(), reason="MySQL Docker and/or BigQuery SA env not configured")
def test_mysql_to_bigquery_full_refresh_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_fr"
    mysql, bq, dataset, columns = _ready(table)
    config = cfg.full_refresh(mysql_database=mysql.database, dataset=dataset, tmp_path=tmp_path, table=table)
    extract = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=bq).extract(config, None)
    assert getattr(extract.artifact, "format", None) == "csv"
    assert getattr(extract.artifact, "rows_exported", None) == 2
    result = _load(BigQuerySink(bq, state_storage=None, logger=NoopLogger()), config, extract.artifact, extract.schema)
    assert result.total_rows == 2
    assert_row_count(bq, dataset=dataset, table=table, expected=2)
    assert_typed_spot_checks(bq, dataset=dataset, table=table, columns=columns)


@pytest.mark.skipif(not mysql_bigquery_enabled(), reason="MySQL Docker and/or BigQuery SA env not configured")
def test_mysql_to_bigquery_incremental_append_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_append"
    mysql, bq, dataset, columns = _ready(table)
    config = cfg.incremental_append(mysql_database=mysql.database, dataset=dataset, tmp_path=tmp_path, table=table)
    strategy = MySQLIncrementalExtractStrategy(mysql, logger=NoopLogger(), sink_connector=bq)
    sink = BigQuerySink(bq, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    assert getattr(first.artifact, "rows_exported", None) == 2
    _load(sink, config, first.artifact, first.schema)
    assert_row_count(bq, dataset=dataset, table=table, expected=2)
    insert_wide_watermark_row(mysql, table=table)
    third = strategy.extract(config, strategy.get_state(config))
    assert getattr(third.artifact, "rows_exported", None) == 1
    _load(sink, config, third.artifact, third.schema)
    assert_row_count(bq, dataset=dataset, table=table, expected=3)
    assert_typed_spot_checks(bq, dataset=dataset, table=table, columns=columns)


@pytest.mark.skipif(not mysql_bigquery_enabled(), reason="MySQL Docker and/or BigQuery SA env not configured")
def test_mysql_to_bigquery_incremental_merge_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_merge"
    mysql, bq, dataset, columns = _ready(table)
    config = cfg.incremental_merge(mysql_database=mysql.database, dataset=dataset, tmp_path=tmp_path, table=table)
    strategy = MySQLIncrementalExtractStrategy(mysql, logger=NoopLogger(), sink_connector=bq)
    sink = BigQuerySink(bq, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    assert getattr(first.artifact, "rows_exported", None) == 2
    _load(sink, config, first.artifact, first.schema)
    idle = strategy.extract(config, strategy.get_state(config))
    assert getattr(idle.artifact, "rows_exported", None) == 0
    insert_wide_watermark_row(mysql, table=table)
    third = strategy.extract(config, strategy.get_state(config))
    assert getattr(third.artifact, "rows_exported", None) == 1
    _load(sink, config, third.artifact, third.schema)
    assert_row_count(bq, dataset=dataset, table=table, expected=3)
    assert_typed_spot_checks(bq, dataset=dataset, table=table, columns=columns)


@pytest.mark.skipif(not mysql_bigquery_enabled(), reason="MySQL Docker and/or BigQuery SA env not configured")
def test_mysql_to_bigquery_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_replace"
    mysql, bq, dataset, columns = _ready(table)
    fr = cfg.full_refresh(mysql_database=mysql.database, dataset=dataset, tmp_path=tmp_path, table=table)
    extract = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=bq).extract(fr, None)
    sink = BigQuerySink(bq, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    bq.execute_query(
        f"INSERT INTO {fq(bq, dataset, table)} (id, business_date, updated_at, c_name) "
        f"VALUES (99, DATE '2020-01-01', DATETIME '2020-01-01 00:00:00', 'outside')"
    )
    mysql.execute_query(f"UPDATE `{mysql.database}`.`{table}` SET c_name = 'replaced' WHERE id = 1")
    config = cfg.replace(mysql_database=mysql.database, dataset=dataset, tmp_path=tmp_path, table=table)
    replaced = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=bq).extract(config, None)
    _load(sink, config, replaced.artifact, replaced.schema)
    rows = bq.get_records(f"SELECT id, c_name FROM {fq(bq, dataset, table)} ORDER BY id")
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert by_id[1] == "replaced"
    assert by_id[99] == "outside"
    assert_unique_ids(bq, dataset=dataset, table=table)
    assert_typed_spot_checks(bq, dataset=dataset, table=table, columns=columns, expected_name="replaced")


@pytest.mark.skipif(not mysql_bigquery_enabled(), reason="MySQL Docker and/or BigQuery SA env not configured")
def test_mysql_to_bigquery_partition_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_part"
    mysql, bq, dataset, columns = _ready(table)
    fr = cfg.full_refresh(mysql_database=mysql.database, dataset=dataset, tmp_path=tmp_path, table=table)
    extract = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=bq).extract(fr, None)
    sink = BigQuerySink(bq, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    bq.execute_query(
        f"INSERT INTO {fq(bq, dataset, table)} (id, business_date, updated_at, c_name) "
        f"VALUES (99, DATE '2020-01-01', DATETIME '2020-01-01 00:00:00', 'other-partition')"
    )
    mysql.execute_query(f"UPDATE `{mysql.database}`.`{table}` SET c_name = 'part-replaced' WHERE id = 1")
    config = cfg.partition_replace(mysql_database=mysql.database, dataset=dataset, tmp_path=tmp_path, table=table)
    batch = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=bq).extract(config, None)
    _load(sink, config, batch.artifact, batch.schema)
    rows = bq.get_records(f"SELECT id, c_name FROM {fq(bq, dataset, table)} ORDER BY id")
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert by_id[1] == "part-replaced"
    assert by_id[99] == "other-partition"
    assert_unique_ids(bq, dataset=dataset, table=table)
    assert_typed_spot_checks(bq, dataset=dataset, table=table, columns=columns, expected_name="part-replaced")


@pytest.mark.skipif(not mysql_bigquery_enabled(), reason="MySQL Docker and/or BigQuery SA env not configured")
def test_mysql_to_bigquery_snapshot_diff_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_diff"
    mysql, bq, dataset, columns = _ready(table)
    config = cfg.snapshot_diff(mysql_database=mysql.database, dataset=dataset, tmp_path=tmp_path, table=table)
    strategy = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=bq)
    sink = BigQuerySink(bq, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, None)
    _load(sink, config, first.artifact, first.schema)
    assert_row_count(bq, dataset=dataset, table=table, expected=2)
    mysql.execute_query(f"DELETE FROM `{mysql.database}`.`{table}` WHERE id = 2")
    mysql.execute_query(f"UPDATE `{mysql.database}`.`{table}` SET c_name = 'diff-updated' WHERE id = 1")
    insert_wide_watermark_row(mysql, table=table, row_id=3)
    second = strategy.extract(config, None)
    _load(sink, config, second.artifact, second.schema)
    rows = bq.get_records(f"SELECT id, c_name FROM {fq(bq, dataset, table)} ORDER BY id")
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert 2 not in by_id
    assert by_id[1] == "diff-updated"
    assert by_id[3] == "row-three"
    assert_typed_spot_checks(bq, dataset=dataset, table=table, columns=columns, expected_name="diff-updated")


@pytest.mark.skipif(not mysql_bigquery_enabled(), reason="MySQL Docker and/or BigQuery SA env not configured")
def test_mysql_to_bigquery_scd2_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_scd2"
    mysql, bq, dataset, columns = _ready(table)
    config = cfg.scd2(mysql_database=mysql.database, dataset=dataset, tmp_path=tmp_path, table=table)
    strategy = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=bq)
    sink = BigQuerySink(bq, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, None)
    _load(sink, config, first.artifact, first.schema)
    fields = {f.name for f in bq.connection.get_table(f"{bq.project_id}.{dataset}.{table}").schema}
    assert "__dpone__is_current" in fields
    assert "__dpone__row_hash" in fields
    mysql.execute_query(
        f"UPDATE `{mysql.database}`.`{table}` SET c_name = 'scd2-v2', updated_at = '2026-07-21 13:00:00' WHERE id = 1"
    )
    second = strategy.extract(config, None)
    _load(sink, config, second.artifact, second.schema)
    currents = bq.get_records(
        f"SELECT id, c_name, __dpone__is_current AS is_current "
        f"FROM {fq(bq, dataset, table)} WHERE id = 1 ORDER BY __dpone__is_current DESC"
    )
    assert any(bool(r["is_current"]) and r["c_name"] == "scd2-v2" for r in currents)
    assert any(not bool(r["is_current"]) for r in currents)
    assert_typed_spot_checks(
        bq,
        dataset=dataset,
        table=table,
        columns=columns,
        expected_name="scd2-v2",
        check_seed_scalars=False,
    )


@pytest.mark.skipif(not mysql_bigquery_enabled(), reason="MySQL Docker and/or BigQuery SA env not configured")
def test_mysql_to_bigquery_backfill_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_backfill"
    mysql, bq, dataset, columns = _ready(table)
    fr = cfg.full_refresh(mysql_database=mysql.database, dataset=dataset, tmp_path=tmp_path, table=table)
    extract = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=bq).extract(fr, None)
    sink = BigQuerySink(bq, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    mysql.execute_query(f"UPDATE `{mysql.database}`.`{table}` SET c_name = 'backfilled' WHERE id = 1")
    config = cfg.backfill_replace(mysql_database=mysql.database, dataset=dataset, tmp_path=tmp_path, table=table)
    batch = MySQLFullExtractStrategy(mysql, logger=NoopLogger(), sink_connector=bq).extract(config, None)
    _load(sink, config, batch.artifact, batch.schema)
    row = bq.get_records(f"SELECT c_name FROM {fq(bq, dataset, table)} WHERE id = 1")[0]
    assert row["c_name"] == "backfilled"
    assert_typed_spot_checks(bq, dataset=dataset, table=table, columns=columns, expected_name="backfilled")

"""Docker MSSQL → BigQuery vendor-live: wide types + all BQ load strategies.

Credentials (never commit, never log values):
  BIGQUERY_DWH_PROJECT_ID
  BIGQUERY_DWH_SERVICE_ACCOUNT_KEY_FILE

Optional:
  DPONE_IT_BQ_DATASET  (default: dpone_it_mysql; use dpone_it_mssql for isolation)

Load path: configs set batch_commit_mode=whole so BQ uses direct CSV load jobs
(no GCS bucket).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tests.integration.mssql import mssql_bigquery_strategy_configs as cfg
from tests.integration.mssql.mssql_bigquery_assertions import (
    assert_row_count,
    assert_typed_spot_checks,
    assert_unique_ids,
    fq,
)
from tests.integration.mssql.mssql_bigquery_wide_fixtures import (
    SOURCE_SCHEMA,
    WIDE_TABLE,
    create_wide_mssql_table,
    insert_wide_watermark_row,
)
from tests.integration.mssql.mssql_live_support import (
    NoopLogger,
    bigquery_connector,
    drop_bq_table,
    ensure_bq_dataset,
    ensure_mssql_database_and_schemas,
    mssql_bigquery_enabled,
    mssql_connector,
    skip_if_billing_blocked,
    wait_until_ready,
)

from dpone.runtime.lineage.strategy_metadata import StrategyMetadataEnricher
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.bigquery import BigQuerySink
from dpone.runtime.sources.strategies.mssql.mssql_full import MSSQLFullExtractStrategy
from dpone.runtime.sources.strategies.mssql.mssql_incremental import MSSQLIncrementalExtractStrategy

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_mssql,
]


def _ready(table: str):
    ensure_mssql_database_and_schemas(source_schema=SOURCE_SCHEMA)
    mssql = mssql_connector()
    wait_until_ready("mssql", lambda: mssql.get_records("SELECT 1"))
    bq = bigquery_connector()
    wait_until_ready("bigquery", lambda: bq.get_records("SELECT 1"))
    dataset = ensure_bq_dataset(bq)
    drop_bq_table(bq, table, dataset=dataset)
    mssql.execute_query(f"DROP TABLE IF EXISTS [{SOURCE_SCHEMA}].[{table}]")
    columns = create_wide_mssql_table(mssql, table=table, schema=SOURCE_SCHEMA)
    return mssql, bq, dataset, columns


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


@pytest.mark.skipif(not mssql_bigquery_enabled(), reason="MSSQL Docker and/or BigQuery SA env not configured")
def test_mssql_to_bigquery_full_refresh_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_fr"
    mssql, bq, dataset, columns = _ready(table)
    config = cfg.full_refresh(dataset=dataset, tmp_path=tmp_path, table=table)
    extract = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=bq).extract(config, None)
    assert getattr(extract.artifact, "format", None) == "csv"
    assert getattr(extract.artifact, "rows_exported", None) == 2
    result = _load(BigQuerySink(bq, state_storage=None, logger=NoopLogger()), config, extract.artifact, extract.schema)
    assert result.total_rows == 2
    assert_row_count(bq, dataset=dataset, table=table, expected=2)
    assert_typed_spot_checks(bq, dataset=dataset, table=table, columns=columns)


@pytest.mark.skipif(not mssql_bigquery_enabled(), reason="MSSQL Docker and/or BigQuery SA env not configured")
def test_mssql_to_bigquery_incremental_append_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_append"
    mssql, bq, dataset, columns = _ready(table)
    config = cfg.incremental_append(dataset=dataset, tmp_path=tmp_path, table=table)
    strategy = MSSQLIncrementalExtractStrategy(mssql, logger=NoopLogger(), sink_connector=bq)
    sink = BigQuerySink(bq, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    assert getattr(first.artifact, "rows_exported", None) == 2
    _load(sink, config, first.artifact, first.schema)
    assert_row_count(bq, dataset=dataset, table=table, expected=2)
    insert_wide_watermark_row(mssql, table=table)
    third = strategy.extract(config, strategy.get_state(config))
    assert getattr(third.artifact, "rows_exported", None) == 1
    _load(sink, config, third.artifact, third.schema)
    assert_row_count(bq, dataset=dataset, table=table, expected=3)
    assert_typed_spot_checks(bq, dataset=dataset, table=table, columns=columns)


@pytest.mark.skipif(not mssql_bigquery_enabled(), reason="MSSQL Docker and/or BigQuery SA env not configured")
def test_mssql_to_bigquery_incremental_merge_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_merge"
    mssql, bq, dataset, columns = _ready(table)
    config = cfg.incremental_merge(dataset=dataset, tmp_path=tmp_path, table=table)
    strategy = MSSQLIncrementalExtractStrategy(mssql, logger=NoopLogger(), sink_connector=bq)
    sink = BigQuerySink(bq, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    assert getattr(first.artifact, "rows_exported", None) == 2
    _load(sink, config, first.artifact, first.schema)
    idle = strategy.extract(config, strategy.get_state(config))
    assert getattr(idle.artifact, "rows_exported", None) == 0
    mssql.execute_query(
        f"UPDATE [{SOURCE_SCHEMA}].[{table}] "
        f"SET c_name = N'merged', updated_at = '2026-07-21 11:30:00.000' WHERE id = 1"
    )
    updated = strategy.extract(config, strategy.get_state(config))
    assert getattr(updated.artifact, "rows_exported", None) == 1
    _load(sink, config, updated.artifact, updated.schema)
    assert_row_count(bq, dataset=dataset, table=table, expected=2)
    row = bq.get_records(f"SELECT c_name FROM {fq(bq, dataset, table)} WHERE id = 1")[0]
    assert row["c_name"] == "merged"
    insert_wide_watermark_row(mssql, table=table)
    third = strategy.extract(config, strategy.get_state(config))
    assert getattr(third.artifact, "rows_exported", None) == 1
    _load(sink, config, third.artifact, third.schema)
    assert_row_count(bq, dataset=dataset, table=table, expected=3)
    assert_typed_spot_checks(bq, dataset=dataset, table=table, columns=columns, expected_name="merged")


@pytest.mark.skipif(not mssql_bigquery_enabled(), reason="MSSQL Docker and/or BigQuery SA env not configured")
def test_mssql_to_bigquery_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_replace"
    mssql, bq, dataset, columns = _ready(table)
    fr = cfg.full_refresh(dataset=dataset, tmp_path=tmp_path, table=table)
    extract = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=bq).extract(fr, None)
    sink = BigQuerySink(bq, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    bq.execute_query(
        f"INSERT INTO {fq(bq, dataset, table)} (id, business_date, updated_at, c_name) "
        f"VALUES (99, DATE '2020-01-01', DATETIME '2020-01-01 00:00:00', 'outside')"
    )
    mssql.execute_query(f"UPDATE [{SOURCE_SCHEMA}].[{table}] SET c_name = N'replaced' WHERE id = 1")
    config = cfg.replace(dataset=dataset, tmp_path=tmp_path, table=table)
    replaced = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=bq).extract(config, None)
    _load(sink, config, replaced.artifact, replaced.schema)
    rows = bq.get_records(f"SELECT id, c_name FROM {fq(bq, dataset, table)} ORDER BY id")
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert by_id[1] == "replaced"
    assert by_id[99] == "outside"
    assert_unique_ids(bq, dataset=dataset, table=table)
    assert_typed_spot_checks(bq, dataset=dataset, table=table, columns=columns, expected_name="replaced")


@pytest.mark.skipif(not mssql_bigquery_enabled(), reason="MSSQL Docker and/or BigQuery SA env not configured")
def test_mssql_to_bigquery_partition_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_part"
    mssql, bq, dataset, columns = _ready(table)
    fr = cfg.full_refresh(dataset=dataset, tmp_path=tmp_path, table=table)
    extract = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=bq).extract(fr, None)
    sink = BigQuerySink(bq, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    bq.execute_query(
        f"INSERT INTO {fq(bq, dataset, table)} (id, business_date, updated_at, c_name) "
        f"VALUES (99, DATE '2020-01-01', DATETIME '2020-01-01 00:00:00', 'other-partition')"
    )
    mssql.execute_query(f"UPDATE [{SOURCE_SCHEMA}].[{table}] SET c_name = N'part-replaced' WHERE id = 1")
    config = cfg.partition_replace(dataset=dataset, tmp_path=tmp_path, table=table)
    batch = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=bq).extract(config, None)
    _load(sink, config, batch.artifact, batch.schema)
    rows = bq.get_records(f"SELECT id, c_name FROM {fq(bq, dataset, table)} ORDER BY id")
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert by_id[1] == "part-replaced"
    assert by_id[99] == "other-partition"
    assert_unique_ids(bq, dataset=dataset, table=table)
    assert_typed_spot_checks(bq, dataset=dataset, table=table, columns=columns, expected_name="part-replaced")


@pytest.mark.skipif(not mssql_bigquery_enabled(), reason="MSSQL Docker and/or BigQuery SA env not configured")
def test_mssql_to_bigquery_snapshot_diff_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_diff"
    mssql, bq, dataset, columns = _ready(table)
    config = cfg.snapshot_diff(dataset=dataset, tmp_path=tmp_path, table=table)
    strategy = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=bq)
    sink = BigQuerySink(bq, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, None)
    _load(sink, config, first.artifact, first.schema)
    assert_row_count(bq, dataset=dataset, table=table, expected=2)
    mssql.execute_query(f"DELETE FROM [{SOURCE_SCHEMA}].[{table}] WHERE id = 2")
    mssql.execute_query(f"UPDATE [{SOURCE_SCHEMA}].[{table}] SET c_name = N'diff-updated' WHERE id = 1")
    insert_wide_watermark_row(mssql, table=table)
    second = strategy.extract(config, None)
    _load(sink, config, second.artifact, second.schema)
    rows = bq.get_records(f"SELECT id, c_name FROM {fq(bq, dataset, table)} ORDER BY id")
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert 2 not in by_id
    assert by_id[1] == "diff-updated"
    assert by_id[3] == "row-three"
    assert_typed_spot_checks(bq, dataset=dataset, table=table, columns=columns, expected_name="diff-updated")


@pytest.mark.skipif(not mssql_bigquery_enabled(), reason="MSSQL Docker and/or BigQuery SA env not configured")
def test_mssql_to_bigquery_scd2_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_scd2"
    mssql, bq, dataset, columns = _ready(table)
    config = cfg.scd2(dataset=dataset, tmp_path=tmp_path, table=table)
    strategy = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=bq)
    sink = BigQuerySink(bq, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, None)
    _load(sink, config, first.artifact, first.schema)
    fields = {f.name for f in bq.connection.get_table(f"{bq.project_id}.{dataset}.{table}").schema}
    assert "__dpone__is_current" in fields
    assert "__dpone__row_hash" in fields
    mssql.execute_query(
        f"UPDATE [{SOURCE_SCHEMA}].[{table}] "
        f"SET c_name = N'scd2-v2', updated_at = '2026-07-21 13:00:00.000' WHERE id = 1"
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
        bq, dataset=dataset, table=table, columns=columns, expected_name="scd2-v2", check_seed_scalars=False
    )


@pytest.mark.skipif(not mssql_bigquery_enabled(), reason="MSSQL Docker and/or BigQuery SA env not configured")
def test_mssql_to_bigquery_backfill_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_backfill"
    mssql, bq, dataset, columns = _ready(table)
    fr = cfg.full_refresh(dataset=dataset, tmp_path=tmp_path, table=table)
    extract = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=bq).extract(fr, None)
    sink = BigQuerySink(bq, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    mssql.execute_query(f"UPDATE [{SOURCE_SCHEMA}].[{table}] SET c_name = N'backfilled' WHERE id = 1")
    config = cfg.backfill_replace(dataset=dataset, tmp_path=tmp_path, table=table)
    batch = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=bq).extract(config, None)
    _load(sink, config, batch.artifact, batch.schema)
    row = bq.get_records(f"SELECT c_name FROM {fq(bq, dataset, table)} WHERE id = 1")[0]
    assert row["c_name"] == "backfilled"
    assert_typed_spot_checks(bq, dataset=dataset, table=table, columns=columns, expected_name="backfilled")

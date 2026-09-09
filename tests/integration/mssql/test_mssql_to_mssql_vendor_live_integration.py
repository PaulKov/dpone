"""Docker MSSQL → MSSQL vendor-live: wide types + all MSSQL load strategies.

Requires Docker integration MSSQL (same instance, cross-schema dpone_src → dpone_it).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tests.integration.mssql import mssql_mssql_strategy_configs as cfg
from tests.integration.mssql.mssql_live_support import (
    NoopLogger,
    drop_mssql_table,
    ensure_mssql_database_and_schemas,
    mssql_connector,
    mssql_mssql_enabled,
    wait_until_ready,
)
from tests.integration.mssql.mssql_mssql_assertions import (
    assert_row_count,
    assert_typed_spot_checks,
    assert_unique_ids,
    fq,
)
from tests.integration.mssql.mssql_mssql_wide_fixtures import (
    SOURCE_SCHEMA,
    WIDE_TABLE,
    create_wide_mssql_table,
    insert_wide_watermark_row,
)

from dpone.runtime.lineage.strategy_metadata import StrategyMetadataEnricher
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sources.strategies.mssql.mssql_full import MSSQLFullExtractStrategy
from dpone.runtime.sources.strategies.mssql.mssql_incremental import MSSQLIncrementalExtractStrategy

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_mssql,
]


def _ready(table: str):
    ensure_mssql_database_and_schemas(source_schema=SOURCE_SCHEMA)
    source = mssql_connector()
    sink = mssql_connector()
    wait_until_ready("mssql", lambda: source.get_records("SELECT 1"))
    drop_mssql_table(sink, table)
    source.execute_query(f"DROP TABLE IF EXISTS [{SOURCE_SCHEMA}].[{table}]")
    columns = create_wide_mssql_table(source, table=table, schema=SOURCE_SCHEMA)
    return source, sink, columns


def _load(sink, config, artifact, schema):
    """Mirror ETLProcessor: enrich strategy metadata before sink.load."""

    payload = StrategyMetadataEnricher().enrich_payload(
        LoadPayload(artifact=artifact, schema=schema),
        load_config=config,
    )
    return sink.load(config, payload)


def _assert_rows_exported(artifact, expected: int) -> None:
    """File exports expose rows_exported; streaming queryout may leave it unset."""

    exported = getattr(artifact, "rows_exported", None)
    if exported is None:
        return
    assert exported == expected


@pytest.mark.skipif(not mssql_mssql_enabled(), reason="MSSQL Docker IT not configured")
def test_mssql_to_mssql_full_refresh_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_fr"
    source, sink_conn, columns = _ready(table)
    config = cfg.full_refresh(tmp_path=tmp_path, table=table)
    extract = MSSQLFullExtractStrategy(source, logger=NoopLogger(), sink_connector=sink_conn).extract(config, None)
    assert getattr(extract.artifact, "format", None) == "mssql-delimited"
    assert getattr(extract.artifact, "rows_exported", None) == 2
    result = _load(MSSQLSink(sink_conn, logger=NoopLogger()), config, extract.artifact, extract.schema)
    assert result.total_rows == 2
    assert_row_count(sink_conn, table=table, expected=2)
    assert_typed_spot_checks(sink_conn, table=table, columns=columns)


@pytest.mark.skipif(not mssql_mssql_enabled(), reason="MSSQL Docker IT not configured")
def test_mssql_to_mssql_incremental_append_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_append"
    source, sink_conn, columns = _ready(table)
    config = cfg.incremental_append(tmp_path=tmp_path, table=table)
    strategy = MSSQLIncrementalExtractStrategy(source, logger=NoopLogger(), sink_connector=sink_conn)
    sink = MSSQLSink(sink_conn, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    _assert_rows_exported(first.artifact, 2)
    _load(sink, config, first.artifact, first.schema)
    assert_row_count(sink_conn, table=table, expected=2)
    insert_wide_watermark_row(source, table=table)
    third = strategy.extract(config, strategy.get_state(config))
    assert third.force_full_refresh is False
    _assert_rows_exported(third.artifact, 1)
    _load(sink, config, third.artifact, third.schema)
    assert_row_count(sink_conn, table=table, expected=3)
    assert_typed_spot_checks(sink_conn, table=table, columns=columns)


@pytest.mark.skipif(not mssql_mssql_enabled(), reason="MSSQL Docker IT not configured")
def test_mssql_to_mssql_incremental_merge_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_merge"
    source, sink_conn, columns = _ready(table)
    config = cfg.incremental_merge(tmp_path=tmp_path, table=table)
    strategy = MSSQLIncrementalExtractStrategy(source, logger=NoopLogger(), sink_connector=sink_conn)
    sink = MSSQLSink(sink_conn, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    _assert_rows_exported(first.artifact, 2)
    _load(sink, config, first.artifact, first.schema)
    idle = strategy.extract(config, strategy.get_state(config))
    assert idle.force_full_refresh is False
    _assert_rows_exported(idle.artifact, 0)
    assert_row_count(sink_conn, table=table, expected=2)
    source.execute_query(
        f"UPDATE [{SOURCE_SCHEMA}].[{table}] "
        f"SET c_name = N'merged', updated_at = '2026-07-21 11:30:00.000' WHERE id = 1"
    )
    updated = strategy.extract(config, strategy.get_state(config))
    assert updated.force_full_refresh is False
    _assert_rows_exported(updated.artifact, 1)
    _load(sink, config, updated.artifact, updated.schema)
    assert_row_count(sink_conn, table=table, expected=2)
    row = sink_conn.get_records(f"SELECT c_name FROM {fq(table)} WHERE id = 1", as_dict=True)[0]
    assert row["c_name"] == "merged"
    insert_wide_watermark_row(source, table=table)
    third = strategy.extract(config, strategy.get_state(config))
    assert third.force_full_refresh is False
    _assert_rows_exported(third.artifact, 1)
    _load(sink, config, third.artifact, third.schema)
    assert_row_count(sink_conn, table=table, expected=3)
    assert_typed_spot_checks(sink_conn, table=table, columns=columns, expected_name="merged")


@pytest.mark.skipif(not mssql_mssql_enabled(), reason="MSSQL Docker IT not configured")
def test_mssql_to_mssql_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_replace"
    source, sink_conn, columns = _ready(table)
    fr = cfg.full_refresh(tmp_path=tmp_path, table=table)
    extract = MSSQLFullExtractStrategy(source, logger=NoopLogger(), sink_connector=sink_conn).extract(fr, None)
    sink = MSSQLSink(sink_conn, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    sink_conn.execute_query(
        f"INSERT INTO {fq(table)} (id, business_date, updated_at, c_name) "
        f"VALUES (99, CAST('2020-01-01' AS date), CAST('2020-01-01T00:00:00' AS datetime2), N'outside')"
    )
    source.execute_query(f"UPDATE [{SOURCE_SCHEMA}].[{table}] SET c_name = N'replaced' WHERE id = 1")
    config = cfg.replace(tmp_path=tmp_path, table=table)
    replaced = MSSQLFullExtractStrategy(source, logger=NoopLogger(), sink_connector=sink_conn).extract(config, None)
    _load(sink, config, replaced.artifact, replaced.schema)
    rows = sink_conn.get_records(f"SELECT id, c_name FROM {fq(table)} ORDER BY id", as_dict=True)
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert by_id[1] == "replaced"
    assert by_id[99] == "outside"
    assert_unique_ids(sink_conn, table=table)
    assert_typed_spot_checks(sink_conn, table=table, columns=columns, expected_name="replaced")


@pytest.mark.skipif(not mssql_mssql_enabled(), reason="MSSQL Docker IT not configured")
def test_mssql_to_mssql_partition_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_part"
    source, sink_conn, columns = _ready(table)
    fr = cfg.full_refresh(tmp_path=tmp_path, table=table)
    extract = MSSQLFullExtractStrategy(source, logger=NoopLogger(), sink_connector=sink_conn).extract(fr, None)
    sink = MSSQLSink(sink_conn, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    sink_conn.execute_query(
        f"INSERT INTO {fq(table)} (id, business_date, updated_at, c_name) "
        f"VALUES (99, CAST('2020-01-01' AS date), CAST('2020-01-01T00:00:00' AS datetime2), N'other-partition')"
    )
    source.execute_query(f"UPDATE [{SOURCE_SCHEMA}].[{table}] SET c_name = N'part-replaced' WHERE id = 1")
    config = cfg.partition_replace(tmp_path=tmp_path, table=table)
    batch = MSSQLFullExtractStrategy(source, logger=NoopLogger(), sink_connector=sink_conn).extract(config, None)
    _load(sink, config, batch.artifact, batch.schema)
    rows = sink_conn.get_records(f"SELECT id, c_name FROM {fq(table)} ORDER BY id", as_dict=True)
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert by_id[1] == "part-replaced"
    assert by_id[99] == "other-partition"
    assert_unique_ids(sink_conn, table=table)
    assert_typed_spot_checks(sink_conn, table=table, columns=columns, expected_name="part-replaced")


@pytest.mark.skipif(not mssql_mssql_enabled(), reason="MSSQL Docker IT not configured")
def test_mssql_to_mssql_snapshot_diff_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_diff"
    source, sink_conn, columns = _ready(table)
    config = cfg.snapshot_diff(tmp_path=tmp_path, table=table)
    strategy = MSSQLFullExtractStrategy(source, logger=NoopLogger(), sink_connector=sink_conn)
    sink = MSSQLSink(sink_conn, logger=NoopLogger())
    first = strategy.extract(config, None)
    _load(sink, config, first.artifact, first.schema)
    assert_row_count(sink_conn, table=table, expected=2)
    source.execute_query(f"DELETE FROM [{SOURCE_SCHEMA}].[{table}] WHERE id = 2")
    source.execute_query(f"UPDATE [{SOURCE_SCHEMA}].[{table}] SET c_name = N'diff-updated' WHERE id = 1")
    insert_wide_watermark_row(source, table=table)
    second = strategy.extract(config, None)
    _load(sink, config, second.artifact, second.schema)
    rows = sink_conn.get_records(f"SELECT id, c_name FROM {fq(table)} ORDER BY id", as_dict=True)
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert 2 not in by_id
    assert by_id[1] == "diff-updated"
    assert by_id[3] == "row-three"
    assert_typed_spot_checks(sink_conn, table=table, columns=columns, expected_name="diff-updated")


@pytest.mark.skipif(not mssql_mssql_enabled(), reason="MSSQL Docker IT not configured")
def test_mssql_to_mssql_scd2_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_scd2"
    source, sink_conn, columns = _ready(table)
    config = cfg.scd2(tmp_path=tmp_path, table=table)
    strategy = MSSQLFullExtractStrategy(source, logger=NoopLogger(), sink_connector=sink_conn)
    sink = MSSQLSink(sink_conn, logger=NoopLogger())
    first = strategy.extract(config, None)
    _load(sink, config, first.artifact, first.schema)
    cols = sink_conn.get_records(
        f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dpone_it' AND TABLE_NAME = '{table}'
        """,
        as_dict=True,
    )
    names = {row["COLUMN_NAME"] for row in cols}
    assert "__dpone__is_current" in names
    assert "__dpone__row_hash" in names
    assert "__dpone__valid_from_at" in names
    assert "__dpone__valid_to_at" in names
    source.execute_query(
        f"UPDATE [{SOURCE_SCHEMA}].[{table}] "
        f"SET c_name = N'scd2-v2', updated_at = '2026-07-21 13:00:00.000' WHERE id = 1"
    )
    second = strategy.extract(config, None)
    _load(sink, config, second.artifact, second.schema)
    currents = sink_conn.get_records(
        f"SELECT id, c_name, __dpone__is_current AS is_current FROM {fq(table)} "
        f"WHERE id = 1 ORDER BY __dpone__is_current DESC",
        as_dict=True,
    )
    assert any(bool(r["is_current"]) and r["c_name"] == "scd2-v2" for r in currents)
    assert any(not bool(r["is_current"]) for r in currents)
    assert_typed_spot_checks(sink_conn, table=table, columns=columns, expected_name="scd2-v2", check_seed_scalars=False)


@pytest.mark.skipif(not mssql_mssql_enabled(), reason="MSSQL Docker IT not configured")
def test_mssql_to_mssql_backfill_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_backfill"
    source, sink_conn, columns = _ready(table)
    fr = cfg.full_refresh(tmp_path=tmp_path, table=table)
    extract = MSSQLFullExtractStrategy(source, logger=NoopLogger(), sink_connector=sink_conn).extract(fr, None)
    sink = MSSQLSink(sink_conn, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    source.execute_query(f"UPDATE [{SOURCE_SCHEMA}].[{table}] SET c_name = N'backfilled' WHERE id = 1")
    config = cfg.backfill_replace(tmp_path=tmp_path, table=table)
    batch = MSSQLFullExtractStrategy(source, logger=NoopLogger(), sink_connector=sink_conn).extract(config, None)
    _load(sink, config, batch.artifact, batch.schema)
    row = sink_conn.get_records(f"SELECT c_name FROM {fq(table)} WHERE id = 1", as_dict=True)[0]
    assert row["c_name"] == "backfilled"
    assert_typed_spot_checks(sink_conn, table=table, columns=columns, expected_name="backfilled")

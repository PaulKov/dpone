"""Docker MSSQL → Postgres vendor-live: wide types + all Postgres load strategies.

Requires Docker integration MSSQL + Postgres.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tests.integration.mssql import mssql_postgres_strategy_configs as cfg
from tests.integration.mssql.mssql_live_support import (
    NoopLogger,
    drop_postgres_table,
    ensure_mssql_database_and_schemas,
    ensure_postgres_schemas,
    mssql_connector,
    mssql_postgres_enabled,
    postgres_connector,
    wait_until_ready,
)
from tests.integration.mssql.mssql_postgres_assertions import (
    assert_row_count,
    assert_typed_spot_checks,
    assert_unique_ids,
    fq,
)
from tests.integration.mssql.mssql_postgres_wide_fixtures import (
    SOURCE_SCHEMA,
    WIDE_TABLE,
    create_wide_mssql_table,
    insert_wide_watermark_row,
)

from dpone.runtime.lineage.strategy_metadata import StrategyMetadataEnricher
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.postgres import PostgresSink
from dpone.runtime.sources.strategies.mssql.mssql_full import MSSQLFullExtractStrategy
from dpone.runtime.sources.strategies.mssql.mssql_incremental import MSSQLIncrementalExtractStrategy

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_mssql,
    pytest.mark.integration_postgres,
]


def _ready(table: str):
    ensure_mssql_database_and_schemas(source_schema=SOURCE_SCHEMA)
    mssql = mssql_connector()
    postgres = postgres_connector()
    wait_until_ready("mssql", lambda: mssql.get_records("SELECT 1"))
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres)
    drop_postgres_table(postgres, table)
    mssql.execute_query(f"DROP TABLE IF EXISTS [{SOURCE_SCHEMA}].[{table}]")
    columns = create_wide_mssql_table(mssql, table=table, schema=SOURCE_SCHEMA)
    return mssql, postgres, columns


def _load(sink, config, artifact, schema):
    """Mirror ETLProcessor: enrich strategy metadata before sink.load."""

    payload = StrategyMetadataEnricher().enrich_payload(
        LoadPayload(artifact=artifact, schema=schema),
        load_config=config,
    )
    return sink.load(config, payload)


@pytest.mark.skipif(not mssql_postgres_enabled(), reason="MSSQL/Postgres Docker IT not configured")
def test_mssql_to_postgres_full_refresh_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_fr"
    mssql, postgres, columns = _ready(table)
    config = cfg.full_refresh(tmp_path=tmp_path, table=table)
    extract = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=postgres).extract(config, None)
    assert getattr(extract.artifact, "format", None) == "csv"
    assert getattr(extract.artifact, "rows_exported", None) == 2
    result = _load(
        PostgresSink(postgres, state_storage=None, logger=NoopLogger()),
        config,
        extract.artifact,
        extract.schema,
    )
    assert result.total_rows == 2
    assert_row_count(postgres, table=table, expected=2)
    assert_typed_spot_checks(postgres, table=table, columns=columns)


@pytest.mark.skipif(not mssql_postgres_enabled(), reason="MSSQL/Postgres Docker IT not configured")
def test_mssql_to_postgres_incremental_append_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_append"
    mssql, postgres, columns = _ready(table)
    config = cfg.incremental_append(tmp_path=tmp_path, table=table)
    strategy = MSSQLIncrementalExtractStrategy(mssql, logger=NoopLogger(), sink_connector=postgres)
    sink = PostgresSink(postgres, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    assert getattr(first.artifact, "rows_exported", None) == 2
    _load(sink, config, first.artifact, first.schema)
    assert_row_count(postgres, table=table, expected=2)
    insert_wide_watermark_row(mssql, table=table)
    third = strategy.extract(config, strategy.get_state(config))
    assert third.force_full_refresh is False
    assert getattr(third.artifact, "rows_exported", None) == 1
    _load(sink, config, third.artifact, third.schema)
    assert_row_count(postgres, table=table, expected=3)
    assert_typed_spot_checks(postgres, table=table, columns=columns)


@pytest.mark.skipif(not mssql_postgres_enabled(), reason="MSSQL/Postgres Docker IT not configured")
def test_mssql_to_postgres_incremental_merge_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_merge"
    mssql, postgres, columns = _ready(table)
    config = cfg.incremental_merge(tmp_path=tmp_path, table=table)
    strategy = MSSQLIncrementalExtractStrategy(mssql, logger=NoopLogger(), sink_connector=postgres)
    sink = PostgresSink(postgres, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    assert getattr(first.artifact, "rows_exported", None) == 2
    _load(sink, config, first.artifact, first.schema)
    idle = strategy.extract(config, strategy.get_state(config))
    assert idle.force_full_refresh is False
    assert getattr(idle.artifact, "rows_exported", None) == 0
    assert_row_count(postgres, table=table, expected=2)
    mssql.execute_query(
        f"UPDATE [{SOURCE_SCHEMA}].[{table}] "
        f"SET c_name = N'merged', updated_at = '2026-07-21 11:30:00.000' WHERE id = 1"
    )
    updated = strategy.extract(config, strategy.get_state(config))
    assert updated.force_full_refresh is False
    assert getattr(updated.artifact, "rows_exported", None) == 1
    _load(sink, config, updated.artifact, updated.schema)
    assert_row_count(postgres, table=table, expected=2)
    row = postgres.get_records(f"SELECT c_name FROM {fq(table)} WHERE id = 1")[0]
    assert (row["c_name"] if isinstance(row, dict) else row[0]) == "merged"
    insert_wide_watermark_row(mssql, table=table)
    third = strategy.extract(config, strategy.get_state(config))
    assert third.force_full_refresh is False
    assert getattr(third.artifact, "rows_exported", None) == 1
    _load(sink, config, third.artifact, third.schema)
    assert_row_count(postgres, table=table, expected=3)
    assert_typed_spot_checks(postgres, table=table, columns=columns, expected_name="merged")


@pytest.mark.skipif(not mssql_postgres_enabled(), reason="MSSQL/Postgres Docker IT not configured")
def test_mssql_to_postgres_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_replace"
    mssql, postgres, columns = _ready(table)
    fr = cfg.full_refresh(tmp_path=tmp_path, table=table)
    extract = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=postgres).extract(fr, None)
    sink = PostgresSink(postgres, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    postgres.execute_query(
        f"INSERT INTO {fq(table)} (id, business_date, updated_at, c_name) "
        f"VALUES (99, DATE '2020-01-01', TIMESTAMP '2020-01-01 00:00:00', 'outside')"
    )
    mssql.execute_query(f"UPDATE [{SOURCE_SCHEMA}].[{table}] SET c_name = N'replaced' WHERE id = 1")
    config = cfg.replace(tmp_path=tmp_path, table=table)
    replaced = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=postgres).extract(config, None)
    _load(sink, config, replaced.artifact, replaced.schema)
    rows = postgres.get_records(f"SELECT id, c_name FROM {fq(table)} ORDER BY id")
    by_id = {
        int(r["id"] if isinstance(r, dict) else r[0]): (r["c_name"] if isinstance(r, dict) else r[1]) for r in rows
    }
    assert by_id[1] == "replaced"
    assert by_id[99] == "outside"
    assert_unique_ids(postgres, table=table)
    assert_typed_spot_checks(postgres, table=table, columns=columns, expected_name="replaced")


@pytest.mark.skipif(not mssql_postgres_enabled(), reason="MSSQL/Postgres Docker IT not configured")
def test_mssql_to_postgres_partition_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_part"
    mssql, postgres, columns = _ready(table)
    fr = cfg.full_refresh(tmp_path=tmp_path, table=table)
    extract = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=postgres).extract(fr, None)
    sink = PostgresSink(postgres, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    postgres.execute_query(
        f"INSERT INTO {fq(table)} (id, business_date, updated_at, c_name) "
        f"VALUES (99, DATE '2020-01-01', TIMESTAMP '2020-01-01 00:00:00', 'other-partition')"
    )
    mssql.execute_query(f"UPDATE [{SOURCE_SCHEMA}].[{table}] SET c_name = N'part-replaced' WHERE id = 1")
    config = cfg.partition_replace(tmp_path=tmp_path, table=table)
    batch = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=postgres).extract(config, None)
    _load(sink, config, batch.artifact, batch.schema)
    rows = postgres.get_records(f"SELECT id, c_name FROM {fq(table)} ORDER BY id")
    by_id = {
        int(r["id"] if isinstance(r, dict) else r[0]): (r["c_name"] if isinstance(r, dict) else r[1]) for r in rows
    }
    assert by_id[1] == "part-replaced"
    assert by_id[99] == "other-partition"
    assert_unique_ids(postgres, table=table)
    assert_typed_spot_checks(postgres, table=table, columns=columns, expected_name="part-replaced")


@pytest.mark.skipif(not mssql_postgres_enabled(), reason="MSSQL/Postgres Docker IT not configured")
def test_mssql_to_postgres_snapshot_diff_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_diff"
    mssql, postgres, columns = _ready(table)
    config = cfg.snapshot_diff(tmp_path=tmp_path, table=table)
    strategy = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=postgres)
    sink = PostgresSink(postgres, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, None)
    _load(sink, config, first.artifact, first.schema)
    assert_row_count(postgres, table=table, expected=2)
    mssql.execute_query(f"DELETE FROM [{SOURCE_SCHEMA}].[{table}] WHERE id = 2")
    mssql.execute_query(f"UPDATE [{SOURCE_SCHEMA}].[{table}] SET c_name = N'diff-updated' WHERE id = 1")
    insert_wide_watermark_row(mssql, table=table)
    second = strategy.extract(config, None)
    _load(sink, config, second.artifact, second.schema)
    rows = postgres.get_records(f"SELECT id, c_name FROM {fq(table)} ORDER BY id")
    by_id = {
        int(r["id"] if isinstance(r, dict) else r[0]): (r["c_name"] if isinstance(r, dict) else r[1]) for r in rows
    }
    assert 2 not in by_id
    assert by_id[1] == "diff-updated"
    assert by_id[3] == "row-three"
    assert_typed_spot_checks(postgres, table=table, columns=columns, expected_name="diff-updated")


@pytest.mark.skipif(not mssql_postgres_enabled(), reason="MSSQL/Postgres Docker IT not configured")
def test_mssql_to_postgres_scd2_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_scd2"
    mssql, postgres, columns = _ready(table)
    config = cfg.scd2(tmp_path=tmp_path, table=table)
    strategy = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=postgres)
    sink = PostgresSink(postgres, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, None)
    _load(sink, config, first.artifact, first.schema)
    cols = postgres.get_records(
        f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'dpone_it' AND table_name = '{table}'
        """
    )
    names = {row["column_name"] if isinstance(row, dict) else row[0] for row in cols}
    assert "__dpone__is_current" in names
    assert "__dpone__row_hash" in names
    mssql.execute_query(
        f"UPDATE [{SOURCE_SCHEMA}].[{table}] "
        f"SET c_name = N'scd2-v2', updated_at = '2026-07-21 13:00:00.000' WHERE id = 1"
    )
    second = strategy.extract(config, None)
    _load(sink, config, second.artifact, second.schema)
    currents = postgres.get_records(
        f"SELECT id, c_name, __dpone__is_current AS is_current FROM {fq(table)} "
        f"WHERE id = 1 ORDER BY __dpone__is_current DESC"
    )
    assert any(
        (
            bool(r["is_current"] if isinstance(r, dict) else r[2])
            and (r["c_name"] if isinstance(r, dict) else r[1]) == "scd2-v2"
        )
        for r in currents
    )
    assert any(not bool(r["is_current"] if isinstance(r, dict) else r[2]) for r in currents)
    assert_typed_spot_checks(postgres, table=table, columns=columns, expected_name="scd2-v2", check_seed_scalars=False)


@pytest.mark.skipif(not mssql_postgres_enabled(), reason="MSSQL/Postgres Docker IT not configured")
def test_mssql_to_postgres_backfill_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_backfill"
    mssql, postgres, columns = _ready(table)
    fr = cfg.full_refresh(tmp_path=tmp_path, table=table)
    extract = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=postgres).extract(fr, None)
    sink = PostgresSink(postgres, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    mssql.execute_query(f"UPDATE [{SOURCE_SCHEMA}].[{table}] SET c_name = N'backfilled' WHERE id = 1")
    config = cfg.backfill_replace(tmp_path=tmp_path, table=table)
    batch = MSSQLFullExtractStrategy(mssql, logger=NoopLogger(), sink_connector=postgres).extract(config, None)
    _load(sink, config, batch.artifact, batch.schema)
    row = postgres.get_records(f"SELECT c_name FROM {fq(table)} WHERE id = 1")[0]
    assert (row["c_name"] if isinstance(row, dict) else row[0]) == "backfilled"
    assert_typed_spot_checks(postgres, table=table, columns=columns, expected_name="backfilled")

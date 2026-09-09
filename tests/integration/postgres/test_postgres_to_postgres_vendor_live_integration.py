"""Docker Postgres → Postgres vendor-live: wide types + all PG load strategies.

Requires Docker integration Postgres (same instance, cross-schema).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dpone.runtime.lineage.strategy_metadata import StrategyMetadataEnricher
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.postgres import PostgresSink
from dpone.runtime.sources.strategies.postgres.postgres_full_extract import PostgresFullExtractStrategy
from dpone.runtime.sources.strategies.postgres.postgres_incremental_extract import (
    PostgresIncrementalExtractStrategy,
)
from tests.integration.postgres import postgres_postgres_strategy_configs as cfg
from tests.integration.postgres.postgres_live_support import (
    NoopLogger,
    drop_postgres_table,
    ensure_postgres_schemas,
    postgres_connector,
    postgres_postgres_enabled,
    wait_until_ready,
)
from tests.integration.postgres.postgres_postgres_assertions import (
    assert_row_count,
    assert_typed_spot_checks,
    assert_unique_ids,
    fq,
)
from tests.integration.postgres.postgres_postgres_wide_fixtures import (
    SOURCE_SCHEMA,
    WIDE_TABLE,
    create_wide_postgres_table,
    insert_wide_watermark_row,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
]


def _ready(table: str):
    source = postgres_connector()
    sink = postgres_connector()
    wait_until_ready("postgres", lambda: source.get_records("SELECT 1"))
    ensure_postgres_schemas(sink, source_schema=SOURCE_SCHEMA)
    drop_postgres_table(sink, table)
    source.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{table}" CASCADE')
    columns = create_wide_postgres_table(source, table=table, schema=SOURCE_SCHEMA)
    return source, sink, columns


def _load(sink, config, artifact, schema):
    payload = StrategyMetadataEnricher().enrich_payload(
        LoadPayload(artifact=artifact, schema=schema),
        load_config=config,
    )
    return sink.load(config, payload)


@pytest.mark.skipif(not postgres_postgres_enabled(), reason="Postgres Docker IT not configured")
def test_postgres_to_postgres_full_refresh_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_fr"
    source, sink_conn, columns = _ready(table)
    config = cfg.full_refresh(tmp_path=tmp_path, table=table)
    extract = PostgresFullExtractStrategy(source, logger=NoopLogger()).extract(config, None)
    assert getattr(extract.artifact, "format", None) == "csv"
    assert getattr(extract.artifact, "rows_exported", None) == 2
    result = _load(
        PostgresSink(sink_conn, state_storage=None, logger=NoopLogger()),
        config,
        extract.artifact,
        extract.schema,
    )
    assert result.total_rows == 2
    assert_row_count(sink_conn, table=table, expected=2)
    assert_typed_spot_checks(sink_conn, table=table, columns=columns)


@pytest.mark.skipif(not postgres_postgres_enabled(), reason="Postgres Docker IT not configured")
def test_postgres_to_postgres_incremental_append_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_append"
    source, sink_conn, columns = _ready(table)
    config = cfg.incremental_append(tmp_path=tmp_path, table=table)
    strategy = PostgresIncrementalExtractStrategy(source, sink_connector=sink_conn, logger=NoopLogger())
    sink = PostgresSink(sink_conn, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    assert getattr(first.artifact, "rows_exported", None) == 2
    _load(sink, config, first.artifact, first.schema)
    assert_row_count(sink_conn, table=table, expected=2)
    insert_wide_watermark_row(source, table=table)
    third = strategy.extract(config, strategy.get_state(config))
    assert getattr(third.artifact, "rows_exported", None) == 1
    _load(sink, config, third.artifact, third.schema)
    assert_row_count(sink_conn, table=table, expected=3)
    assert_typed_spot_checks(sink_conn, table=table, columns=columns)


@pytest.mark.skipif(not postgres_postgres_enabled(), reason="Postgres Docker IT not configured")
def test_postgres_to_postgres_incremental_merge_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_merge"
    source, sink_conn, columns = _ready(table)
    config = cfg.incremental_merge(tmp_path=tmp_path, table=table)
    strategy = PostgresIncrementalExtractStrategy(source, sink_connector=sink_conn, logger=NoopLogger())
    sink = PostgresSink(sink_conn, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, strategy.get_state(config))
    assert getattr(first.artifact, "rows_exported", None) == 2
    _load(sink, config, first.artifact, first.schema)
    idle = strategy.extract(config, strategy.get_state(config))
    assert getattr(idle.artifact, "rows_exported", None) == 0
    source.execute_query(
        f'UPDATE "{SOURCE_SCHEMA}"."{table}" '
        f"SET c_name = 'merged', updated_at = TIMESTAMP '2026-07-21 11:30:00' WHERE id = 1"
    )
    updated = strategy.extract(config, strategy.get_state(config))
    assert getattr(updated.artifact, "rows_exported", None) == 1
    _load(sink, config, updated.artifact, updated.schema)
    assert_row_count(sink_conn, table=table, expected=2)
    row = sink_conn.get_records(f"SELECT c_name FROM {fq(table)} WHERE id = 1", as_dict=True)[0]
    assert row["c_name"] == "merged"
    insert_wide_watermark_row(source, table=table)
    third = strategy.extract(config, strategy.get_state(config))
    assert getattr(third.artifact, "rows_exported", None) == 1
    _load(sink, config, third.artifact, third.schema)
    assert_row_count(sink_conn, table=table, expected=3)
    assert_typed_spot_checks(sink_conn, table=table, columns=columns, expected_name="merged")


@pytest.mark.skipif(not postgres_postgres_enabled(), reason="Postgres Docker IT not configured")
def test_postgres_to_postgres_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_replace"
    source, sink_conn, columns = _ready(table)
    fr = cfg.full_refresh(tmp_path=tmp_path, table=table)
    extract = PostgresFullExtractStrategy(source, logger=NoopLogger()).extract(fr, None)
    sink = PostgresSink(sink_conn, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    sink_conn.execute_query(
        f"INSERT INTO {fq(table)} (id, business_date, updated_at, c_name) "
        f"VALUES (99, DATE '2020-01-01', TIMESTAMP '2020-01-01 00:00:00', 'outside')"
    )
    source.execute_query(f'UPDATE "{SOURCE_SCHEMA}"."{table}" SET c_name = \'replaced\' WHERE id = 1')
    config = cfg.replace(tmp_path=tmp_path, table=table)
    replaced = PostgresFullExtractStrategy(source, logger=NoopLogger()).extract(config, None)
    _load(sink, config, replaced.artifact, replaced.schema)
    rows = sink_conn.get_records(f"SELECT id, c_name FROM {fq(table)} ORDER BY id", as_dict=True)
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert by_id[1] == "replaced"
    assert by_id[99] == "outside"
    assert_unique_ids(sink_conn, table=table)
    assert_typed_spot_checks(sink_conn, table=table, columns=columns, expected_name="replaced")


@pytest.mark.skipif(not postgres_postgres_enabled(), reason="Postgres Docker IT not configured")
def test_postgres_to_postgres_partition_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_part"
    source, sink_conn, columns = _ready(table)
    fr = cfg.full_refresh(tmp_path=tmp_path, table=table)
    extract = PostgresFullExtractStrategy(source, logger=NoopLogger()).extract(fr, None)
    sink = PostgresSink(sink_conn, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    sink_conn.execute_query(
        f"INSERT INTO {fq(table)} (id, business_date, updated_at, c_name) "
        f"VALUES (99, DATE '2020-01-01', TIMESTAMP '2020-01-01 00:00:00', 'other-partition')"
    )
    source.execute_query(f'UPDATE "{SOURCE_SCHEMA}"."{table}" SET c_name = \'part-replaced\' WHERE id = 1')
    config = cfg.partition_replace(tmp_path=tmp_path, table=table)
    batch = PostgresFullExtractStrategy(source, logger=NoopLogger()).extract(config, None)
    _load(sink, config, batch.artifact, batch.schema)
    rows = sink_conn.get_records(f"SELECT id, c_name FROM {fq(table)} ORDER BY id", as_dict=True)
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert by_id[1] == "part-replaced"
    assert by_id[99] == "other-partition"
    assert_unique_ids(sink_conn, table=table)
    assert_typed_spot_checks(sink_conn, table=table, columns=columns, expected_name="part-replaced")


@pytest.mark.skipif(not postgres_postgres_enabled(), reason="Postgres Docker IT not configured")
def test_postgres_to_postgres_snapshot_diff_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_diff"
    source, sink_conn, columns = _ready(table)
    config = cfg.snapshot_diff(tmp_path=tmp_path, table=table)
    strategy = PostgresFullExtractStrategy(source, logger=NoopLogger())
    sink = PostgresSink(sink_conn, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, None)
    _load(sink, config, first.artifact, first.schema)
    assert_row_count(sink_conn, table=table, expected=2)
    source.execute_query(f'DELETE FROM "{SOURCE_SCHEMA}"."{table}" WHERE id = 2')
    source.execute_query(f'UPDATE "{SOURCE_SCHEMA}"."{table}" SET c_name = \'diff-updated\' WHERE id = 1')
    insert_wide_watermark_row(source, table=table)
    second = strategy.extract(config, None)
    _load(sink, config, second.artifact, second.schema)
    rows = sink_conn.get_records(f"SELECT id, c_name FROM {fq(table)} ORDER BY id", as_dict=True)
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert 2 not in by_id
    assert by_id[1] == "diff-updated"
    assert by_id[3] == "row-three"
    assert_typed_spot_checks(sink_conn, table=table, columns=columns, expected_name="diff-updated")


@pytest.mark.skipif(not postgres_postgres_enabled(), reason="Postgres Docker IT not configured")
def test_postgres_to_postgres_scd2_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_scd2"
    source, sink_conn, columns = _ready(table)
    config = cfg.scd2(tmp_path=tmp_path, table=table)
    strategy = PostgresFullExtractStrategy(source, logger=NoopLogger())
    sink = PostgresSink(sink_conn, state_storage=None, logger=NoopLogger())
    first = strategy.extract(config, None)
    _load(sink, config, first.artifact, first.schema)
    cols = sink_conn.get_records(
        f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'dpone_it' AND table_name = '{table}'
        """,
        as_dict=True,
    )
    names = {row["column_name"] for row in cols}
    assert "__dpone__is_current" in names
    assert "__dpone__row_hash" in names
    source.execute_query(
        f'UPDATE "{SOURCE_SCHEMA}"."{table}" '
        f"SET c_name = 'scd2-v2', updated_at = TIMESTAMP '2026-07-21 13:00:00' WHERE id = 1"
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


@pytest.mark.skipif(not postgres_postgres_enabled(), reason="Postgres Docker IT not configured")
def test_postgres_to_postgres_backfill_replace_wide_live(tmp_path: Path) -> None:
    table = f"{WIDE_TABLE}_backfill"
    source, sink_conn, columns = _ready(table)
    fr = cfg.full_refresh(tmp_path=tmp_path, table=table)
    extract = PostgresFullExtractStrategy(source, logger=NoopLogger()).extract(fr, None)
    sink = PostgresSink(sink_conn, state_storage=None, logger=NoopLogger())
    _load(sink, fr, extract.artifact, extract.schema)
    source.execute_query(f'UPDATE "{SOURCE_SCHEMA}"."{table}" SET c_name = \'backfilled\' WHERE id = 1')
    config = cfg.backfill_replace(tmp_path=tmp_path, table=table)
    batch = PostgresFullExtractStrategy(source, logger=NoopLogger()).extract(config, None)
    _load(sink, config, batch.artifact, batch.schema)
    row = sink_conn.get_records(f"SELECT c_name FROM {fq(table)} WHERE id = 1", as_dict=True)[0]
    assert row["c_name"] == "backfilled"
    assert_typed_spot_checks(sink_conn, table=table, columns=columns, expected_name="backfilled")

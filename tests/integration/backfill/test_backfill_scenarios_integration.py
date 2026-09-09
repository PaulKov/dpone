"""Operational backfill scenarios on the PostgreSQL -> ClickHouse route.

Covers the production guarantees on real services:

- resume after a failed chunk (committed chunks are not re-executed);
- idempotent re-run of a completed campaign (no data movement, no duplicates);
- ``parallel_workers > 1`` over independent partition chunks;
- verification bridge document for the route-refresh toolchain;
- fail-fast validation of invalid strategy combinations.
"""

from __future__ import annotations

import json
import os
import uuid
from contextlib import contextmanager
from pathlib import Path

import pytest
from backfill_toolkit import (
    BackfillCase,
    BackfillWindow,
    SeedSpec,
    assert_campaign_committed,
    assert_parity,
    build_load_config,
    date_window,
    integer_window,
    read_ledger,
    run_backfill,
)
from endpoints import ClickHouseEndpoint, PostgresEndpoint

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.etl.processor import ETLProcessor

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_backfill,
    pytest.mark.integration_postgres,
    pytest.mark.integration_clickhouse,
]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

SPEC = SeedSpec()
_PG_SOURCE_OPTIONS = {"batch_commit_mode": "whole"}


@pytest.fixture
def route(postgres_connector, postgres_schema, clickhouse_connector, clickhouse_settings):
    source = PostgresEndpoint(postgres_connector)
    target = ClickHouseEndpoint(clickhouse_connector, database=clickhouse_settings.database)
    table = f"bf_scn_{uuid.uuid4().hex[:8]}"
    source.seed(postgres_schema, table, SPEC)
    target.drop(clickhouse_settings.database, table)
    yield source, target, postgres_schema, table
    target.drop(clickhouse_settings.database, table)


def _merge_case() -> BackfillCase:
    return BackfillCase(
        inner_mode="incremental_merge",
        window=date_window(SPEC),
        unique_key="id",
        source_options=dict(_PG_SOURCE_OPTIONS),
    )


def _config(route, tmp_path: Path, case: BackfillCase):
    source, target, schema, table = route
    return build_load_config(
        case=case,
        source_schema=schema,
        source_table=table,
        target_schema=target.database,
        target_table=table,
        source_type="postgres",
        sink_type="clickhouse",
        state_dir=tmp_path / "ledger",
    )


class _FailOnceProcessor(ETLProcessor):
    """Fails exactly one configured chunk on its first attempt."""

    fail_chunk_index = 2
    _failed = False

    def run(self, load_config, run_context=None, dag_id=None, execution_date=None):
        chunk = (load_config.options or {}).get("backfill", {}).get("chunk_context", {})
        if chunk.get("index") == self.fail_chunk_index and not type(self)._failed:
            type(self)._failed = True
            raise RuntimeError("injected chunk failure for resume scenario")
        return super().run(load_config, run_context, dag_id=dag_id, execution_date=execution_date)


def test_backfill_resume_skips_committed_chunks_and_recovers(route, tmp_path) -> None:
    from dpone.runtime.etl.backfill_orchestrator import execute_process_with_backfill

    source, target, schema, table = route
    load_config = _config(route, tmp_path, _merge_case())
    _FailOnceProcessor._failed = False

    failing = execute_process_with_backfill(
        _FailOnceProcessor(source=source.create_source(), sink=target.create_sink()), load_config
    )

    assert failing["status"] == "error"
    assert failing["backfill"]["chunks_committed"] == 1
    assert failing["backfill"]["chunks_failed"] == 1
    failed_ledger = read_ledger(failing)
    assert [chunk["status"] for chunk in failed_ledger["chunks"]] == ["success", "failed", "pending"]

    resumed = execute_process_with_backfill(
        ETLProcessor(source=source.create_source(), sink=target.create_sink()), load_config
    )

    assert_campaign_committed(resumed, chunks=3)
    assert resumed["backfill"]["chunks_skipped_resume"] == 1
    resumed_ledger = read_ledger(resumed)
    assert resumed_ledger["chunks"][0]["attempts"] == 1, "committed chunk must not be re-executed"
    assert resumed_ledger["chunks"][1]["attempts"] == 2, "failed chunk must be retried once"
    assert_parity(
        target.checksum(target.database, table),
        source.checksum(schema, table),
        context="resume scenario",
    )


def test_backfill_completed_campaign_rerun_is_idempotent(route, tmp_path) -> None:
    source, target, schema, table = route
    load_config = _config(route, tmp_path, _merge_case())

    first = run_backfill(source.create_source(), target.create_sink(), load_config)
    assert_campaign_committed(first, chunks=3)
    expected = source.checksum(schema, table)

    second = run_backfill(source.create_source(), target.create_sink(), load_config)

    assert second["status"] == "success"
    assert second["backfill"]["chunks_skipped_resume"] == 3
    assert second["extracted_rows"] == first["extracted_rows"], "re-run must not move data"
    ledger = read_ledger(second)
    assert all(chunk["attempts"] == 1 for chunk in ledger["chunks"])
    assert_parity(target.checksum(target.database, table), expected, context="idempotent re-run")
    assert target.duplicate_id_count(target.database, table) == 0


def test_backfill_commits_empty_noop_chunks_without_blocking_later_windows(route, tmp_path) -> None:
    source, target, schema, table = route
    case = BackfillCase(
        inner_mode="incremental_merge",
        window=BackfillWindow(column="business_date", start="2024-12-29", end="2025-01-09", step="2d"),
        unique_key="id",
        source_options=dict(_PG_SOURCE_OPTIONS),
    )
    load_config = _config(route, tmp_path, case)

    result = run_backfill(source.create_source(), target.create_sink(), load_config)

    assert_campaign_committed(result, chunks=6)
    ledger = read_ledger(result)
    zero_row_chunks = [chunk for chunk in ledger["chunks"] if chunk["rows_extracted"] == chunk["rows_loaded"] == 0]
    assert [chunk["index"] for chunk in zero_row_chunks] == [1, 6]
    assert_parity(
        target.checksum(target.database, table),
        source.checksum(schema, table),
        context="empty no-op chunks",
    )
    assert target.duplicate_id_count(target.database, table) == 0


def test_backfill_parallel_workers_over_partition_chunks(
    route, tmp_path, postgres_settings, clickhouse_settings
) -> None:
    """Parallel chunks require independent connections (one session per worker),
    mirroring the production pod-per-chunk topology."""

    from dpone.backfill.state import FileBackfillStateStore
    from dpone.runtime.connectors.clickhouse import ClickHouseConnector
    from dpone.runtime.connectors.postgres import PostgresConnector
    from dpone.runtime.etl.backfill_orchestrator import BackfillOrchestrator
    from dpone.runtime.process_logging import create_etl_logger
    from dpone.runtime.sinks.clickhouse import ClickHouseSink
    from dpone.runtime.sources.postgres import PostgresSource

    source, target, schema, table = route
    case = BackfillCase(
        inner_mode="partition_replace",
        window=integer_window(SPEC),
        partition_column="bucket",
        parallel_workers=2,
        source_options=dict(_PG_SOURCE_OPTIONS),
    )
    target.create_partitioned_target(target.database, table, partition_column="bucket")
    load_config = _config(route, tmp_path, case)
    shared_state_store = FileBackfillStateStore(tmp_path / "ledger")

    def run_chunk_with_fresh_connections(chunk_config):
        pg = PostgresConnector(
            host=postgres_settings.host,
            port=postgres_settings.port,
            database=postgres_settings.database,
            user=postgres_settings.user,
            password=postgres_settings.password,
            application_name="dpone-backfill-parallel",
        )
        ch = ClickHouseConnector(
            host=clickhouse_settings.host,
            port=clickhouse_settings.port,
            database=clickhouse_settings.database,
            user=clickhouse_settings.user,
            password=clickhouse_settings.password,
            secure=clickhouse_settings.secure,
            application_name="dpone-backfill-parallel",
        )
        try:
            processor = ETLProcessor(
                source=PostgresSource(pg, state_storage=None, logger=create_etl_logger()),
                sink=ClickHouseSink(ch),
            )
            return processor.run(chunk_config)
        finally:
            pg.close()
            ch.close()

    @contextmanager
    def worker_state_store(_worker_id: int):
        yield shared_state_store

    result = BackfillOrchestrator(
        chunk_runner=run_chunk_with_fresh_connections,
        state_store=shared_state_store,
        worker_state_store_factory=worker_state_store,
    ).run(load_config)

    assert_campaign_committed(result, chunks=SPEC.buckets)
    assert_parity(
        target.checksum(target.database, table),
        source.checksum(schema, table),
        context="parallel workers",
    )
    assert target.duplicate_id_count(target.database, table) == 0


def test_backfill_emits_route_refresh_verification_bridge(route, tmp_path) -> None:
    source, target, schema, table = route
    load_config = _config(route, tmp_path, _merge_case())

    result = run_backfill(source.create_source(), target.create_sink(), load_config)

    assert_campaign_committed(result, chunks=3)
    execution_path = Path(result["backfill"]["verification_execution_path"])
    payload = json.loads(execution_path.read_text(encoding="utf-8"))
    assert payload["route"] == {"source": "postgres", "sink": "clickhouse", "strategy": "backfill"}
    assert payload["status"] == "succeeded"
    assert payload["passed"] is True and payload["executed"] is True
    assert [chunk["ordinal"] for chunk in payload["chunks"]] == [1, 2, 3]
    assert all(chunk["idempotency_key"] for chunk in payload["chunks"])
    assert result["backfill"]["verification"]["status"] == "passed"


def test_backfill_rejects_invalid_strategy_combinations(route, tmp_path) -> None:
    source, target, schema, table = route

    multi_chunk_full_refresh = _config(
        route,
        tmp_path,
        BackfillCase(inner_mode="full_refresh", window=date_window(SPEC), source_options=dict(_PG_SOURCE_OPTIONS)),
    )
    with pytest.raises(ValueError, match="single-chunk plan"):
        run_backfill(source.create_source(), target.create_sink(), multi_chunk_full_refresh)

    unsupported_inner = _config(route, tmp_path, _merge_case())
    unsupported_inner.options["backfill"]["inner_mode"] = "scd2"
    with pytest.raises(ValueError, match="backfill.inner_mode"):
        run_backfill(source.create_source(), target.create_sink(), unsupported_inner)

    merge_without_key = _config(route, tmp_path, _merge_case())
    merge_without_key.unique_key = None
    failed = run_backfill(source.create_source(), target.create_sink(), merge_without_key)
    assert failed["status"] == "error"
    assert "unique_key" in failed["errors"][0]

    kafka_partition_replace = _config(route, tmp_path, _merge_case())
    kafka_partition_replace.options["backfill"]["inner_mode"] = "partition_replace"
    from dpone.runtime.sinks.kafka import KafkaSink

    kafka_failed = run_backfill(
        source.create_source(),
        KafkaSink(connector=object()),
        replace_sink_type(kafka_partition_replace),
    )
    assert kafka_failed["status"] == "error"
    assert "keyed upsert replay" in kafka_failed["errors"][0]

    assert load_strategy_is_backfill(multi_chunk_full_refresh)


def replace_sink_type(load_config):
    load_config.options["sink_type"] = "kafka"
    return load_config


def load_strategy_is_backfill(load_config) -> bool:
    return load_config.load_strategy == LoadStrategy.BACKFILL

"""Chunked backfill E2E: PostgreSQL source -> ClickHouse sink.

Every case runs the real runtime path (extract -> staging -> inner strategy
finalization per chunk) against the docker integration stack and asserts
row/checksum parity plus ledger invariants.
"""

from __future__ import annotations

import os
import uuid

import pytest
from backfill_toolkit import (
    BackfillCase,
    SeedSpec,
    assert_campaign_committed,
    assert_parity,
    build_load_config,
    date_window,
    full_window,
    integer_window,
    run_backfill,
)
from endpoints import ClickHouseEndpoint, PostgresEndpoint

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

CASES = {
    "replace_date_chunks": BackfillCase(
        inner_mode="replace", window=date_window(SPEC), source_options=dict(_PG_SOURCE_OPTIONS)
    ),
    "incremental_merge_date_chunks": BackfillCase(
        inner_mode="incremental_merge",
        window=date_window(SPEC),
        unique_key="id",
        source_options=dict(_PG_SOURCE_OPTIONS),
    ),
    "partition_replace_bucket_chunks": BackfillCase(
        inner_mode="partition_replace",
        window=integer_window(SPEC),
        partition_column="bucket",
        source_options=dict(_PG_SOURCE_OPTIONS),
    ),
    "full_refresh_single_chunk": BackfillCase(
        inner_mode="full_refresh", window=full_window(SPEC), source_options=dict(_PG_SOURCE_OPTIONS)
    ),
}


@pytest.fixture
def route(postgres_connector, postgres_schema, clickhouse_connector, clickhouse_settings):
    source = PostgresEndpoint(postgres_connector)
    target = ClickHouseEndpoint(clickhouse_connector, database=clickhouse_settings.database)
    table = f"bf_pg_ch_{uuid.uuid4().hex[:8]}"
    source.seed(postgres_schema, table, SPEC)
    target.drop(clickhouse_settings.database, table)
    yield source, target, postgres_schema, table
    target.drop(clickhouse_settings.database, table)


@pytest.mark.parametrize("case_id", sorted(CASES))
def test_backfill_matrix_postgres_to_clickhouse(case_id: str, route, tmp_path) -> None:
    source, target, schema, table = route
    case = CASES[case_id]
    if case.partition_column:
        target.create_partitioned_target(target.database, table, partition_column=case.partition_column)

    load_config = build_load_config(
        case=case,
        source_schema=schema,
        source_table=table,
        target_schema=target.database,
        target_table=table,
        source_type="postgres",
        sink_type="clickhouse",
        state_dir=tmp_path / "ledger",
    )

    result = run_backfill(source.create_source(), target.create_sink(), load_config)

    expected_chunks = _expected_chunks(case)
    assert_campaign_committed(result, chunks=expected_chunks)
    assert_parity(
        target.checksum(target.database, table),
        source.checksum(schema, table),
        context=f"postgres->clickhouse {case_id}",
    )
    assert target.duplicate_id_count(target.database, table) == 0, "half-open chunk windows must not duplicate rows"


def _expected_chunks(case: BackfillCase) -> int:
    if case.inner_mode == "full_refresh":
        return 1
    if case.window.kind == "integer":
        return SPEC.buckets
    return 3  # 6 days / 2d step

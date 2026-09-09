"""Chunked backfill E2E: SQL Server source -> ClickHouse sink."""

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
    integer_window,
    run_backfill,
)
from endpoints import ClickHouseEndpoint, MSSQLEndpoint

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_backfill,
    pytest.mark.integration_mssql,
    pytest.mark.integration_clickhouse,
]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

SPEC = SeedSpec()
_MSSQL_SOURCE_OPTIONS = {"mssql_export_mode": "streaming"}

CASES = {
    "replace_date_chunks": BackfillCase(
        inner_mode="replace", window=date_window(SPEC), source_options=dict(_MSSQL_SOURCE_OPTIONS)
    ),
    "incremental_merge_date_chunks": BackfillCase(
        inner_mode="incremental_merge",
        window=date_window(SPEC),
        unique_key="id",
        source_options=dict(_MSSQL_SOURCE_OPTIONS),
    ),
    "partition_replace_bucket_chunks": BackfillCase(
        inner_mode="partition_replace",
        window=integer_window(SPEC),
        partition_column="bucket",
        source_options=dict(_MSSQL_SOURCE_OPTIONS),
    ),
}


@pytest.fixture
def route(mssql_connector, mssql_schema, clickhouse_connector, clickhouse_settings):
    source = MSSQLEndpoint(mssql_connector)
    target = ClickHouseEndpoint(clickhouse_connector, database=clickhouse_settings.database)
    table = f"bf_ms_ch_{uuid.uuid4().hex[:8]}"
    source.seed(mssql_schema, table, SPEC)
    target.drop(clickhouse_settings.database, table)
    yield source, target, mssql_schema, table
    target.drop(clickhouse_settings.database, table)


@pytest.mark.parametrize("case_id", sorted(CASES))
def test_backfill_matrix_mssql_to_clickhouse(case_id: str, route, tmp_path) -> None:
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
        source_type="mssql",
        sink_type="clickhouse",
        state_dir=tmp_path / "ledger",
    )

    result = run_backfill(source.create_source(), target.create_sink(), load_config)

    expected_chunks = SPEC.buckets if case.window.kind == "integer" else 3
    assert_campaign_committed(result, chunks=expected_chunks)
    assert_parity(
        target.checksum(target.database, table),
        source.checksum(schema, table),
        context=f"mssql->clickhouse {case_id}",
    )
    assert target.duplicate_id_count(target.database, table) == 0

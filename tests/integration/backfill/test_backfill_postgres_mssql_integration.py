"""Chunked backfill E2E: PostgreSQL source -> SQL Server sink."""

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
from endpoints import MSSQLEndpoint, PostgresEndpoint

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_backfill,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

SPEC = SeedSpec()
_PG_SOURCE_OPTIONS = {
    "batch_commit_mode": "whole",
    "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000, "packet_size": 16384}},
}

CASES = {
    "replace_date_chunks": BackfillCase(
        inner_mode="replace",
        window=date_window(SPEC),
        export_format="csv",
        source_options=dict(_PG_SOURCE_OPTIONS),
    ),
    "incremental_merge_date_chunks": BackfillCase(
        inner_mode="incremental_merge",
        window=date_window(SPEC),
        unique_key="id",
        export_format="csv",
        source_options=dict(_PG_SOURCE_OPTIONS),
    ),
    "partition_replace_bucket_chunks": BackfillCase(
        inner_mode="partition_replace",
        window=integer_window(SPEC),
        partition_column="bucket",
        export_format="csv",
        source_options=dict(_PG_SOURCE_OPTIONS),
    ),
}


@pytest.fixture
def route(postgres_connector, postgres_schema, mssql_connector, mssql_schema):
    source = PostgresEndpoint(postgres_connector)
    target = MSSQLEndpoint(mssql_connector)
    table = f"bf_pg_ms_{uuid.uuid4().hex[:8]}"
    source.seed(postgres_schema, table, SPEC)
    yield source, target, postgres_schema, mssql_schema, table
    target.drop(mssql_schema, table)


@pytest.mark.parametrize("case_id", sorted(CASES))
def test_backfill_matrix_postgres_to_mssql(case_id: str, route, tmp_path) -> None:
    source, target, source_schema, target_schema, table = route
    case = CASES[case_id]

    load_config = build_load_config(
        case=case,
        source_schema=source_schema,
        source_table=table,
        target_schema=target_schema,
        target_table=table,
        source_type="postgres",
        sink_type="mssql",
        state_dir=tmp_path / "ledger",
    )

    result = run_backfill(source.create_source(), target.create_sink(), load_config)

    expected_chunks = SPEC.buckets if case.window.kind == "integer" else 3
    assert_campaign_committed(result, chunks=expected_chunks)
    assert_parity(
        target.checksum(target_schema, table),
        source.checksum(source_schema, table),
        context=f"postgres->mssql {case_id}",
    )

"""Chunked backfill E2E: PostgreSQL source -> PostgreSQL sink (cross-schema)."""

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
from endpoints import PostgresEndpoint

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_backfill,
    pytest.mark.integration_postgres,
]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

SPEC = SeedSpec()
# Batched export materializes through the sink staging manager (strategy-aware path).
_PG_SOURCE_OPTIONS = {"batch_commit_mode": "separate"}

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
}


@pytest.fixture
def sink_postgres_connector(postgres_settings):
    """Dedicated sink connection: source streaming and sink staging must not
    interleave statements on one session (mirrors real deployments)."""

    from dpone.runtime.connectors.postgres import PostgresConnector

    connector = PostgresConnector(
        host=postgres_settings.host,
        port=postgres_settings.port,
        database=postgres_settings.database,
        user=postgres_settings.user,
        password=postgres_settings.password,
        application_name="dpone-backfill-it-sink",
    )
    try:
        yield connector
    finally:
        connector.close()


@pytest.fixture
def route(postgres_connector, sink_postgres_connector, postgres_schema):
    source_endpoint = PostgresEndpoint(postgres_connector)
    sink_endpoint = PostgresEndpoint(sink_postgres_connector)
    source_endpoint.reset_session()
    source_table = f"bf_src_{uuid.uuid4().hex[:8]}"
    target_table = f"bf_dst_{uuid.uuid4().hex[:8]}"
    source_endpoint.seed(postgres_schema, source_table, SPEC)
    yield source_endpoint, sink_endpoint, postgres_schema, source_table, target_table


@pytest.mark.parametrize("case_id", sorted(CASES))
def test_backfill_matrix_postgres_to_postgres(case_id: str, route, tmp_path) -> None:
    source_endpoint, sink_endpoint, schema, source_table, target_table = route
    case = CASES[case_id]

    load_config = build_load_config(
        case=case,
        source_schema=schema,
        source_table=source_table,
        target_schema=schema,
        target_table=target_table,
        source_type="postgres",
        sink_type="postgres",
        state_dir=tmp_path / "ledger",
    )
    # Distinct conn ids force the cross-connection file export path even though
    # the physical database is shared by source and target schemas.
    load_config.target_conn_id = "postgres_it_target"
    load_config.staging_schema = schema
    # The multi-chunk matrix exercises backfill strategy semantics, not target
    # bootstrap. Pre-provision the exact source nullability once so later
    # chunks cannot confuse a permissive inferred target with source drift.
    sink_endpoint.create_empty_table(schema, target_table, with_technical_columns=True)

    result = run_backfill(source_endpoint.create_source(), sink_endpoint.create_sink(), load_config)

    expected_chunks = SPEC.buckets if case.window.kind == "integer" else 3
    assert_campaign_committed(result, chunks=expected_chunks)
    assert_parity(
        sink_endpoint.checksum(schema, target_table),
        source_endpoint.checksum(schema, source_table),
        context=f"postgres->postgres {case_id}",
    )

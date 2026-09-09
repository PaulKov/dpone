"""Chunked backfill E2E: PostgreSQL source -> Kafka topic (keyed upsert replay)."""

from __future__ import annotations

import os
import uuid

import pytest
from backfill_toolkit import (
    BackfillCase,
    SeedSpec,
    assert_campaign_committed,
    build_load_config,
    date_window,
    run_backfill,
)
from endpoints import KafkaTargetEndpoint, PostgresEndpoint

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_backfill,
    pytest.mark.integration_postgres,
    pytest.mark.integration_kafka,
]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

SPEC = SeedSpec(rows=40)


def test_backfill_postgres_to_kafka_replays_chunks_as_keyed_upserts(
    postgres_connector, postgres_schema, kafka_connector, tmp_path
) -> None:
    source = PostgresEndpoint(postgres_connector)
    target = KafkaTargetEndpoint(kafka_connector)
    table = f"bf_pg_kafka_{uuid.uuid4().hex[:8]}"
    topic = f"dpone_bf_{uuid.uuid4().hex[:10]}"
    source.seed(postgres_schema, table, SPEC)

    case = BackfillCase(
        inner_mode="incremental_merge",
        window=date_window(SPEC),
        unique_key="id",
        source_options={"batch_commit_mode": "whole"},
        sink_options={"topic": topic, "message_format": "json"},
    )
    load_config = build_load_config(
        case=case,
        source_schema=postgres_schema,
        source_table=table,
        target_schema="kafka",
        target_table=topic,
        source_type="postgres",
        sink_type="kafka",
        state_dir=tmp_path / "ledger",
    )

    result = run_backfill(source.create_source(), target.create_sink(), load_config)

    assert_campaign_committed(result, chunks=3)
    ids = target.consume_ids(topic, expected=SPEC.rows, group_id=f"dpone-bf-it-{uuid.uuid4().hex}")
    assert ids == set(range(1, SPEC.rows + 1)), "every seeded row must be replayed exactly once per key"

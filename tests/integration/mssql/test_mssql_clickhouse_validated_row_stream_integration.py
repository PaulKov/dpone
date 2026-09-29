"""Real-row acceptance for the opt-in strict MSSQL → ClickHouse row stream.

This profile uses the existing disposable Docker endpoints. It is not a
production route certificate or a substitute for a signed live proof.
"""

from __future__ import annotations

import uuid

import pytest
from tests.integration.mssql.mssql_clickhouse_live_support import IntegrationLogger, open_mssql_connector

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.etl.lifecycle import RuntimeLifecycleService
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sources.strategies.mssql.mssql_strategies import MSSQLFullExtractStrategy

pytestmark = [pytest.mark.integration_mssql, pytest.mark.integration_clickhouse]


def test_validated_stream_loads_real_rows_and_preserves_target_after_bad_row(
    clickhouse_connector, clickhouse_settings
) -> None:
    mssql = open_mssql_connector()
    schema = f"vrs_{uuid.uuid4().hex[:8]}"
    source_table = "source_rows"
    target_table = f"vrs_rows_{uuid.uuid4().hex[:8]}"
    source_name = f"[{schema}].[{source_table}]"
    target_name = f"`{clickhouse_settings.database}`.`{target_table}`"
    config = LoadConfig(
        source_conn_id="mssql-it",
        target_conn_id="clickhouse-it",
        source_schema=schema,
        source_table=source_table,
        target_schema=clickhouse_settings.database,
        target_table=target_table,
        load_strategy=LoadStrategy.FULL_REFRESH,
        batch_size=1,
        options={
            "sink_type": "clickhouse",
            "mssql_export_mode": "streaming",
            "schema_contract": {
                "enforcement": "strict",
                "columns": {
                    "id": {"type": "bigint", "nullable": False},
                    "amount": {"type": "integer", "nullable": False},
                },
            },
            "physical_design": {"storage": {"clickhouse": {"engine": "MergeTree", "order_by": ["id"]}}},
        },
    )
    try:
        mssql.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
        mssql.execute_query(f"CREATE TABLE {source_name} ([id] bigint NOT NULL, [amount] int NULL)")
        mssql.execute_query(f"INSERT INTO {source_name} VALUES (1, 10), (2, 20)")
        source = MSSQLFullExtractStrategy(mssql, IntegrationLogger(), sink_connector=clickhouse_connector)
        sink = ClickHouseSink(clickhouse_connector)

        def load_once(attempt: str):
            extracted = source.extract(config, None)
            context = RuntimeLifecycleService().prepare_before_schema_evolution(
                load_config=config,
                payload=LoadPayload(artifact=extracted.artifact, schema=extracted.schema),
                run_id=attempt,
                load_id=attempt,
            )
            return sink.load(config, context.payload)

        first = load_once("validated-stream-first")
        assert first.total_rows == 2
        assert clickhouse_connector.get_records(f"SELECT count(), sum(amount) FROM {target_name}")[0] == (2, 30)

        mssql.execute_query(f"INSERT INTO {source_name} VALUES (3, NULL)")
        with pytest.raises(RuntimeError):
            load_once("validated-stream-invalid")
        assert clickhouse_connector.get_records(f"SELECT count(), sum(amount) FROM {target_name}")[0] == (2, 30)
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {target_name}")
        mssql.execute_query(f"DROP TABLE IF EXISTS {source_name}")
        mssql.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")
        mssql.close()

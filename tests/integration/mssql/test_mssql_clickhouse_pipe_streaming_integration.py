from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest
from tests.integration.mssql.mssql_clickhouse_live_support import IntegrationLogger as _Logger
from tests.integration.mssql.mssql_clickhouse_live_support import open_mssql_connector as _mssql_connector

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.physical_chunking import PhysicalChunkedFileExportArtifact
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sources.strategies.mssql.mssql_strategies import MSSQLFullExtractStrategy

pytestmark = [pytest.mark.integration_mssql, pytest.mark.integration_clickhouse]


@pytest.mark.parametrize("transfer_mode", ["streaming", "physical_chunks"])
def test_mssql_bcp_transfer_loads_clickhouse_via_http(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
    transfer_mode: str,
) -> None:
    """Live certification for BCP pipe and bounded physical-chunk routes."""

    mssql = _mssql_connector()
    schema = f"pipe_{uuid.uuid4().hex[:8]}"
    source_table = "sales"
    target_table = f"pipe_sales_{uuid.uuid4().hex[:8]}"
    rows = 10_000
    try:
        mssql.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
        mssql.execute_query(
            f"""
            CREATE TABLE [{schema}].[{source_table}] (
                [sale_id] int NOT NULL PRIMARY KEY,
                [status_code] varchar(20) NOT NULL,
                [amount_cents] bigint NOT NULL
            )
            """
        )
        mssql.execute_query(
            f"""
            ;WITH n AS (
                SELECT CAST(1 AS int) AS n
                UNION ALL
                SELECT n + 1 FROM n WHERE n < {rows}
            )
            INSERT INTO [{schema}].[{source_table}] ([sale_id], [status_code], [amount_cents])
            SELECT n, CONCAT('status-', n % 7), CAST(n AS bigint) * 100
            FROM n
            OPTION (MAXRECURSION 0)
            """
        )

        snapshot = (
            {
                "streaming": {
                    "mode": "required",
                    "provider": "bcp_pipe",
                    "pipe_mode": "fifo",
                    "read_buffer_bytes": "1MiB",
                    "cleanup_policy": "eager",
                    "delimiter_safety": "advisory",
                }
            }
            if transfer_mode == "streaming"
            else {
                "scan": {
                    "mode": "single_scan",
                    "heap_policy": "single_scan_chunks",
                    "require_index_for_range": True,
                },
                "physical_chunking": {
                    "mode": "required",
                    "target_chunk_bytes": "32KiB",
                    "max_chunk_bytes": "64KiB",
                    "cleanup_policy": "eager",
                },
            }
        )
        load_config = LoadConfig(
            source_conn_id="mssql-it",
            target_conn_id="clickhouse-it",
            source_schema=schema,
            source_table=source_table,
            target_schema=clickhouse_settings.database,
            target_table=target_table,
            load_strategy=LoadStrategy.FULL_REFRESH,
            batch_size=1000,
            options={
                "extract_mode": "bcp_queryout",
                "mssql_export_mode": "bcp",
                "bulk": _bcp_bulk_options(batch_size=1000),
                "runtime_storage": {"work_dir": str(tmp_path)},
                "native_transfer": {
                    "wire": {"mode": "typed_raw"},
                    "snapshot": snapshot,
                },
                "clickhouse_bulk": _clickhouse_http_streaming_bulk_options(clickhouse_settings),
            },
        )

        extract = MSSQLFullExtractStrategy(mssql, _Logger(), sink_connector=clickhouse_connector).extract(
            load_config,
            None,
        )
        if transfer_mode == "streaming":
            assert isinstance(extract.artifact, ByteStreamArtifact)
            assert extract.artifact.source_export_provider == "mssql_bcp_pipe"
        else:
            assert isinstance(extract.artifact, PhysicalChunkedFileExportArtifact)

        result = ClickHouseSink(clickhouse_connector).load(
            load_config,
            LoadPayload(artifact=extract.artifact, schema=extract.schema),
        )

        count, total = clickhouse_connector.get_records(
            f"""
            SELECT count(), sum(amount_cents)
            FROM `{clickhouse_settings.database}`.`{target_table}`
            """
        )[0]
        leftovers = clickhouse_connector.get_records(
            f"""
            SELECT count()
            FROM system.tables
            WHERE database = '{clickhouse_settings.database}'
              AND name LIKE '{target_table}__dpone_staging_%'
            """
        )[0][0]
        assert result.inserted_rows == rows
        assert count == rows
        assert total == sum(range(1, rows + 1)) * 100
        assert leftovers == 0
        if transfer_mode == "streaming":
            assert extract.artifact.stats.size_bytes > 0
        else:
            evidence = json.loads(extract.artifact.evidence_path.read_text(encoding="utf-8"))
            loaded = [item for item in evidence["chunks"] if item["status"] == "loaded_to_staging"]
            assert len(loaded) >= 2
            assert all(item["bytes"] <= 64 * 1024 for item in loaded)
            assert "generation_failure" not in evidence
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{target_table}`")
        mssql.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{source_table}]")
        mssql.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")
        mssql.close()


def _bcp_bulk_options(*, batch_size: int) -> dict[str, object]:
    return {
        "mode": "bcp",
        "bcp": {
            "batch_size": batch_size,
            "packet_size": 65535,
            "timeout_seconds": 120,
        },
    }


def _clickhouse_http_streaming_bulk_options(clickhouse_settings) -> dict[str, object]:
    return {
        "mode": "http",
        "ingest_contract": "typed_raw_streaming_staging",
        "http": {
            "host": clickhouse_settings.host,
            "port": int(os.getenv("DPONE_IT_CH_HTTP_PORT", "58123")),
            "database": clickhouse_settings.database,
            "user": clickhouse_settings.user,
            "password": clickhouse_settings.password,
        },
        "streaming": {
            "format": "CustomSeparated",
            "async_insert": True,
            "wait_for_async_insert": True,
        },
    }

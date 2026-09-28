"""Synthetic live proof of BCP FIFO -> bounded native files -> ClickHouse."""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest
from tests.integration.mssql import mssql_clickhouse_strategy_configs as cfg
from tests.integration.mssql.mssql_live_support import (
    NoopLogger,
    clickhouse_connector,
    ensure_clickhouse_database,
    ensure_mssql_database_and_schemas,
    mssql_clickhouse_enabled,
    mssql_connector,
)

from dpone.runtime.physical_chunking import PhysicalChunkedFileExportArtifact
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sources.strategies.mssql.mssql_full import MSSQLFullExtractStrategy

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_mssql,
    pytest.mark.integration_clickhouse,
]


@pytest.mark.skipif(not mssql_clickhouse_enabled(), reason="Docker database fixtures not configured")
def test_bounded_native_fifo_fidelity_and_eager_cleanup(tmp_path):
    table = "bounded_native_" + uuid4().hex[:12]
    ensure_mssql_database_and_schemas()
    ensure_clickhouse_database()
    source, target = mssql_connector(), clickhouse_connector()
    source.execute_query(
        f"CREATE TABLE [dpone_src].[{table}] (id int NOT NULL, value nvarchar(100) NULL, amount decimal(18,4) NULL)"
    )
    try:
        source.execute_query(
            f"INSERT INTO [dpone_src].[{table}] VALUES (1,NULL,NULL),(2,N'',0),(3,N'line'+NCHAR(10)+NCHAR(9)+N'終',-12.3456),(4,N'end',999.0001)"
        )
        config = cfg.full_refresh(tmp_path=tmp_path, table=table)
        config.options.update(
            {
                "extract_mode": "bcp_queryout",
                "mssql_export_mode": "bcp",
                "bulk": {"mode": "bcp", "bcp": {"file_format": "native"}},
                "clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"},
                "physical_design": {"storage": {"clickhouse": {"engine": "MergeTree", "order_by": ["id"]}}},
                "native_transfer": {
                    "wire": {
                        "mode": "typed_binary",
                        "source_native_format": "bcp_native",
                        "binary_format": "native",
                        "acceleration": {"mode": "required"},
                    },
                    "snapshot": {
                        "scan": {"mode": "single_scan", "heap_policy": "single_scan_chunks"},
                        "physical_chunking": {
                            "mode": "required",
                            "target_chunk_bytes": "32B",
                            "max_chunk_bytes": "128B",
                            "cleanup_policy": "eager",
                        },
                    },
                },
            }
        )
        extracted = MSSQLFullExtractStrategy(source, logger=NoopLogger(), sink_connector=target).extract(config, None)
        assert isinstance(extracted.artifact, PhysicalChunkedFileExportArtifact)
        assert extracted.artifact.native_wire_contract.target_format == "Native"
        result = ClickHouseSink(target, state_storage=None, logger=NoopLogger()).load(
            config, LoadPayload(artifact=extracted.artifact, schema=extracted.schema)
        )
        assert result.total_rows == 4
        assert target.get_records(f"SELECT id,value,amount FROM dpone_it.{table} ORDER BY id") == [
            (1, None, None),
            (2, "", Decimal("0.0000")),
            (3, "line\n\t終", Decimal("-12.3456")),
            (4, "end", Decimal("999.0001")),
        ]
        assert not list(tmp_path.rglob("dpone_physical_chunk_*.bcp"))
        assert not list(tmp_path.rglob("*.fifo"))
    finally:
        source.execute_query(f"DROP TABLE IF EXISTS [dpone_src].[{table}]")
        target.execute_query(f"DROP TABLE IF EXISTS dpone_it.{table} SYNC")

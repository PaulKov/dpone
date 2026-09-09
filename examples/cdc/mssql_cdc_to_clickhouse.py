"""Minimal SQL Server CDC -> ClickHouse landing example."""

from __future__ import annotations

from dpone.readiness.cdc import CDCBackend
from dpone.runtime.cdc import MSSQLCDCReader, MSSQLTableCDCReaderConfig
from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.state.cdc import MSSQLCDCOffsetStorage

mssql = MSSQLConnector(host="sql.example.com", port=1433, database="app", user="etl", password="secret")
state = MSSQLCDCOffsetStorage(mssql)
reader = MSSQLCDCReader(
    mssql,
    MSSQLTableCDCReaderConfig(source_schema="dbo", source_table="orders", capture_instance="dbo_orders"),
)
reader.setup()

offset = state.load_offset("orders-mssql-to-clickhouse", "dbo", "orders", CDCBackend.MSSQL_CDC)
batch = reader.read_batch(start_offset=offset, max_changes=50000)

# Pass batch.to_artifact() into ClickHouseSink. Use clickhouse_bulk.mode=http for large batches.
if batch.next_offset is not None:
    state.save_offset("orders-mssql-to-clickhouse", "dbo", "orders", batch.next_offset)

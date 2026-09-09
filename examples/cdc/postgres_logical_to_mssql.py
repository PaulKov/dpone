"""Minimal PostgreSQL logical CDC -> MSSQL landing example."""

from __future__ import annotations

from dpone.readiness.cdc import CDCBackend
from dpone.runtime.cdc import PostgresLogicalCDCReader, PostgresLogicalCDCReaderConfig
from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.connectors.postgres import PostgresConnector
from dpone.runtime.state.cdc import MSSQLCDCOffsetStorage

postgres = PostgresConnector(host="postgres.example.com", port=5432, database="app", user="etl", password="secret")
mssql = MSSQLConnector(host="sql.example.com", port=1433, database="landing", user="etl", password="secret")
state = MSSQLCDCOffsetStorage(mssql)
reader = PostgresLogicalCDCReader(
    postgres,
    PostgresLogicalCDCReaderConfig(source_schema="public", source_table="orders", slot_name="dpone_orders"),
)
reader.setup()

offset = state.load_offset("orders-pg-to-mssql", "public", "orders", CDCBackend.POSTGRES_LOGICAL)
batch = reader.read_batch(start_offset=offset, max_changes=50000)

# Pass batch.to_artifact() into the regular MSSQLSink load path, then commit offset.
if batch.next_offset is not None:
    state.save_offset("orders-pg-to-mssql", "public", "orders", batch.next_offset)

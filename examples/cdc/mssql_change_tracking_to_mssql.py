"""Minimal SQL Server Change Tracking net-sync example."""

from __future__ import annotations

from dpone.readiness.cdc import CDCBackend
from dpone.runtime.cdc import MSSQLChangeTrackingReader, MSSQLChangeTrackingReaderConfig
from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.state.cdc import MSSQLCDCOffsetStorage

source = MSSQLConnector(host="source-sql.example.com", port=1433, database="app", user="etl", password="secret")
state_db = MSSQLConnector(host="state-sql.example.com", port=1433, database="landing", user="etl", password="secret")
state = MSSQLCDCOffsetStorage(state_db)
reader = MSSQLChangeTrackingReader(source, MSSQLChangeTrackingReaderConfig(source_schema="dbo", source_table="orders"))
reader.setup()

offset = state.load_offset("orders-ct-net-sync", "dbo", "orders", CDCBackend.MSSQL_CHANGE_TRACKING)
batch = reader.read_batch(start_offset=offset, max_changes=50000)

# Change Tracking returns net changes since the previous version; load idempotently by primary key.
if batch.next_offset is not None:
    state.save_offset("orders-ct-net-sync", "dbo", "orders", batch.next_offset)

from __future__ import annotations

import os
import time
import uuid

import pytest

from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc import (
    CDCOperation,
    MSSQLCDCReader,
    MSSQLChangeTrackingReader,
    MSSQLChangeTrackingReaderConfig,
    MSSQLTableCDCReaderConfig,
)
from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.state.cdc import MSSQLCDCOffsetStorage

pytestmark = pytest.mark.integration_mssql


def _mssql_cdc_connector() -> MSSQLConnector:
    host = os.getenv("DPONE_IT_MSSQL_CDC_HOST") or os.getenv("DPONE_IT_MSSQL_HOST")
    if not host:
        pytest.skip("DPONE_IT_MSSQL_CDC_HOST or DPONE_IT_MSSQL_HOST is not configured")
    return MSSQLConnector(
        host=host,
        port=int(os.getenv("DPONE_IT_MSSQL_CDC_PORT", os.getenv("DPONE_IT_MSSQL_PORT", "1433"))),
        database=os.getenv("DPONE_IT_MSSQL_CDC_DATABASE", os.getenv("DPONE_IT_MSSQL_DATABASE", "dpone")),
        user=os.getenv("DPONE_IT_MSSQL_CDC_USER", os.getenv("DPONE_IT_MSSQL_USER", "sa")),
        password=os.getenv("DPONE_IT_MSSQL_CDC_PASSWORD", os.getenv("DPONE_IT_MSSQL_PASSWORD", "")),
        driver=os.getenv("DPONE_IT_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        trust_server_certificate=os.getenv("DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE", "yes"),
        bcp_path=os.getenv("DPONE_IT_MSSQL_BCP_PATH", "bcp"),
    )


def _run_cdc_scan_once(connector: MSSQLConnector) -> None:
    """Run one bounded SQL Server CDC capture scan for Docker-based integration tests.

    SQL Server containers often expose CDC metadata and table functions without
    running SQL Server Agent capture jobs in the background. Production readers
    should not force scans, but the local integration harness needs a
    deterministic way to advance capture tables after test DML.
    """

    connector.execute_query("EXEC sys.sp_cdc_scan @maxtrans = 500, @maxscans = 10, @continuous = 0")


def test_mssql_change_tracking_reader_reads_insert_update_delete() -> None:
    connector = _mssql_cdc_connector()
    schema = f"ct_{uuid.uuid4().hex[:8]}"
    state_schema = f"state_{uuid.uuid4().hex[:8]}"
    try:
        connector.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
        connector.execute_query(
            f"CREATE TABLE [{schema}].[orders] ([id] int NOT NULL PRIMARY KEY, [name] nvarchar(100) NULL)"
        )
        reader = MSSQLChangeTrackingReader(
            connector, MSSQLChangeTrackingReaderConfig(source_schema=schema, source_table="orders")
        )
        reader.setup()
        start = reader.current_offset()

        connector.execute_query(f"INSERT INTO [{schema}].[orders] ([id], [name]) VALUES (1, N'alpha')")
        insert_batch = reader.read_batch(start_offset=start)
        assert [change.operation for change in insert_batch.changes] == [CDCOperation.INSERT]
        assert insert_batch.next_offset is not None

        connector.execute_query(f"UPDATE [{schema}].[orders] SET [name] = N'beta' WHERE [id] = 1")
        update_batch = reader.read_batch(start_offset=insert_batch.next_offset)
        assert [change.operation for change in update_batch.changes] == [CDCOperation.UPDATE]
        assert update_batch.next_offset is not None

        connector.execute_query(f"DELETE FROM [{schema}].[orders] WHERE [id] = 1")
        delete_batch = reader.read_batch(start_offset=update_batch.next_offset)

        assert [change.operation for change in delete_batch.changes] == [CDCOperation.DELETE]
        assert delete_batch.next_offset is not None
        assert delete_batch.next_offset.backend == CDCBackend.MSSQL_CHANGE_TRACKING
        assert delete_batch.to_rows()[-1]["_dpone_cdc_deleted"] is True

        state = MSSQLCDCOffsetStorage(connector, schema=state_schema, table="cdc_offset")
        state.save_offset("orders-mssql-ct-it", schema, "orders", delete_batch.next_offset)
        assert (
            state.load_offset("orders-mssql-ct-it", schema, "orders", CDCBackend.MSSQL_CHANGE_TRACKING)
            == delete_batch.next_offset
        )
    finally:
        connector.execute_query(f"DROP TABLE IF EXISTS [{schema}].[orders]")
        connector.execute_query(f"DROP TABLE IF EXISTS [{state_schema}].[cdc_offset]")
        connector.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")
        connector.execute_query(f"DROP SCHEMA IF EXISTS [{state_schema}]")
        connector.close()


def test_mssql_cdc_reader_reads_insert_update_delete() -> None:
    connector = _mssql_cdc_connector()
    schema = f"cdc_{uuid.uuid4().hex[:8]}"
    state_schema = f"state_{uuid.uuid4().hex[:8]}"
    capture_instance = f"{schema}_orders"
    reader = MSSQLCDCReader(
        connector,
        MSSQLTableCDCReaderConfig(source_schema=schema, source_table="orders", capture_instance=capture_instance),
    )
    try:
        connector.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
        connector.execute_query(
            f"CREATE TABLE [{schema}].[orders] ([id] int NOT NULL PRIMARY KEY, [name] nvarchar(100) NULL)"
        )
        try:
            reader.setup()
        except Exception as exc:
            if "Agent" in str(exc) or "22832" in str(exc):
                pytest.skip(f"SQL Server CDC is unavailable without SQL Server Agent: {exc}")
            raise

        start = CDCOffset(backend=CDCBackend.MSSQL_CDC, token=reader.current_lsn())
        connector.execute_query(f"INSERT INTO [{schema}].[orders] ([id], [name]) VALUES (1, N'alpha')")
        connector.execute_query(f"UPDATE [{schema}].[orders] SET [name] = N'beta' WHERE [id] = 1")
        connector.execute_query(f"DELETE FROM [{schema}].[orders] WHERE [id] = 1")

        deadline = time.monotonic() + 60
        batch = None
        while time.monotonic() < deadline:
            _run_cdc_scan_once(connector)
            batch = reader.read_batch(start_offset=start, max_changes=100)
            if len(batch.changes) >= 3:
                break
            time.sleep(1)

        assert batch is not None
        assert [change.operation for change in batch.changes] == [
            CDCOperation.INSERT,
            CDCOperation.UPDATE,
            CDCOperation.DELETE,
        ]
        assert batch.next_offset is not None
        assert batch.next_offset.backend == CDCBackend.MSSQL_CDC

        state = MSSQLCDCOffsetStorage(connector, schema=state_schema, table="cdc_offset")
        state.save_offset("orders-mssql-cdc-it", schema, "orders", batch.next_offset)
        assert state.load_offset("orders-mssql-cdc-it", schema, "orders", CDCBackend.MSSQL_CDC) == batch.next_offset
    finally:
        try:
            connector.execute_query(
                "EXEC sys.sp_cdc_disable_table @source_schema = ?, @source_name = ?, @capture_instance = ?",
                (schema, "orders", capture_instance),
            )
        except Exception:
            pass
        connector.execute_query(f"DROP TABLE IF EXISTS [{schema}].[orders]")
        connector.execute_query(f"DROP TABLE IF EXISTS [{state_schema}].[cdc_offset]")
        connector.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")
        connector.execute_query(f"DROP SCHEMA IF EXISTS [{state_schema}]")
        connector.close()

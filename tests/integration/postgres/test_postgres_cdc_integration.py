from __future__ import annotations

import os
import time
import uuid

import pytest

from dpone.readiness.cdc import CDCBackend
from dpone.runtime.cdc import CDCOperation, PostgresLogicalCDCReader, PostgresLogicalCDCReaderConfig
from dpone.runtime.connectors.postgres import PostgresConnector
from dpone.runtime.state.cdc import PostgresCDCOffsetStorage

pytestmark = [pytest.mark.integration, pytest.mark.integration_postgres_cdc]


def _postgres_cdc_connector() -> PostgresConnector:
    host = os.getenv("DPONE_IT_PG_CDC_HOST")
    if not host:
        pytest.skip("DPONE_IT_PG_CDC_HOST is not configured")
    return PostgresConnector(
        host=host,
        port=int(os.getenv("DPONE_IT_PG_CDC_PORT", "5432")),
        database=os.getenv("DPONE_IT_PG_CDC_DATABASE", "dpone"),
        user=os.getenv("DPONE_IT_PG_CDC_USER", "dpone"),
        password=os.getenv("DPONE_IT_PG_CDC_PASSWORD", "dpone_pass"),
        application_name="dpone-postgres-cdc-integration",
    )


def test_postgres_logical_cdc_reader_reads_insert_update_delete() -> None:
    connector = _postgres_cdc_connector()
    schema = f"cdc_{uuid.uuid4().hex[:8]}"
    slot_name = f"dpone_{schema}_orders"
    reader = PostgresLogicalCDCReader(
        connector,
        PostgresLogicalCDCReaderConfig(
            source_schema=schema,
            source_table="orders",
            slot_name=slot_name,
            drop_existing_slot=True,
        ),
    )
    try:
        connector.execute_query(f'CREATE SCHEMA "{schema}"')
        connector.execute_query(f'CREATE TABLE "{schema}"."orders" (id integer PRIMARY KEY, name text, amount numeric)')
        reader.setup()
        connector.execute_query(f'INSERT INTO "{schema}"."orders" VALUES (1, \'alpha\', 10.5)')
        connector.execute_query(f'UPDATE "{schema}"."orders" SET name = \'beta\', amount = 11.5 WHERE id = 1')
        connector.execute_query(f'DELETE FROM "{schema}"."orders" WHERE id = 1')

        deadline = time.monotonic() + 20
        batch = None
        while time.monotonic() < deadline:
            batch = reader.read_batch(max_changes=100)
            if len(batch.changes) >= 3:
                break
            time.sleep(0.25)

        assert batch is not None
        assert [change.operation for change in batch.changes] == [
            CDCOperation.INSERT,
            CDCOperation.UPDATE,
            CDCOperation.DELETE,
        ]
        assert batch.next_offset is not None
        assert batch.next_offset.backend == CDCBackend.POSTGRES_LOGICAL
        assert batch.to_rows()[1]["name"] == "beta"

        state = PostgresCDCOffsetStorage(connector, schema=schema, table="cdc_offset")
        state.save_offset("orders-postgres-cdc-it", schema, "orders", batch.next_offset)
        assert (
            state.load_offset("orders-postgres-cdc-it", schema, "orders", CDCBackend.POSTGRES_LOGICAL)
            == batch.next_offset
        )
    finally:
        reader.drop_slot(missing_ok=True)
        connector.execute_query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        connector.close()

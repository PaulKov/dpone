"""Real PostgreSQL and SQL Server roundtrips for the backfill journal port."""

from __future__ import annotations

import json
import uuid
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

import pytest

from dpone.backfill.sql_state import MSSQLBackfillStateStore, PostgresBackfillStateStore
from dpone.backfill.sql_state_rendering import sql_string
from dpone.backfill.state import BackfillChunkRecord, BackfillLedger
from tests.integration.postgres.postgres_live_support import (
    mssql_connector,
    mssql_enabled,
    postgres_connector,
    postgres_enabled,
    wait_until_ready,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
]


@pytest.mark.skipif(not postgres_enabled(), reason="PostgreSQL Docker integration is not configured")
def test_postgres_backfill_journal_campaign_and_chunk_roundtrip_live(tmp_path) -> None:
    """Persist and reload exact campaign/chunk transitions through psycopg."""

    schema = f"bf_journal_{uuid.uuid4().hex[:12]}"
    connector = postgres_connector()
    wait_until_ready("postgres", lambda: connector.get_records("SELECT 1"))
    try:
        _assert_roundtrip(PostgresBackfillStateStore(connector, schema=schema, cache_dir=tmp_path))
    finally:
        with suppress(Exception):
            connector.execute_query(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        with suppress(Exception):
            connector.close()


@pytest.mark.integration_mssql
@pytest.mark.skipif(not mssql_enabled(), reason="MSSQL Docker integration is not configured")
def test_mssql_backfill_journal_campaign_and_chunk_roundtrip_live(tmp_path) -> None:
    """Persist and reload exact campaign/chunk transitions through pyodbc."""

    schema = f"bf_journal_{uuid.uuid4().hex[:12]}"
    connector = mssql_connector()
    wait_until_ready("mssql", lambda: connector.get_records("SELECT 1"))
    try:
        store = MSSQLBackfillStateStore(connector, schema=schema, cache_dir=tmp_path)
        _assert_roundtrip(store)
        _assert_mssql_expired_running_reacquire(store)
    finally:
        with suppress(Exception):
            connector.execute_query(f"DROP TABLE IF EXISTS [{schema}].[__dpone__backfill_chunks]")
        with suppress(Exception):
            connector.execute_query(f"DROP TABLE IF EXISTS [{schema}].[__dpone__backfill_campaigns]")
        with suppress(Exception):
            connector.execute_query(f"DROP SCHEMA IF EXISTS [{schema}]")
        with suppress(Exception):
            connector.close()


def _assert_roundtrip(store: BackfillStateStorePort) -> None:
    ledger = _ledger()
    store.save(ledger)

    initial = store.load(ledger.run_key)
    assert initial is not None
    assert initial.to_jsonable() == ledger.to_jsonable()
    lease_expiry = datetime.now(UTC) + timedelta(minutes=5)
    assert store.acquire_chunk_lease(ledger.run_key, 1, owner="worker-a", lease_expires_at=lease_expiry)

    running = store.load(ledger.run_key)
    assert running is not None
    assert running.chunk(1).status == "running"
    assert running.chunk(1).lease_owner == "worker-a"
    completion = BackfillChunkRecord.from_dict(running.chunk(1).to_jsonable())
    completion.status = "success"
    completion.rows_extracted = 17
    completion.rows_loaded = 17
    completion.run_id = "run-vendor-roundtrip"
    completion.load_id = "load-vendor-roundtrip"
    completion.finished_at = datetime.now(UTC).isoformat()
    assert store.complete_chunk_if_owned(ledger.run_key, completion, owner="worker-a")

    committed = store.load(ledger.run_key)
    assert committed is not None
    assert committed.run_key == ledger.run_key
    assert committed.dataset == ledger.dataset
    assert committed.plan_hash == ledger.plan_hash
    assert committed.config_hash == ledger.config_hash
    assert committed.chunk(1).status == "success"
    assert committed.chunk(1).rows_extracted == 17
    assert committed.chunk(1).rows_loaded == 17
    assert committed.chunk(1).lease_owner is None
    assert committed.chunk(1).lease_expires_at is None
    assert store.state_capabilities()["durable_read"] is True
    assert store.state_capabilities()["durable_write"] is True
    _assert_causal_journal_ids(store, ledger.run_key)


def _assert_causal_journal_ids(store: BackfillStateStorePort, run_key: str) -> None:
    """Read back positive, unique, increasing IDs from both physical journals."""

    for table in (store.campaign_fq_table, store.chunk_fq_table):
        rows = store.connector.get_records(
            f"SELECT journal_id FROM {table} WHERE run_key = {sql_string(run_key)} ORDER BY journal_id ASC",
            as_dict=True,
        )
        journal_ids = [int(row["journal_id"]) for row in rows]
        assert len(journal_ids) >= 3, (table, journal_ids)
        assert journal_ids == sorted(set(journal_ids)), (table, journal_ids)
        assert journal_ids[0] > 0, (table, journal_ids)


def _assert_mssql_expired_running_reacquire(store: MSSQLBackfillStateStore) -> None:
    """Prove latest-row authority for an expired running lease on SQL Server."""

    ledger = _ledger()
    store.save(ledger)
    campaign_owner = "campaign-owner"
    assert store.acquire_campaign_lock(
        ledger.run_key,
        owner=campaign_owner,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    try:
        assert store.acquire_chunk_lease(
            ledger.run_key,
            1,
            owner="worker-stale",
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )
        running = store.load(ledger.run_key)
        assert running is not None
        expired = running.chunk(1)
        expired.lease_expires_at = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        store.update_chunk(running, expired)

        rows = store.connector.get_records(
            f"SELECT journal_id, status, details_json, __dpone__loaded_at, "
            "sys.fn_PhysLocFormatter(%%physloc%%) AS physical_row_locator "
            f"FROM {store.chunk_fq_table} WHERE run_key = ? AND chunk_index = 1 "
            "AND status = N'running' ORDER BY journal_id DESC",
            (ledger.run_key,),
            as_dict=True,
        )
        transitions = tuple(
            {
                "journal_id": int(row["journal_id"]),
                "loaded_at": row["__dpone__loaded_at"],
                "physical_row_locator": row["physical_row_locator"],
                "lease_owner": json.loads(str(row["details_json"]))["lease_owner"],
                "lease_expires_at": json.loads(str(row["details_json"]))["lease_expires_at"],
            }
            for row in rows
        )
        assert len(transitions) == 2, transitions
        assert transitions[0]["journal_id"] > transitions[1]["journal_id"], transitions
        assert datetime.fromisoformat(str(transitions[0]["lease_expires_at"])) < datetime.now(UTC), transitions
        assert store.acquire_chunk_lease(
            ledger.run_key,
            1,
            owner="worker-replacement",
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        ), transitions
    finally:
        store.release_campaign_lock(ledger.run_key, owner=campaign_owner)


class JournalConnectorPort(Protocol):
    """Minimal read port used to inspect the physical vendor journal."""

    def get_records(self, sql: str, params: Any = None, as_dict: bool = False) -> list[Any]: ...


class BackfillStateStorePort(Protocol):
    """Structural annotation kept local to avoid widening a production port."""

    connector: JournalConnectorPort
    campaign_fq_table: str
    chunk_fq_table: str

    def save(self, ledger: BackfillLedger): ...

    def load(self, run_key: str) -> BackfillLedger | None: ...

    def acquire_chunk_lease(
        self,
        run_key: str,
        index: int,
        *,
        owner: str,
        lease_expires_at: datetime,
    ) -> bool: ...

    def complete_chunk_if_owned(self, run_key: str, record: BackfillChunkRecord, *, owner: str) -> bool: ...

    def state_capabilities(self) -> dict[str, object]: ...


def _ledger() -> BackfillLedger:
    return BackfillLedger(
        run_key=f"vendor-{uuid.uuid4().hex}",
        dataset="integration.orders",
        inner_mode="incremental_merge",
        chunk_config={"column": "id", "from": "1", "to": "17", "step": "17", "kind": "integer"},
        plan_hash="vendor-plan-sha256",
        config_hash="vendor-config-sha256",
        chunks=[
            BackfillChunkRecord(
                index=1,
                start="1",
                end="17",
                idempotency_key="vendor:1:1..17",
            )
        ],
    )

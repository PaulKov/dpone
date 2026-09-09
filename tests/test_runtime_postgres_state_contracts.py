from __future__ import annotations

from datetime import datetime

from dpone._compat import UTC
from dpone.runtime.lineage.audit import LoadAuditRecord
from dpone.runtime.state.factory import StateFactory
from dpone.runtime.state.models import RunState, RunStateStatus
from dpone.runtime.state.postgres import PostgresLoadAuditStorage, PostgresRunStateStorage, PostgresXMinStateStorage
from dpone.runtime.state.xmin_storage import XMinState


class FakePostgresConnector:
    def __init__(self) -> None:
        self.queries = []
        self.records = []

    def execute_query(self, query, params=None):
        self.queries.append((query, params))
        return 1

    def get_records(self, query, params=None, as_dict=False):
        self.queries.append((query, params, as_dict))
        if self.records:
            return self.records.pop(0)
        return []


def test_postgres_xmin_state_storage_saves_loads_and_deletes() -> None:
    now = datetime.now(UTC)
    connector = FakePostgresConnector()
    connector.records = [
        [
            {
                "xmin_value": 123,
                "__dpone__loaded_at": now,
                "is_initial": True,
                "wraparound_detected": False,
                "frozen_xid": 10,
            }
        ]
    ]
    storage = PostgresXMinStateStorage(connector, schema="state", table="xmin")

    storage.save_state("public", "orders", XMinState(123, now, is_initial=True, frozen_xid=10))
    loaded = storage.load_state("public", "orders")
    storage.delete_state("public", "orders")

    assert loaded == XMinState(123, now, is_initial=True, wraparound_detected=False, frozen_xid=10)
    assert len(connector.queries) >= 5


def test_postgres_run_state_storage_roundtrips_model() -> None:
    started = datetime.now(UTC)
    connector = FakePostgresConnector()
    connector.records = [
        [
            {
                "id": 1,
                "dag_id": "dag",
                "source_schema": "public",
                "source_table": "orders",
                "target_schema": "dbo",
                "target_table": "orders",
                "load_strategy": "replace",
                "execution_date": started,
                "state": "success",
                "started_at": started,
                "ended_at": started,
                "duration_min": 2.0,
                "error_message": None,
                "rows_read": 3,
                "rows_written": 2,
                "rows_updated": 1,
                "rows_deleted": 1,
            }
        ]
    ]
    storage = PostgresRunStateStorage(connector, schema="state", table="runs")
    state = RunState(
        dag_id="dag",
        source_schema="public",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        load_strategy="replace",
        execution_date=started,
        state=RunStateStatus.SUCCESS,
        started_at=started,
        ended_at=started,
        rows_read=3,
        rows_written=2,
        rows_updated=1,
        rows_deleted=1,
    )

    storage.save_run_state(state)
    loaded = storage.get_run_state("dag", started)

    assert loaded is not None
    assert loaded.state == RunStateStatus.SUCCESS
    assert loaded.rows_written == 2


def test_postgres_load_audit_storage_uses_canonical_dpone_loads_table() -> None:
    now = datetime.now(UTC)
    connector = FakePostgresConnector()
    storage = PostgresLoadAuditStorage(connector, schema="state", table="__dpone__loads")
    record = LoadAuditRecord(
        run_id="01HY6G9SKW8S7TE4R97X1E2J3K",
        load_id="01HY6G9SKW8S7TE4R97X1E2J3M",
        status="committed",
        process_name="orders",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        strategy="incremental_merge",
        started_at=now,
        committed_at=now,
        inserted_rows=10,
        updated_rows=2,
        loaded_rows=12,
    )

    storage.record_load_committed(record)

    rendered = "\n".join(str(query) for query, *_ in connector.queries)
    assert "__dpone__loads" in rendered
    assert "__dpone__loaded_at" in rendered
    assert "meta__" not in rendered
    assert connector.queries[-1][1][0] == record.run_id
    assert connector.queries[-1][1][2] == "committed"


def test_state_factory_creates_postgres_load_audit_storage_with_injected_connector() -> None:
    connector = FakePostgresConnector()

    storage = StateFactory.create_postgres_load_audit_storage(postgres_connector=connector, schema="state")

    assert isinstance(storage, PostgresLoadAuditStorage)
    assert storage.connector is connector
    assert storage.schema == "state"
    assert storage.table == "__dpone__loads"

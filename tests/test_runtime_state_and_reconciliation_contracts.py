from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest

from dpone._compat import UTC
from dpone.runtime.reconciliation.manager import ReconciliationManager
from dpone.runtime.reconciliation.soft_delete.registry import SoftDeleteRegistry
from dpone.runtime.reconciliation.tech_tables import BigQueryTechTables
from dpone.runtime.state.factory import StateFactory
from dpone.runtime.state.xmin_storage import XMinState, XMinStateStorage
from dpone.runtime.xmin.manager import XMinStateManager


class NotFound(Exception):
    """Local Google-compatible sentinel for the reconciliation boundary."""


NotFound.__module__ = "google.cloud.exceptions"


class FakeLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []
        self.warnings: list[str] = []

    def log_etl_progress(self, event: str, payload: dict[str, Any]) -> None:
        self.events.append((event, payload))

    def warning(self, message: str) -> None:
        self.warnings.append(message)


class FakePostgresConnector:
    def __init__(self, responses: list[list[dict[str, Any]]]) -> None:
        self.responses = responses
        self.queries: list[str] = []

    def get_records(self, query: str, *, as_dict: bool = False) -> list[dict[str, Any]]:
        self.queries.append(query)
        assert as_dict is True
        return self.responses.pop(0) if self.responses else []


class FakeBigQueryConnector:
    project_id = "demo-project"

    def __init__(self, *, connection: Any | None = None, records: list[dict[str, Any]] | None = None) -> None:
        self.connection = connection
        self.records = records or []
        self.executed: list[tuple[str, Any]] = []

    def execute_query(self, query: str, *, params: Any = None) -> None:
        self.executed.append((query, params))

    def get_records(self, query: str, *, params: Any = None, as_dict: bool = False) -> list[dict[str, Any]]:
        self.executed.append((query, params))
        assert as_dict is True
        return self.records


class FakeBigQueryClient:
    def __init__(self, existing_schema: list[str] | None = None) -> None:
        self.existing_schema = existing_schema
        self.created_tables: list[Any] = []
        self.deleted_tables: list[tuple[str, bool]] = []

    def get_table(self, table_id: str) -> Any:
        if self.existing_schema is None:
            raise NotFound(f"{table_id} does not exist")
        return SimpleNamespace(schema=[SimpleNamespace(name=name) for name in self.existing_schema])

    def create_table(self, table: Any) -> Any:
        self.created_tables.append(table)
        return table

    def delete_table(self, table_id: str, *, not_found_ok: bool = False) -> None:
        self.deleted_tables.append((table_id, not_found_ok))


def test_xmin_state_manager_reads_anchors_and_builds_safe_state() -> None:
    connector = FakePostgresConnector(
        [
            [{"xmin_raw_value": "42"}],
            [{"datfrozenxid": "7"}],
        ]
    )
    manager = XMinStateManager(connector)  # type: ignore[arg-type]

    assert manager.get_snapshot_xmin_anchor() == 42
    assert manager.get_frozen_xid() == 7
    assert "txid_snapshot_xmin" in connector.queries[0]
    assert "datfrozenxid" in connector.queries[1]

    initial_manager = XMinStateManager(FakePostgresConnector([[{"datfrozenxid": 10}]]))  # type: ignore[arg-type]
    initial_state = initial_manager.calculate_safe_xmin(100)

    assert initial_state.xmin_value == 100
    assert initial_state.is_initial is True
    assert initial_state.wraparound_detected is False
    assert initial_state.frozen_xid is None


def test_xmin_state_manager_detects_wraparound_and_unsafe_gaps() -> None:
    epoch_mod = 2**32
    previous = XMinState(xmin_value=400, timestamp=datetime.now(UTC))

    wrap_manager = XMinStateManager(FakePostgresConnector([[{"datfrozenxid": 10}]]))  # type: ignore[arg-type]
    wrap_state = wrap_manager.calculate_safe_xmin(epoch_mod + 5, previous)

    assert wrap_state.is_initial is False
    assert wrap_state.wraparound_detected is True
    assert wrap_state.frozen_xid == 10

    unsafe_manager = XMinStateManager(FakePostgresConnector([]))  # type: ignore[arg-type]
    unsafe_state = unsafe_manager.calculate_safe_xmin((epoch_mod * 2) + 5, previous)

    assert unsafe_state.is_initial is True
    assert unsafe_state.wraparound_detected is True


def test_xmin_state_manager_builds_incremental_queries() -> None:
    epoch_mod = 2**32
    manager = XMinStateManager(FakePostgresConnector([]))  # type: ignore[arg-type]

    initial_query = manager.build_incremental_query(
        "public",
        "orders",
        prev_state=None,
        anchor_full=25,
        columns=["id", "amount"],
        custom_predicate="status = 'active'",
    )

    assert 'SELECT "id", "amount", t.xmin AS __dpone__xmin' in initial_query
    assert 'FROM "public"."orders" AS t' in initial_query
    assert "WHERE (TRUE) AND (status = 'active')" in initial_query

    same_epoch_query = manager.build_incremental_query(
        "public",
        "orders",
        prev_state=XMinState(10, datetime.now(UTC)),
        anchor_full=20,
    )
    assert "t.xmin::text::bigint >= 10" in same_epoch_query
    assert "t.xmin::text::bigint < 20" in same_epoch_query

    wrap_query = manager.build_incremental_query(
        "public",
        "orders",
        prev_state=XMinState(epoch_mod + 10, datetime.now(UTC)),
        anchor_full=(epoch_mod * 2) + 20,
    )
    assert "(t.xmin::text::bigint >= 10) OR (t.xmin::text::bigint < 20)" in wrap_query

    unsafe_query = manager.build_incremental_query(
        "public",
        "orders",
        prev_state=XMinState(10, datetime.now(UTC)),
        anchor_full=(epoch_mod * 2) + 20,
    )
    assert "WHERE TRUE" in unsafe_query


def test_xmin_state_manager_extracts_max_xmin_from_rows() -> None:
    manager = XMinStateManager(FakePostgresConnector([]))  # type: ignore[arg-type]

    assert manager.get_max_xmin_from_data([]) is None
    assert manager.get_max_xmin_from_data([{"id": 1}]) is None
    assert manager.get_max_xmin_from_data([{"__dpone__xmin": "7"}, {"__dpone__xmin": 11}]) == 11
    assert manager.should_perform_full_refresh(XMinState(1, datetime.now(UTC), is_initial=True)) is True


def test_xmin_state_storage_saves_loads_and_deletes_with_fake_connector() -> None:
    now = datetime.now(UTC)
    connector = FakeBigQueryConnector(
        records=[
            {
                "xmin_value": "123",
                "__dpone__loaded_at": now,
                "is_initial": True,
                "wraparound_detected": False,
                "frozen_xid": None,
            }
        ]
    )
    storage = XMinStateStorage(connector, dataset="state", table="xmin")  # type: ignore[arg-type]
    storage._table_created = True

    storage.save_state("public", "orders", XMinState(123, now, is_initial=True))

    save_query, save_params = connector.executed[0]
    assert "MERGE" in save_query
    assert {param.name: param.value for param in save_params} == {
        "source_schema": "public",
        "source_table": "orders",
        "xmin_value": 123,
        "is_initial": True,
        "wraparound_detected": False,
        "frozen_xid": None,
    }

    loaded = storage.load_state("public", "orders")

    assert loaded == XMinState(123, now, is_initial=True, wraparound_detected=False, frozen_xid=None)

    storage.delete_state("public", "orders")

    _, delete_params = connector.executed[-1]
    assert delete_params == {"source_schema": "public", "source_table": "orders"}


def test_state_factory_uses_injected_bigquery_connector_without_credentials() -> None:
    connector = FakeBigQueryConnector()

    run_storage = StateFactory.create_run_state_storage(
        bigquery_connector=connector,
        schema="custom_state",
        state_table="runs",
    )
    xmin_storage = StateFactory.create_xmin_state_storage(
        bigquery_connector=connector,
        schema="custom_state",
        state_table="xmin",
    )

    assert run_storage.bigquery_connector is connector
    assert run_storage.dataset == "custom_state"
    assert run_storage.table == "runs"
    assert xmin_storage.bigquery_connector is connector
    assert xmin_storage.dataset == "custom_state"
    assert xmin_storage.table == "xmin"


def test_soft_delete_registry_dispatches_custom_handlers_and_reports_unsupported_types() -> None:
    class CustomConnector:
        pass

    class UnsupportedConnector:
        pass

    logger = FakeLogger()
    registry = SoftDeleteRegistry(logger)  # type: ignore[arg-type]
    captured: dict[str, Any] = {}

    def handler(**kwargs: Any) -> int:
        captured.update(kwargs)
        return 3

    registry._handlers["CustomConnector"] = handler

    result = registry.execute_soft_delete(
        CustomConnector(),
        target_schema="public",
        target_table="orders",
        unique_key_list=["id"],
        deleted_keys=[{"id": "1"}],
        tech_schema="tech",
    )

    assert result == 3
    assert captured["target_table"] == "orders"
    assert captured["meta_load_dtm"] == "__dpone__loaded_at"
    assert captured["meta_delete_dtm"] == "__dpone__deleted_at"
    assert logger.events[0][0] == "RECONCILIATION_SOFT_DELETE_TARGET"

    with pytest.raises(ValueError, match="UnsupportedConnector"):
        registry.execute_soft_delete(UnsupportedConnector(), "public", "orders", ["id"], [], "tech")


def test_bigquery_tech_tables_create_reuse_and_recreate_snapshot_tables() -> None:
    logger = FakeLogger()
    missing_client = FakeBigQueryClient()
    missing_tables = BigQueryTechTables(
        FakeBigQueryConnector(connection=missing_client),
        logger,  # type: ignore[arg-type]
        tech_schema="tech",
    )

    assert missing_tables.ensure_snapshot_table("orders", ["id", "tenant_id"]) is True
    created_table = missing_client.created_tables[0]
    assert created_table.table_id == "orders__rs"
    assert [field.name for field in created_table.schema] == ["id", "tenant_id", "__dpone__loaded_at"]

    existing_client = FakeBigQueryClient(existing_schema=["id", "__dpone__loaded_at"])
    existing_tables = BigQueryTechTables(
        FakeBigQueryConnector(connection=existing_client),
        logger,  # type: ignore[arg-type]
        tech_schema="tech",
    )

    assert existing_tables.ensure_snapshot_table("orders", "id") is False
    assert existing_client.created_tables == []

    changed_client = FakeBigQueryClient(existing_schema=["legacy_id", "__dpone__loaded_at"])
    changed_tables = BigQueryTechTables(
        FakeBigQueryConnector(connection=changed_client),
        logger,  # type: ignore[arg-type]
        tech_schema="tech",
    )

    assert changed_tables.ensure_snapshot_table("orders", "id") is True
    assert changed_client.deleted_tables == [("demo-project.tech.orders__rs", True)]
    assert changed_client.created_tables


def test_bigquery_tech_tables_create_deleted_log_tables_with_delete_partition() -> None:
    logger = FakeLogger()
    client = FakeBigQueryClient()
    tables = BigQueryTechTables(
        FakeBigQueryConnector(connection=client),
        logger,  # type: ignore[arg-type]
        tech_schema="tech",
    )

    assert tables.ensure_deleted_log_table("orders", "id") is True

    created_table = client.created_tables[0]
    assert created_table.table_id == "orders__deleted_log"
    assert [field.name for field in created_table.schema] == ["id", "__dpone__loaded_at", "__dpone__deleted_at"]
    assert created_table.time_partitioning.field == "__dpone__deleted_at"


class FakeTargetConnector:
    pass


class FakeSourceConnector:
    pass


class FakeTechTables:
    def __init__(self, snapshot_count: int) -> None:
        self.snapshot_count = snapshot_count
        self.calls: list[str] = []

    def ensure_tech_schema(self) -> None:
        self.calls.append("ensure_tech_schema")

    def ensure_snapshot_table(self, target_table: str, unique_key: str | list[str]) -> bool:
        self.calls.append(f"ensure_snapshot_table:{target_table}:{unique_key}")
        return True

    def ensure_deleted_log_table(self, target_table: str, unique_key: str | list[str]) -> bool:
        self.calls.append(f"ensure_deleted_log_table:{target_table}:{unique_key}")
        return False

    def insert_snapshot_from_source(self, **kwargs: Any) -> int:
        self.calls.append(f"insert_snapshot_from_source:{kwargs['rs_table']}")
        return 10

    def get_snapshot_count(self, rs_table: str) -> int:
        self.calls.append(f"get_snapshot_count:{rs_table}")
        return self.snapshot_count

    def prune_old_snapshots(self, rs_table: str) -> None:
        self.calls.append(f"prune_old_snapshots:{rs_table}")


def test_reconciliation_manager_ensures_infrastructure_with_fake_components() -> None:
    manager = ReconciliationManager(
        FakeBigQueryConnector(),
        FakeTargetConnector(),
        FakeLogger(),  # type: ignore[arg-type]
        "tech",
    )
    fake_tech_tables = FakeTechTables(snapshot_count=0)
    manager.tech_tables = fake_tech_tables  # type: ignore[assignment]

    result = manager.ensure_reconciliation_infrastructure(
        "public",
        "orders",
        [("id", "STRING")],
        unique_key=["id"],
    )

    assert result == {
        "tech_schema_created": True,
        "meta_columns_managed_by_strategies": True,
        "snapshot_table_created": True,
        "deleted_log_table_created": False,
    }
    assert fake_tech_tables.calls == [
        "ensure_tech_schema",
        "ensure_snapshot_table:orders:['id']",
        "ensure_deleted_log_table:orders:['id']",
    ]


def test_reconciliation_manager_processes_skipped_and_complete_paths() -> None:
    logger = FakeLogger()
    manager = ReconciliationManager(
        FakeBigQueryConnector(),
        FakeTargetConnector(),
        logger,  # type: ignore[arg-type]
        "tech",
    )

    skipped_tech_tables = FakeTechTables(snapshot_count=1)
    manager.tech_tables = skipped_tech_tables  # type: ignore[assignment]

    skipped = manager.process_reconciliation(
        "public",
        "orders",
        "id",
        source_connector=FakeSourceConnector(),
        source_schema="public",
        source_table="orders",
    )

    assert skipped == {
        "snapshot_count": 1,
        "soft_deleted_rows": 0,
        "deleted_log_entries": 0,
        "skipped": True,
    }
    assert "prune_old_snapshots:orders__rs" not in skipped_tech_tables.calls

    complete_tech_tables = FakeTechTables(snapshot_count=2)
    manager.tech_tables = complete_tech_tables  # type: ignore[assignment]
    manager._compare_snapshots_and_soft_delete = lambda *args: {  # type: ignore[method-assign]
        "soft_deleted_rows": 4,
        "deleted_log_entries": 4,
    }

    complete = manager.process_reconciliation(
        "public",
        "orders",
        ["id"],
        source_connector=FakeSourceConnector(),
        source_schema="public",
        source_table="orders",
        batch_commit_mode="separate",
    )

    assert complete == {
        "snapshot_count": 2,
        "soft_deleted_rows": 4,
        "deleted_log_entries": 4,
        "skipped": False,
    }
    assert "prune_old_snapshots:orders__rs" in complete_tech_tables.calls

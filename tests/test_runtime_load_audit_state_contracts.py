from __future__ import annotations

import sys
from datetime import datetime

from dpone._compat import UTC
from dpone.governance.hooks import LoadStepAuditRecord
from dpone.runtime.lineage.audit import LoadAuditRecord
from dpone.runtime.state.clickhouse import ClickHouseLoadAuditStorage, ClickHouseLoadStepAuditStorage
from dpone.runtime.state.factory import StateFactory
from dpone.runtime.state.load_audit import BigQueryLoadAuditStorage


class FakeBigQueryClient:
    def __init__(self) -> None:
        self.datasets = []
        self.tables = []

    def create_dataset(self, dataset, exists_ok=False):
        self.datasets.append((dataset, exists_ok))

    def create_table(self, table, exists_ok=False):
        self.tables.append((table, exists_ok))


class FakeBigQueryConnector:
    def __init__(self) -> None:
        self.project_id = "demo"
        self.connection = FakeBigQueryClient()
        self.queries = []

    def execute_query(self, query, job_config=None):
        self.queries.append((query, job_config))
        return 1


def _record() -> LoadAuditRecord:
    now = datetime.now(UTC)
    return LoadAuditRecord(
        run_id="01HY6G9SKW8S7TE4R97X1E2J3K",
        load_id="01HY6G9SKW8S7TE4R97X1E2J3M",
        status="committed",
        process_name="orders",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        strategy="snapshot_diff",
        started_at=now,
        staged_at=now,
        committed_at=now,
        extracted_rows=10,
        staged_rows=10,
        inserted_rows=1,
        updated_rows=2,
        loaded_rows=10,
    )


def test_bigquery_load_audit_storage_uses_canonical_dpone_loads_table() -> None:
    connector = FakeBigQueryConnector()
    storage = BigQueryLoadAuditStorage(connector, dataset="state", table="__dpone__loads")

    try:
        storage.record_load_committed(_record())

        assert connector.connection.datasets
        table = connector.connection.tables[0][0]
        assert table.table_id == "__dpone__loads"
        assert "__dpone__loaded_at" in {field.name for field in table.schema}
        query, job_config = connector.queries[0]
        assert "`demo.state.__dpone__loads`" in query
        assert "__dpone__loaded_at" in query
        assert "meta__" not in query
        assert {param.name for param in job_config.query_parameters} >= {"run_id", "load_id", "loaded_rows"}
    finally:
        _clear_google_modules()


def test_state_factory_creates_bigquery_load_audit_storage_with_injected_connector() -> None:
    connector = FakeBigQueryConnector()

    storage = StateFactory.create_bigquery_load_audit_storage(bigquery_connector=connector, schema="state")

    assert isinstance(storage, BigQueryLoadAuditStorage)
    assert storage.connector is connector
    assert storage.dataset == "state"
    assert storage.table == "__dpone__loads"


class FakeClickHouseConnector:
    def __init__(self) -> None:
        self.queries: list[tuple[str, object | None]] = []

    def execute_query(self, query, params=None):  # noqa: ANN001
        self.queries.append((str(query), params))
        return 0


def test_clickhouse_load_audit_storage_uses_replacing_merge_tree_state_table() -> None:
    connector = FakeClickHouseConnector()
    storage = ClickHouseLoadAuditStorage(connector, schema="state", table="__dpone__loads")

    storage.record_load_committed(_record())

    rendered = "\n".join(query for query, _ in connector.queries)
    assert "CREATE DATABASE IF NOT EXISTS `state`" in rendered
    assert "CREATE TABLE IF NOT EXISTS `state`.`__dpone__loads`" in rendered
    assert "ReplacingMergeTree(__dpone__loaded_at)" in rendered
    assert "__dpone__loaded_at" in rendered
    assert "meta__" not in rendered
    assert connector.queries[-1][1][0][0] == "01HY6G9SKW8S7TE4R97X1E2J3K"
    assert connector.queries[-1][1][0][2] == "committed"


def test_clickhouse_load_step_audit_storage_records_governance_steps() -> None:
    connector = FakeClickHouseConnector()
    storage = ClickHouseLoadStepAuditStorage(connector, schema="state", table="__dpone__load_steps")
    now = datetime.now(UTC)

    storage.record_step(
        LoadStepAuditRecord(
            run_id="run_1",
            load_id="load_1",
            step_id="lineage_projected",
            phase="load_governance",
            kind="target_preparation",
            status="succeeded",
            started_at=now,
            finished_at=now,
            details={"staged_rows": 10, "memory_bytes": 2048},
        )
    )

    rendered = "\n".join(query for query, _ in connector.queries)
    assert "CREATE TABLE IF NOT EXISTS `state`.`__dpone__load_steps`" in rendered
    assert "details_json" in rendered
    assert "INSERT INTO `state`.`__dpone__load_steps`" in rendered
    assert connector.queries[-1][1][0][2] == "lineage_projected"
    assert connector.queries[-1][1][0][5] == "succeeded"
    assert connector.queries[-1][1][0][9] == '{"memory_bytes": 2048, "staged_rows": 10}'


def _clear_google_modules() -> None:
    for name in list(sys.modules):
        if name.startswith("google"):
            del sys.modules[name]

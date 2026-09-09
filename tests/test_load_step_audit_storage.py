from __future__ import annotations

from datetime import UTC, datetime

from dpone.runtime.route_runtime import LoadStepAuditRecord
from dpone.runtime.state.load_step_audit import (
    ClickHouseLoadStepAuditStorage,
    MSSQLLoadStepAuditStorage,
    PostgresLoadStepAuditStorage,
)


def test_postgres_load_step_audit_storage_creates_and_inserts_json_details() -> None:
    connector = _Connector()
    storage = PostgresLoadStepAuditStorage(connector, schema="etl_state")

    storage.record_load_step(_record())

    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert "CREATE TABLE IF NOT EXISTS" in rendered
    assert "__dpone__load_steps" in rendered
    assert connector.calls[-1][1][-1] == '{"selected_route_id":"object_storage_pull_s3"}'


def test_mssql_load_step_audit_storage_uses_parameterized_details_json() -> None:
    connector = _Connector()
    storage = MSSQLLoadStepAuditStorage(connector, schema="etl_state")

    storage.record_load_step(_record())

    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert "IF OBJECT_ID" in rendered
    assert "[etl_state].[__dpone__load_steps]" in rendered
    assert "ADD error_message nvarchar(max) NULL" in rendered
    assert "ALTER COLUMN finished_at datetime2 NULL" in rendered
    assert connector.calls[-1][1][-1] == '{"selected_route_id":"object_storage_pull_s3"}'


def test_clickhouse_load_step_audit_storage_uses_merge_tree_and_json_string() -> None:
    connector = _Connector()
    storage = ClickHouseLoadStepAuditStorage(connector, schema="etl_state")

    storage.record_load_step(_record())

    rendered = "\n".join(str(sql) for sql, _ in connector.calls)
    assert "ENGINE = MergeTree" in rendered
    assert "`etl_state`.`__dpone__load_steps`" in rendered
    assert connector.calls[-1][1] is not None
    assert isinstance(connector.calls[-1][1], list)
    assert connector.calls[-1][1][0][-1] == '{"selected_route_id":"object_storage_pull_s3"}'


def test_load_step_audit_storage_enriches_route_record_with_throughput() -> None:
    connector = _Connector()
    storage = ClickHouseLoadStepAuditStorage(connector, schema="etl_state")
    now = datetime(2026, 1, 1, tzinfo=UTC)
    record = LoadStepAuditRecord(
        run_id="run-1",
        load_id="load-1",
        step_id="staged",
        phase="load",
        kind="target_preparation",
        status="succeeded",
        started_at=now,
        finished_at=now.replace(second=4),
        details_json={"staged_rows": 20},
    )

    storage.record_load_step(record)

    assert '"throughput":{"confidence":"measured","duration_seconds":4.0' in connector.calls[-1][1][0][-1]
    assert '"rows_per_second":5.0' in connector.calls[-1][1][0][-1]


def _record() -> LoadStepAuditRecord:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return LoadStepAuditRecord(
        run_id="run-1",
        load_id="load-1",
        step_id="route_capability_decision",
        phase="pre_extract",
        kind="runtime_route_decision",
        status="warning",
        started_at=now,
        finished_at=now,
        details_json={"selected_route_id": "object_storage_pull_s3"},
    )


class _Connector:
    def __init__(self) -> None:
        self.calls: list[tuple[object, object | None]] = []

    def execute_query(self, sql, params=None):
        self.calls.append((sql, params))

    def quote_identifier(self, value: str) -> str:
        return f"[{value}]"

    def qualified_name(self, schema: str, table: str) -> str:
        return f"[{schema}].[{table}]"

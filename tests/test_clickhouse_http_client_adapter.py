from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from dpone.runtime.connectors.clickhouse_http_client import ClickHouseHttpClientAdapter


@dataclass(frozen=True)
class _ColumnType:
    name: str


@dataclass(frozen=True)
class _QueryResult:
    result_rows: list[tuple[object, ...]]
    column_names: list[str]
    column_types: list[_ColumnType]


class _HttpClient:
    def __init__(self) -> None:
        self.queries: list[tuple[str, object, object]] = []
        self.commands: list[tuple[str, object, object]] = []
        self.inserts: list[tuple[str, object, object, object]] = []
        self.closed = False

    def query(self, query: str, *, parameters=None, settings=None):
        self.queries.append((query, parameters, settings))
        return _QueryResult([(1, "ok")], ["id", "status"], [_ColumnType("UInt8"), _ColumnType("String")])

    def command(self, cmd: str, *, parameters=None, settings=None):
        self.commands.append((cmd, parameters, settings))
        return "OK"

    def query_rows_stream(self, query: str, *, parameters=None, settings=None):
        self.queries.append((query, parameters, settings))
        return iter([(1,), (2,)])

    def insert(self, table: str, data, *, column_names=None, settings=None):
        self.inserts.append((table, data, column_names, settings))

    def close(self) -> None:
        self.closed = True


def test_http_adapter_executes_select_with_clickhouse_driver_shape() -> None:
    client = _HttpClient()
    adapter = ClickHouseHttpClientAdapter(client)

    rows, columns = adapter.execute("SELECT id, status FROM events", with_column_types=True)

    assert rows == [(1, "ok")]
    assert columns == [("id", "UInt8"), ("status", "String")]
    assert client.queries == [("SELECT id, status FROM events", None, None)]


def test_http_adapter_preserves_exists_table_result() -> None:
    """EXISTS is a query: its row controls atomic full-refresh replacement."""

    client = _HttpClient()
    adapter = ClickHouseHttpClientAdapter(client)

    rows = adapter.execute("EXISTS TABLE marketing.cash_orders_marts")

    assert rows == [(1, "ok")]
    assert client.queries == [("EXISTS TABLE marketing.cash_orders_marts", None, None)]
    assert client.commands == []


def test_http_adapter_executes_commands_and_streams_rows() -> None:
    client = _HttpClient()
    adapter = ClickHouseHttpClientAdapter(client)

    assert adapter.execute("CREATE TEMPORARY TABLE t (id UInt8)") == []
    assert list(adapter.execute_iter("SELECT id FROM events")) == [(1,), (2,)]
    adapter.disconnect()

    assert client.commands == [("CREATE TEMPORARY TABLE t (id UInt8)", None, None)]
    assert client.closed is True


def test_http_adapter_forwards_query_id_as_transport_parameter() -> None:
    client = _HttpClient()
    adapter = ClickHouseHttpClientAdapter(client, settings={"max_threads": 2})

    adapter.execute("EXCHANGE TABLES a AND b", query_id="stable-publication-id")

    assert client.commands == [
        ("EXCHANGE TABLES a AND b", None, {"max_threads": 2, "query_id": "stable-publication-id"})
    ]


def test_http_adapter_inserts_typed_rows_without_treating_them_as_query_parameters() -> None:
    client = _HttpClient()
    adapter = ClickHouseHttpClientAdapter(client, settings={"max_threads": 2})

    adapter.insert_rows(
        "analytics.candidate",
        [(10,), (20,)],
        column_names=["id"],
        settings={"async_insert": 0},
        query_id="external-member-load",
    )

    assert client.inserts == [
        (
            "analytics.candidate",
            [(10,), (20,)],
            ["id"],
            {"max_threads": 2, "async_insert": 0, "query_id": "external-member-load"},
        )
    ]
    assert client.commands == []


def test_native_values_protocol_uses_typed_http_insert() -> None:
    client = _HttpClient()
    adapter = ClickHouseHttpClientAdapter(client, settings={"max_threads": 2})
    rows = [("load-1", datetime(2026, 1, 1, tzinfo=UTC), None, "100%")]
    result = adapter.execute(
        "INSERT INTO `audit`.`loads` (id, started_at, error, description) VALUES",
        rows,
        settings={"async_insert": 0},
        query_id="audit-write",
    )
    assert result == []
    assert client.commands == []
    assert client.inserts == [
        (
            "`audit`.`loads`",
            rows,
            ["id", "started_at", "error", "description"],
            {"max_threads": 2, "async_insert": 0, "query_id": "audit-write"},
        )
    ]


@pytest.mark.parametrize("rows", [[], iter(())])
def test_empty_native_insert_is_noop(rows) -> None:
    client = _HttpClient()
    assert ClickHouseHttpClientAdapter(client).execute("INSERT INTO events (id) VALUES", rows) == []
    assert client.commands == client.inserts == []


def test_parameterized_insert_select_remains_a_command() -> None:
    client = _HttpClient()
    sql = "INSERT INTO events SELECT %(id)s"
    ClickHouseHttpClientAdapter(client).execute(sql, {"id": 3})
    assert client.commands == [(sql, {"id": 3}, None)]
    assert client.inserts == []


def test_inline_values_binding_remains_a_command() -> None:
    client = _HttpClient()
    sql = "INSERT INTO events (id) VALUES (%s)"
    ClickHouseHttpClientAdapter(client, settings={"max_threads": 2}).execute(sql, (3,), settings={"max_threads": 1})
    assert client.commands == [(sql, (3,), {"max_threads": 1})]
    assert client.inserts == []


def test_multiline_values_with_quoted_columns_and_generator() -> None:
    client = _HttpClient()
    ClickHouseHttpClientAdapter(client).execute(
        ' INSERT INTO "audit"."loads" (\n "a,b", `c``d`, plain\n) VALUES; ',
        (row for row in [(1, None, "x")]),
    )
    assert client.inserts == [('"audit"."loads"', [(1, None, "x")], ["a,b", "c`d", "plain"], None)]


def test_load_and_step_audits_use_http_typed_rows() -> None:
    from dpone.governance.hooks import LoadStepAuditRecord
    from dpone.runtime.lineage.audit import LoadAuditRecord
    from dpone.runtime.state.clickhouse import ClickHouseLoadAuditStorage, ClickHouseLoadStepAuditStorage

    client = _HttpClient()
    adapter = ClickHouseHttpClientAdapter(client)

    class Connector:
        execute_query = staticmethod(adapter.execute)

    now = datetime(2026, 1, 1, tzinfo=UTC)
    ClickHouseLoadAuditStorage(Connector(), schema="audit").record_load_started(
        LoadAuditRecord("run-1", "load-1", "started", None, "source", "events", "target", "events", "replace", now)
    )
    ClickHouseLoadStepAuditStorage(Connector(), schema="audit").record_step(
        LoadStepAuditRecord("run-1", "load-1", "step-1", "stage", "transfer", "passed", now)
    )
    assert len(client.inserts) == 2
    assert client.inserts[0][1][0][9] == now
    assert client.inserts[0][1][0][10] is None
    assert client.inserts[1][1][0][6] == now
    assert all(parameters is None for _, parameters, _ in client.commands)

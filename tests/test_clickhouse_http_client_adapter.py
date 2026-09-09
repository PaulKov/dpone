from __future__ import annotations

from dataclasses import dataclass

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
        self.queries: list[tuple[str, object]] = []
        self.commands: list[tuple[str, object]] = []
        self.closed = False

    def query(self, query: str, *, parameters=None, settings=None):
        self.queries.append((query, parameters))
        return _QueryResult([(1, "ok")], ["id", "status"], [_ColumnType("UInt8"), _ColumnType("String")])

    def command(self, cmd: str, *, parameters=None, settings=None):
        self.commands.append((cmd, parameters))
        return "OK"

    def query_rows_stream(self, query: str, *, parameters=None, settings=None):
        self.queries.append((query, parameters))
        return iter([(1,), (2,)])

    def close(self) -> None:
        self.closed = True


def test_http_adapter_executes_select_with_clickhouse_driver_shape() -> None:
    client = _HttpClient()
    adapter = ClickHouseHttpClientAdapter(client)

    rows, columns = adapter.execute("SELECT id, status FROM events", with_column_types=True)

    assert rows == [(1, "ok")]
    assert columns == [("id", "UInt8"), ("status", "String")]
    assert client.queries == [("SELECT id, status FROM events", None)]


def test_http_adapter_preserves_exists_table_result() -> None:
    """EXISTS is a query: its row controls atomic full-refresh replacement."""

    client = _HttpClient()
    adapter = ClickHouseHttpClientAdapter(client)

    rows = adapter.execute("EXISTS TABLE marketing.cash_orders_marts")

    assert rows == [(1, "ok")]
    assert client.queries == [("EXISTS TABLE marketing.cash_orders_marts", None)]
    assert client.commands == []


def test_http_adapter_executes_commands_and_streams_rows() -> None:
    client = _HttpClient()
    adapter = ClickHouseHttpClientAdapter(client)

    assert adapter.execute("CREATE TEMPORARY TABLE t (id UInt8)") == []
    assert list(adapter.execute_iter("SELECT id FROM events")) == [(1,), (2,)]
    adapter.disconnect()

    assert client.commands == [("CREATE TEMPORARY TABLE t (id UInt8)", None)]
    assert client.closed is True

from __future__ import annotations

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.connectors.clickhouse_http_bulk import (
    ClickHouseHttpBulkRunner,
    ClickHouseHttpCredentials,
    ClickHouseHttpOptions,
)
from dpone.runtime.connectors.postgres import PostgresConnector
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink


def test_clickhouse_http_runner_streams_tabseparated_bytes_without_file() -> None:
    seen: dict[str, object] = {}

    class FakeResponse:
        status = 200

        def read(self):
            return b""

    class FakeConnection:
        def __init__(self, host: str, port: int, timeout: int):
            seen["host"] = host
            seen["port"] = port
            seen["timeout"] = timeout
            self.body = bytearray()

        def putrequest(self, method: str, url: str) -> None:
            seen["method"] = method
            seen["url"] = url

        def putheader(self, name: str, value: str) -> None:
            seen.setdefault("headers", {})[name] = value

        def endheaders(self) -> None:
            pass

        def send(self, chunk: bytes) -> None:
            self.body.extend(chunk)
            seen["body"] = bytes(self.body)

        def getresponse(self):
            return FakeResponse()

        def close(self) -> None:
            seen["closed"] = True

    runner = ClickHouseHttpBulkRunner(
        ClickHouseHttpCredentials(
            host="127.0.0.1",
            port=18123,
            database="dpone",
            user="dpone",
            password="secret",
        ),
        ClickHouseHttpOptions(chunk_size=4),
        connection_factory=FakeConnection,
    )

    result = runner.insert_stream(
        "`dpone`.`orders`",
        ["id", "name"],
        iter((b"1\talpha\n", b"2\tbeta\n")),
    )

    assert seen["host"] == "127.0.0.1"
    assert seen["port"] == 18123
    assert seen["method"] == "POST"
    assert seen["headers"] == {"Transfer-Encoding": "chunked"}
    assert _decode_chunked(bytes(seen["body"])) == b"1\talpha\n2\tbeta\n"
    assert "FORMAT+TabSeparated" in seen["url"]
    assert "password=secret" in result.url
    assert "password=%2A%2A%2A" in result.redacted_url


def test_postgres_copy_to_stream_returns_zero_file_byte_stream() -> None:
    class FakeCopy:
        def __init__(self, query):
            self.query = query
            self.chunks = [b"1\talpha\n", memoryview(b"2\tbeta\n")]

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            if not self.chunks:
                return b""
            return self.chunks.pop(0)

    class FakeCursor:
        def __init__(self):
            self.copy_query = None

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def copy(self, query):
            self.copy_query = query
            return FakeCopy(query)

    class FakeConnection:
        def __init__(self):
            self.cursor_obj = FakeCursor()

        def cursor(self):
            return self.cursor_obj

    connector = PostgresConnector.__new__(PostgresConnector)
    connector._connection = FakeConnection()

    artifact = connector.copy_to_stream(
        "SELECT 1 AS id, 'alpha' AS name",
        columns=["id", "name"],
        format="MSSQL_DELIMITED",
    )

    assert b"".join(artifact.iter_bytes()) == b"1\talpha\n2\tbeta\n"
    assert artifact.format == "mssql-delimited"
    assert artifact.columns == ("id", "name")
    assert artifact.stats.size_bytes == 15
    copy_query = str(connector.connection.cursor_obj.copy_query)
    assert "COPY (SELECT 1 AS id, 'alpha' AS name) TO STDOUT" in copy_query
    assert "DELIMITER E'\\t'" in copy_query


def test_clickhouse_sink_uses_http_bulk_for_mssql_delimited_byte_stream() -> None:
    class FakeConnector:
        host = "127.0.0.1"
        port = 19000
        database = "dpone"
        user = "dpone"
        password = "dpone_pass"
        application_name = "test"
        secure = False
        compression = True
        connect_timeout = 10
        send_receive_timeout = 3600
        settings = {}

        def execute_query(self, query: str) -> int:
            del query
            return 0

        def get_records(self, query: str):
            del query
            return [(2,)]

    class FakeHttpRunner:
        calls: list[tuple[ClickHouseHttpCredentials, ClickHouseHttpOptions, str, list[str], bytes]] = []

        def __init__(self, credentials, options):
            self.credentials = credentials
            self.options = options

        def insert_stream(self, table: str, columns: list[str], chunks):
            self.calls.append((self.credentials, self.options, table, columns, b"".join(chunks)))
            return None

    artifact = ByteStreamArtifact(
        lambda: iter((b"1\talpha\n", b"2\tbeta\n")),
        columns=("id", "name"),
        format="mssql-delimited",
        estimated_rows=2,
    )
    config = LoadConfig(
        source_conn_id="postgres",
        target_conn_id="clickhouse",
        source_schema="public",
        source_table="orders",
        target_schema="dpone",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "clickhouse_bulk": {
                "mode": "http",
                "http": {"host": "127.0.0.1", "port": 18123},
            },
        },
    )

    result = ClickHouseSink(FakeConnector(), http_runner_cls=FakeHttpRunner).load(
        config,
        LoadPayload(artifact=artifact, schema=[("id", "int"), ("name", "String")]),
    )

    assert result.inserted_rows == 2
    assert artifact.stats.size_bytes == 15
    assert FakeHttpRunner.calls[0][0].port == 18123
    assert FakeHttpRunner.calls[0][2].startswith("`dpone`.`orders__dpone_staging_")
    assert FakeHttpRunner.calls[0][3] == ["id", "name"]
    assert FakeHttpRunner.calls[0][4] == b"1\talpha\n2\tbeta\n"


def _decode_chunked(payload: bytes) -> bytes:
    output = bytearray()
    rest = payload
    while rest:
        size_text, rest = rest.split(b"\r\n", 1)
        size = int(size_text, 16)
        if size == 0:
            break
        output.extend(rest[:size])
        rest = rest[size + 2 :]
    return bytes(output)

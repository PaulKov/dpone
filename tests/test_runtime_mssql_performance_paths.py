from __future__ import annotations

import ast
import hashlib
from datetime import date
from pathlib import Path

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.connectors.clickhouse_bulk import (
    ClickHouseClientCredentials,
    ClickHouseClientOptions,
    ClickHouseClientRunner,
)
from dpone.runtime.connectors.clickhouse_http_bulk import (
    ClickHouseHttpBulkRunner,
    ClickHouseHttpCredentials,
    ClickHouseHttpOptions,
)
from dpone.runtime.connectors.mssql_bulk import BcpCredentials, BcpOptions, BcpRunner
from dpone.runtime.connectors.postgres import PostgresConnector
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink


def test_runtime_data_plane_has_no_sqlite_dependency() -> None:
    offenders: list[str] = []
    for path in Path("src/dpone/runtime").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            names = (
                tuple(alias.name for alias in node.names)
                if isinstance(node, ast.Import)
                else ((node.module or ""),)
                if isinstance(node, ast.ImportFrom)
                else ()
            )
            if any(name == "sqlite3" or name.startswith("sqlite3.") for name in names):
                offenders.append(str(path))
    assert offenders == []


class _Completed:
    stdout = "100 rows copied."
    stderr = ""
    returncode = 0


class _BytesCompleted:
    stdout = b""
    stderr = b""
    returncode = 0


def test_bcp_runner_adds_trust_server_certificate_flag() -> None:
    seen: dict[str, list[str]] = {}

    def fake_run(command, **kwargs):
        seen["command"] = command
        seen["input"] = kwargs.get("input")
        return _Completed()

    runner = BcpRunner(
        BcpCredentials(
            host="localhost",
            port=15433,
            database="dpone",
            user="sa",
            password="secret",
        ),
        BcpOptions(trust_server_certificate=True),
        run=fake_run,
    )

    runner.import_file("dbo.orders", "/tmp/orders.bcp")

    assert seen["command"][:4] == ["bcp", "dbo.orders", "in", "/tmp/orders.bcp"]
    assert "-u" in seen["command"]
    assert "-k" in seen["command"]
    assert "-P" not in seen["command"]
    assert seen["input"] == "secret\n"
    assert seen["command"][seen["command"].index("-t") + 1] == "\\t"
    assert seen["command"][seen["command"].index("-r") + 1] == "\\n"
    assert seen["command"][seen["command"].index("-h") + 1] == "TABLOCK"


def test_clickhouse_client_runner_builds_redacted_tabseparated_insert(tmp_path: Path) -> None:
    source_file = tmp_path / "orders.tsv"
    source_file.write_text("1\talpha\n", encoding="utf-8")
    seen: dict[str, object] = {}

    def fake_run(command, **kwargs):
        seen["command"] = command
        seen["stdin_bytes"] = kwargs["stdin"].read()
        return _BytesCompleted()

    runner = ClickHouseClientRunner(
        ClickHouseClientCredentials(
            host="127.0.0.1",
            port=9000,
            database="dpone",
            user="dpone",
            password="secret",
        ),
        ClickHouseClientOptions(client_command="docker exec -i dpone-it-clickhouse clickhouse-client"),
        run=fake_run,
    )

    result = runner.insert_file("`dpone`.`orders`", ["id", "name"], str(source_file))

    command = seen["command"]
    assert command[:5] == ["docker", "exec", "-i", "dpone-it-clickhouse", "clickhouse-client"]
    assert command[command.index("--query") + 1] == "INSERT INTO `dpone`.`orders` (`id`, `name`) FORMAT TabSeparated"
    assert seen["stdin_bytes"] == b"1\talpha\n"
    assert result.redacted_command[result.command.index("--password") + 1] == "***"


def test_clickhouse_http_runner_streams_tabseparated_file(tmp_path: Path) -> None:
    source_file = tmp_path / "orders.tsv"
    source_file.write_text("1\talpha\n", encoding="utf-8")
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

    result = runner.insert_file("`dpone`.`orders`", ["id", "name"], str(source_file))

    assert seen["host"] == "127.0.0.1"
    assert seen["port"] == 18123
    assert seen["method"] == "POST"
    assert seen["headers"] == {"Content-Length": "8"}
    assert seen["body"] == b"1\talpha\n"
    assert "FORMAT+TabSeparated" in seen["url"]
    assert "password=secret" in result.url
    assert "password=%2A%2A%2A" in result.redacted_url


def test_postgres_copy_to_file_supports_bcp_friendly_mssql_delimited_export(tmp_path: Path) -> None:
    class FakeCopy:
        def __init__(self, query):
            self.query = query
            self._read = False

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            if self._read:
                return b""
            self._read = True
            return b"1\talpha\n2\tbeta\n"

    class FakeCursor:
        def __init__(self):
            self.copy_query = None

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def copy(self, query, params=()):
            del params
            self.copy_query = query
            return FakeCopy(query)

    class FakeConnection:
        def __init__(self):
            self.cursor_obj = FakeCursor()

        def cursor(self):
            return self.cursor_obj

    connector = PostgresConnector(host="localhost", port=5432, database="test", user="test", password="")
    connector._connection = FakeConnection()

    output_path = tmp_path / "orders.bcp"
    stats = connector.copy_to_file(
        "SELECT 1 AS id, 'alpha' AS name",
        str(output_path),
        format="MSSQL_DELIMITED",
        compress=False,
    )

    copy_query = str(connector.connection.cursor_obj.copy_query)
    assert "DELIMITER E'\\t'" in copy_query
    assert "QUOTE E'\\x1f'" in copy_query
    assert output_path.read_text(encoding="utf-8") == "1\talpha\n2\tbeta\n"
    assert stats["rows_exported"] == 2
    assert stats["copy_read_count"] == 1
    assert stats["chunk_count"] == 1
    assert stats["sha256"] == hashlib.sha256(b"1\talpha\n2\tbeta\n").hexdigest()


def test_clickhouse_file_coercion_uses_schema_types() -> None:
    row = ClickHouseSink._coerce_file_row(
        [
            "42",
            "9000000000",
            "12.25",
            "2026-01-02",
            "2026-01-02 03:04:05.1234567",
            "12:30:00",
            "11111111-1111-1111-1111-111111111111",
            "1",
            "hello",
        ],
        [
            ("id", "int"),
            ("big_id", "bigint"),
            ("amount", "numeric(12,2)"),
            ("day", "date"),
            ("updated_at", "datetime2"),
            ("business_time", "time(0) nullable"),
            ("trace_id", "uniqueidentifier nullable"),
            ("is_active", "bit nullable"),
            ("name", "nvarchar(max)"),
        ],
    )

    assert row[0] == 42
    assert row[1] == 9_000_000_000
    assert row[2] == 12.25
    assert row[3].isoformat() == "2026-01-02"
    assert row[4].isoformat() == "2026-01-02T03:04:05.123456"
    assert row[5] == "12:30:00"
    assert row[6] == "11111111-1111-1111-1111-111111111111"
    assert row[7] == 1
    assert row[8] == "hello"


def test_clickhouse_file_coercion_handles_mssql_nullable_scalars() -> None:
    row = ClickHouseSink._coerce_file_row(
        ["7", "70", "2026-01-15", "1"],
        [
            ("c_tinyint", "tinyint nullable"),
            ("c_smallint", "smallint nullable"),
            ("c_date", "date nullable"),
            ("c_bool", "bit nullable"),
        ],
    )
    assert row == (7, 70, date(2026, 1, 15), 1)


def test_clickhouse_sink_loads_partitioned_file_artifact(tmp_path: Path) -> None:
    class FakeConnection:
        def __init__(self) -> None:
            self.rows: list[tuple[object, ...]] = []

        def execute(self, query: str, rows):
            del query
            self.rows.extend(rows)

    class FakeConnector:
        host = "localhost"
        port = 9000
        database = "dpone"
        user = "default"
        password = ""
        application_name = "test"
        secure = False
        compression = True
        connect_timeout = 10
        send_receive_timeout = 3600
        settings = {}

        def __init__(self) -> None:
            self.connection = FakeConnection()
            self.queries: list[str] = []

        def execute_query(self, query: str) -> int:
            self.queries.append(query)
            return 0

        def get_records(self, query: str):
            self.queries.append(query)
            return [(len(self.connection.rows),)]

    files = [tmp_path / "part1.bcp", tmp_path / "part2.bcp"]
    files[0].write_text("1\talpha\n2\tbeta\n", encoding="utf-8")
    files[1].write_text("3\tgamma\n", encoding="utf-8")
    artifact = PartitionedFileExportArtifact(
        [FileExportArtifact(str(file_path), ["id", "name"], format="mssql-delimited") for file_path in files],
        columns=["id", "name"],
        max_workers=1,
    )
    config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="dpone",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        batch_size=2,
    )
    connector = FakeConnector()

    result = ClickHouseSink(connector).load(
        config, LoadPayload(artifact=artifact, schema=[("id", "int"), ("name", "String")])
    )

    assert result.inserted_rows == 3
    assert connector.connection.rows == [(1, "alpha"), (2, "beta"), (3, "gamma")]
    assert all(file_path.exists() for file_path in files)
    artifact.terminate(ArtifactTerminalOutcome.SUCCESS)
    assert all(not file_path.exists() for file_path in files)


def test_clickhouse_sink_uses_client_bulk_for_mssql_delimited_file(tmp_path: Path) -> None:
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

        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute_query(self, query: str) -> int:
            self.queries.append(query)
            return 0

        def get_records(self, query: str):
            self.queries.append(query)
            return [(2,)]

    class FakeRunner:
        calls: list[tuple[ClickHouseClientCredentials, ClickHouseClientOptions, str, list[str], str]] = []

        def __init__(self, credentials, options):
            self.credentials = credentials
            self.options = options

        def insert_file(self, table: str, columns: list[str], input_path: str):
            self.calls.append((self.credentials, self.options, table, columns, input_path))
            return None

    file_path = tmp_path / "orders.bcp"
    file_path.write_text("1\talpha\n2\tbeta\n", encoding="utf-8")
    artifact = FileExportArtifact(str(file_path), ["id", "name"], format="mssql-delimited", estimated_rows=2)
    config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="dpone",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "clickhouse_bulk": {
                "mode": "client",
                "client": {
                    "command": "docker exec -i dpone-it-clickhouse clickhouse-client",
                    "host": "127.0.0.1",
                    "port": 9000,
                },
            },
        },
    )

    result = ClickHouseSink(FakeConnector(), client_runner_cls=FakeRunner).load(
        config,
        LoadPayload(artifact=artifact, schema=[("id", "int"), ("name", "String")]),
    )

    assert result.inserted_rows == 2
    assert FakeRunner.calls[0][0].port == 9000
    assert FakeRunner.calls[0][1].client_command.startswith("docker exec")
    assert FakeRunner.calls[0][2].startswith("`dpone`.`orders__dpone_staging_")
    assert FakeRunner.calls[0][3] == ["id", "name"]


def test_clickhouse_sink_uses_http_bulk_for_mssql_delimited_file(tmp_path: Path) -> None:
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
        calls: list[tuple[ClickHouseHttpCredentials, ClickHouseHttpOptions, str, list[str], str]] = []

        def __init__(self, credentials, options):
            self.credentials = credentials
            self.options = options

        def insert_file(self, table: str, columns: list[str], input_path: str):
            self.calls.append((self.credentials, self.options, table, columns, input_path))
            return None

    file_path = tmp_path / "orders.bcp"
    file_path.write_text("1\talpha\n2\tbeta\n", encoding="utf-8")
    artifact = FileExportArtifact(str(file_path), ["id", "name"], format="mssql-delimited", estimated_rows=2)
    config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
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
    assert FakeHttpRunner.calls[0][0].port == 18123
    assert FakeHttpRunner.calls[0][2].startswith("`dpone`.`orders__dpone_staging_")
    assert FakeHttpRunner.calls[0][3] == ["id", "name"]

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from dpone.config import LoadConfig
from dpone.runtime.bulk_wire import BulkWirePlanner
from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
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
from dpone.runtime.partitioning import RangePartitioner
from dpone.runtime.sinks.clickhouse_bulk_mixin import ClickHouseBulkMixin


def test_nested_partitioning_auto_bounds_builds_numeric_ranges() -> None:
    partitioner = RangePartitioner.from_options(
        {
            "partitioning": {
                "strategy": "range",
                "column": "order_id",
                "bounds": {"lower": 0, "upper": 100},
                "num_partitions": 4,
                "export_workers": 3,
            }
        }
    )

    assert partitioner.enabled
    assert partitioner.column == "order_id"
    assert partitioner.max_workers == 3
    assert [(p.lower_bound, p.upper_bound, p.include_upper) for p in partitioner.partitions()] == [
        (0, 25, False),
        (25, 50, False),
        (50, 75, False),
        (75, 100, True),
    ]


def test_auto_bounds_target_rows_calculates_partition_count() -> None:
    partitioner = RangePartitioner.from_options(
        {
            "partitioning": {
                "strategy": "auto",
                "column": "order_id",
                "bounds": "auto",
                "target_rows_per_partition": 2,
                "max_partitions": 4,
                "export_workers": 4,
            }
        },
        bounds_resolver=lambda _column: (0, 9, 10),
    )

    assert partitioner.enabled
    assert partitioner.num_partitions == 4
    assert partitioner.max_workers == 4
    assert [(p.lower_bound, p.upper_bound, p.include_upper) for p in partitioner.partitions()] == [
        (0, 3, False),
        (3, 6, False),
        (6, 9, False),
        (9, 9, True),
    ]


def test_nested_partitioning_builds_datetime_windows() -> None:
    partitioner = RangePartitioner.from_options(
        {
            "partitioning": {
                "strategy": "time_window",
                "column": "updated_at",
                "bounds": {
                    "lower": "2026-01-01T00:00:00",
                    "upper": "2026-01-04T00:00:00",
                },
                "num_partitions": 3,
            }
        }
    )

    partitions = partitioner.partitions()

    assert partitioner.enabled
    assert partitions[0].lower_bound == datetime(2026, 1, 1)
    assert partitions[-1].upper_bound == datetime(2026, 1, 4)
    assert "updated_at" in partitioner.wrap_query("SELECT * FROM dbo.orders", "[updated_at]", partitions[0])


def test_bcp_queryout_command_includes_native_transfer_safety_options() -> None:
    runner = BcpRunner(
        credentials=BcpCredentials(
            host="localhost",
            port=1433,
            database="DWH",
            user="sa",
            password="super-secret",
        ),
    )
    command = runner.build_queryout_command(
        "SELECT * FROM dbo.orders",
        "/tmp/orders.tsv",
        options=BcpOptions(
            bcp_path="bcp",
            batch_size=100_000,
            packet_size=65_535,
            timeout_seconds=600,
            error_file="/tmp/orders.err",
            password_transport="argument",
        ),
    )

    joined = " ".join(command)
    redacted = runner.redact_command(command)

    assert "-b 100000" in joined
    assert "-a 16384" in joined
    assert "-l 600" in joined
    assert "-e /tmp/orders.err" in joined
    assert "super-secret" not in " ".join(redacted)
    assert "***" in redacted


def test_clickhouse_client_runner_renders_insert_settings_and_idempotency() -> None:
    runner = ClickHouseClientRunner(
        credentials=ClickHouseClientCredentials(
            host="localhost",
            port=9000,
            database="default",
            user="default",
        ),
        options=ClickHouseClientOptions(
            settings={
                "async_insert": 1,
                "wait_for_async_insert": 1,
                "input_format_parallel_parsing": 1,
            },
            query_id="dpone-query-1",
            insert_deduplication_token="dpone-token-1",
        ),
    )

    command = runner.build_insert_command("landing.orders", ["order_id", "amount"])

    assert "--async_insert" in command
    assert "--wait_for_async_insert" in command
    assert "--input_format_parallel_parsing" in command
    assert "--query_id" in command
    assert "dpone-query-1" in command
    assert "--insert_deduplication_token" in command
    assert "dpone-token-1" in command


def test_clickhouse_client_runner_executes_standalone_query_with_redaction() -> None:
    calls: list[list[str]] = []

    def fake_run(command, **kwargs):
        calls.append(command)
        assert kwargs["check"] is False
        assert kwargs["capture_output"] is True
        return SimpleNamespace(returncode=0, stdout=b"ok\n", stderr=b"")

    runner = ClickHouseClientRunner(
        credentials=ClickHouseClientCredentials(
            host="localhost",
            port=9000,
            database="default",
            user="default",
            password="super-secret",
        ),
        run=fake_run,
    )

    result = runner.execute_query("ALTER TABLE landing.orders DELETE WHERE id BETWEEN 1 AND 10")

    assert result.returncode == 0
    assert result.stdout == "ok\n"
    assert "--query" in calls[0]
    assert "ALTER TABLE landing.orders DELETE WHERE id BETWEEN 1 AND 10" in calls[0]
    assert "super-secret" not in " ".join(result.redacted_command)
    assert "***" in result.redacted_command


def test_clickhouse_client_runner_streams_native_blocks_to_stdin() -> None:
    writes: list[bytes] = []
    commands: list[list[str]] = []

    class FakeStdin:
        def write(self, chunk: bytes) -> None:
            writes.append(chunk)

        def close(self) -> None:
            writes.append(b"<closed>")

    class FakeProcess:
        def __init__(self, command, **kwargs):
            commands.append(command)
            assert kwargs["stdin"] == -1
            assert kwargs["stdout"] == -1
            assert kwargs["stderr"] == -1
            self.stdin = FakeStdin()
            self.returncode = 0

        def communicate(self, timeout=None):
            assert timeout == 30
            return b"ok\n", b""

    runner = ClickHouseClientRunner(
        credentials=ClickHouseClientCredentials(
            host="localhost",
            port=9000,
            database="default",
            user="default",
            password="super-secret",
        ),
        options=ClickHouseClientOptions(input_format="Native", timeout_seconds=30),
        popen=FakeProcess,
    )

    result = runner.insert_stream("landing.orders", ["id"], [b"block-1", b"block-2"])

    assert result.returncode == 0
    assert writes == [b"block-1", b"block-2", b"<closed>"]
    assert "FORMAT Native" in " ".join(commands[0])
    assert "super-secret" not in " ".join(result.redacted_command)


def test_clickhouse_client_runner_skips_empty_stream_without_subprocess() -> None:
    def fail_popen(*args, **kwargs):
        raise AssertionError("empty stream must not start clickhouse-client")

    runner = ClickHouseClientRunner(
        credentials=ClickHouseClientCredentials(
            host="localhost",
            port=9000,
            database="default",
            user="default",
            password="super-secret",
        ),
        options=ClickHouseClientOptions(input_format="Native", timeout_seconds=30),
        popen=fail_popen,
    )

    result = runner.insert_stream("landing.orders", ["id"], [])

    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""
    assert "FORMAT Native" in " ".join(result.command)
    assert "super-secret" not in " ".join(result.redacted_command)


def test_clickhouse_native_tcp_stream_ingest_uses_client_runner() -> None:
    captured: dict[str, object] = {}

    class FakeClientRunner:
        def __init__(self, credentials, options):
            captured["credentials"] = credentials
            captured["options"] = options

        def insert_stream(self, table, columns, chunks):
            captured["table"] = table
            captured["columns"] = tuple(columns)
            captured["payload"] = b"".join(chunks)
            return SimpleNamespace(returncode=0)

    class FakeSink(ClickHouseBulkMixin):
        _client_runner_cls = FakeClientRunner
        _http_runner_cls = object
        connector = SimpleNamespace(
            host="clickhouse.local",
            port=8123,
            database="default",
            user="default",
            password="secret",
            secure=False,
        )

        @staticmethod
        def _table(load_config):
            return f"{load_config.target_schema}.{load_config.target_table}"

    artifact = ByteStreamArtifact(
        lambda: [b"native-block"], columns=["id"], format="clickhouse-native", estimated_rows=1
    )
    artifact.bulk_wire_contract = BulkWirePlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        schema=[("id", "int")],
        source_options={
            "native_transfer": {
                "wire": {
                    "mode": "typed_binary",
                    "source_native_format": "bcp_native",
                    "binary_format": "native",
                }
            }
        },
        sink_options={"clickhouse_bulk": {"mode": "native_tcp", "ingest_contract": "typed_binary_staging"}},
    )
    load_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={
            "clickhouse_bulk": {
                "mode": "native_tcp",
                "native_tcp": {"enabled": True, "compression": "lz4", "host": "native.host", "port": 9000},
            }
        },
    )

    inserted = FakeSink()._insert_stream_with_client(load_config, artifact, [("id", "int")])

    assert inserted == 1
    assert captured["payload"] == b"native-block"
    assert captured["table"] == "landing.orders"
    assert captured["columns"] == ("id",)
    assert captured["credentials"].host == "native.host"
    assert captured["credentials"].port == 9000
    assert captured["options"].input_format == "Native"
    assert captured["options"].settings["compression"] == 1
    assert captured["options"].settings["network_compression_method"] == "lz4"


def test_clickhouse_client_stream_ingest_accepts_typed_raw_custom_separated() -> None:
    captured: dict[str, object] = {}

    class FakeClientRunner:
        def __init__(self, credentials, options):
            del credentials
            captured["options"] = options

        def insert_stream(self, table, columns, chunks):
            captured["table"] = table
            captured["columns"] = tuple(columns)
            captured["payload"] = b"".join(chunks)
            return SimpleNamespace(returncode=0)

    class FakeSink(ClickHouseBulkMixin):
        _client_runner_cls = FakeClientRunner
        _http_runner_cls = object
        connector = SimpleNamespace(
            host="clickhouse.local",
            port=8123,
            database="default",
            user="default",
            password="secret",
            secure=False,
        )

        @staticmethod
        def _table(load_config):
            return f"{load_config.target_schema}.{load_config.target_table}"

    artifact = ByteStreamArtifact(
        lambda: [b"1\x1fabc\x1e\n"], columns=["id", "name"], format="mssql-delimited", estimated_rows=1
    )
    artifact.bulk_wire_contract = BulkWirePlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        schema=[("id", "int"), ("name", "varchar(50)")],
        source_options={"native_transfer": {"wire": {"mode": "typed_raw", "delimiter_profile": "ascii_control"}}},
        sink_options={"clickhouse_bulk": {"mode": "client", "ingest_contract": "typed_raw_streaming_staging"}},
    )
    load_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={"clickhouse_bulk": {"mode": "client", "ingest_contract": "typed_raw_streaming_staging"}},
    )

    assert FakeSink()._should_use_client_stream(load_config, artifact) is True
    inserted = FakeSink()._insert_stream_with_client(load_config, artifact, [("id", "int"), ("name", "String")])

    assert inserted == 1
    assert captured["payload"] == b"1\x1fabc\x1e\n"
    assert captured["table"] == "landing.orders"
    assert captured["columns"] == ("id", "name")
    assert captured["options"].input_format == "CustomSeparated"
    assert captured["options"].settings["format_custom_field_delimiter"] == "\x1f"


def test_clickhouse_http_runner_renders_insert_settings_and_query_id() -> None:
    runner = ClickHouseHttpBulkRunner(
        credentials=ClickHouseHttpCredentials(
            host="localhost",
            port=8123,
            database="default",
            user="default",
        ),
        options=ClickHouseHttpOptions(
            settings={
                "async_insert": 1,
                "wait_for_async_insert": 1,
            },
            query_id="dpone-query-2",
            insert_deduplication_token="dpone-token-2",
        ),
    )

    url = runner.build_insert_url("landing.orders", ["order_id"])

    assert "async_insert=1" in url
    assert "wait_for_async_insert=1" in url
    assert "query_id=dpone-query-2" in url
    assert "insert_deduplication_token=dpone-token-2" in url

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from types import SimpleNamespace

from dpone.config import LoadConfig
from dpone.runtime.bulk_wire import BulkWirePlanner
from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.clickhouse_rowbinary import ClickHouseRowBinaryEncoder
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts import MSSQLQueryoutArtifactFactory
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy
from dpone.strategy_intelligence.native_transfer import NativeTransferPlanBuilder, NativeTransferRequest


class ClickHouseConnector:
    host = "localhost"
    port = 9000
    database = "default"
    user = "default"
    password = ""
    secure = False
    compression = True
    connect_timeout = 10
    send_receive_timeout = 3600
    settings = {}
    application_name = "test"

    def execute_query(self, _query: str) -> int:
        return 0

    def get_records(self, _query: str):
        return [(1,)]


class MssqlStreamingConnector:
    bcp_path = "bcp"
    trust_server_certificate = "yes"

    def __init__(self) -> None:
        self.bcp_queries: list[str] = []
        self.streaming_queries: list[str] = []

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"

    def bcp_queryout(self, query: str, output_path: str, *, options=None) -> int:
        del output_path, options
        self.bcp_queries.append(query)
        return 0

    def get_records_streaming(self, query: str, *, batch_size: int, as_dict: bool = False):
        assert as_dict is True
        self.streaming_queries.append(query)
        assert batch_size == 2
        yield [
            {"id": 1, "payload": 'raw "quote"\t tab\nline'},
            {"id": 2, "payload": None},
        ]


class Logger:
    def log_etl_progress(self, *_args, **_kwargs) -> None:
        pass


def _load_config(options: dict) -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        batch_size=2,
        options=options,
    )


def test_typed_binary_policy_selects_clickhouse_rowbinary_contract() -> None:
    contract = BulkWirePlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        schema=[("id", "int"), ("payload", "nvarchar(max) nullable")],
        source_options={"native_transfer": {"wire": {"mode": "typed_binary", "binary_format": "rowbinary"}}},
        sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
    )

    assert contract.selected_route == "typed_binary_row_stream"
    assert contract.input_format == "RowBinary"
    assert contract.source_escaping is False
    assert contract.delimiter_profile.clickhouse_settings == {}


def test_rowbinary_encoder_preserves_problematic_text_without_delimiters() -> None:
    encoder = ClickHouseRowBinaryEncoder([("id", "int"), ("payload", "nvarchar(max) nullable")])

    encoded = b"".join(
        encoder.iter_batches(
            [
                {"id": 1, "payload": 'raw "quote"\t tab\nline'},
                {"id": 2, "payload": None},
            ]
        )
    )

    assert b"REPLACE(" not in encoded
    assert b'raw "quote"\t tab\nline' in encoded
    assert encoded.endswith(b"\x02\x00\x00\x00\x01")


def test_rowbinary_encoder_uses_manifest_type_policy_for_mssql_time() -> None:
    encoder = ClickHouseRowBinaryEncoder(
        [("id", "int"), ("business_time", "time nullable")],
        type_policy=MssqlClickHouseTypePolicy(time_encoding="seconds_since_midnight"),
    )

    encoded = b"".join(encoder.iter_batches([{"id": 1, "business_time": 45296}]))

    assert encoded == b"\x01\x00\x00\x00\x00\xf0\xb0\x00\x00"


def test_rowbinary_encoder_uses_target_schema_and_binary_policy() -> None:
    encoder = ClickHouseRowBinaryEncoder(
        [("payload", "varbinary(4) nullable")],
        target_schema=[("payload", "Nullable(String)")],
        type_policy=MssqlClickHouseTypePolicy(binary_encoding="hex"),
    )

    encoded = b"".join(encoder.iter_batches([{"payload": bytes.fromhex("0102abcd")}]))

    assert encoded == b"\x00\x08" + b"0102abcd"


def test_rowbinary_encoder_uses_clickhouse_uuid_wire_order() -> None:
    encoder = ClickHouseRowBinaryEncoder([("row_guid", "uniqueidentifier")])

    encoded = b"".join(encoder.iter_batches([{"row_guid": "00112233-4455-6677-8899-aabbccddeeff"}]))

    assert encoded.hex() == "7766554433221100ffeeddccbbaa9988"


def test_rowbinary_encoder_truncates_datetime64_fraction_to_target_scale() -> None:
    encoder = ClickHouseRowBinaryEncoder([("created_at", "DateTime64(3)")], schema_kind="clickhouse")

    encoded = b"".join(encoder.iter_batches([{"created_at": datetime(2026, 1, 1, 0, 0, 0, 3333)}]))

    assert int.from_bytes(encoded, byteorder="little", signed=True) % 1000 == 3


def test_mssql_typed_binary_uses_row_stream_not_bcp_queryout() -> None:
    connector = MssqlStreamingConnector()
    factory = MSSQLQueryoutArtifactFactory(connector, Logger(), sink_connector=ClickHouseConnector())

    artifact = factory.artifact_for_query(
        _load_config(
            {
                "mssql_export_mode": "row_stream",
                "native_transfer": {"wire": {"mode": "typed_binary", "binary_format": "rowbinary"}},
                "clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"},
            }
        ),
        "SELECT [id], [payload] FROM [dbo].[orders]",
        [("id", "int"), ("payload", "nvarchar(max) nullable")],
    )

    assert isinstance(artifact, ByteStreamArtifact)
    assert artifact.format == "clickhouse-rowbinary"
    assert getattr(artifact, "bulk_wire_contract").selected_route == "typed_binary_row_stream"
    assert connector.bcp_queries == []
    assert b'raw "quote"\t tab\nline' in b"".join(artifact.iter_bytes())
    assert connector.streaming_queries == ["SELECT [id], [payload] FROM [dbo].[orders]"]


def test_clickhouse_http_stream_uses_rowbinary_contract() -> None:
    class FakeRunner:
        calls: list[SimpleNamespace] = []

        def __init__(self, _credentials, options):
            self.options = options

        def insert_stream(self, _table: str, _columns: list[str], chunks: Iterable[bytes]):
            self.calls.append(SimpleNamespace(options=self.options, payload=b"".join(chunks)))
            return None

    contract = BulkWirePlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        schema=[("id", "int")],
        source_options={"native_transfer": {"wire": {"mode": "typed_binary"}}},
        sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
    )
    artifact = ByteStreamArtifact(
        lambda: [b"\x01\x00\x00\x00"],
        columns=["id"],
        format="clickhouse-rowbinary",
    )
    artifact.bulk_wire_contract = contract
    sink = ClickHouseSink(ClickHouseConnector(), http_runner_cls=FakeRunner)

    rows = sink._payload_ingestion.insert_byte_stream(
        _load_config({"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}}),
        artifact,
        [("id", "int")],
    )

    assert rows == 1
    assert FakeRunner.calls[0].options.input_format == "RowBinary"
    assert FakeRunner.calls[0].payload == b"\x01\x00\x00\x00"


def test_strategy_plan_names_typed_binary_fast_path() -> None:
    plan = NativeTransferPlanBuilder().build(
        NativeTransferRequest(
            source_type="mssql",
            sink_type="clickhouse",
            source_table="dbo.orders",
            target_table="landing.orders",
            strategy="full_refresh",
            source_options={
                "mssql_export_mode": "row_stream",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "payload", "type": "nvarchar(max) nullable"},
                ],
                "native_transfer": {"wire": {"mode": "typed_binary", "binary_format": "rowbinary"}},
            },
            sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
        )
    )

    assert plan.fast_path_id == "mssql_odbc_row_stream_to_clickhouse_rowbinary"
    assert plan.export_method == "row_stream"
    assert plan.ingest_method == "clickhouse_rowbinary_staging"
    assert plan.native_ingest_settings["bulk_wire"]["selected_route"] == "typed_binary_row_stream"

from __future__ import annotations

import json
import struct
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from dpone.config import LoadConfig
from dpone.runtime.bulk_wire import BulkWirePlanner
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.native_wire_artifacts import SourceNativeArtifact
from dpone.runtime.native_wire_mssql import MssqlBcpNativeDecoder, build_mssql_bcp_native_contract
from dpone.runtime.native_wire_transcoder import NativeWireTranscoder
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts import MSSQLQueryoutArtifactFactory
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


class MssqlBcpConnector:
    bcp_path = "bcp"
    trust_server_certificate = "yes"

    def __init__(self) -> None:
        self.queries: list[str] = []
        self.options: list[object] = []

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"

    def bcp_queryout(self, query: str, output_path: str, *, options=None) -> int:
        Path(output_path).write_bytes(b"")
        self.queries.append(query)
        self.options.append(options)
        return 0


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


def test_bcp_native_policy_selects_clickhouse_rowbinary_contract() -> None:
    contract = BulkWirePlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        schema=[("id", "int"), ("payload", "nvarchar(max) nullable")],
        source_options={
            "native_transfer": {
                "wire": {
                    "mode": "typed_binary",
                    "source_native_format": "bcp_native",
                    "binary_format": "rowbinary",
                }
            }
        },
        sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
    )

    assert contract.selected_route == "typed_binary_bcp_native"
    assert contract.input_format == "RowBinary"
    assert contract.source_escaping is False
    assert "bulk_wire_bcp_native_source_query_is_not_sink_escaped" in contract.warnings


def test_mssql_bcp_native_artifact_uses_native_bcp_without_source_escaping(tmp_path: Path) -> None:
    connector = MssqlBcpConnector()
    factory = MSSQLQueryoutArtifactFactory(connector, Logger(), sink_connector=ClickHouseConnector())
    artifact = factory.artifact_for_query(
        _load_config(
            {
                "runtime": {"storage": {"work_dir": str(tmp_path)}},
                "native_transfer": {
                    "wire": {"mode": "typed_binary", "source_native_format": "bcp_native"},
                },
                "clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"},
            }
        ),
        "SELECT [id], [payload] FROM [dbo].[orders] WHERE [id] >= 1",
        [("id", "int"), ("payload", "nvarchar(max) nullable")],
    )

    assert isinstance(artifact, SourceNativeArtifact)
    assert artifact.format == "mssql-bcp-native"
    assert artifact.native_wire_contract.source_format == "mssql-bcp-native"
    assert connector.options[-1].file_format == "native"
    assert connector.queries[-1] == "SELECT [id], [payload] FROM [dbo].[orders] WHERE [id] >= 1"
    assert "REPLACE(" not in connector.queries[-1]


def test_mssql_bcp_native_decoder_decodes_supported_golden_row(tmp_path: Path) -> None:
    schema = [
        ("id", "int"),
        ("flag", "bit nullable"),
        ("amount", "decimal(10,2)"),
        ("cash", "money"),
        ("fixed_text", "char(5) nullable"),
        ("fixed_unicode", "nchar(5) nullable"),
        ("payload", "nvarchar(max) nullable"),
        ("ascii_payload", "varchar(max) nullable"),
        ("created_at", "datetime2(7)"),
        ("row_guid", "uniqueidentifier"),
    ]
    contract = build_mssql_bcp_native_contract(
        schema=schema,
        query="SELECT ...",
        bcp_version="test-bcp",
    )
    payload = (
        _int32(7)
        + _nullable_fixed(True, b"\x01")
        + _decimal("123.45", precision=10, scale=2)
        + _money("12.34")
        + _nullable_var(b"a    ", prefix_width=2)
        + _nullable_var("я    ".encode("utf-16le"), prefix_width=2)
        + _nullable_var("Привет\t\n".encode("utf-16le"), prefix_width=8)
        + _nullable_var(b"ascii\t\n", prefix_width=8)
        + _datetime2(datetime(2026, 6, 22, 13, 14, 15, 123456), scale=7)
        + uuid.UUID("12345678-1234-5678-9abc-def012345678").bytes_le
    )
    path = tmp_path / "orders.bcp"
    path.write_bytes(payload)

    rows = list(MssqlBcpNativeDecoder(contract).iter_rows(path))

    assert rows == [
        {
            "id": 7,
            "flag": True,
            "amount": Decimal("123.45"),
            "cash": Decimal("12.34"),
            "fixed_text": "a    ",
            "fixed_unicode": "я    ",
            "payload": "Привет\t\n",
            "ascii_payload": "ascii\t\n",
            "created_at": datetime(2026, 6, 22, 13, 14, 15, 123456),
            "row_guid": "12345678-1234-5678-9abc-def012345678",
        }
    ]


def test_clickhouse_ingestion_transcodes_bcp_native_to_rowbinary_stream(tmp_path: Path) -> None:
    class FakeRunner:
        calls: list[SimpleNamespace] = []

        def __init__(self, _credentials, options):
            self.options = options

        def insert_stream(self, _table: str, _columns: list[str], chunks):
            self.calls.append(SimpleNamespace(options=self.options, payload=b"".join(chunks)))

    schema = [("id", "int"), ("payload", "nvarchar(max) nullable")]
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT ...", bcp_version="test-bcp")
    path = tmp_path / "orders.bcp"
    path.write_bytes(_int32(1) + _nullable_var("raw\ttext\n".encode("utf-16le"), prefix_width=8))
    artifact = SourceNativeArtifact(path, columns=["id", "payload"], native_wire_contract=contract, estimated_rows=1)
    sink = ClickHouseSink(ClickHouseConnector(), http_runner_cls=FakeRunner)

    rows = sink._payload_ingestion.insert_file(
        _load_config({"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}}),
        artifact,
        schema,
    )

    assert rows == 1
    assert FakeRunner.calls[0].options.input_format == "RowBinary"
    assert b"raw\ttext\n" in FakeRunner.calls[0].payload


def test_mssql_bcp_native_decoder_preserves_100ns_tick_beyond_python_microseconds(tmp_path: Path) -> None:
    schema = [("id", "int"), ("created_at", "datetime2(7)")]
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT ...")
    path = tmp_path / "datetime2.bcp"
    path.write_bytes(_int32(1) + _datetime2_ticks(date(2026, 6, 22), ticks=45296_1234567, scale=7))

    rows = list(MssqlBcpNativeDecoder(contract).iter_rows(path))

    assert rows[0]["created_at"] == datetime(2026, 6, 22, 12, 34, 56, 123456)
    assert rows[0]["created_at"].submicrosecond_100ns == 7


def test_mssql_bcp_native_decoder_preserves_datetimeoffset_and_100ns_tick(tmp_path: Path) -> None:
    schema = [("offset_at", "datetimeoffset(7)")]
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT ...")
    path = tmp_path / "datetimeoffset.bcp"
    payload = _datetime2(datetime(2026, 6, 9, 9, 30, 0, 123456), scale=7)
    ticks = int.from_bytes(payload[:5], byteorder="little", signed=False) + 7
    path.write_bytes(ticks.to_bytes(5, byteorder="little") + payload[5:] + struct.pack("<h", 180))

    rows = list(MssqlBcpNativeDecoder(contract).iter_rows(path))
    value = rows[0]["offset_at"]

    assert value.astimezone(UTC) == datetime(2026, 6, 9, 9, 30, 0, 123456, tzinfo=UTC)
    assert value.submicrosecond_100ns == 7


def test_mssql_bcp_native_decoder_rounds_datetime_ticks_like_sql_server(tmp_path: Path) -> None:
    schema = [("id", "int"), ("created_dt", "datetime")]
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT ...")
    path = tmp_path / "datetime.bcp"
    path.write_bytes(_int32(1) + _datetime(date(2026, 1, 1), ticks=2))

    rows = list(MssqlBcpNativeDecoder(contract).iter_rows(path))

    assert rows[0]["created_dt"] == datetime(2026, 1, 1, 0, 0, 0, 7000)


def test_mssql_bcp_native_decoder_reconstructs_local_datetimeoffset_from_bcp_utc_clock(tmp_path: Path) -> None:
    schema = [("offset_at", "datetimeoffset(7) nullable")]
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT ...")
    assert contract.blockers == ()
    assert contract.columns[0].storage_type == "datetimeoffset"
    assert contract.columns[0].fixed_length == 10
    path = tmp_path / "datetimeoffset.bcp"
    # bcp -n stores the 09:30 UTC clock plus the original +03:00 offset.
    path.write_bytes(
        (10).to_bytes(1, byteorder="little", signed=True)
        + _datetime2(datetime(2026, 6, 9, 9, 30, 0, 123456), scale=7)
        + struct.pack("<h", 180)
    )

    rows = list(MssqlBcpNativeDecoder(contract).iter_rows(path))
    value = rows[0]["offset_at"]

    assert value.replace(tzinfo=None) == datetime(2026, 6, 9, 12, 30, 0, 123456)
    assert value.astimezone(UTC) == datetime(2026, 6, 9, 9, 30, 0, 123456, tzinfo=UTC)


def test_native_wire_transcoder_records_safe_evidence(tmp_path: Path) -> None:
    schema = [("id", "int")]
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT [id] FROM [dbo].[orders]", bcp_version="18")
    path = tmp_path / "orders.bcp"
    path.write_bytes(_int32(42))
    artifact = SourceNativeArtifact(path, columns=["id"], native_wire_contract=contract, estimated_rows=1)

    stream = NativeWireTranscoder().to_clickhouse_rowbinary(artifact, schema)
    payload = b"".join(stream.iter_bytes())
    evidence = stream.native_wire_evidence.to_dict()

    assert payload == _int32(42)
    assert evidence["schema_version"] == "dpone.native_transfer.native_wire.v1"
    assert evidence["source_format"] == "mssql-bcp-native"
    assert evidence["target_format"] == "RowBinary"
    assert evidence["rows"] == 1
    assert evidence["decoded_bytes"] == 4
    assert evidence["encoded_bytes"] == 4
    assert "SELECT [id]" not in json.dumps(evidence)


def test_strategy_plan_names_bcp_native_fast_path() -> None:
    plan = NativeTransferPlanBuilder().build(
        NativeTransferRequest(
            source_type="mssql",
            sink_type="clickhouse",
            source_table="dbo.orders",
            target_table="landing.orders",
            strategy="full_refresh",
            source_options={
                "columns": [("id", "int"), ("payload", "nvarchar(max) nullable")],
                "native_transfer": {
                    "wire": {"mode": "typed_binary", "source_native_format": "bcp_native"},
                },
            },
            sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
        )
    )

    assert plan.fast_path_id == "mssql_bcp_native_to_clickhouse_rowbinary"
    assert plan.export_method == "bcp_queryout_native"
    assert plan.ingest_method == "clickhouse_rowbinary_staging"
    assert plan.native_ingest_settings["bulk_wire"]["selected_route"] == "typed_binary_bcp_native"


def test_partitioned_bcp_native_plan_exports_source_native_artifacts(tmp_path: Path) -> None:
    connector = MssqlBcpConnector()
    factory = MSSQLQueryoutArtifactFactory(connector, Logger(), sink_connector=ClickHouseConnector())
    artifact = factory.artifact_for_query(
        _load_config(
            {
                "runtime": {"storage": {"work_dir": str(tmp_path)}},
                "sink_type": "clickhouse",
                "partitioning": {"column": "id", "bounds": {"lower": 0, "upper": 10}, "num_partitions": 2},
                "native_transfer": {
                    "execution": {"mode": "pipelined"},
                    "wire": {"mode": "typed_binary", "source_native_format": "bcp_native"},
                },
                "clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"},
            }
        ),
        "SELECT [id], [payload] FROM [dbo].[orders]",
        [("id", "int"), ("payload", "nvarchar(max) nullable")],
    )

    assert isinstance(artifact, PartitionedTransferPlanArtifact)
    exported = artifact.exporter(artifact.slices[0])

    assert isinstance(exported, SourceNativeArtifact)
    assert exported.format == "mssql-bcp-native"
    assert exported.native_wire_contract.source_format == "mssql-bcp-native"
    assert "REPLACE(" not in connector.queries[-1]


def _int32(value: int) -> bytes:
    return struct.pack("<i", value)


def _nullable_fixed(value: object | None, payload: bytes) -> bytes:
    if value is None:
        return b"\xff"
    return bytes([len(payload)]) + payload


def _nullable_var(payload: bytes | None, *, prefix_width: int) -> bytes:
    if payload is None:
        return (-1).to_bytes(prefix_width, byteorder="little", signed=True)
    return len(payload).to_bytes(prefix_width, byteorder="little", signed=True) + payload


def _decimal(value: str, *, precision: int, scale: int) -> bytes:
    scaled = int((Decimal(value) * (Decimal(10) ** scale)).to_integral_value())
    width = 16
    sign = b"\x01" if scaled >= 0 else b"\x00"
    magnitude = abs(scaled).to_bytes(width, byteorder="little", signed=False)
    return bytes([precision, scale]) + sign + magnitude


def _money(value: str) -> bytes:
    scaled = int((Decimal(value) * Decimal("10000")).to_integral_value())
    high = scaled >> 32
    low = scaled & 0xFFFFFFFF
    return high.to_bytes(4, byteorder="little", signed=True) + low.to_bytes(4, byteorder="little", signed=False)


def _datetime2(value: datetime, *, scale: int) -> bytes:
    midnight = datetime(value.year, value.month, value.day)
    ticks = int((value - midnight).total_seconds() * (10**scale))
    days = value.toordinal() - date(1, 1, 1).toordinal()
    return ticks.to_bytes(5, byteorder="little", signed=False) + days.to_bytes(3, byteorder="little", signed=False)


def _datetime2_ticks(value: date, *, ticks: int, scale: int) -> bytes:
    days = value.toordinal() - date(1, 1, 1).toordinal()
    time_width = 3 if scale <= 2 else 4 if scale <= 4 else 5
    return ticks.to_bytes(time_width, byteorder="little", signed=False) + days.to_bytes(
        3,
        byteorder="little",
        signed=False,
    )


def _datetime(value: date, *, ticks: int) -> bytes:
    days = (value - date(1900, 1, 1)).days
    return days.to_bytes(4, byteorder="little", signed=True) + ticks.to_bytes(4, byteorder="little", signed=True)

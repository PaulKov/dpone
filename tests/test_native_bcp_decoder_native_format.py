from __future__ import annotations

import json
import struct
from pathlib import Path
from types import SimpleNamespace

from dpone.config import LoadConfig
from dpone.runtime.bulk_wire import BulkWirePlanner
from dpone.runtime.native_wire_artifacts import SourceNativeArtifact
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.native_wire_transcoder import NativeWireTranscoder
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.strategy_intelligence.native_transfer import NativeTransferPlanBuilder, NativeTransferRequest

ROOT = Path(__file__).resolve().parents[1]


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


def test_public_schemas_expose_clickhouse_native_binary_format() -> None:
    for relative in ("src/dpone/schema/etl-config.schema.json", "src/dpone/schema/etl-batch-manifest.schema.json"):
        schema = json.loads((ROOT / relative).read_text())
        policy = schema["definitions"]["native_transfer_wire_policy"]["properties"]

        assert policy["binary_format"]["enum"] == ["rowbinary", "native"]
        assert "block_rows" in policy
        assert "block_bytes" in policy


def test_bcp_native_policy_selects_clickhouse_native_contract() -> None:
    contract = BulkWirePlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        schema=[("id", "int")],
        source_options={
            "native_transfer": {
                "wire": {
                    "mode": "typed_binary",
                    "source_native_format": "bcp_native",
                    "binary_format": "native",
                    "block_rows": 65536,
                    "block_bytes": "64MiB",
                }
            }
        },
        sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
    )

    assert contract.selected_route == "typed_binary_bcp_native"
    assert contract.input_format == "Native"
    assert contract.binary_format == "native"
    assert contract.block_rows == 65536
    assert contract.source_escaping is False


def test_native_wire_transcoder_emits_clickhouse_native_stream(tmp_path: Path) -> None:
    schema = [("id", "int")]
    contract = build_mssql_bcp_native_contract(
        schema=schema,
        query="SELECT [id] FROM [dbo].[orders]",
        bcp_version="18",
        target_format="Native",
    )
    path = tmp_path / "orders.bcp"
    path.write_bytes(struct.pack("<i", 42))
    artifact = SourceNativeArtifact(path, columns=["id"], native_wire_contract=contract, estimated_rows=1)
    artifact.bulk_wire_contract = BulkWirePlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        schema=schema,
        source_options={
            "native_transfer": {
                "wire": {"mode": "typed_binary", "source_native_format": "bcp_native", "binary_format": "native"}
            }
        },
        sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
    )

    stream = NativeWireTranscoder().to_clickhouse_binary(artifact, schema)
    payload = b"".join(stream.iter_bytes())
    evidence = stream.native_wire_evidence.to_dict()

    assert stream.format == "clickhouse-native"
    assert stream.bulk_wire_contract.input_format == "Native"
    assert payload.endswith(struct.pack("<i", 42))
    assert evidence["target_format"] == "Native"
    assert evidence["rows"] == 1
    assert evidence["encoded_bytes"] == len(payload)
    sink_evidence = stream.sink_binary_evidence.to_dict()
    assert sink_evidence["schema_version"] == "dpone.native_transfer.sink_binary.v1"
    assert sink_evidence["format"] == "native"
    assert sink_evidence["input_format"] == "Native"
    assert sink_evidence["block_count"] == 1


def test_clickhouse_ingestion_accepts_native_byte_stream(tmp_path: Path) -> None:
    class FakeRunner:
        calls: list[SimpleNamespace] = []

        def __init__(self, _credentials, options):
            self.options = options

        def insert_stream(self, _table: str, _columns: list[str], chunks):
            self.calls.append(SimpleNamespace(options=self.options, payload=b"".join(chunks)))

    schema = [("id", "int")]
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT ...", target_format="Native")
    path = tmp_path / "orders.bcp"
    path.write_bytes(struct.pack("<i", 7))
    artifact = SourceNativeArtifact(path, columns=["id"], native_wire_contract=contract, estimated_rows=1)
    artifact.bulk_wire_contract = BulkWirePlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        schema=schema,
        source_options={
            "native_transfer": {
                "wire": {"mode": "typed_binary", "source_native_format": "bcp_native", "binary_format": "native"}
            }
        },
        sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
    )
    sink = ClickHouseSink(ClickHouseConnector(), http_runner_cls=FakeRunner)

    rows = sink._payload_ingestion.insert_payload(
        _load_config({"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}}),
        LoadPayload(artifact=artifact, schema=schema),
    )

    assert rows == 1
    assert FakeRunner.calls[0].options.input_format == "Native"
    assert FakeRunner.calls[0].payload.endswith(struct.pack("<i", 7))


def test_strategy_plan_names_bcp_native_clickhouse_native_fast_path() -> None:
    plan = NativeTransferPlanBuilder().build(
        NativeTransferRequest(
            source_type="mssql",
            sink_type="clickhouse",
            source_table="dbo.orders",
            target_table="landing.orders",
            strategy="full_refresh",
            source_options={
                "columns": [("id", "int")],
                "native_transfer": {
                    "wire": {"mode": "typed_binary", "source_native_format": "bcp_native", "binary_format": "native"}
                },
            },
            sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
        )
    )

    assert plan.fast_path_id == "mssql_bcp_native_to_clickhouse_native"
    assert plan.native_ingest_settings["bulk_wire"]["input_format"] == "Native"

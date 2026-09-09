from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from dpone.config.load_config import LoadConfig
from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.direct_ingest import DIRECT_INGEST_SCHEMA_VERSION
from dpone.runtime.sinks.clickhouse_bulk_mixin import ClickHouseBulkMixin


def test_native_tcp_stream_uses_direct_runner_when_provider_is_certified(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    provider = SimpleNamespace(
        direct_ingest_capabilities=lambda: {
            "schema_version": DIRECT_INGEST_SCHEMA_VERSION,
            "backends": [
                {
                    "backend_id": "clickhouse_native_tcp_direct",
                    "sink": "clickhouse",
                    "input_format": "Native",
                    "certified": True,
                }
            ],
        },
        insert_clickhouse_native=lambda request: {"rows": 1},
    )
    monkeypatch.setitem(sys.modules, "dpone_native_accel", provider)

    class FailClientRunner:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("certified direct provider must bypass clickhouse-client")

    class FakeDirectRunner:
        def __init__(self, *, credentials, options):
            captured["credentials"] = credentials
            captured["options"] = options

        def insert_stream(self, table, columns, chunks):
            captured["table"] = table
            captured["columns"] = tuple(columns)
            captured["payload"] = b"".join(chunks)
            return SimpleNamespace(rows=1)

    class FakeSink(ClickHouseBulkMixin):
        _client_runner_cls = FailClientRunner
        _http_runner_cls = object
        _direct_native_runner_cls = FakeDirectRunner
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
        lambda: [b"native-block"],
        columns=["id"],
        format="clickhouse-native",
        estimated_rows=1,
    )
    artifact.bulk_wire_contract = SimpleNamespace(input_format="Native", route_certified=True)
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
                "native_tcp": {"enabled": True, "backend": "auto", "compression": "lz4"},
            }
        },
    )

    inserted = FakeSink()._insert_stream_with_client(load_config, artifact, [("id", "Int32")])

    assert inserted == 1
    assert captured["payload"] == b"native-block"
    assert captured["table"] == "landing.orders"
    assert captured["columns"] == ("id",)
    assert captured["options"].compression == "lz4"


def test_native_tcp_stream_uses_direct_runner_for_advisory_uncertified_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    provider = SimpleNamespace(
        direct_ingest_capabilities=lambda: {
            "schema_version": DIRECT_INGEST_SCHEMA_VERSION,
            "backends": [
                {
                    "backend_id": "clickhouse_native_tcp_direct",
                    "sink": "clickhouse",
                    "input_format": "Native",
                    "certified": True,
                }
            ],
        },
        insert_clickhouse_native=lambda request: {"rows": 1},
    )
    monkeypatch.setitem(sys.modules, "dpone_native_accel", provider)

    class FailClientRunner:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("advisory direct provider must not fallback to clickhouse-client")

    class FakeDirectRunner:
        def __init__(self, *, credentials, options):
            captured["credentials"] = credentials
            captured["options"] = options

        def insert_stream(self, table, columns, chunks):
            captured["table"] = table
            captured["columns"] = tuple(columns)
            captured["payload"] = b"".join(chunks)
            return SimpleNamespace(rows=1)

    class FakeSink(ClickHouseBulkMixin):
        _client_runner_cls = FailClientRunner
        _http_runner_cls = object
        _direct_native_runner_cls = FakeDirectRunner
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
        lambda: [b"native-block"],
        columns=["id"],
        format="clickhouse-native",
        estimated_rows=1,
    )
    artifact.bulk_wire_contract = SimpleNamespace(input_format="Native", route_certified=False)
    load_config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={
            "native_transfer": {"execution": {"certification": {"mode": "advisory"}}},
            "clickhouse_bulk": {
                "mode": "native_tcp",
                "native_tcp": {"enabled": True, "backend": "direct", "compression": "lz4"},
            },
        },
    )

    inserted = FakeSink()._insert_stream_with_client(load_config, artifact, [("id", "Int32")])

    assert inserted == 1
    assert captured["payload"] == b"native-block"

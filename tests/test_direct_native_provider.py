from __future__ import annotations

from importlib import import_module
from io import BytesIO

import pytest

from dpone.readiness.native_acceleration import NativeAccelerationReadinessService
from dpone.runtime.bulk_options import ClickHouseBulkOptionsResolver
from dpone.runtime.direct_ingest import DirectIngestResolver, DirectIngestRouteRequest

dpone_native_accel = pytest.importorskip(
    "dpone_native_accel",
    reason="dpone-native-accel is an optional direct-ingest distribution",
)
_native_direct = import_module("dpone_native_accel._clickhouse_native_direct")
NativeBlockFramer = _native_direct.NativeBlockFramer
NativeInsertRequest = _native_direct.NativeInsertRequest
NativeInsertResult = _native_direct.NativeInsertResult
NativePacketWriter = _native_direct.NativePacketWriter


def _var_uint(value: int) -> bytes:
    output = bytearray()
    current = int(value)
    while current >= 0x80:
        output.append((current & 0x7F) | 0x80)
        current >>= 7
    output.append(current)
    return bytes(output)


def _native_block() -> bytes:
    return b"".join(
        [
            _var_uint(1),
            _var_uint(2),
            _var_uint(2),
            b"id",
            _var_uint(5),
            b"Int32",
            b"\x01\x00\x00\x00",
            b"\x02\x00\x00\x00",
        ]
    )


def test_provider_exposes_certified_direct_insert_backend() -> None:
    capabilities = dpone_native_accel.direct_ingest_capabilities()

    assert callable(getattr(dpone_native_accel, "insert_clickhouse_native"))
    assert capabilities["backends"][0]["certified"] is True

    decision = DirectIngestResolver(provider_loader=lambda: dpone_native_accel).decide(
        DirectIngestRouteRequest(
            bulk_options=ClickHouseBulkOptionsResolver.resolve(
                {"clickhouse_bulk": {"mode": "native_tcp", "native_tcp": {"backend": "auto"}}}
            ),
            input_format="Native",
            route_certified=True,
        )
    )

    assert decision.selected_backend == "direct"
    assert decision.fallback_reason is None


def test_native_accel_doctor_reports_direct_protocol_metadata() -> None:
    payload = NativeAccelerationReadinessService().doctor()
    direct = payload["direct_ingest"]

    assert direct["selected_backend"] == "direct"
    assert direct["fallback_reason"] is None
    assert direct["provider_version"]
    assert direct["protocol_revision"] == 54453
    assert direct["supported_compression"] == ["none", "lz4", "zstd"]


def test_native_block_framer_prepends_block_info_and_preserves_payload() -> None:
    block = _native_block()
    framed = NativeBlockFramer().frame(block)

    expected_block_info = b"\x01\x00\x02\xff\xff\xff\xff\x00"
    assert framed == expected_block_info + block
    assert NativeBlockFramer().row_count(block) == 2


def test_native_packet_writer_sends_query_data_and_empty_block() -> None:
    stream = BytesIO()
    writer = NativePacketWriter(stream, include_temporary_table_name=True, compression=None)
    request = NativeInsertRequest(
        table="landing.orders",
        columns=("id",),
        query_id="q-direct-1",
        settings={"max_insert_threads": 2},
    )

    writer.write_query(request)
    writer.write_data(_native_block())
    writer.write_empty_data()

    payload = stream.getvalue()
    assert payload.startswith(b"\x01")
    assert b"q-direct-1" in payload
    assert b"INSERT INTO `landing`.`orders` (`id`) VALUES" in payload
    assert payload.count(b"\x02") >= 2
    assert b"\x01\x00\x02\xff\xff\xff\xff\x00" in payload


def test_direct_insert_result_marks_backend_for_runtime_evidence() -> None:
    result = NativeInsertResult(
        rows=2,
        source_bytes=128,
        compressed_bytes=64,
        blocks=1,
        duration_seconds=0.5,
        query_id="q-direct-1",
        compression="lz4",
        provider_version="0.36.0",
    )

    assert result.to_provider_dict()["backend"] == "direct"

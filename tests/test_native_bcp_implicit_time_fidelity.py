"""Independent exact frames for admitted implicit/explicit nullable TIME sources."""

from __future__ import annotations

from pathlib import Path

import pytest

from dpone.runtime.native_acceleration import NativeAccelerationRegistry
from dpone.runtime.native_wire_artifacts import SourceNativeArtifact
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.native_wire_transcoder import NativeWireTranscoder


def _text(value: str) -> bytes:
    """Encode only these short expected strings without production encoders."""
    payload = value.encode("ascii")
    assert len(payload) < 128
    return bytes([len(payload)]) + payload


@pytest.mark.parametrize("source_type", ["time nullable", "TIME NULLABLE", "time  nullable", "time(7) nullable"])
@pytest.mark.parametrize("backend", ["rowbinary", "python_native", "accelerated"])
def test_nullable_time_keeps_exact_values_and_distinct_row_boundaries(
    tmp_path: Path, source_type: str, backend: str
) -> None:
    """Midnight, 100ns, the final day tick and NULL retain all sentinels."""
    ticks = (0, 10000001, 863999999999)
    texts = ("00:00:00.0000000", "00:00:01.0000001", "23:59:59.9999999")
    before = [bytes.fromhex(value) for value in ("04030201", "14131211", "24232221", "34333231")]
    after = [bytes.fromhex(value) for value in ("08070605", "18171615", "28272625", "38373635")]
    wire = b"".join(before[i] + b"\x05" + value.to_bytes(5, "little") + after[i] for i, value in enumerate(ticks))
    wire += before[3] + b"\xff" + after[3]
    schema = [("before", "int"), ("value", source_type), ("after", "int")]
    target = [("before", "Int32"), ("value", "Nullable(String)"), ("after", "Int32")]
    path = tmp_path / "synthetic-time.bcp"
    path.write_bytes(wire)
    fmt = "RowBinary" if backend == "rowbinary" else "Native"
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic", target_format=fmt)
    artifact = SourceNativeArtifact(path, columns=[name for name, _ in schema], native_wire_contract=contract)
    if backend == "accelerated":
        pytest.importorskip("dpone_native_accel")
        registry = NativeAccelerationRegistry()
    else:
        registry = NativeAccelerationRegistry(module_loader=lambda: None)
    stream = NativeWireTranscoder(registry=registry).to_clickhouse_binary(artifact, schema, clickhouse_schema=target)

    actual = b"".join(stream.iter_bytes())

    if fmt == "RowBinary":
        expected = b"".join(before[i] + b"\x00" + _text(value) + after[i] for i, value in enumerate(texts))
        expected += before[3] + b"\x01" + after[3]
    else:
        expected = (
            b"\x03\x04"
            + _text("before")
            + _text("Int32")
            + b"".join(before)
            + _text("value")
            + _text("Nullable(String)")
            + b"\x00\x00\x00\x01"
            + b"".join(_text(value) for value in texts)
            + b"\x00"
            + _text("after")
            + _text("Int32")
            + b"".join(after)
        )
    assert actual == expected
    assert stream.native_wire_evidence.rows == 4
    assert stream.native_wire_evidence.failure_code is None
    if backend == "accelerated":
        assert stream.native_acceleration_evidence.selected_backend == "native_accelerated"

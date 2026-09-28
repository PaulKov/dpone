"""Native chunk framing preserves bytes across arbitrary transport boundaries."""

from pathlib import Path

import pytest

from dpone.runtime.native_wire_chunk_rows import NativeWireRowFramer
from dpone.runtime.native_wire_mssql import MssqlBcpNativeDecoder, build_mssql_bcp_native_contract
from dpone.runtime.physical_chunk_policy import PhysicalChunkLimitExceeded, PhysicalChunkPolicy
from dpone.runtime.physical_chunk_writer import RowBoundaryChunkWriter
from dpone.runtime.physical_chunking import PhysicalChunkedFileExportArtifact


def contract(schema):
    return build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic")


@pytest.mark.parametrize("split", range(1, 32))
def test_all_segment_boundaries_preserve_rows(split):
    layout = contract([("id", "int"), ("text", "varbinary(max) nullable")])
    rows = [
        b"\x01\x00\x00\x00" + (4).to_bytes(8, "little") + b"\n\t\x00x",
        b"\x02\x00\x00\x00" + b"\xff" * 8,
        b"\x03\x00\x00\x00" + bytes(8),
    ]
    data = b"".join(rows)
    assert (
        list(
            NativeWireRowFramer(layout, max_row_bytes=16).rows(data[i : i + split] for i in range(0, len(data), split))
        )
        == rows
    )


@pytest.mark.parametrize("wire", [b"\x10", b"\x10" + bytes(15), b"\xfe", b"\x11" + bytes(17)])
def test_truncation_and_invalid_prefixes_fail_closed(wire):
    with pytest.raises(ValueError, match="native_wire"):
        list(NativeWireRowFramer(contract([("id", "uniqueidentifier")]), max_row_bytes=32).rows([wire]))


def test_large_declared_field_fails_before_reading_payload():
    def segments():
        yield (2**40).to_bytes(8, "little")
        pytest.fail("must reject declared size before requesting payload")

    with pytest.raises(PhysicalChunkLimitExceeded):
        list(NativeWireRowFramer(contract([("v", "varbinary(max)")]), max_row_bytes=64).rows(segments()))


def test_empty_stream():
    assert list(NativeWireRowFramer(contract([("v", "int")]), max_row_bytes=4).rows([])) == []


def test_framed_chunks_preserve_native_contract_and_cleanup(tmp_path):
    layout = contract([("v", "int")])
    data = b"".join(i.to_bytes(4, "little") for i in range(9))
    writer = RowBoundaryChunkWriter(
        policy=PhysicalChunkPolicy(target_chunk_bytes=8, max_chunk_bytes=12),
        columns=["v"],
        directory=tmp_path,
        format="mssql-bcp-native",
    )

    def chunks():
        yield from writer.write_rows(NativeWireRowFramer(layout, max_row_bytes=12).rows([data]))

    artifact = PhysicalChunkedFileExportArtifact(
        chunk_generator=chunks,
        columns=["v"],
        evidence_path=tmp_path / "evidence.json",
        native_wire_contract=layout,
        cleanup_policy="eager",
    )
    values = []
    paths = []

    def load(child):
        assert all(not path.exists() for path in paths)
        assert child.native_wire_contract is layout
        path = Path(child.file_path)
        assert path.stat().st_size <= 12
        paths.append(path)
        decoded = list(MssqlBcpNativeDecoder(layout).iter_rows(path))
        values.extend(row["v"] for row in decoded)
        return len(decoded)

    assert artifact.load_with(load) == 9
    assert values == list(range(9))
    assert all(not path.exists() for path in paths)

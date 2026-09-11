"""Frame bounds apply to live source objects and serialized IPC independently."""

import pytest

from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import NativeChunkLimits
from dpone.runtime.mssql_native_chunks_files import encode_native_frame, native_frames, verify_native_file
from dpone.runtime.mssql_native_sized_frames import sized_native_frames
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract


def test_reused_mutable_source_rows_are_frozen_before_next_pull():
    contract = build_mssql_bcp_native_contract(schema=[("value", "int")], query="SELECT synthetic")
    limits = NativeChunkLimits(10000, 10000, max_bytes=1024, max_row_bytes=100)

    def source():
        row = [1]
        yield row
        row[0] = 2
        yield row

    assert list(native_frames(source(), contract, limits)) == [((1,), (2,))]


def test_ipc_overhead_has_independent_bound():
    contract = build_mssql_bcp_native_contract(schema=[("value", "int")], query="SELECT synthetic")
    limits = NativeChunkLimits(10000, 10000, max_bytes=16, max_row_bytes=16)
    with pytest.raises(WindowContractError, match="frame_limit"):
        list(native_frames(iter([(1,)]), contract, limits))


@pytest.mark.parametrize("mapping", [False, True])
@pytest.mark.parametrize("sized", [False, True])
def test_frame_file_golden_bytes_digests_and_ordinals(mapping, sized, tmp_path):
    schema = [("id", "int"), ("text", "nvarchar(4) nullable"), ("raw", "varbinary(4) nullable")]
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic")
    rows = [(-2, "A😀", b"\x00\xff"), (0, None, None), (-2, "A😀", b"\x00\xff"), (1, "", b"")]
    if mapping:
        rows = [dict(zip((name for name, _ in schema), row, strict=True)) for row in rows]
    limits = NativeChunkLimits(10000, 10000, max_rows=2, max_bytes=4096, max_row_bytes=1024)
    # Independent wire literals, including NULL/empty distinction and duplicates.
    expected = [
        (
            "feffffff060041003dd800de020000ff00000000ffffffff",
            "22abe6cd3c35eee4546d5d320bc3e7194d585e98c73c11568f8d2be9716d233f",
            "7afd696a351029e0699fda9d5b1549bd6dd1c81c9ca62207ef894e71fc895651",
        ),
        (
            "feffffff060041003dd800de020000ff0100000000000000",
            "d1e032f2034cf60f8d2c5f8eab00d9fbde7cc1524bee66e338d35df66fb74905",
            "ac47f2147fe7292bc6a6208cc2974b805756f6f32ed38790feb591e873b4326e",
        ),
    ]
    framer = sized_native_frames if sized else native_frames
    frames = list(framer(iter(rows), contract, limits))
    assert len(frames) == 2
    for ordinal, (frame, (hex_bytes, file_hash, typed_hash)) in enumerate(zip(frames, expected, strict=True), 7):
        if sized:
            assert frame.encoded_bytes == 24
            frame = frame.rows
        assert type(frame) is tuple
        assert all(type(row) is (dict if mapping else tuple) for row in frame)
        file = encode_native_frame(contract, frame, tmp_path / f"{ordinal}.native", ordinal, 1024, 24)
        assert file.path.read_bytes() == bytes.fromhex(hex_bytes)
        assert (file.ordinal, file.rows, file.encoded_bytes) == (ordinal, 2, 24)
        assert (file.file_sha256, file.typed_digest) == (file_hash, typed_hash)
        verify_native_file(file)


def test_worker_frame_ceiling_still_fails_and_removes_partial_file(tmp_path):
    contract = build_mssql_bcp_native_contract(schema=[("value", "int")], query="SELECT synthetic")
    path = tmp_path / "0.native"
    with pytest.raises(WindowContractError, match="^mssql_native.chunk_bytes_exceeded$"):
        encode_native_frame(contract, ((1,), (2,)), path, 0, 4, 7)
    assert not path.exists()


def test_tuple_adapter_closes_its_sized_iterator(monkeypatch):
    from dpone.runtime import mssql_native_chunks_files as files

    closed = []

    def sized(*args):
        try:
            yield next(sized_native_frames(*args))
        finally:
            closed.append(True)

    monkeypatch.setattr(files, "sized_native_frames", sized)
    contract = build_mssql_bcp_native_contract(schema=[("value", "int")], query="SELECT synthetic")
    limits = NativeChunkLimits(10000, 10000, max_bytes=1024, max_row_bytes=100)
    frames = native_frames(iter([(1,)]), contract, limits)
    assert next(frames) == ((1,),)
    frames.close()
    assert closed == [True]

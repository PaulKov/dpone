"""Cached reservations preserve native framing; local doubles do not certify SQL."""

import pickle
from contextlib import closing
from dataclasses import FrozenInstanceError, replace

import pytest

from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import NativeChunkLimits
from dpone.runtime.mssql_native_chunks_files import encode_native_frame, native_frames
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.mssql_native_sized_frames import SizedNativeFrame, sized_native_frames
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract


def wire(dtype="int"):
    return build_mssql_bcp_native_contract(schema=[("value", dtype)], query="SELECT synthetic")


def limits(**overrides):
    return replace(NativeChunkLimits(10000, 10000, max_bytes=4096, max_row_bytes=2048), **overrides)


def test_frame_envelope_is_frozen_and_pickle_roundtrips():
    frame = SizedNativeFrame(((1,),), 4)
    assert pickle.loads(pickle.dumps(frame, protocol=5)) == frame
    with pytest.raises(FrozenInstanceError):
        frame.encoded_bytes = 8
    with pytest.raises(FrozenInstanceError):
        frame.rows = ((2,),)


@pytest.mark.parametrize("count,expected", [(0, [0]), (1, [1]), (3, [3]), (4, [3, 1]), (6, [3, 3])])
def test_one_shot_source_and_row_count_boundaries(count, expected):
    class OneShot:
        def __init__(self):
            self.iterator = iter((i,) for i in range(count))
            self.iterations = 0

        def __iter__(self):
            self.iterations += 1
            assert self.iterations == 1
            return self

        def __next__(self):
            return next(self.iterator)

    frames = list(sized_native_frames(OneShot(), wire(), limits(max_rows=3)))
    assert [len(frame.rows) for frame in frames] == expected
    assert [frame.encoded_bytes for frame in frames] == [4 * n for n in expected]
    assert [row for frame in frames for row in frame.rows] == [(i,) for i in range(count)]


@pytest.mark.parametrize("mapping", [False, True])
@pytest.mark.parametrize("buffer_type", [bytearray, memoryview])
def test_reused_containers_and_binary_buffers_detach_before_sizing(mapping, buffer_type, monkeypatch):
    buffer = bytearray(b"a")
    value = memoryview(buffer) if buffer_type is memoryview else buffer
    row = {"value": value} if mapping else [value]
    original_size = MssqlNativeEncoder.encoded_row_size

    def size(encoder, snapshot):
        value = snapshot["value"] if mapping else snapshot[0]
        assert type(value) is bytes
        return original_size(encoder, snapshot)

    monkeypatch.setattr(MssqlNativeEncoder, "encoded_row_size", size)

    def source():
        yield row
        buffer[:] = b"b"
        yield row
        row["value" if mapping else 0] = b"changed"
        buffer[:] = b"c"

    frames = list(sized_native_frames(source(), wire("varbinary(max)"), limits(max_rows=1)))
    expected = ({"value": b"a"}, {"value": b"b"}) if mapping else ((b"a",), (b"b",))
    assert tuple(frame.rows[0] for frame in frames) == expected
    assert [frame.encoded_bytes for frame in frames] == [9, 9]


@pytest.mark.parametrize("ceiling,expected", [(4016, [2, 1]), (4015, [1, 1, 1])])
def test_native_frame_exact_bound(ceiling, expected):
    frames = list(sized_native_frames(iter([("a" * 1000,)] * 3), wire("nvarchar(max)"), limits(max_bytes=ceiling)))
    assert [len(frame.rows) for frame in frames] == expected
    assert [frame.encoded_bytes for frame in frames] == [2008 * n for n in expected]


@pytest.mark.parametrize("mapping", [False, True])
@pytest.mark.parametrize("overhead", [0, 37])
@pytest.mark.parametrize("extra_byte", [0, 1])
def test_conservative_ipc_frame_exact_bound(mapping, overhead, extra_byte):
    row = {"value": 1} if mapping else (1,)
    row_ipc = len(pickle.dumps(row, protocol=5)) + 16
    cap = 64 + overhead + 2 * row_ipc - extra_byte
    frames = list(
        sized_native_frames(iter([row] * 3), wire(), limits(max_bytes=cap, max_row_bytes=4), ipc_overhead=overhead)
    )
    assert [len(frame.rows) for frame in frames] == ([2, 1] if extra_byte == 0 else [1, 1, 1])


@pytest.mark.parametrize("overhead", [0, 37])
def test_single_row_pickle_exact_bound(overhead):
    cap = 64 + overhead + len(pickle.dumps((1,), protocol=5)) + 16
    bounds = limits(max_bytes=cap, max_row_bytes=4)
    assert list(sized_native_frames(iter([(1,)]), wire(), bounds, ipc_overhead=overhead))[0].encoded_bytes == 4
    with pytest.raises(WindowContractError, match="^mssql_native.row_exceeds_frame_limit$"):
        list(sized_native_frames(iter([(1,)]), wire(), replace(bounds, max_bytes=cap - 1), ipc_overhead=overhead))


def test_native_row_exact_bound():
    bounds = limits(max_row_bytes=10)
    assert list(sized_native_frames(iter([("a",)]), wire("nvarchar(max)"), bounds))[0].encoded_bytes == 10
    with pytest.raises(ValueError, match="^mssql_native_row_bytes_exceeded$"):
        list(sized_native_frames(iter([("a",)]), wire("nvarchar(max)"), replace(bounds, max_row_bytes=9)))


@pytest.mark.parametrize(
    "dtype,row,error",
    [
        ("int", (), "mssql_native_columns_mismatch"),
        ("int", {"other": 1}, "mssql_native_columns_mismatch"),
        ("int", (None,), "mssql_native_unexpected_null"),
        ("nvarchar(1)", ("aa",), "mssql_native_field_length_exceeded"),
        ("varbinary(max)", ("a",), "mssql_native_invalid_value"),
    ],
)
@pytest.mark.parametrize("framer", [native_frames, sized_native_frames])
def test_framing_error_contract(framer, dtype, row, error):
    with pytest.raises(ValueError, match=f"^{error}$"):
        list(framer(iter([row]), wire(dtype), limits()))


@pytest.mark.parametrize("framer", [native_frames, sized_native_frames])
def test_cancellation_cadence_and_caller_owned_source_closure(framer):
    pulled, checked, closed = [], [], []

    def source():
        try:
            for index in range(2050):
                pulled.append(index)
                yield (index,)
        finally:
            closed.append(True)

    def check():
        checked.append(pulled[-1])
        if len(checked) == 2:
            raise WindowContractError("mssql_native.cancelled_or_lease_lost")

    with closing(source()) as rows:
        with closing(framer(rows, wire(), limits(), check=check)) as frames:
            with pytest.raises(WindowContractError, match="cancelled_or_lease_lost"):
                list(frames)
        assert closed == []  # BoundedNativeChunks.stage owns closing the source.
    assert checked == [0, 1024]
    assert pulled == list(range(1025))
    assert closed == [True]


@pytest.mark.parametrize("framer", [native_frames, sized_native_frames])
def test_early_close_preserves_one_row_lookahead_without_consuming_source(framer):
    pulled = []

    def source():
        for index in range(10):
            pulled.append(index)
            yield (index,)

    with closing(source()) as rows:
        frames = framer(rows, wire(), limits(max_rows=2))
        next(frames)
        assert pulled == [0, 1, 2]
        frames.close()
        assert list(frames) == []
        assert pulled == [0, 1, 2]
        assert next(rows) == (3,)


def _submission(frame, contract, bounds, path, ordinal, total):
    """Local DDA-06 handoff double: retain the scheduler's independent guards."""
    if len(pickle.dumps(frame.rows, protocol=5)) > bounds.max_bytes:
        raise WindowContractError("mssql_native.IPC_frame_limit_exceeded")
    if total + frame.encoded_bytes > bounds.max_total_encoded_bytes:
        raise WindowContractError("mssql_native.total_encoded_bytes_exceeded")
    args = (contract, frame.rows, path, ordinal, bounds.max_row_bytes, frame.encoded_bytes)
    if len(pickle.dumps(args, protocol=5)) + 128 > bounds.max_bytes:
        raise WindowContractError("mssql_native.IPC_task_limit_exceeded")
    return args


@pytest.mark.parametrize("count", [0, 1, 11])
def test_handoff_eliminates_second_sizing_pass_and_preserves_worker_bytes(count, monkeypatch, tmp_path):
    calls = []
    original_size = MssqlNativeEncoder.encoded_row_size

    def size(encoder, row):
        calls.append(row)
        return original_size(encoder, row)

    monkeypatch.setattr(MssqlNativeEncoder, "encoded_row_size", size)
    profile, bounds = wire("nvarchar(max)"), limits(max_rows=3)
    rows = [("A😀" + str(index),) for index in range(count)]
    envelope = (
        profile,
        (),
        tmp_path / f"{bounds.max_staging_tables}.native",
        bounds.max_staging_tables,
        bounds.max_row_bytes,
        bounds.max_bytes,
    )
    overhead = len(pickle.dumps(envelope, protocol=5)) + 128
    encoder = MssqlNativeEncoder(profile, max_row_bytes=bounds.max_row_bytes)
    legacy = [
        SizedNativeFrame(frame, sum(encoder.encoded_row_size(row) for row in frame))
        for frame in native_frames(iter(rows), profile, bounds, ipc_overhead=overhead)
    ]
    assert len(calls) == 2 * count
    calls.clear()
    candidate = list(sized_native_frames(iter(rows), profile, bounds, ipc_overhead=overhead))
    assert candidate == legacy
    total = 0
    for ordinal, frame in enumerate(candidate):
        args = _submission(frame, profile, bounds, tmp_path / f"{ordinal}.native", ordinal, total)
        file = encode_native_frame(*args)
        assert file.encoded_bytes == frame.encoded_bytes
        assert file.path.read_bytes() == b"".join(encoder.encode_row(row) for row in frame.rows)
        total += frame.encoded_bytes
    assert len(calls) == count


def test_handoff_keeps_exact_frame_task_and_cumulative_ceilings(tmp_path):
    profile, frame = wire(), SizedNativeFrame(((1,),), 4)
    path = tmp_path / "0.native"
    args = (profile, frame.rows, path, 0, 4, 4)
    cap = len(pickle.dumps(args, protocol=5)) + 128
    bounds = limits(max_bytes=cap, max_row_bytes=4, max_total_encoded_bytes=4)
    assert _submission(frame, profile, bounds, path, 0, 0) == args
    with pytest.raises(WindowContractError, match="IPC_task_limit_exceeded"):
        _submission(frame, profile, replace(bounds, max_bytes=cap - 1), path, 0, 0)
    with pytest.raises(WindowContractError, match="total_encoded_bytes_exceeded"):
        _submission(frame, profile, bounds, path, 0, 1)
    frame_cap = len(pickle.dumps(frame.rows, protocol=5)) - 1
    with pytest.raises(WindowContractError, match="IPC_frame_limit_exceeded"):
        _submission(frame, profile, replace(bounds, max_bytes=frame_cap), path, 0, 0)


def test_worker_still_rejects_invalid_fixed_width_values_and_cleans_file(tmp_path):
    profile = wire()
    frame = next(sized_native_frames(iter([(2**31,)]), profile, limits()))
    assert frame.encoded_bytes == 4  # Sizing is not value validation.
    path = tmp_path / "0.native"
    with pytest.raises(ValueError, match="^mssql_native_invalid_value$"):
        encode_native_frame(profile, frame.rows, path, 0, 4, frame.encoded_bytes)
    assert not path.exists()

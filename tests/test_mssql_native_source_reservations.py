"""Source reservations reduce scans without changing framing or error authority."""

import pickle
from contextlib import closing
from dataclasses import replace

import pytest

from dpone.contracts.mssql_native_chunks import NativeChunkLimits
from dpone.runtime.mssql_native_chunks_observations import delivery_session
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.mssql_native_sized_frames import sized_native_frames
from dpone.runtime.native_delivery_observations import BoundedNativeDeliveryObserver
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_source_values import native_source_rows


def wire(dtype="nvarchar(max) nullable"):
    return build_mssql_bcp_native_contract(schema=(("value", dtype),), query="source-reservations")


def bounds(**overrides):
    return replace(NativeChunkLimits(100000, 100000, max_bytes=4096, max_row_bytes=1024), **overrides)


def reserved(rows, contract, max_row_bytes):
    from dpone.runtime.sinks.mssql_native_source_values import _sized_native_source_rows

    return _sized_native_source_rows(rows, contract, max_row_bytes)


@pytest.mark.parametrize("observed", [False, True])
@pytest.mark.parametrize("mapping", [False, True])
def test_production_adapter_observer_frame_path_sizes_once_and_preserves_payload(observed, mapping, monkeypatch):
    contract, limits = wire(), bounds(max_rows=2)
    values = [b"a", "中".encode(), None, b"a", b""]
    rows = [{"value": value} if mapping else (value,) for value in values]
    legacy = list(sized_native_frames(native_source_rows(iter(rows), contract, 1024), contract, limits))
    calls = []
    original = MssqlNativeEncoder.encoded_row_size

    def size(encoder, row):
        calls.append(row)
        return original(encoder, row)

    monkeypatch.setattr(MssqlNativeEncoder, "encoded_row_size", size)
    session = delivery_session(BoundedNativeDeliveryObserver() if observed else None)
    source = session.source_rows(iter(rows), lambda stream: reserved(stream, contract, 1024))
    with closing(source):
        actual = list(sized_native_frames(source, contract, limits))
    assert len(calls) == len(rows)
    assert actual == legacy
    assert [pickle.dumps(frame.rows, protocol=5) for frame in actual] == [
        pickle.dumps(frame.rows, protocol=5) for frame in legacy
    ]
    encoder = MssqlNativeEncoder(contract, max_row_bytes=1024)
    for frame in actual:
        assert all(type(row) is tuple for row in frame.rows)
        assert sum(len(encoder.encode_row(row)) for row in frame.rows) == frame.encoded_bytes


@pytest.mark.parametrize("adapter", [native_source_rows, reserved])
@pytest.mark.parametrize(
    "row,dtype,limit,error",
    [
        ((b"a" * 11,), "nvarchar(max)", 28, "mssql_native_row_bytes_exceeded"),
        ((b"\xff",), "nvarchar(max)", 1024, "mssql_native.invalid_utf8_source_text"),
        ((None,), "int", 1024, "mssql_native_unexpected_null"),
        ({"wrong": b"a"}, "nvarchar(max)", 1024, "mssql_native.source_columns_mismatch"),
        ((bytearray(b"a"),), "varbinary(max)", 1024, "mssql_native"),
        ((memoryview(b"a"),), "varbinary(max)", 1024, "mssql_native"),
    ],
)
def test_source_errors_still_precede_cancellation_and_close_source(adapter, row, dtype, limit, error):
    events = []

    def source():
        try:
            yield row
        finally:
            events.append("closed")

    def cancel():
        events.append("checked")
        raise RuntimeError("cancelled")

    contract = wire(dtype)
    with closing(adapter(source(), contract, limit)) as adapted:
        with pytest.raises(ValueError, match=error):
            list(sized_native_frames(adapted, contract, bounds(max_row_bytes=limit), check=cancel))
    assert events == ["closed"]


@pytest.mark.parametrize("consumer", ["limit", "contract"])
def test_mismatched_sizing_context_rechecks_under_consumer_bounds(consumer):
    contract = wire()
    target = wire("nvarchar(1) nullable") if consumer == "contract" else contract
    limits = bounds(max_row_bytes=1024 if consumer == "contract" else 11)
    with closing(reserved(iter([(b"ab",)]), contract, 1024)) as source:
        with pytest.raises(ValueError, match="mssql_native"):
            list(sized_native_frames(source, target, limits))


@pytest.mark.parametrize("adapter", [native_source_rows, reserved])
def test_early_close_after_lookahead_closes_source_once(adapter):
    events = []

    def source():
        try:
            for value in (b"a", b"b", b"c"):
                events.append(value)
                yield (value,)
        finally:
            events.append("closed")

    contract = wire()
    with closing(adapter(source(), contract, 1024)) as adapted:
        with closing(sized_native_frames(adapted, contract, bounds(max_rows=1))) as frames:
            assert next(frames).rows == (("a",),)
            assert events == [b"a", b"b"]
    assert events == [b"a", b"b", "closed"]


def test_empty_reserved_source_preserves_empty_frame_authority():
    contract = wire()
    frames = list(sized_native_frames(reserved(iter(()), contract, 1024), contract, bounds()))
    assert [(frame.rows, frame.encoded_bytes) for frame in frames] == [((), 0)]


@pytest.mark.parametrize("adapter", [native_source_rows, reserved])
def test_invalid_row_at_next_cancellation_boundary_retains_source_error(adapter):
    events = []

    def source():
        try:
            yield from ((1,) for _ in range(1024))
            yield (None,)
        finally:
            events.append("closed")

    def check():
        events.append("checked")
        if len(events) > 1:
            raise RuntimeError("cancelled")

    contract = wire("int")
    with closing(adapter(source(), contract, 1024)) as adapted:
        with pytest.raises(ValueError, match="mssql_native_unexpected_null"):
            list(sized_native_frames(adapted, contract, bounds(), check=check))
    assert events == ["checked", "closed"]


def test_reservation_is_immutable_and_is_not_scalar_validation():
    from dataclasses import FrozenInstanceError

    contract = wire("int")
    row = next(reserved(iter([(2**31,)]), contract, 1024))
    with pytest.raises(FrozenInstanceError):
        row.encoded_bytes = 1
    frame = next(sized_native_frames(iter([row]), contract, bounds()))
    assert frame.encoded_bytes == 4
    with pytest.raises(ValueError, match="mssql_native"):
        MssqlNativeEncoder(contract, max_row_bytes=1024).encode_row(frame.rows[0])

"""Bound native allocations before payload reads and observe consumed bytes once."""

import hashlib
import io
import struct

import pytest

from dpone.runtime.native_wire_mssql import MssqlBcpNativeDecoder, build_mssql_bcp_native_contract


def decoder():
    return MssqlBcpNativeDecoder(build_mssql_bcp_native_contract(schema=[("value", "nvarchar(max)")], query="q"))


def test_bound_rejects_declared_payload_before_allocating_or_reading_it():
    class ObservedFile(io.BytesIO):
        reads = []

        def read(self, size=-1):
            self.reads.append(size)
            return super().read(size)

    stream = ObservedFile(struct.pack("<q", 1000000) + b"x" * 1000000)
    with pytest.raises(ValueError, match="native_wire_row_bytes_exceeded"):
        list(decoder().iter_stream(stream, max_row_bytes=100))
    assert max(stream.reads) <= 8


def test_consumption_hash_counts_prefix_payload_once_and_resets_per_row():
    data = struct.pack("<q", 4) + "ab".encode("utf-16le")
    data *= 2
    digest = hashlib.sha256()
    assert (
        list(decoder().iter_stream(io.BytesIO(data), max_row_bytes=12, on_bytes=digest.update)) == [{"value": "ab"}] * 2
    )
    assert digest.hexdigest() == hashlib.sha256(data).hexdigest()


def test_bound_is_cumulative_across_columns():
    contract = build_mssql_bcp_native_contract(schema=[("a", "bigint"), ("b", "bigint")], query="q")
    with pytest.raises(ValueError, match="native_wire_row_bytes_exceeded"):
        list(MssqlBcpNativeDecoder(contract).iter_stream(io.BytesIO(struct.pack("<qq", 1, 2)), max_row_bytes=15))


@pytest.mark.parametrize("limit", [True, 0, -1, 1.0, None])
def test_invalid_bound_rejected_before_read(limit):
    with pytest.raises(ValueError, match="native_wire_invalid_row_bound"):
        list(decoder().iter_stream(io.BytesIO(b""), max_row_bytes=limit))


@pytest.mark.parametrize("payload_size", [4, 70000])
def test_short_reads_and_seek_probes_preserve_consumed_hash(payload_size):
    class ShortReads(io.BytesIO):
        def read(self, size=-1):
            return super().read(min(size, 3))

    data = struct.pack("<q", payload_size) + b"a\x00" * (payload_size // 2)
    digest = hashlib.sha256()
    rows = list(decoder().iter_stream(ShortReads(data), max_row_bytes=len(data), on_bytes=digest.update))
    assert rows == [{"value": "a" * (payload_size // 2)}]
    assert digest.digest() == hashlib.sha256(data).digest()


@pytest.mark.parametrize("data", [b"\x04\x00", struct.pack("<q", 4) + b"a\x00"])
def test_truncated_input_reports_only_consumed_bytes(data):
    observed = bytearray()
    with pytest.raises(EOFError):
        list(decoder().iter_stream(io.BytesIO(data), max_row_bytes=100, on_bytes=observed.extend))
    assert observed == data


def test_early_close_does_not_observe_unconsumed_next_row():
    row = struct.pack("<q", 2) + b"a\x00"
    observed = bytearray()
    iterator = decoder().iter_stream(io.BytesIO(row * 2), max_row_bytes=len(row), on_bytes=observed.extend)
    assert next(iterator) == {"value": "a"}
    iterator.close()
    assert observed == row

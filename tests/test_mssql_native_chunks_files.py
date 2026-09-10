"""Frame bounds apply to live source objects and serialized IPC independently."""

import pytest

from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import NativeChunkLimits
from dpone.runtime.mssql_native_chunks_files import native_frames
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

"""Runtime policy supplies typed rows without owning file or SDK resources."""

from __future__ import annotations

import builtins
from dataclasses import FrozenInstanceError
from datetime import datetime
from io import BytesIO
from typing import get_type_hints

import pytest

from dpone.adapters.mssql_tds_input import NativeTdsInput
from dpone.contracts.mssql_native_chunks import EncodedNativeFile, TdsInputReceipt
from dpone.ports.mssql_tds_input import TdsRowDecoder
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.mssql_tds_decoder import MssqlTdsRowDecoder
from dpone.runtime.native_wire_models import SourceNativeWireContract
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract


def wire(dtype="bigint"):
    return build_mssql_bcp_native_contract(schema=[("value", dtype)], query="synthetic")


@pytest.mark.parametrize("mode", ["rows", "arrow"])
def test_decoder_preserves_values_bounds_byte_observer_and_stream_ownership(mode):
    contract = build_mssql_bcp_native_contract(
        schema=[("id", "bigint"), ("text", "nvarchar(max) nullable"), ("when", "datetime2(6)"), ("value", "float(53)")],
        query="synthetic",
    )
    row = (2**63 - 1, "😀\0text", datetime(1970, 1, 1, 0, 0, 0, 1), 1.25)
    data = MssqlNativeEncoder(contract, max_row_bytes=1024).encode_row(row)
    source = BytesIO(data)
    observed = []
    decoder = MssqlTdsRowDecoder(contract, input_mode=mode)
    expected = row if mode == "rows" else (row[0], row[1], 1, row[3])
    assert list(decoder.iter_rows(source, max_row_bytes=1024, on_bytes=observed.append)) == [expected]
    assert b"".join(observed) == data
    assert not source.closed and source.tell() == len(data)
    assert decoder.columns == contract.columns and decoder.input_mode == mode


def test_decoder_enforces_row_bound_before_payload_consumption():
    contract = wire("nvarchar(max)")
    data = MssqlNativeEncoder(contract, max_row_bytes=2048).encode_row(("x" * 500,))
    source = BytesIO(data)
    with pytest.raises(ValueError):
        list(
            MssqlTdsRowDecoder(contract, input_mode="rows").iter_rows(source, max_row_bytes=16, on_bytes=lambda _: None)
        )
    assert source.tell() < len(data)
    assert not source.closed


@pytest.mark.parametrize("dtype", ["int", "datetime2(7)", "varbinary(max)"])
def test_unsupported_layout_rejected_at_construction(dtype):
    with pytest.raises(ValueError):
        MssqlTdsRowDecoder(wire(dtype), input_mode="rows")


def test_partial_close_never_closes_caller_stream():
    contract = wire()
    encoded = MssqlNativeEncoder(contract, max_row_bytes=1024).encode_row((1,))
    stream = BytesIO(encoded * 2)
    rows = MssqlTdsRowDecoder(contract, input_mode="rows").iter_rows(
        stream, max_row_bytes=1024, on_bytes=lambda _: None
    )
    assert next(rows) == (1,)
    rows.close()
    assert not stream.closed and stream.tell() == len(encoded)


def test_reflection_resolves_public_boundary_types_without_type_checking_stubs():
    assert get_type_hints(NativeTdsInput.__init__)["file"] is EncodedNativeFile
    assert get_type_hints(NativeTdsInput.__init__)["decoder"] is TdsRowDecoder
    assert get_type_hints(NativeTdsInput.require_complete)["return"] is TdsInputReceipt
    assert get_type_hints(MssqlTdsRowDecoder.__init__)["contract"] is SourceNativeWireContract
    assert get_type_hints(MssqlTdsRowDecoder.iter_rows)
    assert get_type_hints(MssqlTdsRowDecoder.columns.fget)


def test_decoder_policy_binding_is_immutable():
    decoder = MssqlTdsRowDecoder(wire(), input_mode="rows")
    with pytest.raises(FrozenInstanceError):
        decoder._input_mode = "arrow"
    with pytest.raises(FrozenInstanceError):
        decoder.columns[0].name = "different"


def test_decoder_uses_no_optional_sdk(monkeypatch):
    original = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name.split(".")[0] in {"pyarrow", "mssql_python", "mssql_py_core"}:
            raise AssertionError("unexpected optional dependency")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    assert (
        list(
            MssqlTdsRowDecoder(wire(), input_mode="arrow").iter_rows(
                BytesIO(), max_row_bytes=1024, on_bytes=lambda _: None
            )
        )
        == []
    )

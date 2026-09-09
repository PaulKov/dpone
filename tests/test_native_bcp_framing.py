"""Conformance at both BCP binary reader boundaries, including hostile lengths."""

from __future__ import annotations

from dataclasses import replace
from io import BytesIO

import pytest

from dpone.runtime.native_wire_mssql import MssqlBcpNativeDecoder, _read_column, build_mssql_bcp_native_contract


class RecordingStream(BytesIO):
    def __init__(self, payload: bytes, short: bool = False):
        super().__init__(payload)
        self.calls: list[int] = []
        self.short = short

    def read(self, size=-1):
        self.calls.append(size)
        assert size >= 0, "negative read must never reach IO"
        return super().read(min(size, 1) if self.short else size)


def _reader(backend, dtype):
    layout = build_mssql_bcp_native_contract(schema=[("value", dtype)], query="SELECT synthetic").columns[0]
    if backend == "python":
        return lambda stream: _read_column(stream, layout)
    pytest.importorskip("dpone_native_accel")
    from dpone_native_accel._mssql_clickhouse_native import ColumnLayout, _read_cell

    column = ColumnLayout.from_contract(layout.to_dict(), None, binary_encoding="none")
    return lambda stream: _read_cell(stream, column)


@pytest.mark.parametrize("backend", ["python", "accelerated"])
@pytest.mark.parametrize(
    ("dtype", "wire", "prefix_size"),
    [
        ("uniqueidentifier", b"\xff", 1),
        ("uniqueidentifier nullable", b"\xfe", 1),
        ("uniqueidentifier", b"\x0f", 1),
        ("uniqueidentifier", b"\x11", 1),
        ("decimal(10,2)", b"\x12", 1),
        ("decimal(10,2)", b"\x14", 1),
        ("varchar(5)", b"\xfe\xff", 2),
        ("varchar(5)", b"\x15\x00", 2),
        ("nvarchar(5)", b"\x0c\x00", 2),
        ("varbinary(max)", b"\xfe" + b"\xff" * 7, 8),
        ("varbinary(max)", b"\xff" * 7 + b"\x7f", 8),
    ],
)
def test_invalid_lengths_stop_before_payload_read(backend, dtype, wire, prefix_size):
    stream = RecordingStream(wire)
    with pytest.raises((ValueError, EOFError), match="native"):
        _reader(backend, dtype)(stream)
    assert stream.calls == [prefix_size]


@pytest.mark.parametrize("backend", ["python", "accelerated"])
@pytest.mark.parametrize("wire", [b"", b"\x10", b"\x10" + bytes(15)])
def test_truncated_prefix_or_payload_is_never_success(backend, wire):
    with pytest.raises(EOFError, match="native.*eof"):
        _reader(backend, "uniqueidentifier")(RecordingStream(wire))


@pytest.mark.parametrize("backend", ["python", "accelerated"])
def test_short_reads_and_null_empty_distinction(backend):
    assert _reader(backend, "varchar(5) nullable")(RecordingStream(b"\xff\xff")) is None
    empty = _reader(backend, "varchar(5) nullable")(RecordingStream(b"\x00\x00"))
    assert empty in ("", b"")
    value = _reader(backend, "varchar(5)")(RecordingStream(b"\x03\x00abc", short=True))
    assert value in ("abc", b"abc")


@pytest.mark.parametrize("backend", ["python", "accelerated"])
@pytest.mark.parametrize("header", [bytes([9, 2, 1]), bytes([10, 3, 1]), bytes([10, 2, 2])])
def test_decimal_header_must_match_declared_contract(backend, header):
    with pytest.raises(ValueError, match="native.*decimal"):
        _reader(backend, "decimal(10,2)")(BytesIO(b"\x13" + header + bytes(16)))


@pytest.mark.parametrize("backend", ["python", "accelerated"])
def test_decimal_magnitude_must_fit_precision(backend):
    wire = b"\x13\x01\x00\x01" + (10).to_bytes(16, "little")
    with pytest.raises(ValueError, match="native.*decimal"):
        _reader(backend, "decimal(1,0)")(BytesIO(wire))


@pytest.mark.parametrize("mutation", ["empty", "duplicate", "schema_version", "source_format", "hash"])
def test_invalid_contract_rejected_before_open(mutation):
    contract = build_mssql_bcp_native_contract(schema=[("value", "int")], query="SELECT synthetic")
    if mutation == "empty":
        contract = replace(contract, columns=())
    elif mutation == "duplicate":
        contract = replace(contract, columns=contract.columns * 2)
    elif mutation == "hash":
        contract = replace(contract, type_layout_hash="invalid")
    else:
        contract = replace(contract, **{mutation: "unsupported"})
    with pytest.raises(ValueError):
        MssqlBcpNativeDecoder(contract)


@pytest.mark.parametrize(
    "dtype,prefix,width",
    [
        ("bit", 1, 1),
        ("tinyint", 0, 1),
        ("smallint", 0, 2),
        ("int", 0, 4),
        ("bigint", 0, 8),
        ("real", 0, 4),
        ("float", 0, 8),
        ("money", 0, 8),
        ("smallmoney", 0, 4),
        ("date", 0, 3),
        ("datetime", 0, 8),
        ("smalldatetime", 0, 4),
        ("time(0)", 0, 5),
        ("time(3)", 0, 5),
        ("time(7)", 0, 5),
        ("datetime2(0)", 0, 8),
        ("datetime2(3)", 0, 8),
        ("datetime2(7)", 0, 8),
        ("datetimeoffset(0)", 0, 10),
        ("datetimeoffset(3)", 0, 10),
        ("datetimeoffset(7)", 0, 10),
        ("uniqueidentifier", 1, 16),
        ("decimal(10,2)", 1, 19),
        ("numeric(38,9)", 1, 19),
        ("char(8)", 2, None),
        ("varchar(8)", 2, None),
        ("nchar(8)", 2, None),
        ("nvarchar(8)", 2, None),
        ("binary(8)", 2, None),
        ("varbinary(8)", 2, None),
        ("varchar(max)", 8, None),
        ("nvarchar(max)", 8, None),
        ("varbinary(max)", 8, None),
    ],
)
@pytest.mark.parametrize("nullable", [False, True])
def test_all_admitted_type_frames_match_independent_table_and_provider(dtype, prefix, width, nullable):
    schema = [("value", dtype + (" nullable" if nullable else ""))]
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic", target_format="Native")
    if dtype.startswith("char(") and not nullable:
        assert contract.blockers
        with pytest.raises(ValueError):
            MssqlBcpNativeDecoder(contract)
        return
    column = contract.columns[0]
    assert column.prefix_width == (1 if nullable and prefix == 0 else prefix)
    assert column.fixed_length == width
    MssqlBcpNativeDecoder(contract)
    pytest.importorskip("dpone_native_accel")
    from dpone_native_accel._mssql_clickhouse_native import MssqlBcpClickHouseNativeBackend

    MssqlBcpClickHouseNativeBackend.from_request(
        {"artifact_path": "missing.bcp", "native_wire_contract": contract.to_dict(), "bulk_wire_contract": {}}
    )


@pytest.mark.parametrize("backend", ["python", "accelerated"])
@pytest.mark.parametrize(
    "dtype,payload",
    [
        ("time(7)", (86400 * 10**7).to_bytes(5, "little")),
        ("time(0)", b"\x01" + bytes(4)),
        ("datetime2(3)", b"\x01" + bytes(7)),
        ("datetime2(7)", (86400 * 10**7).to_bytes(5, "little") + bytes(3)),
        ("datetimeoffset(7)", bytes(8) + (841).to_bytes(2, "little")),
        ("datetimeoffset(7)", bytes(8) + (-1).to_bytes(2, "little", signed=True)),
        ("date", (3652059).to_bytes(3, "little")),
        ("datetime", bytes(4) + (25920000).to_bytes(4, "little")),
        ("smalldatetime", bytes(2) + (1440).to_bytes(2, "little")),
    ],
)
def test_temporal_out_of_domain_never_wraps_into_valid_value(backend, dtype, payload):
    with pytest.raises(ValueError, match="native_wire_invalid_temporal"):
        _reader(backend, dtype)(BytesIO(payload))


@pytest.mark.parametrize("backend", ["python", "accelerated"])
@pytest.mark.parametrize(
    "dtype,payload",
    [("nvarchar(4)", b"\x01\x00a"), ("nvarchar(4)", b"\x02\x00\x00\xd8"), ("varchar(4)", b"\x01\x00\xff")],
)
def test_malformed_text_diagnostic_does_not_echo_values(backend, dtype, payload):
    with pytest.raises(ValueError, match="native_wire_invalid_text") as caught:
        _reader(backend, dtype)(BytesIO(payload))
    assert "\\xff" not in str(caught.value)
    assert "ordinal=" in str(caught.value)

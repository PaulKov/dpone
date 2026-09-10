"""Lossless native BCP encoder contract; no live server certification."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal, localcontext
from pathlib import Path
from uuid import UUID

import pytest

from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.native_wire_mssql import MssqlBcpNativeDecoder, build_mssql_bcp_native_contract


def contract(dtype: str):
    return build_mssql_bcp_native_contract(schema=[("value", dtype)], query="SELECT value")


@pytest.mark.parametrize(
    ("dtype", "value", "expected"),
    [
        ("int", -2, bytes.fromhex("feffffff")),
        ("bit", True, bytes.fromhex("0101")),
        ("int nullable", None, b"\xff"),
        ("nvarchar(4)", "A😀", bytes.fromhex("060041003dd800de")),
        ("nvarchar(max) nullable", "", b"\x00" * 8),
        ("nvarchar(max) nullable", None, b"\xff" * 8),
        ("varbinary(4)", b"\x00\xff", b"\x02\x00\x00\xff"),
        (
            "uniqueidentifier",
            UUID("00112233-4455-6677-8899-aabbccddeeff"),
            bytes.fromhex("1033221100554477668899aabbccddeeff"),
        ),
        ("decimal(20,0)", 2**64 - 1, b"\x13\x14\x00\x01" + b"\xff" * 8 + b"\x00" * 8),
        ("decimal(5,2)", Decimal("-12.34"), b"\x13\x05\x02\x00\xd2\x04" + b"\x00" * 14),
        ("date", date(1, 1, 2), b"\x01\x00\x00"),
        ("datetime2(7)", datetime(1, 1, 1, 0, 0, 0, 1), b"\x0a" + b"\x00" * 7),
    ],
)
def test_golden_bytes(dtype, value, expected):
    assert MssqlNativeEncoder(contract(dtype), max_row_bytes=1024).encode_row([value]) == expected


@pytest.mark.parametrize(
    ("dtype", "value"),
    [
        ("bigint", -(2**63)),
        ("bigint", 2**63 - 1),
        ("tinyint", 255),
        ("decimal(38,9)", Decimal("12345678901234567890123456789.123456789")),
        ("nvarchar(20)", "\\N\t\r\n😀"),
        ("varchar(20)", "NULL"),
        ("real", 1.5),
        ("float", -1.123456789),
        ("time(6)", time(23, 59, 59, 123456)),
        ("datetime2(6)", datetime.max),
        ("money", Decimal("-922337203685477.5808")),
        ("smallmoney", Decimal("214748.3647")),
    ],
)
def test_decoder_roundtrip(dtype, value, tmp_path: Path):
    profile = contract(dtype)
    path = tmp_path / "chunk.bcp"
    with localcontext() as ctx:
        ctx.prec = 4
        path.write_bytes(MssqlNativeEncoder(profile, max_row_bytes=1024).encode_row({"value": value}))
        assert list(MssqlBcpNativeDecoder(profile).iter_rows(path)) == [{"value": value}]


@pytest.mark.parametrize(
    ("dtype", "value"),
    [
        ("int", None),
        ("int", 1.5),
        ("int", True),
        ("int", 2**31),
        ("tinyint", -1),
        ("bigint", 2**63),
        ("bit", 2),
        ("decimal(3,2)", Decimal("1.234")),
        ("decimal(3,2)", Decimal("10")),
        ("decimal(38,2)", Decimal("1e999999999")),
        ("decimal(38,2)", Decimal("NaN")),
        ("decimal(10,2)", 1.25),
        ("real", 0.1),
        ("float", float("inf")),
        ("float", float("nan")),
        ("nvarchar(1)", "😀"),
        ("nvarchar(max)", b"abc"),
        ("binary(4)", b"a"),
        ("nchar(4)", "a"),
        ("datetime2(3)", datetime(2020, 1, 1, microsecond=1)),
        ("datetime", datetime(2020, 1, 1, microsecond=1)),
        ("date", datetime(2020, 1, 1)),
        ("uniqueidentifier", "bad"),
    ],
)
def test_reject_loss_or_invalid_values(dtype, value):
    with pytest.raises(ValueError, match="mssql_native"):
        MssqlNativeEncoder(contract(dtype), max_row_bytes=1024).encode_row([value])


def test_bound_includes_prefix_and_all_columns():
    profile = build_mssql_bcp_native_contract(schema=[("a", "int"), ("b", "nvarchar(max)")], query="q")
    encoder = MssqlNativeEncoder(profile, max_row_bytes=14)
    assert len(encoder.encode_row([1, "x"])) == 14
    with pytest.raises(ValueError, match="row_bytes"):
        encoder.encode_row([1, "xx"])
    for row in ([1], [1, "x", 3], {"a": 1, "b": "x", "extra": 2}):
        with pytest.raises(ValueError, match="columns"):
            encoder.encode_row(row)


@pytest.mark.parametrize("limit", [True, 0, -1, 1.5])
def test_invalid_row_limit(limit):
    with pytest.raises(ValueError, match="max_row_bytes"):
        MssqlNativeEncoder(contract("int"), max_row_bytes=limit)


@pytest.mark.parametrize(
    "dtype,value",
    [
        ("int", 1),
        ("int nullable", None),
        ("nvarchar(max)", "😀\x00é"),
        ("decimal(38,0)", Decimal("9" * 38)),
        ("varbinary(max)", b"\x00\xff"),
    ],
)
def test_allocation_free_size_agrees_with_encoding(dtype, value):
    encoder = MssqlNativeEncoder(contract(dtype), max_row_bytes=1024)
    assert encoder.encoded_row_size([value]) == len(encoder.encode_row([value]))


def test_size_enforces_prefix_budget_and_shape():
    encoder = MssqlNativeEncoder(contract("nvarchar(max) nullable"), max_row_bytes=10)
    assert encoder.encoded_row_size([None]) == 8
    assert encoder.encoded_row_size(["é"]) == 10
    for value in (["😀"], [], ["a", "b"]):
        with pytest.raises(ValueError, match="mssql_native"):
            encoder.encoded_row_size(value)


def test_datetimeoffset_golden_and_remainder_roundtrip(tmp_path):
    from datetime import timedelta, timezone

    profile = contract("datetimeoffset(7)")
    encoder = MssqlNativeEncoder(profile, max_row_bytes=100)
    # UTC day 0 at 00:00:00.0000001, represented locally at +01:00.
    golden = bytes.fromhex("01000000000000003c00")
    path = tmp_path / "ticks.bcp"
    path.write_bytes(golden)
    decoded = list(MssqlBcpNativeDecoder(profile).iter_rows(path))[0]["value"]
    assert decoded.utcoffset() == timedelta(hours=1)
    assert decoded.submicrosecond_100ns == 1
    assert encoder.encode_row([decoded]) == golden
    aware = datetime(1, 1, 1, 1, tzinfo=timezone(timedelta(hours=1)))
    assert encoder.encode_row([aware]) == bytes.fromhex("00000000000000003c00")


@pytest.mark.parametrize("dtype", ["time(7)", "datetime2(7)"])
def test_seventh_digit_from_decoder_is_preserved(dtype, tmp_path):
    profile = contract(dtype)
    data = b"\x09" + b"\x00" * (4 if dtype.startswith("time(") else 7)
    path = tmp_path / "fraction.bcp"
    path.write_bytes(data)
    value = list(MssqlBcpNativeDecoder(profile).iter_rows(path))[0]["value"]
    assert value.submicrosecond_100ns == 9
    assert MssqlNativeEncoder(profile, max_row_bytes=100).encode_row([value]) == data


@pytest.mark.parametrize(
    "dtype,value",
    [
        ("varchar(2)", "😀"),
        ("nvarchar(2)", "\ud800"),
        ("decimal(2,1)", Decimal("0.01")),
        ("smallmoney", Decimal("214748.3648")),
        ("money", Decimal("922337203685477.5808")),
    ],
)
def test_additional_value_boundaries(dtype, value):
    with pytest.raises(ValueError, match="mssql_native"):
        MssqlNativeEncoder(contract(dtype), max_row_bytes=100).encode_row([value])

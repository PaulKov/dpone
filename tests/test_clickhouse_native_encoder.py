from __future__ import annotations

import struct
import uuid
from datetime import date, datetime
from decimal import Decimal

import pytest

from dpone.runtime.clickhouse_native import ClickHouseNativeEncoder


def _var_uint(value: int) -> bytes:
    output = bytearray()
    current = int(value)
    while current >= 0x80:
        output.append((current & 0x7F) | 0x80)
        current >>= 7
    output.append(current)
    return bytes(output)


def _string(value: str | bytes) -> bytes:
    payload = value if isinstance(value, bytes) else value.encode("utf-8")
    return _var_uint(len(payload)) + payload


def test_clickhouse_native_encoder_writes_columnar_block_with_nullable_values() -> None:
    encoder = ClickHouseNativeEncoder(
        [("id", "int"), ("payload", "nvarchar(max) nullable"), ("amount", "decimal(10,2)")],
        target_schema=[
            ("id", "Int32"),
            ("payload", "Nullable(String)"),
            ("amount", "Decimal(10, 2)"),
        ],
        block_rows=2,
    )

    payload = b"".join(
        encoder.iter_batches(
            [
                {"id": 1, "payload": "a\tb", "amount": Decimal("12.34")},
                {"id": 2, "payload": None, "amount": Decimal("-0.01")},
            ]
        )
    )

    expected = (
        _var_uint(3)
        + _var_uint(2)
        + _string("id")
        + _string("Int32")
        + struct.pack("<ii", 1, 2)
        + _string("payload")
        + _string("Nullable(String)")
        + b"\x00\x01"
        + _string("a\tb")
        + _string("")
        + _string("amount")
        + _string("Decimal(10, 2)")
        + struct.pack("<q", 1234)
        + struct.pack("<q", -1)
    )
    assert payload == expected


def test_clickhouse_native_encoder_splits_blocks_by_row_limit() -> None:
    encoder = ClickHouseNativeEncoder(
        [("id", "int")],
        target_schema=[("id", "Int32")],
        block_rows=1,
    )

    chunks = list(encoder.iter_batches([{"id": 1}, {"id": 2}]))

    assert chunks == [
        _var_uint(1) + _var_uint(1) + _string("id") + _string("Int32") + struct.pack("<i", 1),
        _var_uint(1) + _var_uint(1) + _string("id") + _string("Int32") + struct.pack("<i", 2),
    ]


def test_clickhouse_native_encoder_preserves_decimal128_precision() -> None:
    encoder = ClickHouseNativeEncoder(
        [("amount", "decimal(38,9)")],
        target_schema=[("amount", "Decimal(38, 9)")],
    )

    payload = b"".join(encoder.iter_batches([{"amount": Decimal("12345678901234567890.123456789")}]))

    expected_scaled = 12_345_678_901_234_567_890_123_456_789
    assert expected_scaled.to_bytes(16, byteorder="little", signed=True) in payload


def test_clickhouse_native_encoder_preserves_decimal_extremes() -> None:
    decimal128_max = Decimal("99999999999999999999999999999.999999999")
    decimal128_min = Decimal("-99999999999999999999999999999.999999999")
    decimal256_max = Decimal("99999999999999999999999999999999999999.99999999999999999999999999999999999999")
    decimal256_min = Decimal("-99999999999999999999999999999999999999.99999999999999999999999999999999999999")

    payload = b"".join(
        ClickHouseNativeEncoder(
            [("d128", "decimal(38,9)"), ("d256", "decimal(76,38)")],
            target_schema=[("d128", "Decimal(38, 9)"), ("d256", "Decimal(76, 38)")],
        ).iter_batches(
            [
                {"d128": decimal128_max, "d256": decimal256_max},
                {"d128": decimal128_min, "d256": decimal256_min},
            ]
        )
    )

    scaled128 = 10**38 - 1
    scaled256 = 10**76 - 1
    assert scaled128.to_bytes(16, byteorder="little", signed=True) in payload
    assert (-scaled128).to_bytes(16, byteorder="little", signed=True) in payload
    assert scaled256.to_bytes(32, byteorder="little", signed=True) in payload
    assert (-scaled256).to_bytes(32, byteorder="little", signed=True) in payload


def test_clickhouse_native_encoder_preserves_float_extremes() -> None:
    payload = b"".join(
        ClickHouseNativeEncoder(
            [("f32", "real"), ("f64", "float")],
            target_schema=[("f32", "Float32"), ("f64", "Float64")],
        ).iter_batches(
            [
                {"f32": 3.4028234663852886e38, "f64": 1.7976931348623157e308},
                {"f32": -3.4028234663852886e38, "f64": -1.7976931348623157e308},
            ]
        )
    )

    assert struct.pack("<f", 3.4028234663852886e38) in payload
    assert struct.pack("<f", -3.4028234663852886e38) in payload
    assert struct.pack("<d", 1.7976931348623157e308) in payload
    assert struct.pack("<d", -1.7976931348623157e308) in payload


def test_clickhouse_native_encoder_covers_supported_scalar_type_matrix() -> None:
    row = {
        "flag": True,
        "i8": -128,
        "u8": 255,
        "i16": -32_768,
        "u16": 65_535,
        "i32": -2_147_483_648,
        "u32": 4_294_967_295,
        "i64": -9_223_372_036_854_775_808,
        "u64": 18_446_744_073_709_551_615,
        "f32": 1.25,
        "f64": -2.5,
        "s": "hello",
        "fs": "xy",
        "d": date(1970, 1, 2),
        "d32": date(1900, 1, 1),
        "dt": datetime(1970, 1, 1, 0, 0, 1),
        "dt64": datetime(1970, 1, 1, 0, 0, 1, 123456),
    }

    payload = b"".join(
        ClickHouseNativeEncoder(
            [(name, "ignored") for name in row],
            target_schema=[
                ("flag", "Bool"),
                ("i8", "Int8"),
                ("u8", "UInt8"),
                ("i16", "Int16"),
                ("u16", "UInt16"),
                ("i32", "Int32"),
                ("u32", "UInt32"),
                ("i64", "Int64"),
                ("u64", "UInt64"),
                ("f32", "Float32"),
                ("f64", "Float64"),
                ("s", "String"),
                ("fs", "FixedString(4)"),
                ("d", "Date"),
                ("d32", "Date32"),
                ("dt", "DateTime"),
                ("dt64", "DateTime64(6)"),
            ],
        ).iter_batches([row])
    )

    assert struct.pack("<B", 1) in payload
    assert struct.pack("<b", -128) in payload
    assert struct.pack("<B", 255) in payload
    assert struct.pack("<h", -32_768) in payload
    assert struct.pack("<H", 65_535) in payload
    assert struct.pack("<i", -2_147_483_648) in payload
    assert struct.pack("<I", 4_294_967_295) in payload
    assert struct.pack("<q", -9_223_372_036_854_775_808) in payload
    assert struct.pack("<Q", 18_446_744_073_709_551_615) in payload
    assert struct.pack("<f", 1.25) in payload
    assert struct.pack("<d", -2.5) in payload
    assert _string("hello") in payload
    assert b"xy\x00\x00" in payload
    assert struct.pack("<H", 1) in payload
    assert struct.pack("<i", (date(1900, 1, 1) - date(1970, 1, 1)).days) in payload
    assert struct.pack("<I", 1) in payload
    assert struct.pack("<q", 1_123_456) in payload


def test_clickhouse_native_encoder_rejects_oversized_fixed_string() -> None:
    encoder = ClickHouseNativeEncoder(
        [("fs", "ignored")],
        target_schema=[("fs", "FixedString(2)")],
    )

    with pytest.raises(ValueError, match="FixedString\\(2\\) value is too long"):
        list(encoder.iter_batches([{"fs": "abcd"}]))


def test_clickhouse_native_encoder_preserves_temporal_uuid_and_binary_values() -> None:
    guid = uuid.UUID("12345678-1234-5678-9abc-def012345678")
    encoder = ClickHouseNativeEncoder(
        [("d", "date"), ("ts", "datetime2(7)"), ("guid", "uniqueidentifier"), ("raw", "varbinary(max)")],
        target_schema=[
            ("d", "Date"),
            ("ts", "DateTime64(7)"),
            ("guid", "UUID"),
            ("raw", "String"),
        ],
    )

    payload = b"".join(
        encoder.iter_batches(
            [
                {
                    "d": date(2026, 6, 22),
                    "ts": datetime(2026, 6, 22, 13, 14, 15, 123456),
                    "guid": str(guid),
                    "raw": b"\x00\xff",
                }
            ]
        )
    )

    assert _string("Date") in payload
    assert struct.pack("<H", (date(2026, 6, 22) - date(1970, 1, 1)).days) in payload
    assert struct.pack("<q", 17821340551234560) in payload
    assert guid.bytes[:8][::-1] + guid.bytes[8:][::-1] in payload
    assert _string(b"\x00\xff") in payload

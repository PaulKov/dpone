"""Bounded text acceleration preserves bytes and first-error semantics."""

from dataclasses import replace

import pytest

from dpone.runtime import mssql_native_encoder_values as values
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract


def layout(dtype):
    return build_mssql_bcp_native_contract(schema=[("value", dtype)], query="q").columns[0]


def outcome(call):
    try:
        return ("bytes", call())
    except (ValueError, TypeError, UnicodeError) as error:
        return (type(error).__name__, str(error))


@pytest.mark.parametrize("dtype", ["varchar(8)", "nvarchar(8)", "varchar(max)", "nvarchar(max)"])
@pytest.mark.parametrize(
    "value", ["", "ascii", "é漢", "😀", "e\u0301", "\x00\t\r\n", "x" * 9, "\ud800", "xx\udfff", "\ud800xxx", b"x", 1]
)
@pytest.mark.parametrize("remaining", [0, 1, 2, 3, 4, 8, 16, 128])
def test_text_matches_original_size_then_encode(dtype, value, remaining, monkeypatch):
    column = layout(dtype)
    actual = outcome(lambda: values.encode_native_value(value, column, remaining))

    def original(value, column, remaining):
        values.native_value_size(value, column, remaining)
        return value if column.storage_type in {"binary", "varbinary"} else value.encode(column.encoding or "utf-8")

    monkeypatch.setattr(values, "_variable", original)
    assert actual == outcome(lambda: values.encode_native_value(value, column, remaining))


@pytest.mark.parametrize(
    "dtype,expected", [("varchar(8)", b"A\xf0\x9f\x98\x80"), ("nvarchar(8)", bytes.fromhex("41003dd800de"))]
)
def test_small_text_avoids_python_character_scan(dtype, expected, monkeypatch):
    def unexpected_scan(*args, **kwargs):
        pytest.fail("bounded native encoding must not scan text twice")

    monkeypatch.setattr(values, "_character_width", unexpected_scan)
    assert values.encode_native_value("A😀", layout(dtype), 64) == expected


def test_sizing_remains_allocation_free(monkeypatch):
    monkeypatch.setattr(values, "_variable", lambda *args: pytest.fail("sizing allocated payload"))
    assert values.native_value_size("A😀", layout("nvarchar(8)"), 64) == 6


def test_small_prefix_overflow_is_still_rejected():
    column = replace(layout("varchar(max)"), prefix_width=1)
    with pytest.raises(ValueError, match="^mssql_native_field_length_exceeded$"):
        values.encode_native_value("a" * 128, column, 1024)


def test_string_subclass_is_still_rejected():
    class Text(str):
        pass

    with pytest.raises(ValueError, match="^mssql_native_invalid_value$"):
        values.encode_native_value(Text("a"), layout("nvarchar(8)"), 64)


@pytest.mark.parametrize(
    "dtype,value,remaining,code",
    [
        ("nvarchar(1)", "😀", 16, "field_length_exceeded"),
        ("nvarchar(1)", "😀", 3, "row_bytes_exceeded"),
        ("varchar(1)", "é", 8, "field_length_exceeded"),
        ("varchar(1)", "é", 1, "row_bytes_exceeded"),
        ("nvarchar(1)", "xx\ud800", 2, "row_bytes_exceeded"),
        ("nvarchar(1)", "xx\ud800", 16, "invalid_value"),
        ("nvarchar(1)", "\ud800xx", 2, "invalid_value"),
    ],
)
def test_literal_error_precedence(dtype, value, remaining, code):
    with pytest.raises(ValueError, match=f"^mssql_native_{code}$"):
        values.encode_native_value(value, layout(dtype), remaining)

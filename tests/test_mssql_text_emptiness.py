"""SQL-shape and local-wire regressions; these tests do not execute SQL Server."""

from __future__ import annotations

from io import BytesIO

import pytest

from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec, iter_rows
from dpone.runtime.connectors.clickhouse_tsv_codec import ClickHouseTabSeparatedCodec


@pytest.mark.parametrize(
    "value_sql",
    ["src.[text]", "src.[odd]] name]", "CAST(N'    ' AS NCHAR(4))", "CAST('    ' AS CHAR(4))"],
)
def test_bulk_encode_measures_the_actual_serialized_unicode_text(value_sql: str) -> None:
    expression = BulkTextCodec().mssql_encode_expression(value_sql)
    converted = f"CONVERT(NVARCHAR(MAX), {value_sql})"

    assert f"WHEN DATALENGTH({converted}) = 0 THEN" in expression
    assert f"CASE WHEN {value_sql} IS NULL THEN NULL" in expression
    assert f"WHEN {value_sql} = N''" not in expression
    assert "LEN(" not in expression
    assert "TRIM(" not in expression
    assert f"REPLACE({converted}," in expression


@pytest.mark.parametrize("source_type", ["char(4)", "nchar(4)", "varchar(max)", "nvarchar(max)"])
def test_tabseparated_encode_measures_pre_escape_utf8_text(source_type: str) -> None:
    value_sql = "src.[odd]] name]"
    expression = ClickHouseTabSeparatedCodec().mssql_select_expression(
        value_sql, text_column=True, source_type=source_type
    )
    converted = f"CONVERT(VARCHAR(MAX), CONVERT(NVARCHAR(MAX), {value_sql}) COLLATE Latin1_General_100_CI_AS_SC_UTF8)"

    assert f"WHEN DATALENGTH({converted}) = 0 THEN N'__dpone__tsv__empty'" in expression
    assert f"CASE WHEN {value_sql} IS NULL THEN N'\\N'" in expression
    assert f"WHEN {value_sql} = N''" not in expression
    assert "LEN(" not in expression
    assert "TRIM(" not in expression
    assert f"REPLACE({converted}," in expression


@pytest.mark.parametrize(
    ("codec", "marker_sql"),
    [
        (BulkTextCodec(), "NCHAR(29) + N'E'"),
        (BulkTextCodec(empty_string_marker="blank's end"), "N'blank''s end'"),
        (BulkTextCodec(empty_string_marker="\x1dE "), "NCHAR(29) + N'E '"),
    ],
)
def test_bulk_decode_requires_equal_normalized_length_and_binary_marker_identity(
    codec: BulkTextCodec, marker_sql: str
) -> None:
    value_sql = "raw.[odd]] text]"
    expression = codec.mssql_decode_expression(value_sql)
    value = f"CONVERT(NVARCHAR(MAX), {value_sql})"
    marker = f"CONVERT(NVARCHAR(MAX), ({marker_sql}))"
    collation = "Latin1_General_100_BIN2"

    assert (
        f"WHEN DATALENGTH({value}) = DATALENGTH({marker}) "
        f"AND {value} COLLATE {collation} = {marker} COLLATE {collation} THEN N''"
    ) in expression
    assert f"WHEN CHARINDEX(NCHAR(29), ({value_sql}) COLLATE {collation}) = 0 THEN {value_sql}" in expression
    assert f"REPLACE({value_sql}," in expression
    assert "LEN(" not in expression
    assert "TRIM(" not in expression


# Expected bytes are authored independently of the production encoder.
_TEXT_WIRE_CASES = [
    (None, b""),
    ("", b"\x1dE"),
    (" ", b" "),
    ("    ", b"    "),
    (" leading", b" leading"),
    ("trailing ", b"trailing "),
    (" both ", b" both "),
    ("Юникод", "Юникод".encode()),
    ("😀 ", "😀 ".encode()),
    ("\x1dE", b"\x1dPE"),
    ("\x1dE ", b"\x1dPE "),
    ("\x1dE   ", b"\x1dPE   "),
    ("a\tb\nc\rd", b"a\x1dTb\x1dNc\x1dRd"),
    ("\\N", b"\\N"),
]


@pytest.mark.parametrize(("value", "wire"), _TEXT_WIRE_CASES)
def test_bulk_text_wire_retains_null_empty_spaces_and_literal_markers(value: str | None, wire: bytes) -> None:
    codec = BulkTextCodec()
    if value is not None:
        assert codec.encode(value).encode("utf-8") == wire
    stream = BytesIO(wire + b"\n")

    assert list(iter_rows(stream, (("text", "nvarchar(max)"),), codec)) == [(value,)]


@pytest.mark.parametrize("suffix", ["", " ", "   ", "😀 "])
def test_bulk_literal_custom_empty_marker_roundtrips_with_trailing_text(suffix: str) -> None:
    codec = BulkTextCodec(empty_string_marker="\x1dblank")
    literal = codec.empty_string_marker + suffix

    assert codec.encode(literal) == "\x1dPblank" + suffix
    assert codec.decode(codec.encode(literal)) == literal
    assert codec.decode(codec.empty_string_marker) == ""


@pytest.mark.parametrize("source_type", ["int", "decimal(18,2)", "datetime2(7)", "varbinary(max)"])
def test_tabseparated_nontext_projection_keeps_its_existing_null_branch(source_type: str) -> None:
    expression = ClickHouseTabSeparatedCodec().mssql_select_expression(
        "src.[value]", text_column=False, source_type=source_type
    )

    assert expression.startswith("CASE WHEN src.[value] IS NULL THEN N'\\N' ELSE ")
    assert "DATALENGTH(" not in expression
    assert "N'__dpone__tsv__empty'" not in expression

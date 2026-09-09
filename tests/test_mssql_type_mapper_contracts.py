"""MSSQL DDL type mapping contracts, including the full ClickHouse inventory.

Policy under test (docs/type-mapping-matrix.md):

- every ClickHouse 24.8 type is either **mapped** width/precision-exact or
  **explicitly unsupported** with an actionable workaround — never a silent
  fallback;
- every mapped type round-trips through
  ``dpone.readiness.schema_type_compatibility.types_equal`` (idempotent
  chunk re-runs);
- non-ClickHouse dialects keep the documented ``nvarchar(max)`` safe-fallback
  with a warning.
"""

from __future__ import annotations

import pytest

from dpone.readiness.schema_type_compatibility import types_equal
from dpone.runtime.support.mssql_lossless_projection import resolved_source_type
from dpone.runtime.support.mssql_types import MSSQLTypeMapper, MSSQLTypeMappingError
from dpone.type_system.source_sink.clickhouse_mssql import classify_clickhouse_type

CLICKHOUSE_MAPPED = {
    # Integers: width-exact.
    "Int8": "smallint",
    "Int16": "smallint",
    "Int32": "int",
    "Int64": "bigint",
    "UInt8": "tinyint",
    "UInt16": "int",
    "UInt32": "bigint",
    "UInt64": "decimal(20,0)",
    # Floats.
    "Float32": "real",
    "Float64": "float",
    "BFloat16": "real",
    # Decimals: precision/scale exact up to MSSQL decimal(38).
    "Decimal(9, 2)": "decimal(9,2)",
    "Decimal(38, 10)": "decimal(38,10)",
    "Decimal32(2)": "decimal(9,2)",
    "Decimal64(4)": "decimal(18,4)",
    "Decimal128(10)": "decimal(38,10)",
    # Strings.
    "String": "nvarchar(max)",
    "FixedString(16)": "nvarchar(16)",
    # Temporal.
    "Date": "date",
    "Date32": "date",
    "DateTime": "datetime2(0)",
    "DateTime32": "datetime2(0)",
    "DateTime('Europe/Moscow')": "datetime2(0)",
    "DateTime64": "datetime2(3)",
    "DateTime64(6)": "datetime2(6)",
    "DateTime64(6, 'UTC')": "datetime2(6)",
    "DateTime64(9)": "datetime2(7)",  # documented sub-100ns truncation
    # Scalar specials.
    "Bool": "bit",
    "UUID": "uniqueidentifier",
    "Enum8('red' = 1, 'green' = 2)": "nvarchar(max)",
    "Enum16('a' = 1)": "nvarchar(max)",
    "IPv4": "varchar(15)",
    "IPv6": "varchar(45)",
    # Wrappers and their compositions.
    "Nullable(Int32)": "int",
    "Nullable(String)": "nvarchar(max)",
    "LowCardinality(String)": "nvarchar(max)",
    "LowCardinality(Nullable(String))": "nvarchar(max)",
    "Nullable(LowCardinality(FixedString(16)))": "nvarchar(16)",
    "Nullable(DateTime64(6, 'UTC'))": "datetime2(6)",
    "Nullable(Decimal(20, 4))": "decimal(20,4)",
    "SimpleAggregateFunction(sum, Int64)": "bigint",
    "SimpleAggregateFunction(max, Nullable(DateTime))": "datetime2(0)",
}


def test_clickhouse_source_resolution_preserves_case_sensitive_integer_semantics() -> None:
    assert resolved_source_type("UInt8") == "tinyint"
    assert resolved_source_type("Int8") == "smallint"


CLICKHOUSE_UNSUPPORTED = [
    # Huge integers exceed decimal(38,0).
    "Int128",
    "Int256",
    "UInt128",
    "UInt256",
    # Decimals above MSSQL precision.
    "Decimal(39, 4)",
    "Decimal256(20)",
    # Composite/experimental containers (extraction emits Python literals).
    "Array(Int32)",
    "Array(Nullable(String))",
    "Tuple(Int32, String)",
    "Map(String, Array(Int32))",
    "Nested(id Int32, name String)",
    "JSON",
    "Object('json')",
    "Variant(Int32, String)",
    "Dynamic",
    # Geo.
    "Point",
    "Ring",
    "Polygon",
    "MultiPolygon",
    "LineString",
    "MultiLineString",
    # Engine-internal / non-storable.
    "AggregateFunction(sum, Int64)",
    "IntervalSecond",
    "IntervalMonth",
    "Nothing",
    # No exact MSSQL mapping in this route.
    "Time",
    "Time64(3)",
    # Wrappers around unsupported types stay unsupported.
    "Nullable(Array(Int32))",
    "LowCardinality(Nullable(UInt256))",
]

GENERIC_MAPPED = {
    "integer": "int",
    "bigint": "bigint",
    "text": "nvarchar(max)",
    "timestamp without time zone": "datetime2",
    "timestamp with time zone": "datetimeoffset",
    "numeric(18,4)": "numeric(18,4)",
    "numeric(18, 4)": "numeric(18,4)",
    "varchar(50)": "varchar(50)",
    "datetime2(7)": "datetime2(7)",
    "datetimeoffset(7)": "datetimeoffset(7)",
    "datetimeoffset": "datetimeoffset",
    "uuid": "uniqueidentifier",
    "uniqueidentifier": "uniqueidentifier",
    "jsonb": "nvarchar(max)",
    "int8": "bigint",  # PostgreSQL alias; distinct from ClickHouse Int8
    # Extract schemas append " nullable"; DDL must strip it (mssql→mssql identity).
    "bit nullable": "bit",
    "real nullable": "real",
    "uniqueidentifier nullable": "uniqueidentifier",
    "datetime2(3) nullable": "datetime2(3)",
    "nvarchar(255) nullable": "nvarchar(255)",
    "varbinary(16) nullable": "varbinary(16)",
    "decimal(18,4) nullable": "decimal(18,4)",
}


@pytest.mark.parametrize(("source_type", "expected"), sorted(CLICKHOUSE_MAPPED.items()))
def test_clickhouse_types_map_to_exact_mssql_ddl(source_type: str, expected: str) -> None:
    assert MSSQLTypeMapper.to_mssql(source_type) == expected


@pytest.mark.parametrize(("source_type", "expected"), sorted(CLICKHOUSE_MAPPED.items()))
def test_clickhouse_mapped_types_round_trip_through_schema_compatibility(source_type: str, expected: str) -> None:
    """A freshly created MSSQL target must not report breaking type changes
    against the same ClickHouse source schema on the next chunk."""

    assert types_equal(source_type, expected), f"{source_type} -> {expected} must compare as equal"
    assert types_equal(source_type, f"{expected} nullable"), f"{source_type} vs nullable target must stay equal"


@pytest.mark.parametrize("source_type", CLICKHOUSE_UNSUPPORTED)
def test_unsupported_clickhouse_types_raise_actionable_configuration_errors(source_type: str) -> None:
    with pytest.raises(MSSQLTypeMappingError, match="Workaround") as excinfo:
        MSSQLTypeMapper.to_mssql(source_type)
    message = str(excinfo.value)
    assert "not supported by the MSSQL sink" in message
    assert "on the source" in message or "projection" in message


@pytest.mark.parametrize("source_type", CLICKHOUSE_UNSUPPORTED)
def test_unsupported_clickhouse_types_are_classified_not_silently_fallen_back(source_type: str) -> None:
    decision = classify_clickhouse_type(source_type)
    assert decision is not None, f"{source_type} must be recognized as ClickHouse dialect"
    assert decision.supported is False
    assert decision.reason and decision.workaround


@pytest.mark.parametrize(("source_type", "expected"), sorted(GENERIC_MAPPED.items()))
def test_generic_dialect_mappings_stay_deterministic(source_type: str, expected: str) -> None:
    assert MSSQLTypeMapper.to_mssql(source_type) == expected


def test_unknown_generic_type_uses_documented_safe_fallback_with_warning(caplog) -> None:
    with caplog.at_level("WARNING", logger="dpone.runtime.support.mssql_types"):
        assert MSSQLTypeMapper.to_mssql("tsvector") == "nvarchar(max)"
    assert any("safe-fallback" in record.message for record in caplog.records)


def test_classifier_ignores_non_clickhouse_spellings() -> None:
    for generic in ("integer", "text", "nvarchar(510)", "timestamp with time zone", "int8", "json"):
        assert classify_clickhouse_type(generic) is None, f"{generic} must fall through to the generic dialect"


def test_datetime64_truncation_and_decimal_overflow_are_documented() -> None:
    truncated = classify_clickhouse_type("DateTime64(9)")
    assert truncated is not None and truncated.mssql_type == "datetime2(7)"
    assert truncated.lossless is False and truncated.note is not None

    overflow = classify_clickhouse_type("Decimal(76, 38)")
    assert overflow is not None and overflow.supported is False
    assert "38" in (overflow.reason or "")


@pytest.mark.parametrize(
    ("source_type", "expected"),
    [
        ("tinyint", "smallint"),
        ("tinyint(1)", "bit"),
        ("mediumint", "int"),
        ("time", "time(6)"),
        ("blob", "varbinary(max)"),
        ("longtext", "nvarchar(max)"),
        ("year", "smallint"),
    ],
)
def test_mysql_dialect_mappings_align_with_pair_profile(source_type: str, expected: str) -> None:
    assert MSSQLTypeMapper.to_mssql(source_type) == expected

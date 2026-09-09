"""Assertions for postgres→MSSQL wide live certification."""

from __future__ import annotations

import hashlib
import json
import re
import struct
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from uuid import UUID

from tests.integration.postgres.postgres_mssql_strategy_configs import TARGET_SCHEMA
from tests.integration.postgres.postgres_mssql_wide_fixtures import SOURCE_SCHEMA, WideColumn

_MSSQL_TYPE_ALIASES = {
    "int": {"int"},
    "smallint": {"smallint"},
    "bigint": {"bigint"},
    "bit": {"bit"},
    "real": {"real", "float"},
    "float": {"float", "real"},
    "date": {"date"},
    "time": {"time"},
    "datetime2": {"datetime2", "datetime"},
    "datetimeoffset": {"datetimeoffset"},
    "uniqueidentifier": {"uniqueidentifier"},
    "nvarchar": {"nvarchar"},
    "nchar": {"nchar"},
    "varbinary": {"varbinary"},
    "decimal": {"decimal", "numeric"},
}
_TYPE_DECLARATION = re.compile(r"^([a-z0-9_ ]+?)(?:\((max|-?\d+)(?:,(-?\d+))?\))?$")
_INTEGER_TYPES = frozenset({"smallint", "int2", "integer", "int", "int4", "bigint", "int8"})
_TEXT_TYPES = frozenset({"text", "character", "char", "bpchar", "character varying", "varchar"})
_FIXED_CHARACTER_TYPES = frozenset({"character", "char", "bpchar"})
_EXPLICIT_TEXT_FAMILIES = ("[]", "range", "enum", "json")


@dataclass(frozen=True, slots=True)
class WideRoundtripProof:
    """Complete ordered data/catalog fingerprints for one live target."""

    row_fingerprint: str
    catalog_fingerprint: str
    asserted_rows: int
    asserted_columns: int

    def as_evidence(self) -> dict[str, object]:
        return {
            "row_fingerprint": self.row_fingerprint,
            "catalog_fingerprint": self.catalog_fingerprint,
            "asserted_rows": self.asserted_rows,
            "asserted_columns": self.asserted_columns,
        }


def assert_row_count(mssql, *, table: str, expected: int, schema: str = TARGET_SCHEMA) -> None:
    rows = mssql.get_records(f"SELECT COUNT(*) AS c FROM [{schema}].[{table}]", as_dict=True)
    assert int(rows[0]["c"]) == expected


def assert_unique_ids(mssql, *, table: str, schema: str = TARGET_SCHEMA) -> None:
    rows = mssql.get_records(
        f"SELECT id, COUNT(*) AS c FROM [{schema}].[{table}] GROUP BY id HAVING COUNT(*) > 1",
        as_dict=True,
    )
    assert not rows, f"duplicate ids in {schema}.{table}: {rows!r}"


def assert_typed_spot_checks(
    mssql,
    *,
    table: str,
    columns: Sequence[WideColumn],
    row_id: int = 1,
    expected_name: str = "row-one",
    check_seed_scalars: bool = True,
    current_only: bool = False,
    schema: str = TARGET_SCHEMA,
) -> None:
    type_rows = mssql.get_records(
        f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        """,
        as_dict=True,
    )
    by_name = {row["COLUMN_NAME"]: str(row["DATA_TYPE"]).lower() for row in type_rows}

    for col in columns:
        if col.name.startswith("__"):
            continue
        assert col.name in by_name, f"missing column {col.name} in MSSQL schema"
        data_type = by_name[col.name]
        expected = col.expected_mssql_type.lower()
        family = expected.split("(", 1)[0]
        aliases = _MSSQL_TYPE_ALIASES.get(family, {family})
        assert data_type in aliases, f"{col.name}: expected {col.expected_mssql_type}, got {data_type}"

    if not check_seed_scalars:
        return
    current_predicate = " AND __dpone__is_current = 1" if current_only else ""
    rows = mssql.get_records(
        f"""
        SELECT id, c_name, c_smallint, c_bigint, c_decimal, c_decimal_edge,
               c_bool, c_float, c_double, c_date, c_datetime, c_timestamp,
               c_time, c_varchar, c_text, c_empty, c_null, c_json, c_uuid,
               c_array, c_range, c_enum, b_text_marker_empty,
               CONVERT(varchar(max), c_blob, 2) AS blob_hex
        FROM [{schema}].[{table}] WHERE id = {row_id}{current_predicate}
        """,
        as_dict=True,
    )
    assert len(rows) == 1, f"expected unique id={row_id}, got {rows!r}"
    row = rows[0]
    assert int(row["id"]) == row_id
    assert row["c_name"] == expected_name
    assert int(row["c_smallint"]) == 70
    assert int(row["c_bigint"]) == 7000000000
    assert Decimal(str(row["c_decimal"])) == Decimal("12.3456")
    assert Decimal(str(row["c_decimal_edge"])) == Decimal("99999999999999.9999")
    assert bool(row["c_bool"]) is True
    assert abs(float(row["c_float"]) - 1.25) < 1e-6
    assert abs(float(row["c_double"]) - 2.5) < 1e-12
    assert row["c_date"] == date(2026, 1, 15)
    assert row["c_datetime"] == datetime(2026, 1, 15, 12, 30)
    timestamp = row["c_timestamp"]
    assert isinstance(timestamp, datetime)
    assert timestamp.astimezone(UTC) == datetime(2026, 1, 15, 12, 30, tzinfo=UTC)
    assert row["c_time"] == time(12, 30)
    assert row["c_varchar"] == "alpha,comma"
    assert row["c_text"] == "long\ttext\nmarker\x1d unicode Ω"
    assert row["c_empty"] == ""
    assert row["c_null"] is None
    assert row["c_json"] == '{"k": 1, "unicode": "Ω"}'
    assert str(row["c_uuid"]).lower() == "11111111-1111-1111-1111-111111111111"
    assert "alpha" in row["c_array"]
    assert "comma,value" in row["c_array"]
    assert "line\nbreak" in row["c_array"]
    assert row["c_range"] == "[1,8)"
    assert row["c_enum"] == "alpha"
    assert row["b_text_marker_empty"] == "\x1dE"
    assert str(row["blob_hex"]).lower() == "0001ff", f"c_blob hex: got {row['blob_hex']!r}"

    null_row = mssql.get_records(
        f"""
        SELECT c_smallint, c_bigint, c_decimal, c_decimal_edge, c_bool,
               c_float, c_double, c_date, c_datetime, c_timestamp, c_time,
               c_varchar, c_text, c_empty, c_null, c_json, c_uuid, c_blob,
               c_array, c_range, c_enum, b_text_marker_empty
        FROM [{schema}].[{table}] WHERE id = 2{current_predicate}
        """,
    )
    if null_row:
        assert all(value is None for value in null_row[0])


def assert_complete_wide_roundtrip(
    postgres,
    mssql,
    *,
    table: str,
    columns: Sequence[WideColumn],
    source_schema: str = SOURCE_SCHEMA,
    target_schema: str = TARGET_SCHEMA,
    current_only: bool = False,
) -> WideRoundtripProof:
    """Assert every business value and physical catalog attribute.

    Canonical equality is deliberately family-specific: integers/decimals are
    exact, REAL/FLOAT use IEEE bytes, temporal instants normalize to UTC,
    binary uses bytes, and PostgreSQL serializer-backed values (JSON,
    arrays/ranges/enums) compare their exact catalog ``::text`` form.  This
    avoids collation-dependent SQL equality and lossy six-digit float text.
    """

    business = tuple(column for column in columns if not column.name.startswith("__"))
    source_names = ", ".join(f'"{column.name}"' for column in business)
    source_text = ", ".join(f'"{column.name}"::text AS "{column.name}"' for column in business)
    source_rows = postgres.get_records(
        f'SELECT {source_names} FROM "{source_schema}"."{table}" ORDER BY "id"',
        as_dict=True,
    )
    source_text_rows = postgres.get_records(
        f'SELECT {source_text} FROM "{source_schema}"."{table}" ORDER BY "id"',
        as_dict=True,
    )
    assert len(source_rows) == len(source_text_rows)

    target_names = ", ".join(f"[{column.name}]" for column in business)
    current_predicate = " WHERE [__dpone__is_current] = 1" if current_only else ""
    target_rows = mssql.get_records(
        f"SELECT {target_names} FROM [{target_schema}].[{table}]{current_predicate} ORDER BY [id]",
        as_dict=True,
    )
    target_by_id = {int(row["id"]): row for row in target_rows}
    canonical_rows: list[dict[str, object]] = []
    for source_row, text_row in zip(source_rows, source_text_rows, strict=True):
        row_id = int(source_row["id"])
        assert row_id in target_by_id, f"source id={row_id} is absent from target"
        target_row = target_by_id[row_id]
        canonical: dict[str, object] = {}
        for column in business:
            expected = canonical_wide_value(
                column,
                source_row[column.name],
                serialized=text_row[column.name],
            )
            actual = canonical_wide_value(
                column,
                target_row[column.name],
                serialized=target_row[column.name],
            )
            assert actual == expected, f"{table}.id={row_id}.{column.name}: expected {expected!r}, got {actual!r}"
            canonical[column.name] = actual
        canonical_rows.append(canonical)

    return WideRoundtripProof(
        row_fingerprint=_sha256(canonical_rows),
        catalog_fingerprint=assert_wide_catalog(
            mssql,
            table=table,
            columns=business,
            target_schema=target_schema,
        ),
        asserted_rows=len(source_rows),
        asserted_columns=len(business),
    )


def assert_wide_catalog(
    mssql,
    *,
    table: str,
    columns: Sequence[WideColumn],
    target_schema: str = TARGET_SCHEMA,
) -> str:
    """Assert the complete physical catalog for every business column."""

    business = tuple(column for column in columns if not column.name.startswith("__"))
    catalog_rows = mssql.get_records(
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
        "NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, IS_NULLABLE, COLLATION_NAME "
        "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        (target_schema, table),
        as_dict=True,
    )
    catalog_by_name = {str(row["COLUMN_NAME"]): row for row in catalog_rows}
    database_collation = str(
        mssql.get_records("SELECT CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation'))")[0][0]
    )
    canonical_catalog: list[dict[str, object]] = []
    for column in business:
        assert column.name in catalog_by_name, f"missing target column {column.name}"
        catalog = catalog_by_name[column.name]
        expected_catalog = _expected_catalog(column, database_collation=database_collation)
        actual_catalog = _actual_catalog(catalog)
        assert actual_catalog == expected_catalog, (
            f"{table}.{column.name} catalog: expected {expected_catalog!r}, got {actual_catalog!r}"
        )
        canonical_catalog.append({"name": column.name, **actual_catalog})

    return _sha256(canonical_catalog)


def canonical_wide_value(column: WideColumn, value: object, *, serialized: object) -> object:
    """Normalize one wide-fixture value for exact cross-vendor comparison."""
    if value is None:
        return None
    pg_type = column.pg_type.lower().strip()
    base = pg_type.split("(", 1)[0].strip()
    if base in _INTEGER_TYPES:
        return int(value)
    if base in {"numeric", "decimal"}:
        return str(Decimal(str(value)))
    if base in {"boolean", "bool"}:
        return bool(value)
    if base in {"real", "float4"}:
        return struct.pack(">f", float(value)).hex()
    if base in {"double precision", "float8"}:
        return struct.pack(">d", float(value)).hex()
    if base == "date":
        assert isinstance(value, date)
        return value.isoformat()
    if base in {"timestamp with time zone", "timestamptz"}:
        assert isinstance(value, datetime) and value.tzinfo is not None
        return value.astimezone(UTC).isoformat(timespec="microseconds")
    if base in {"timestamp without time zone", "timestamp"}:
        assert isinstance(value, datetime) and value.tzinfo is None
        return value.isoformat(timespec="microseconds")
    if base in {"time without time zone", "time"}:
        assert isinstance(value, time)
        return value.isoformat(timespec="microseconds")
    if base == "uuid":
        return str(UUID(str(value))).lower()
    if base == "bytea":
        if isinstance(value, memoryview):
            value = value.tobytes()
        assert isinstance(value, bytes)
        return value.hex()
    if any(token in pg_type for token in _EXPLICIT_TEXT_FAMILIES):
        return str(serialized)
    if base in _FIXED_CHARACTER_TYPES:
        # PostgreSQL blank-padded ``char(n)`` values are canonically exposed
        # through ``::text`` without insignificant pad spaces.  The MSSQL
        # projection is intentionally NVARCHAR (not NCHAR), so compare that
        # authoritative value rather than the driver's padded Python object.
        return str(serialized)
    if base in _TEXT_TYPES:
        return str(value)
    return str(serialized)


def _expected_catalog(column: WideColumn, *, database_collation: str) -> dict[str, object]:
    declaration = column.expected_mssql_type.lower().strip()
    match = _TYPE_DECLARATION.fullmatch(declaration)
    assert match is not None, f"unparseable expected MSSQL type: {declaration}"
    family, first, second = match.groups()
    family = family.strip()
    aliases = _MSSQL_TYPE_ALIASES.get(family, {family})
    # The mapper's declaration selects exactly one physical family.  Aliases
    # remain accepted only where INFORMATION_SCHEMA reports the vendor synonym.
    data_type = family if family in aliases else sorted(aliases)[0]
    character_length = None
    precision = None
    scale = None
    datetime_precision = None
    if family in {"nvarchar", "nchar", "varchar", "char", "varbinary", "binary"}:
        character_length = -1 if first == "max" else int(first or 1)
    elif family in {"decimal", "numeric"}:
        precision = int(first or 18)
        scale = int(second or 0)
    elif family in {"smallint", "int", "bigint"}:
        precision = {"smallint": 5, "int": 10, "bigint": 19}[family]
        scale = 0
    elif family in {"time", "datetime2", "datetimeoffset"}:
        datetime_precision = int(first or 7)
    elif family == "date":
        datetime_precision = 0
    elif family == "real":
        precision = 24
    elif family == "float":
        precision = int(first or 53)
    is_character = family in {"nvarchar", "nchar", "varchar", "char"}
    return {
        "data_type": data_type,
        "character_length": character_length,
        "precision": precision,
        "scale": scale,
        "datetime_precision": datetime_precision,
        "nullable": "NOT NULL" not in column.pg_ddl.upper(),
        "collation": database_collation if is_character else None,
    }


def _actual_catalog(row: dict[str, object]) -> dict[str, object]:
    data_type = str(row["DATA_TYPE"]).lower()
    return {
        "data_type": data_type,
        "character_length": _optional_int(row["CHARACTER_MAXIMUM_LENGTH"]),
        "precision": _optional_int(row["NUMERIC_PRECISION"]),
        "scale": _optional_int(row["NUMERIC_SCALE"]),
        "datetime_precision": _optional_int(row["DATETIME_PRECISION"]),
        "nullable": str(row["IS_NULLABLE"]).upper() == "YES",
        "collation": str(row["COLLATION_NAME"]) if row["COLLATION_NAME"] is not None else None,
    }


def _optional_int(value: object) -> int | None:
    return None if value is None else int(value)


def _sha256(value: object) -> str:
    payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(payload).hexdigest()


def fq(table: str, *, schema: str = TARGET_SCHEMA) -> str:
    return f"[{schema}].[{table}]"


__all__ = [
    "WideRoundtripProof",
    "assert_complete_wide_roundtrip",
    "assert_row_count",
    "assert_typed_spot_checks",
    "assert_unique_ids",
    "assert_wide_catalog",
    "canonical_wide_value",
    "fq",
]

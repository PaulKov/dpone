"""Cross-dialect schema type compatibility helpers.

The helpers compare source metadata types with target DDL types across
dialects (PostgreSQL, MSSQL, ClickHouse). ClickHouse spellings are matched
case-sensitively where lowercase normalization would collide with PostgreSQL
aliases (``Int8`` is a 1-byte ClickHouse integer while ``int8`` is a
PostgreSQL bigint), so mapped ``clickhouse -> mssql`` types stay equal across
repeated chunks.
"""

from __future__ import annotations

import re
from typing import Protocol

from dpone.contracts.mssql_lossless_type_contract import mssql_is_safe_widening, mssql_physical_types_equal

_CH_INT_EQUIVALENT_RANK = {
    # Case-sensitive ClickHouse integers -> rank of the narrowest MSSQL type
    # that holds the full value range (see MSSQLTypeMapper).
    "Int8": 2,
    "Int16": 2,
    "Int32": 3,
    "Int64": 4,
    "UInt8": 1,
    "UInt16": 3,
    "UInt32": 4,
}

_CH_TEXTUAL_PREFIXES = ("fixedstring(", "enum8(", "enum16(", "enum(")
_CH_TEXTUAL_EXACT = {"string", "ipv4", "ipv6"}

_WRAPPER_PATTERN = re.compile(r"\A(?:nullable|lowcardinality)\((?P<inner>.+)\)\Z", re.IGNORECASE)
_SIMPLE_AGGREGATE_PATTERN = re.compile(r"\Asimpleaggregatefunction\(\s*[^,]+,\s*(?P<inner>.+)\)\Z", re.IGNORECASE)
_DECIMAL_SHORT_PATTERN = re.compile(r"\Adecimal(?P<bits>32|64|128|256)\(\s*(?P<scale>\d+)\s*\)\Z")
_DECIMAL_SHORT_PRECISION = {"32": 9, "64": 18, "128": 38, "256": 76}


def types_equal(source_type: str, target_type: str) -> bool:
    source = _base_type_name(source_type)
    target = _base_type_name(target_type)
    if source == target:
        return True
    source_rank = _integer_rank_dialect_aware(source_type, source)
    target_rank = _integer_rank_dialect_aware(target_type, target)
    if source_rank is not None or target_rank is not None:
        return source_rank == target_rank
    source_num = _numeric(source)
    target_num = _numeric(target)
    if source_num and target_num:
        return source_num == target_num
    family = _canonical_family(source)
    target_family = _canonical_family(target)
    if family != target_family:
        return False
    if family in {"float", "boolean", "date", "timestamp", "json", "uuid"}:
        return True
    return family == "string" and (_is_clickhouse_string(source) or _is_clickhouse_string(target))


def is_widening(source_type: str, target_type: str) -> bool:
    if types_equal(source_type, target_type):
        return False
    source = _base_type_name(source_type)
    target = _base_type_name(target_type)
    source_rank = _integer_rank_dialect_aware(source_type, source)
    target_rank = _integer_rank_dialect_aware(target_type, target)
    if source_rank is not None and target_rank is not None:
        return source_rank >= target_rank
    source_num = _numeric(source)
    target_num = _numeric(target)
    if source_num and target_num:
        source_precision, source_scale = source_num
        target_precision, target_scale = target_num
        return source_scale >= target_scale and source_precision - source_scale >= target_precision - target_scale
    source_len = _length(source)
    target_len = _length(target)
    if source_len is not None and target_len is not None:
        return source_len >= target_len
    return False


class SchemaTypeCompatibility(Protocol):
    """Injected comparison semantics for one resolved source→sink route."""

    def types_equal(self, source_type: str, target_type: str) -> bool: ...

    def is_widening(self, source_type: str, target_type: str) -> bool: ...


class GenericSchemaTypeCompatibility:
    """Legacy cross-dialect compatibility used outside exact MSSQL routes."""

    def types_equal(self, source_type: str, target_type: str) -> bool:
        return types_equal(source_type, target_type)

    def is_widening(self, source_type: str, target_type: str) -> bool:
        return is_widening(source_type, target_type)


class MssqlSchemaTypeCompatibility:
    """Exact SQL Server physical compatibility, including family and scale."""

    def types_equal(self, source_type: str, target_type: str) -> bool:
        return mssql_physical_types_equal(source_type, target_type)

    def is_widening(self, source_type: str, target_type: str) -> bool:
        return mssql_is_safe_widening(source_type, target_type)


def _normalize_type_name(dtype: str) -> str:
    return re.sub(r"\s+", " ", str(dtype).strip().lower())


def _strip_wrappers(value: str) -> str:
    """Strip Nullable/LowCardinality/SimpleAggregateFunction wrappers (any case)."""

    current = value.strip()
    while True:
        wrapper = _WRAPPER_PATTERN.match(current)
        if wrapper is not None:
            current = wrapper.group("inner").strip()
            continue
        aggregate = _SIMPLE_AGGREGATE_PATTERN.match(current)
        if aggregate is not None:
            current = aggregate.group("inner").strip()
            continue
        return current


def _base_type_name(dtype: str) -> str:
    value = _normalize_type_name(dtype)
    value = value.replace(" nullable", "").strip()
    value = _strip_wrappers(value)
    return value.replace(" nullable", "").strip()


def _integer_rank_dialect_aware(original_type: str, base_name: str) -> int | None:
    """Integer rank; ClickHouse capitalized spellings resolve case-sensitively."""

    cased = re.sub(r"\s+nullable\s*\Z", "", str(original_type).strip(), flags=re.IGNORECASE)
    cased = _strip_wrappers(cased)
    if cased in _CH_INT_EQUIVALENT_RANK:
        return _CH_INT_EQUIVALENT_RANK[cased]
    if base_name == "uint64":
        # UInt64 lands as decimal(20,0); compare via the numeric profile.
        return None
    return _integer_rank(base_name)


def _canonical_family(dtype: str) -> str:
    value = _base_type_name(dtype)
    if value in {
        "int",
        "int4",
        "int32",
        "integer",
        "serial",
        "bigint",
        "int8",
        "int64",
        "bigserial",
        "smallint",
        "int2",
        "int16",
        "tinyint",
        "uint8",
        "uint16",
        "uint32",
    }:
        return "integer"
    if value.startswith(("numeric", "decimal")) or value == "uint64":
        return "numeric"
    if any(token in value for token in ("float", "double", "real")):
        return "float"
    if (
        value.startswith(("varchar", "nvarchar", "character varying", "char", "nchar"))
        or value in {"text", "string"}
        or _is_clickhouse_string(value)
    ):
        return "string"
    if value in {"bool", "boolean", "bit"}:
        return "boolean"
    if value in {"date", "date32"}:
        return "date"
    if value in {"uuid", "uniqueidentifier"}:
        return "uuid"
    if "time" in value or "date" in value:
        return "timestamp"
    if value in {"json", "jsonb"}:
        return "json"
    return value


def _integer_rank(dtype: str) -> int | None:
    return {
        "tinyint": 1,
        "uint8": 1,
        "smallint": 2,
        "int2": 2,
        "int16": 2,
        "int": 3,
        "int4": 3,
        "int32": 3,
        "uint16": 3,
        "integer": 3,
        "bigint": 4,
        "int8": 4,
        "int64": 4,
        "uint32": 4,
    }.get(dtype)


def _numeric(dtype: str) -> tuple[int, int] | None:
    if dtype == "uint64":
        return (20, 0)
    short = _DECIMAL_SHORT_PATTERN.match(dtype)
    if short:
        return (_DECIMAL_SHORT_PRECISION[short.group("bits")], int(short.group("scale")))
    match = re.match(r"(?:numeric|decimal)(?:\d+)?\((\d+)\s*,\s*(\d+)\)", dtype)
    return (int(match.group(1)), int(match.group(2))) if match else None


def _length(dtype: str) -> int | None:
    match = re.match(r"(?:n?var)?char\((\d+)\)", dtype)
    return int(match.group(1)) if match else None


def _is_clickhouse_string(dtype: str) -> bool:
    value = _base_type_name(dtype)
    return value in _CH_TEXTUAL_EXACT or value.startswith(_CH_TEXTUAL_PREFIXES)

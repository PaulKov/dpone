"""Pure PostgreSQL/MSSQL physical-shape helpers for portable scope binding."""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

_PG_INTEGER_RANGES = {
    "smallint": (-(2**15), 2**15 - 1),
    "int2": (-(2**15), 2**15 - 1),
    "integer": (-(2**31), 2**31 - 1),
    "int": (-(2**31), 2**31 - 1),
    "int4": (-(2**31), 2**31 - 1),
    "bigint": (-(2**63), 2**63 - 1),
    "int8": (-(2**63), 2**63 - 1),
}
_MSSQL_INTEGER_RANGES = {
    "tinyint": (0, 2**8 - 1),
    "smallint": (-(2**15), 2**15 - 1),
    "int": (-(2**31), 2**31 - 1),
    "bigint": (-(2**63), 2**63 - 1),
}
_PG_NUMERIC = re.compile(r"^(?:numeric|decimal)(?:\((\d+)\s*,\s*(\d+)\))?$")
_PG_VARCHAR = re.compile(r"^(?:character varying|varchar)(?:\((\d+)\))?$")
_PG_TIMESTAMP = re.compile(r"^(?:timestamp(?:\((\d+)\))? without time zone|timestamp(?:\((\d+)\))?)$")
_PG_TIMESTAMPTZ = re.compile(r"^(?:timestamp(?:\((\d+)\))? with time zone|timestamptz(?:\((\d+)\))?)$")
_MSSQL_DECIMAL = re.compile(r"^(?:decimal|numeric)\((\d+),(\d+)\)$")
_MSSQL_DATETIME2 = re.compile(r"^datetime2\((\d+)\)$")
_MSSQL_DATETIMEOFFSET = re.compile(r"^datetimeoffset\((\d+)\)$")


def postgres_integral_range(source_type: str) -> tuple[int, int] | None:
    """Return the exact integral domain for a PostgreSQL physical type."""

    if source_type in _PG_INTEGER_RANGES:
        return _PG_INTEGER_RANGES[source_type]
    shape = postgres_decimal_shape(source_type)
    if shape is None or shape[1] != 0:
        return None
    limit = 10 ** shape[0] - 1
    return -limit, limit


def mssql_integral_range(target_type: str) -> tuple[int, int] | None:
    """Return the exact integral domain for a SQL Server physical type."""

    if target_type in _MSSQL_INTEGER_RANGES:
        return _MSSQL_INTEGER_RANGES[target_type]
    shape = mssql_decimal_shape(target_type)
    if shape is None or shape[1] != 0:
        return None
    limit = 10 ** shape[0] - 1
    return -limit, limit


def postgres_decimal_shape(source_type: str) -> tuple[int, int] | None:
    """Return a bounded PostgreSQL numeric precision/scale, if explicit."""

    match = _PG_NUMERIC.fullmatch(source_type)
    if match is None or match.group(1) is None:
        return None
    precision, scale = int(match.group(1)), int(match.group(2))
    if precision < 1 or scale < 0 or scale > precision:
        return None
    return precision, scale


def mssql_decimal_shape(target_type: str) -> tuple[int, int] | None:
    """Return a SQL Server decimal precision/scale, if explicit."""

    match = _MSSQL_DECIMAL.fullmatch(target_type)
    return None if match is None else (int(match.group(1)), int(match.group(2)))


def decimal_fits(value: Decimal, shape: tuple[int, int]) -> bool:
    """Test exact decimal representability without endpoint rounding."""

    precision, scale = shape
    quantum = Decimal(1).scaleb(-scale)
    try:
        if value.quantize(quantum) != value:
            return False
    except InvalidOperation:
        return False
    return abs(value) < Decimal(10) ** (precision - scale)


def postgres_timestamp_precision(source_type: str, *, timezone: bool) -> int | None:
    """Return PostgreSQL timestamp fractional precision for the requested family."""

    match = (_PG_TIMESTAMPTZ if timezone else _PG_TIMESTAMP).fullmatch(source_type)
    if match is None:
        return None
    raw = next((group for group in match.groups() if group is not None), None)
    precision = 6 if raw is None else int(raw)
    return precision if 0 <= precision <= 6 else None


def mssql_timestamp_precision(target_type: str, *, timezone: bool) -> int | None:
    """Return SQL Server temporal fractional precision for the requested family."""

    match = (_MSSQL_DATETIMEOFFSET if timezone else _MSSQL_DATETIME2).fullmatch(target_type)
    return None if match is None else int(match.group(1))


def fractional_precision(value: datetime) -> int:
    """Return significant microsecond digits for an authored timestamp."""

    if value.microsecond == 0:
        return 0
    return len(f"{value.microsecond:06d}".rstrip("0"))


def postgres_text_limit(source_type: str) -> int | None | bool:
    """Return text limit, unbounded marker, or False for a non-text type."""

    if source_type == "text":
        return None
    match = _PG_VARCHAR.fullmatch(source_type)
    if match is None:
        return False
    return int(match.group(1)) if match.group(1) is not None else None


def postgres_binary_collation(value: str | None) -> bool:
    """Recognize the finite PostgreSQL binary-comparison collation surface."""

    normalized = str(value or "").casefold()
    return normalized in {"c", "posix", "ucs_basic", "pg_catalog.c", "pg_catalog.posix", "pg_catalog.ucs_basic"}


def mssql_binary_collation(value: str | None) -> bool:
    """Recognize SQL Server BIN2 comparison authorities."""

    normalized = str(value or "").casefold()
    return normalized.endswith("_bin2") or normalized.endswith("_bin2_utf8")


__all__ = [
    "decimal_fits",
    "fractional_precision",
    "mssql_binary_collation",
    "mssql_decimal_shape",
    "mssql_integral_range",
    "mssql_timestamp_precision",
    "postgres_binary_collation",
    "postgres_decimal_shape",
    "postgres_integral_range",
    "postgres_text_limit",
    "postgres_timestamp_precision",
]

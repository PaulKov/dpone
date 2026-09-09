"""ClickHouse row-value coercion for native driver inserts."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any


class ClickHouseRowValueCoercer:
    """Convert row artifact values to Python types expected by clickhouse-driver."""

    def coerce_row(self, row: Sequence[Any], clickhouse_types: Sequence[str]) -> tuple[Any, ...]:
        """Return one driver-ready row for the given ClickHouse column types."""

        return tuple(self.coerce_value(value, column_type) for value, column_type in zip(row, clickhouse_types))

    def coerce_value(self, value: Any, clickhouse_type: str) -> Any:
        """Convert a scalar value for one ClickHouse target type family."""

        if value is None:
            return None
        base_type = _unwrap_type(clickhouse_type)
        if _is_datetime(base_type):
            return _coerce_datetime(value)
        if _is_date(base_type):
            return _coerce_date(value)
        if _is_integer(base_type):
            return int(value)
        if _is_float(base_type):
            return float(value)
        if _is_decimal(base_type):
            return Decimal(str(value))
        if _is_bool(base_type):
            return _coerce_bool(value)
        if _is_string(base_type):
            # Binary columns mapped to String (hex/base64 wire) arrive as bytes on
            # streaming ODBC paths; store lowercase hex to match character BCP.
            if isinstance(value, bytes | bytearray | memoryview):
                return bytes(value).hex()
            if isinstance(value, str):
                return value
            return str(value)
        return value


def _unwrap_type(clickhouse_type: str) -> str:
    value = str(clickhouse_type).strip()
    while True:
        lowered = value.lower()
        if lowered.startswith("nullable(") and value.endswith(")"):
            value = value[len("Nullable(") : -1].strip()
            continue
        if lowered.startswith("lowcardinality(") and value.endswith(")"):
            value = value[len("LowCardinality(") : -1].strip()
            continue
        return value


def _root_type(clickhouse_type: str) -> str:
    return str(clickhouse_type).strip().split("(", 1)[0].strip().lower()


def _is_datetime(base_type: str) -> bool:
    root = _root_type(base_type)
    return root in {"datetime", "datetime64", "timestamp"}


def _is_date(base_type: str) -> bool:
    return _root_type(base_type) in {"date", "date32"}


def _is_integer(base_type: str) -> bool:
    root = _root_type(base_type)
    return root.startswith(("int", "uint")) or root in {"integer", "bigint", "smallint", "tinyint"}


def _is_float(base_type: str) -> bool:
    return _root_type(base_type) in {"float32", "float64", "float", "double", "real"}


def _is_decimal(base_type: str) -> bool:
    root = _root_type(base_type)
    return root.startswith("decimal") or root in {"numeric", "money"}


def _is_bool(base_type: str) -> bool:
    return _root_type(base_type) in {"bool", "boolean"}


def _is_string(base_type: str) -> bool:
    root = _root_type(base_type)
    return root in {"string", "fixedstring", "varchar", "nvarchar", "text", "char"}


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, int | float):
        return datetime.fromtimestamp(value, tz=UTC)
    text = str(value).strip().replace("Z", "+00:00")
    return datetime.fromisoformat(text)


def _coerce_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip()[:10])


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


__all__ = ["ClickHouseRowValueCoercer"]

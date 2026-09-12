"""Type and value normalization for vendor-live conformance SQL stores."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.ops.routes.conformance_models import RouteConformanceColumn


def executemany_insert(
    connector: Any,
    *,
    qualified: str,
    columns: Sequence[RouteConformanceColumn],
    rows: Sequence[Mapping[str, object]],
    placeholder: str,
    prepare_value: Callable[[RouteConformanceColumn, object], object],
    quote_column: Callable[[str], str] | None = None,
) -> int:
    if not rows:
        return 0
    quote = quote_column or (lambda name: f'"{name}"')
    column_list = ", ".join(quote(column.name) for column in columns)
    placeholders = ", ".join(placeholder for _ in columns)
    sql = f"INSERT INTO {qualified} ({column_list}) VALUES ({placeholders})"
    values = [tuple(prepare_value(column, row.get(column.name)) for column in columns) for row in rows]
    cursor = connector.connection.cursor()
    try:
        if hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True
        cursor.executemany(sql, values)
    finally:
        cursor.close()
    return len(values)


def postgres_type(column: RouteConformanceColumn) -> str:
    if column.logical_type == "integer":
        return "bigint"
    if column.logical_type == "decimal":
        return "numeric(38,10)"
    if column.logical_type == "boolean":
        return "boolean"
    if column.logical_type == "date":
        return "date"
    if column.logical_type == "timestamp":
        return "timestamp(6)"
    return "text"


def mssql_type(column: RouteConformanceColumn) -> str:
    type_name = column.physical_contract
    if not column.primary_key and " null" not in type_name.lower() and " not null" not in type_name.lower():
        type_name = f"{type_name} {'NULL' if column.nullable else 'NOT NULL'}"
    return type_name


def clickhouse_type(column: RouteConformanceColumn) -> str:
    base = {
        "integer": "Int64",
        "decimal": "Decimal(38,10)",
        "boolean": "Bool",
        "date": "Date",
        "timestamp": "DateTime64(6)",
        "binary": "String",
        "json": "String",
        "text": "String",
    }.get(column.logical_type, "String")
    if column.nullable and not base.startswith("Nullable("):
        return f"Nullable({base})"
    return base


def postgres_value(column: RouteConformanceColumn, value: object) -> object:
    temporal = _temporal_value(column, value)
    if temporal is not _UNCHANGED:
        return temporal
    if column.logical_type == "decimal" and value is not None:
        return Decimal(str(value))
    return _textual_value(value)


def mssql_value(column: RouteConformanceColumn, value: object) -> object:
    if value is None:
        return None
    if column.logical_type == "binary":
        return bytes.fromhex(str(value))
    temporal = _temporal_value(column, value)
    if temporal is not _UNCHANGED:
        return temporal
    if column.logical_type == "decimal":
        return Decimal(str(value))
    return _textual_value(value)


def clickhouse_value(column: RouteConformanceColumn, value: object) -> object:
    if value is None:
        return None
    if column.logical_type == "decimal":
        return Decimal(str(value))
    if column.logical_type == "integer":
        return int(str(value))
    if column.logical_type == "boolean":
        return bool(value)
    temporal = _temporal_value(column, value)
    if temporal is not _UNCHANGED:
        return temporal
    return _textual_value(value)


def normalize_db_rows(
    rows: Iterable[Mapping[str, object]],
    columns: Sequence[RouteConformanceColumn],
) -> tuple[Mapping[str, object], ...]:
    return tuple({column.name: _normalize_db_value(column, row.get(column.name)) for column in columns} for row in rows)


def _textual_value(value: object) -> object:
    if isinstance(value, dict | list):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return value


def _normalize_db_value(column: RouteConformanceColumn, value: object) -> object:
    if value is None:
        return None
    if column.logical_type == "binary" and isinstance(value, bytes | bytearray):
        return bytes(value).hex()
    if column.logical_type == "timestamp" and isinstance(value, datetime):
        return value.isoformat(timespec="microseconds")
    if column.logical_type == "date" and isinstance(value, date):
        return value.isoformat()
    if column.logical_type == "decimal":
        return _normalize_decimal(column, value)
    if column.logical_type == "integer":
        return int(str(value))
    if column.logical_type == "boolean":
        return bool(value)
    return str(value)


class _Unchanged:
    pass


_UNCHANGED = _Unchanged()


def _temporal_value(column: RouteConformanceColumn, value: object) -> object:
    if value is None:
        return None
    if column.logical_type == "timestamp":
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value))
    if column.logical_type == "date":
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        return date.fromisoformat(str(value))
    return _UNCHANGED


def _normalize_decimal(column: RouteConformanceColumn, value: object) -> str:
    decimal_value = Decimal(str(value))
    scale = _decimal_scale(column)
    if scale <= 0:
        return format(decimal_value, "f")
    quantized = decimal_value.quantize(Decimal(1).scaleb(-scale))
    return f"{quantized:.{scale}f}"


def _decimal_scale(column: RouteConformanceColumn) -> int:
    contract = column.physical_contract.lower()
    if "," not in contract:
        return 0
    raw_scale = contract.rsplit(",", 1)[1].split(")", 1)[0].strip()
    try:
        return int(raw_scale)
    except ValueError:
        return 0


__all__ = [
    "clickhouse_type",
    "clickhouse_value",
    "executemany_insert",
    "mssql_type",
    "mssql_value",
    "normalize_db_rows",
    "postgres_type",
    "postgres_value",
]

"""Shared helpers for ClickHouse CDC typed materialization."""

from __future__ import annotations

import re
from typing import Literal

SCHEMA_VERSION = "dpone.cdc_clickhouse_typed_materialization.v1"

DeleteMode = Literal["exclude_deleted", "tombstone"]

COLUMN_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SUPPORTED_TYPE_RE = re.compile(
    r"^(?:"
    r"(?:Nullable\()?"
    r"(?:String|Bool|Date|DateTime|DateTime64\(\d+(?:,\s*'[^']+')?\)|"
    r"Int(?:8|16|32|64)|UInt(?:8|16|32|64)|Float(?:32|64)|Decimal\(\d+,\s*\d+\))"
    r"\)?"
    r")$"
)


def base_type(clickhouse_type: str) -> str:
    value = clickhouse_type.strip()
    if value.startswith("Nullable(") and value.endswith(")"):
        return value[len("Nullable(") : -1]
    return value


def decimal_scale(clickhouse_type: str) -> int:
    match = re.match(r"Decimal\(\d+,\s*(\d+)\)", clickhouse_type)
    if not match:
        raise ValueError(f"Invalid Decimal type: {clickhouse_type!r}")
    return int(match.group(1))


def datetime64_profile(clickhouse_type: str) -> tuple[int, str]:
    match = re.match(r"DateTime64\((\d+)(?:,\s*'([^']+)')?\)", clickhouse_type)
    if not match:
        raise ValueError(f"Invalid DateTime64 type: {clickhouse_type!r}")
    return int(match.group(1)), match.group(2) or "UTC"


def split_dataset(value: str, *, default_database: str) -> tuple[str, str]:
    parts = [part.strip() for part in value.split(".") if part.strip()]
    if len(parts) == 1:
        return default_database, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError(f"ClickHouse dataset must be table or database.table: {value!r}")


def qualified(database: str, table: str) -> str:
    return f"{quote_identifier(database)}.{quote_identifier(table)}"


def quote_identifier(value: str) -> str:
    if not value:
        raise ValueError("ClickHouse identifier cannot be empty")
    return "`" + value.replace("`", "``") + "`"


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


__all__ = [
    "COLUMN_NAME_RE",
    "SCHEMA_VERSION",
    "SUPPORTED_TYPE_RE",
    "DeleteMode",
    "base_type",
    "datetime64_profile",
    "decimal_scale",
    "qualified",
    "quote_identifier",
    "quote_literal",
    "split_dataset",
]

"""SQL Server schema reconciliation for the wide dbt materialization proof."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_TOOLS_DIR = Path(__file__).resolve().parent
if str(_TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOLS_DIR))

from mssql_clickhouse_bcp_native_fixtures import build_bcp_native_columns  # noqa: E402


@dataclass(frozen=True, slots=True)
class DbtWideSchemaMetrics:
    source_column_count: int
    target_column_count: int
    mismatch_count: int
    source_sha256: str
    passthrough_sha256: str
    canonical_source_sha256: str
    canonical_source_mismatch_count: int
    calculated_column_type: str
    calculated_column_nullable: bool


@dataclass(frozen=True, slots=True)
class _Column:
    name: str
    type_name: str
    max_length: int
    precision: int
    scale: int
    nullable: bool

    def normalized(self) -> tuple[str, str, int, int, int, bool]:
        normalized_type = "binary" if self.type_name in {"timestamp", "rowversion"} else self.type_name
        return (self.name, normalized_type, self.max_length, self.precision, self.scale, self.nullable)

    def rendered_type(self) -> str:
        if self.type_name in {"decimal", "numeric"}:
            return f"{self.type_name}({self.precision},{self.scale})"
        if self.type_name in {"char", "varchar", "binary", "varbinary", "nchar", "nvarchar"}:
            length: int | str = "max" if self.max_length == -1 else self.max_length
            if self.type_name in {"nchar", "nvarchar"} and isinstance(length, int):
                length //= 2
            return f"{self.type_name}({length})"
        if self.type_name in {"datetime2", "datetimeoffset", "time"}:
            return f"{self.type_name}({self.scale})"
        return self.type_name


def dbt_wide_schema_metrics(
    connector: Any,
    *,
    source_schema: str,
    source_table: str,
    target_schema: str,
    target_table: str,
) -> DbtWideSchemaMetrics:
    """Compare the ordered passthrough schema and exact calculated type."""

    source = _columns(connector, source_schema, source_table)
    target = _columns(connector, target_schema, target_table)
    passthrough = target[:-1] if target and target[-1].name == "dbt_calculated_amount" else target
    source_normalized = tuple(column.normalized() for column in source)
    passthrough_normalized = tuple(column.normalized() for column in passthrough)
    source_inventory = tuple((column.name, column.rendered_type(), column.nullable) for column in source)
    canonical_inventory = _canonical_inventory(len(source))
    mismatch_count = abs(len(source_normalized) - len(passthrough_normalized)) + sum(
        left != right for left, right in zip(source_normalized, passthrough_normalized, strict=False)
    )
    calculated = target[-1].rendered_type() if len(target) == len(source) + 1 else ""
    calculated_nullable = target[-1].nullable if len(target) == len(source) + 1 else False
    return DbtWideSchemaMetrics(
        source_column_count=len(source),
        target_column_count=len(target),
        mismatch_count=mismatch_count,
        source_sha256=_schema_sha256(source_normalized),
        passthrough_sha256=_schema_sha256(passthrough_normalized),
        canonical_source_sha256=_inventory_sha256(canonical_inventory),
        canonical_source_mismatch_count=_mismatch_count(source_inventory, canonical_inventory),
        calculated_column_type=calculated,
        calculated_column_nullable=calculated_nullable,
    )


def _canonical_inventory(column_count: int) -> tuple[tuple[str, str, bool], ...]:
    return tuple(
        (column.name, _canonical_type(column.mssql_type), "not null" not in column.mssql_type.lower())
        for column in build_bcp_native_columns(column_count)
    )


def canonical_wide_source_schema_sha256(column_count: int) -> str:
    """Return the trusted ordered wide-source inventory digest."""

    return _inventory_sha256(_canonical_inventory(column_count))


def _canonical_type(value: str) -> str:
    normalized = re.sub(r"\s+not\s+null\s*$", "", value.strip().lower())
    return "binary(8)" if normalized in {"timestamp", "rowversion"} else normalized


def _mismatch_count(
    actual: tuple[tuple[str, str, bool], ...],
    expected: tuple[tuple[str, str, bool], ...],
) -> int:
    return abs(len(actual) - len(expected)) + sum(left != right for left, right in zip(actual, expected, strict=False))


def _inventory_sha256(value: tuple[tuple[str, str, bool], ...]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _columns(connector: Any, schema: str, table: str) -> tuple[_Column, ...]:
    rows = connector.get_records(
        "SELECT c.name, LOWER(t.name), c.max_length, c.precision, c.scale, c.is_nullable "
        "FROM sys.columns c "
        "JOIN sys.tables o ON o.object_id = c.object_id "
        "JOIN sys.schemas s ON s.schema_id = o.schema_id "
        "JOIN sys.types t ON t.user_type_id = c.user_type_id "
        f"WHERE s.name = N'{_sql_text(schema)}' AND o.name = N'{_sql_text(table)}' "
        "ORDER BY c.column_id"
    )
    return tuple(
        _Column(
            name=str(row[0]),
            type_name=str(row[1]),
            max_length=int(row[2]),
            precision=int(row[3]),
            scale=int(row[4]),
            nullable=bool(row[5]),
        )
        for row in rows
    )


def _schema_sha256(value: tuple[tuple[str, str, int, int, int, bool], ...]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _sql_text(value: str) -> str:
    return str(value).replace("'", "''")


__all__ = ["DbtWideSchemaMetrics", "canonical_wide_source_schema_sha256", "dbt_wide_schema_metrics"]

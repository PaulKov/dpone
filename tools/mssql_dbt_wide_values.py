"""Exact SQL Server value reconciliation for the wide dbt certification model."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy
from dpone.strategy_intelligence.typed_reconciliation import TypedColumnSpec, TypedRowHashService


@dataclass(frozen=True, slots=True)
class DbtWideValueMetrics:
    """Full-row source, passthrough, and materialized output digests."""

    source_sha256: str
    passthrough_sha256: str
    output_sha256: str


def dbt_wide_value_metrics(
    connector: Any,
    *,
    source_schema: str,
    source_table: str,
    target_schema: str,
    target_table: str,
    rows: int,
) -> DbtWideValueMetrics:
    """Hash every ordered row and prove all dbt passthrough values are unchanged."""

    if rows <= 0:
        raise ValueError("dbt_wide_value_rows_must_be_positive")
    source_types = tuple(
        (str(name), str(type_name)) for name, type_name in connector.fetch_schema(source_schema, source_table)
    )
    target_types = tuple(
        (str(name), str(type_name)) for name, type_name in connector.fetch_schema(target_schema, target_table)
    )
    if not source_types or len(target_types) != len(source_types) + 1:
        raise RuntimeError("dbt_wide_value_schema_invalid")
    if target_types[-1][0] != "dbt_calculated_amount":
        raise RuntimeError("dbt_wide_calculated_column_missing")
    source_rows = _typed_rows(connector, source_schema, source_table, source_types, rows)
    passthrough_rows = _typed_rows(connector, target_schema, target_table, source_types, rows)
    output_rows = _typed_rows(connector, target_schema, target_table, target_types, rows)
    return DbtWideValueMetrics(
        source_sha256=_hash(source_types, source_rows),
        passthrough_sha256=_hash(source_types, passthrough_rows),
        output_sha256=_hash(target_types, output_rows),
    )


def _typed_rows(
    connector: Any,
    schema: str,
    table: str,
    types: Sequence[tuple[str, str]],
    rows: int,
) -> list[tuple[Any, ...]]:
    expressions = ", ".join(_hash_select_expression(name, type_name) for name, type_name in types)
    observed = connector.get_records(
        f"SELECT TOP ({int(rows)}) {expressions} FROM {connector.qualified_name(schema, table)} ORDER BY [order_id]"
    )
    if len(observed) != rows:
        raise RuntimeError("dbt_wide_value_row_count_mismatch")
    return observed


def _hash(types: Sequence[tuple[str, str]], rows: Sequence[tuple[Any, ...]]) -> str:
    policy = MssqlClickHouseTypePolicy(binary_encoding="hex", time_encoding="seconds_since_midnight")
    service = TypedRowHashService(tuple(TypedColumnSpec(name, type_name) for name, type_name in types), policy=policy)
    return service.hash_rows(rows)


def _hash_select_expression(column: str, source_type: str) -> str:
    quoted = "[" + column.replace("]", "]]") + "]"
    normalized = source_type.strip().lower()
    if normalized.startswith("datetimeoffset"):
        return (
            "CONVERT(VARCHAR(MAX), "
            f"CAST(SWITCHOFFSET(CAST({quoted} AS datetimeoffset), '+00:00') AS datetime2(7)), "
            f"121) AS {quoted}"
        )
    base = normalized.split("(", 1)[0].replace(" nullable", "").strip()
    if base in {"datetime", "datetime2", "smalldatetime"}:
        return f"CONVERT(VARCHAR(33), {quoted}, 121) AS {quoted}"
    return quoted


__all__ = ["DbtWideValueMetrics", "dbt_wide_value_metrics"]

"""Canonical SQL expressions over validated native SQL Server values.

The raw character wire is deliberately not a semantic authority.  Every
strategy first projects it into typed native staging, then uses this module for
row hashes, null-safe equality and partition identity.  This keeps file,
streaming and in-memory artifacts equivalent after lossless conversion.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence


def canonical_value_expression(value: str, mssql_type: str) -> str:
    """Serialize one native MSSQL value without collation or precision loss."""

    dtype = normalized_mssql_type(mssql_type)
    base = dtype.split("(", 1)[0]
    if base in {"float", "real"}:
        return f"CONVERT(nvarchar(99), CONVERT(varchar(99), {value}, 3))"
    if base == "datetimeoffset":
        return f"CONVERT(nvarchar(64), {value}, 127)"
    if base in {"datetime2", "datetime", "smalldatetime"}:
        return f"CONVERT(nvarchar(64), {value}, 126)"
    if base == "date":
        return f"CONVERT(nvarchar(10), {value}, 23)"
    if base == "time":
        return f"CONVERT(nvarchar(64), {value}, 126)"
    if base in {"decimal", "numeric"}:
        return f"CONVERT(nvarchar(100), {value})"
    if base in {"money", "smallmoney"}:
        return f"CONVERT(nvarchar(100), CONVERT(varchar(100), {value}, 2))"
    if base in {"binary", "varbinary", "image", "rowversion", "timestamp"}:
        return f"CONVERT(nvarchar(max), CONVERT(varchar(max), {value}, 2))"
    if base == "uniqueidentifier":
        return f"LOWER(CONVERT(nvarchar(36), {value}))"
    return f"CONVERT(nvarchar(max), {value})"


def canonical_identity_expression(value: str, mssql_type: str) -> str:
    """Return a collation-independent, null-distinct exact identity frame."""

    canonical = canonical_value_expression(value, mssql_type)
    type_tag = _sql_literal(normalized_mssql_type(mssql_type))
    frame = (
        f"CONCAT(N'{type_tag}:', CASE WHEN {value} IS NULL THEN N'N;' ELSE "
        f"CONCAT(N'V', DATALENGTH(CONVERT(varbinary(max), {canonical})), N':', {canonical}, N';') END)"
    )
    return f"CONVERT(varbinary(max), {frame})"


def null_safe_difference_expression(
    left: str,
    right: str,
    mssql_type: str,
) -> str:
    """Compare typed values independently of database/column collation."""

    return (
        f"(({left} IS NULL AND {right} IS NOT NULL) OR "
        f"({left} IS NOT NULL AND {right} IS NULL) OR "
        f"({left} IS NOT NULL AND {right} IS NOT NULL AND "
        f"{canonical_identity_expression(left, mssql_type)} <> "
        f"{canonical_identity_expression(right, mssql_type)}))"
    )


def row_hash_expression(
    resolved_schema: Sequence[tuple[str, str]],
    value_expression: Callable[[str], str],
) -> str:
    """Render SHA-256 from ordered typed business values."""

    frames = [
        f"CONVERT(nvarchar(max), {canonical_identity_expression(value_expression(column), target_type)}, 2)"
        for column, target_type in resolved_schema
    ]
    payload = "N''" if not frames else frames[0] if len(frames) == 1 else "CONCAT(" + ", ".join(frames) + ")"
    return f"CONVERT(char(64), HASHBYTES('SHA2_256', {payload}), 2)"


def lineage_row_id_expression(
    resolved_schema: Sequence[tuple[str, str]],
    value_expression: Callable[[str], str],
    *,
    source_type: str,
    source_schema: str,
    source_table: str,
    unique_key: Sequence[str] = (),
) -> str:
    """Render artifact-neutral row identity over canonical native values.

    When a business key exists, identity is stable across non-key updates.
    Keyless relations use the complete typed business row; identical duplicate
    rows intentionally share a lineage identity because transport row order is
    not a durable source authority.
    """

    by_name = {column: target_type for column, target_type in resolved_schema}
    selected = tuple(unique_key) if unique_key else tuple(by_name)
    if any(column not in by_name for column in selected):
        raise ValueError("mssql_native_lineage.unique_key_missing")
    source_identity = "|".join(("v1", source_type, source_schema, source_table))
    frames = [f"N'{_sql_literal(source_identity)};'"]
    frames.extend(
        f"CONVERT(nvarchar(max), {canonical_identity_expression(value_expression(column), by_name[column])}, 2)"
        for column in selected
    )
    payload = frames[0] if len(frames) == 1 else "CONCAT(" + ", ".join(frames) + ")"
    return f"CONVERT(varchar(64), HASHBYTES('SHA2_256', {payload}), 2)"


def normalized_mssql_type(value: str) -> str:
    """Normalize a physical type for stable type framing."""

    return re.sub(r"\s+", "", str(value).strip().lower())


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


__all__ = [
    "canonical_identity_expression",
    "canonical_value_expression",
    "lineage_row_id_expression",
    "normalized_mssql_type",
    "null_safe_difference_expression",
    "row_hash_expression",
]

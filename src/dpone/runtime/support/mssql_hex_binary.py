"""Hex-on-character-BCP helpers for SQL Server binary columns.

Character ``bcp`` cannot load ``varbinary`` staging columns safely. dpone
projects binary payloads as lowercase hex text, stages them as ``nvarchar(max)``,
and decodes with set-based ``CONVERT(<varbinary>, <expr>, 2)`` into the target
binary contract.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import Any

from dpone.runtime.support.bulk_text_codec import is_bulk_text_type
from dpone.runtime.support.mssql_types import MSSQLTypeMapper

_HEX_BINARY_TOKENS = ("varbinary", "binary", "image")


def is_mssql_hex_binary_wire_type(dtype: str) -> bool:
    """Return true for binary types that use the hex character-BCP wire."""

    normalized = str(dtype or "").lower().strip()
    if not normalized:
        return False
    if normalized in {"timestamp", "rowversion"}:
        return False
    if "rowversion" in normalized:
        return False
    if normalized.startswith("timestamp ") or normalized.startswith("timestamp("):
        return False
    return any(token in normalized for token in _HEX_BINARY_TOKENS)


def plan_mssql_character_staging_types(
    schema: Sequence[tuple[str, str]],
) -> tuple[dict[str, str], frozenset[str], dict[str, str]]:
    """Plan a non-coercing character wire and its declared native types.

    Every wire column is ``nvarchar(max)``.  Bounded text may expand while the
    reversible codec escapes delimiters, and typed raw columns could silently
    round decimal/temporal values before dpone can validate them.  A separate
    native-staging boundary performs guarded conversions after BCP completes.
    """

    column_types: dict[str, str] = {}
    target_column_types: dict[str, str] = {}
    hex_columns: list[str] = []
    for column, dtype in schema:
        target_type = MSSQLTypeMapper.to_mssql(dtype)
        target_column_types[column] = target_type
        column_types[column] = "nvarchar(max)"
        if is_mssql_hex_binary_wire_type(target_type) or is_mssql_hex_binary_wire_type(dtype):
            hex_columns.append(column)
    return column_types, frozenset(hex_columns), target_column_types


def build_mssql_staging_select_expression(
    *,
    column: str,
    alias: str,
    quote_identifier: Callable[[str], str],
    column_types: Mapping[str, str],
    hex_binary_columns: Collection[str],
    target_column_types: Mapping[str, str],
    bulk_text_codec: Any | None,
) -> str:
    """Build one staging→target SELECT expression with text/hex decode."""

    quoted = f"{alias}.{quote_identifier(column)}"
    dtype = column_types.get(column, "")
    expression = quoted
    decoded = False
    if bulk_text_codec is not None and is_bulk_text_type(dtype) and not column.lower().startswith("__dpone__"):
        expression = bulk_text_codec.mssql_decode_expression(quoted)
        decoded = True
    if column in hex_binary_columns:
        target = target_column_types.get(column) or "varbinary(max)"
        return f"CONVERT({target}, {expression}, 2) AS {quote_identifier(column)}"
    if decoded:
        return f"{expression} AS {quote_identifier(column)}"
    return quoted


__all__ = [
    "build_mssql_staging_select_expression",
    "is_mssql_hex_binary_wire_type",
    "plan_mssql_character_staging_types",
]

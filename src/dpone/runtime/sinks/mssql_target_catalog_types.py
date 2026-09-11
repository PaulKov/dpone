"""Canonical SQL Server catalog type shapes and rendering."""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

from dpone.contracts.mssql_type_contract import normalize_mssql_physical_type


def canonical_catalog_scalar(value: Any) -> str | None:
    """Normalize catalog numeric variants without locale- or driver-dependence."""

    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def canonical_mssql_catalog_type(dtype: str) -> str:
    """Return the exact type declaration reconstructed from ``sys.columns``.

    SQL Server expands declaration defaults in its catalog (notably ``float``
    to precision 53). Fresh-target physical planning must model that after-image
    instead of hashing the shorter, semantically equivalent input declaration.
    """

    base, max_length, precision, scale = validated_mssql_catalog_type_shape(dtype)
    return render_mssql_catalog_type(
        base,
        max_length=max_length,
        precision=precision,
        scale=scale,
    )


def validated_mssql_catalog_type_shape(dtype: str) -> tuple[str, int, int, int]:
    """Validate a DDL declaration and project the exact catalog after-image.

    Keep the low-level normalized-shape parser separate so its established
    accepted input and errors remain unchanged for existing Python callers.
    """

    normalized = normalize_mssql_physical_type(dtype)
    return mssql_catalog_type_shape(normalized)


def render_mssql_catalog_type(
    name: str,
    *,
    max_length: int,
    precision: int,
    scale: int,
) -> str:
    """Render one canonical SQL Server type from exact catalog metadata."""

    base = str(name).lower()
    if base in {"binary", "varbinary", "char", "varchar"}:
        return f"{base}({'max' if max_length == -1 else max_length})"
    if base in {"nchar", "nvarchar"}:
        return f"{base}({'max' if max_length == -1 else max_length // 2})"
    if base in {"decimal", "numeric"}:
        return f"{base}({precision},{scale})"
    if base == "float":
        return f"float({precision})"
    if base in {"time", "datetime2", "datetimeoffset"}:
        return f"{base}({scale})"
    return base


def mssql_catalog_type_shape(dtype: str) -> tuple[str, int, int, int]:
    """Project a normalized declaration to SQL Server catalog metadata."""

    match = re.fullmatch(r"([a-z0-9_]+)(?:\((max|\d+)(?:,(\d+))?\))?", dtype)
    if match is None:
        raise ValueError("mssql_transaction.target_catalog_type_invalid")
    base, first, second = match.groups()
    fixed = {
        "bigint": (8, 19, 0),
        "bit": (1, 1, 0),
        "date": (3, 10, 0),
        "datetime": (8, 23, 3),
        "int": (4, 10, 0),
        "money": (8, 19, 4),
        "real": (4, 24, 0),
        "smalldatetime": (4, 16, 0),
        "smallint": (2, 5, 0),
        "smallmoney": (4, 10, 4),
        "tinyint": (1, 3, 0),
        "uniqueidentifier": (16, 0, 0),
    }
    if base in fixed:
        length, precision, scale = fixed[base]
        return base, length, precision, scale
    if base in {"decimal", "numeric"}:
        return base, _decimal_storage(int(first or 0)), int(first or 0), int(second or 0)
    if base == "float":
        precision = int(first or 53)
        return base, 4 if precision <= 24 else 8, precision, 0
    if base in {"binary", "varbinary", "char", "varchar", "nchar", "nvarchar"}:
        length = -1 if first == "max" else int(first or 0)
        return base, length * 2 if base in {"nchar", "nvarchar"} and length != -1 else length, 0, 0
    if base in {"time", "datetime2", "datetimeoffset"}:
        scale = int(first or 7)
        extra = 0 if scale <= 2 else 1 if scale <= 4 else 2
        base_length = {"time": 3, "datetime2": 6, "datetimeoffset": 8}[base]
        precision = {
            "time": 8 if scale == 0 else 9 + scale,
            "datetime2": 19 if scale == 0 else 20 + scale,
            "datetimeoffset": 26 if scale == 0 else 27 + scale,
        }[base]
        return base, base_length + extra, precision, scale
    raise ValueError("mssql_transaction.target_catalog_type_unsupported")


def mssql_catalog_type_is_text(base: str) -> bool:
    """Return whether a catalog base type inherits database collation."""

    return base in {"char", "varchar", "nchar", "nvarchar"}


def _decimal_storage(precision: int) -> int:
    if precision <= 9:
        return 5
    if precision <= 19:
        return 9
    if precision <= 28:
        return 13
    return 17


__all__ = [
    "canonical_catalog_scalar",
    "canonical_mssql_catalog_type",
    "mssql_catalog_type_is_text",
    "mssql_catalog_type_shape",
    "render_mssql_catalog_type",
    "validated_mssql_catalog_type_shape",
]

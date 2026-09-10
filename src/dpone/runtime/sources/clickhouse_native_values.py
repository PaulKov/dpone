"""Lossless source projections avoiding driver floating-point timestamp conversion."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

_EPOCH = datetime(1970, 1, 1)


def temporal(dtype: str) -> bool:
    return dtype.removeprefix("Nullable(").startswith("DateTime")


def projection(name: str, dtype: str) -> tuple[str, str]:
    """Return SQL and exact wire header type for one admitted business column."""
    column = f"`{name}`"
    if temporal(dtype):
        return (
            f"toUnixTimestamp64Micro(toDateTime64({column}, 6, 'UTC')) AS {column}",
            "Nullable(Int64)" if dtype.startswith("Nullable(") else "Int64",
        )
    return column, dtype


def restore(value: Any, dtype: str) -> Any:
    if value is None or not temporal(dtype):
        return value
    if type(value) is not int:
        raise ValueError("mssql_native.temporal_integer_required")
    return _EPOCH + timedelta(microseconds=value)

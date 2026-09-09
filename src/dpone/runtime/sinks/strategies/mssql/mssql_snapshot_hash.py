"""Compatibility facade for canonical native SQL Server value hashing."""

from __future__ import annotations

from dpone.runtime.support.mssql_native_canonical import (
    canonical_value_expression,
    row_hash_expression,
)

__all__ = ["canonical_value_expression", "row_hash_expression"]

"""Compatibility exports for SQL Server object-name primitives."""

from __future__ import annotations

from dpone.runtime.support.mssql_object_name import (
    MSSQLObjectName,
    mssql_dataset_is_safe,
    mssql_ensure_schema_statement,
    quote_mssql_identifier,
    safe_mssql_identifier,
)

__all__ = [
    "MSSQLObjectName",
    "mssql_dataset_is_safe",
    "mssql_ensure_schema_statement",
    "quote_mssql_identifier",
    "safe_mssql_identifier",
]

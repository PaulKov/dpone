"""Shared SQL Server catalog object-name authority."""

from __future__ import annotations

from dpone.contracts.mssql_object_name import MSSQLObjectName, quote_mssql_identifier


def mssql_sys_catalog(target: MSSQLObjectName, catalog: str) -> str:
    """Return the exact cross-database ``sys`` catalog relation name."""

    database = target.database
    if database is None:
        raise RuntimeError("mssql_transaction.target_database_required")
    return quote_mssql_identifier(database) + ".sys." + quote_mssql_identifier(catalog)


__all__ = ["MSSQLObjectName", "mssql_sys_catalog", "quote_mssql_identifier"]

"""Fail-closed SQL Server catalog capability detection."""

from dpone.adapters.composition_mssql_catalog_query import require_exact_catalog_rows
from dpone.ports.sql_connection import SqlControlCursor


def require_database_collation(cursor: SqlControlCursor, collation: str) -> None:
    """Read the current database default independently of reference bytes."""
    require_exact_catalog_rows(
        cursor, "collation", "d.collation_name FROM sys.databases d WHERE d.database_id=DB_ID();", ((collation,),)
    )


def supports_ledger_catalog(cursor: SqlControlCursor) -> bool:
    """Return whether SQL Ledger columns are present in ``sys.tables``."""
    cursor.execute(
        "SELECT TOP (2) /* composition_schema:server_capabilities */ TRY_CONVERT(int,"
        "SERVERPROPERTY('ProductMajorVersion')),TRY_CONVERT(int,SERVERPROPERTY('EngineEdition'));"
    )
    rows = tuple(tuple(row) for row in cursor.fetchall())
    if len(rows) != 1 or len(rows[0]) != 2:
        raise ValueError("server capabilities unavailable")
    major, edition = rows[0]
    if type(major) is not int or type(edition) is not int:
        raise ValueError("server capabilities unavailable")
    return major >= 16 or edition in {5, 8}


def ledger_catalog_projections(supported: bool) -> tuple[str, str, str]:
    """Project impossible SQL Server 2019 ledger state without absent columns."""
    if supported:
        return "t.ledger_type", "CONVERT(int,t.is_dropped_ledger_table)", "t.ledger_view_id"
    return "CONVERT(int,0)", "CONVERT(int,0)", "CAST(NULL AS int)"

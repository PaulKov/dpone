"""Normalize runtime labels before resolving SQL Server target authority."""

from __future__ import annotations

from dpone.runtime.support.mssql_object_name import MSSQLObjectName


def normalize_mssql_target_coordinates(
    *, database: str | None, schema: str | None, table: str | None
) -> tuple[str, str, str]:
    """Return explicit database, bare schema and table without guessing authority.

    Compiled load configs retain a ``database.schema`` label for connector
    compatibility. Registry lookups and the XMin preflight cache need its bare
    schema instead. Require the explicit database even when the label contains
    one, reject conflicting prefixes, and preserve identifier case so SQL
    Server remains responsible for collation and the physical binding.
    """
    requested_database, requested_schema, requested_table = (
        str(value or "").strip() for value in (database, schema, table)
    )
    if not all((requested_database, requested_schema, requested_table)):
        raise ValueError("mssql_physical_target_coordinates_incomplete")
    try:
        name = MSSQLObjectName.from_parts(
            database=requested_database,
            schema=requested_schema,
            table=requested_table,
        )
    except ValueError as exc:
        raise ValueError("mssql_physical_target_coordinates_invalid") from exc
    return requested_database, name.schema, name.table

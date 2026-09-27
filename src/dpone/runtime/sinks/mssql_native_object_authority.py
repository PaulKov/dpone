"""Strict SQL Server object identity and metadata-visibility authority."""

from __future__ import annotations

from typing import Any


def read_exact_object_id(connector: Any, qualified: str) -> int | None:
    """Read one valid object ID; preserve ``NULL`` for an authority check."""
    rows = connector.get_records("SELECT OBJECT_ID(?)", (qualified,))
    if len(rows) != 1 or len(rows[0]) != 1:
        raise ValueError("mssql_native.object_identity_unavailable")
    value = rows[0][0]
    if value is not None and (type(value) is not int or value < 1):
        raise ValueError("mssql_native.object_identity_unavailable")
    return value


def require_unfiltered_object_metadata(connector: Any, *, diagnostic: str) -> None:
    """Allow absence inference only for dbo or a SQL Server sysadmin."""
    rows = connector.get_records("SELECT USER_NAME(), IS_SRVROLEMEMBER('sysadmin')")
    if len(rows) != 1 or len(rows[0]) != 2 or (rows[0][0] != "dbo" and rows[0][1] != 1):
        raise ValueError(diagnostic)


__all__ = ["read_exact_object_id", "require_unfiltered_object_metadata"]

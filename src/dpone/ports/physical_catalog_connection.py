"""Narrow catalog additions to the existing SQL-control DB-API capability."""

from typing import Protocol

from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


class PhysicalCatalogCursor(SqlControlCursor, Protocol):
    """Expose rowset exhaustion so diagnostic/extra rowsets cannot be ignored."""

    def nextset(self) -> bool | None: ...


class PhysicalCatalogConnection(SqlControlConnection, Protocol):
    """Qualified driver with a finite SQL statement timeout, measured in seconds.

    The factory must separately supply a finite login/connect timeout. A driver
    timeout is not proof of server quiescence after a failed or cancelled call.
    """

    timeout: int

    def cursor(self) -> PhysicalCatalogCursor: ...

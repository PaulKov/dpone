"""Thin optional dbt plugin; strict managed execution is deliberately unavailable."""

from typing import Any

from dbt.adapters.base.meta import available
from dbt.adapters.dpone_sqlserver.connections import DpOneSQLServerConnectionManager
from dbt.adapters.sqlserver import SQLServerAdapter


class DpOneSQLServerAdapter(SQLServerAdapter):
    """Preserve inherited ordinary SQL behavior and expose one finite parse-safe API.

    The preparatory wheel does not authorize managed requests. Real admission,
    recorder/factory delivery and live driver qualification must land together;
    method presence is not capability evidence or an automatic fallback.
    """

    ConnectionManager = DpOneSQLServerConnectionManager

    @available.parse_none
    def dpone_physical_protocol_v1(self, operation: str, parameters: dict[str, Any]) -> Any:
        """No SQL until authenticated B1 composition and qualification exist.

        Public arguments are only a finite operation and its bound parameters;
        no cursor, SQL, budget, transaction or credential controls are accepted.
        ParseDatabaseWrapper replaces this method with a no-I/O None function.
        """
        raise ValueError(
            "strict physical transport unavailable: authenticated B1 composition and qualification required"
        )

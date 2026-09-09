"""Read-only SQL Server target metrics used by schema governance."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.contracts.mssql_object_name import MSSQLObjectName

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


@dataclass(frozen=True, slots=True)
class MssqlTargetMetrics:
    """Read exact target metrics only when an operator configures a budget."""

    connector: Any

    def row_count(self, load_config: LoadConfig) -> int:
        """Return an exact SQL Server ``COUNT_BIG`` for the target table."""

        name = MSSQLObjectName.from_parts(
            database=getattr(load_config, "target_database", None),
            schema=str(load_config.target_schema),
            table=str(load_config.target_table),
        )
        rows = self.connector.get_records(f"SELECT COUNT_BIG(*) FROM {name.quoted()}")
        value = rows[0][0] if rows else 0
        if isinstance(value, bool):
            raise ValueError("MSSQL target partition row count must be a non-negative integer")
        result = int(value or 0)
        if result < 0:
            raise ValueError("MSSQL target partition row count must be a non-negative integer")
        return result


__all__ = ["MssqlTargetMetrics"]

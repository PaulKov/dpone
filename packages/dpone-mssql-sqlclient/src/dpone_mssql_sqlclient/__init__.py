"""Locator for the optional dpone Microsoft.Data.SqlClient companion."""

from dpone_mssql_sqlclient._provider import (
    SqlClientCompanion,
    SqlClientCompanionUnavailable,
    capabilities,
    locate,
)

__all__ = ["SqlClientCompanion", "SqlClientCompanionUnavailable", "capabilities", "locate"]

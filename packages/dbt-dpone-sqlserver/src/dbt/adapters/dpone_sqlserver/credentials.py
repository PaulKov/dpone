"""Actual distinct adapter identity; never relabel an ordinary qualification."""

from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials


class DpOneSQLServerCredentials(SQLServerCredentials):
    """Inherit ordinary credentials without analyst-visible private controls."""

    @property
    def type(self) -> str:
        return "dpone_sqlserver"

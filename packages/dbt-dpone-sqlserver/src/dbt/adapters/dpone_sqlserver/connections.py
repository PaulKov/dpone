"""Ordinary inherited SQL Server lifecycle with a distinct manager identity."""

from dbt.adapters.sqlserver.sqlserver_connections import SQLServerConnectionManager


class DpOneSQLServerConnectionManager(SQLServerConnectionManager):
    """No connection hooks, retries or transaction changes added by this plugin."""

    TYPE = "dpone_sqlserver"

from __future__ import annotations

import os

import pytest

from dpone.runtime.connectors.mssql import MSSQLConnector


class IntegrationLogger:
    """No-op ETL logger for source/sink integration tests."""

    def log_etl_progress(self, *_args, **_kwargs) -> None:
        return None


def open_mssql_connector() -> MSSQLConnector:
    """Open the local Docker SQL Server connector or skip when unavailable."""

    host = os.getenv("DPONE_IT_MSSQL_HOST")
    if not host:
        pytest.skip("DPONE_IT_MSSQL_HOST is not configured")
    connector = MSSQLConnector(
        host=host,
        port=int(os.getenv("DPONE_IT_MSSQL_PORT", "1433")),
        database=os.getenv("DPONE_IT_MSSQL_DATABASE", "dpone"),
        user=os.getenv("DPONE_IT_MSSQL_USER", "sa"),
        password=os.getenv("DPONE_IT_MSSQL_PASSWORD", ""),
        driver=os.getenv("DPONE_IT_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        trust_server_certificate=os.getenv("DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE", "yes"),
        bcp_path=os.getenv("DPONE_IT_MSSQL_BCP_PATH", "bcp"),
    )
    try:
        connector.execute_query("SELECT 1")
    except Exception as exc:
        connector.close()
        pytest.skip(f"MSSQL integration endpoint is unavailable: {exc}")
    return connector


__all__ = ["IntegrationLogger", "open_mssql_connector"]

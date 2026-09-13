"""Explicit environment-only credentials for the approved disposable services."""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any


class Environment:
    """Lazy connector composition; repr and inventory never include secrets."""

    def __init__(self, *, root: Path, values: dict[str, str] | None = None) -> None:
        self.root = Path(root)
        self._values = dict(os.environ if values is None else values)
        self.database = self._required("DPONE_IT_MSSQL_DATABASE")
        self.source_database = self._required("DPONE_IT_CH_DATABASE")
        # Provisioning is deliberately confined to the approved DDA database.
        if self.database != "dda_synthetic" or self.source_database != "dda_synthetic":
            raise ValueError("local_fixture.unapproved_database")

    def _required(self, key: str) -> str:
        value = self._values.get(key)
        if not value:
            raise ValueError("local_fixture.missing_environment:" + key)
        return value

    def sql(self, database: str | None = None) -> Any:
        from dpone.runtime.connectors.mssql import MSSQLConnector

        return MSSQLConnector(
            host=self._required("DPONE_IT_MSSQL_HOST"),
            port=int(self._required("DPONE_IT_MSSQL_PORT")),
            database=database or self.database,
            user=self._required("DPONE_IT_MSSQL_USER"),
            password=self._required("DPONE_IT_MSSQL_PASSWORD"),
            driver="ODBC Driver 18 for SQL Server",
            encrypt="yes",
            trust_server_certificate=self._values.get("DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE", "yes"),
            bcp_path=self._required("DPONE_IT_MSSQL_BCP_PATH"),
            autocommit=True,
            application_name="dpone-dda-local",
            query_timeout=120,
        )

    def clickhouse(self, *, binary: bool = False) -> Any:
        from dpone.runtime.connectors.clickhouse import ClickHouseConnector

        return ClickHouseConnector(
            host=self._required("DPONE_IT_CH_HOST"),
            port=int(self._required("DPONE_IT_CH_PORT")),
            database=self.source_database,
            user=self._required("DPONE_IT_CH_USER"),
            password=self._required("DPONE_IT_CH_PASSWORD"),
            settings={"strings_as_bytes": binary},
            application_name="dpone-dda-local",
        )

    @contextmanager
    def sql_scope(self, database: str | None = None):
        connector = self.sql(database)
        try:
            yield connector
        finally:
            connector.close()

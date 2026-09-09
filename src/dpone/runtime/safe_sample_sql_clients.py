"""Runtime SQL clients for the MSSQL -> ClickHouse safe-sample path.

These adapters sit in the runtime plane. They execute already-built,
secret-free service-layer plans through injected connection providers or
connector factories. Importing this module does not open network connections or
resolve credentials.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from dpone.runtime.credentials.config import CredentialsConfig

_PARAM_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ConnectionProvider(Protocol):
    """Return an already configured, possibly lazy runtime connection."""

    def __call__(self) -> Any:
        """Return a DB connection/client compatible with the adapter."""


class MssqlDbApiSafeSampleSqlClient:
    """Execute MSSQL safe-sample read plans through a DB-API-like connection."""

    def __init__(self, *, connection_provider: ConnectionProvider) -> None:
        self._connection_provider = connection_provider

    def fetch(self, plan: Any) -> Mapping[str, Any]:
        cursor = self._connection_provider().cursor()
        try:
            _set_timeout(cursor, plan.timeout_seconds)
            cursor.execute(_mssql_dbapi_sql(plan), _dbapi_params(plan.parameters))
            rows = _rows(cursor.fetchall(), getattr(cursor, "description", None))
        finally:
            close = getattr(cursor, "close", None)
            if callable(close):
                close()
        return {
            "rows": rows,
            "rows_read": len(rows),
            "bytes_read": _estimated_bytes(rows),
            "diagnostics": {"client": "dbapi_mssql", "rows_returned": len(rows)},
        }


class ClickHouseConnectionSafeSampleSqlClient:
    """Execute ClickHouse safe-sample insert plans through a clickhouse-driver-like client."""

    def __init__(self, *, connection_provider: ConnectionProvider) -> None:
        self._connection_provider = connection_provider

    def insert(self, plan: Any) -> Mapping[str, Any]:
        rows = [dict(row) for row in plan.rows]
        if not str(plan.sql).rstrip().endswith(" VALUES"):
            raise ValueError("ClickHouse native safe-sample insert must end with VALUES")
        if not rows:
            return {
                "rows_written": 0,
                "bytes_written": 0,
                "diagnostics": {"client": "clickhouse_connection", "rows_sent": 0},
            }
        inserted_rows = self._connection_provider().execute(plan.sql, rows)
        if type(inserted_rows) is not int:
            raise RuntimeError("ClickHouse safe-sample driver must return an integer inserted-row count")
        if inserted_rows != plan.row_count:
            raise RuntimeError("ClickHouse safe-sample driver row count mismatch")
        return {
            "rows_written": plan.row_count,
            "bytes_written": _estimated_bytes(rows),
            "diagnostics": {"client": "clickhouse_connection", "rows_sent": plan.row_count},
        }


class MssqlCredentialsSafeSampleSqlClientFactory:
    """Create MSSQL safe-sample clients from resolved ``CredentialsConfig`` values."""

    def __init__(self, *, connector_factory: Callable[..., Any] | None = None) -> None:
        self._connector_factory = connector_factory

    def create(self, credentials: CredentialsConfig) -> MssqlDbApiSafeSampleSqlClient:
        connector = self._mssql_connector_factory()(
            host=_required(credentials.host, "host"),
            port=credentials.port or 1433,
            database=_required(credentials.database, "database"),
            user=credentials.username,
            password=credentials.password,
            driver=credentials.driver or "ODBC Driver 18 for SQL Server",
            encrypt=credentials.encrypt if credentials.encrypt is not None else "yes",
            trust_server_certificate=credentials.trust_server_certificate
            if credentials.trust_server_certificate is not None
            else "no",
            connect_timeout=credentials.connect_timeout,
            query_timeout=credentials.query_timeout or 0,
        )
        return MssqlDbApiSafeSampleSqlClient(connection_provider=lambda: connector.connection)

    def _mssql_connector_factory(self) -> Callable[..., Any]:
        if self._connector_factory is not None:
            return self._connector_factory
        from dpone.runtime.connectors.mssql import MSSQLConnector

        return MSSQLConnector


class ClickHouseCredentialsSafeSampleSqlClientFactory:
    """Create ClickHouse safe-sample clients from resolved ``CredentialsConfig`` values."""

    def __init__(self, *, connector_factory: Callable[..., Any] | None = None) -> None:
        self._connection_provider_factory = ClickHouseCredentialsConnectionProviderFactory(
            connector_factory=connector_factory
        )

    def create(self, credentials: CredentialsConfig) -> ClickHouseConnectionSafeSampleSqlClient:
        return ClickHouseConnectionSafeSampleSqlClient(
            connection_provider=self._connection_provider_factory.create(credentials)
        )


class ClickHouseCredentialsConnectionProviderFactory:
    """Create a lazy ClickHouse connection provider from resolved credentials."""

    def __init__(self, *, connector_factory: Callable[..., Any] | None = None) -> None:
        self._connector_factory = connector_factory

    def create(self, credentials: CredentialsConfig) -> ConnectionProvider:
        connector = self._clickhouse_connector_factory()(
            host=_required(credentials.host, "host"),
            port=credentials.port or _clickhouse_default_port(credentials),
            database=credentials.database or "default",
            user=credentials.username or "default",
            password=credentials.password or "",
            secure=bool(credentials.secure),
            compression=credentials.compression,
            connect_timeout=credentials.connect_timeout,
            send_receive_timeout=credentials.send_receive_timeout,
            settings=credentials.settings,
        )
        return lambda: connector.connection

    def _clickhouse_connector_factory(self) -> Callable[..., Any]:
        if self._connector_factory is not None:
            return self._connector_factory
        from dpone.runtime.connectors.clickhouse import ClickHouseConnector

        return ClickHouseConnector


def _mssql_dbapi_sql(plan: Any) -> str:
    declarations = []
    for name, value in plan.parameters.items():
        parameter_name = str(name)
        if not _PARAM_NAME_RE.match(parameter_name):
            raise ValueError(f"Unsafe MSSQL parameter name: {parameter_name!r}")
        declarations.append(f"DECLARE @{parameter_name} {_mssql_parameter_type(value)} = ?;")
    if not declarations:
        return plan.sql
    return "\n".join(declarations + [plan.sql])


def _dbapi_params(parameters: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(parameters.values())


def _mssql_parameter_type(value: Any) -> str:
    if isinstance(value, bool):
        return "bit"
    if isinstance(value, int):
        return "int"
    return "nvarchar(max)"


def _set_timeout(cursor: Any, timeout_seconds: int) -> None:
    if hasattr(cursor, "timeout"):
        try:
            cursor.timeout = timeout_seconds
        except Exception:
            return


def _rows(raw_rows: Any, description: Any) -> list[dict[str, Any]]:
    columns = [str(column[0]) for column in (description or ())]
    rows = []
    for row in raw_rows or ():
        if isinstance(row, Mapping):
            rows.append(dict(row))
            continue
        rows.append(dict(zip(columns, tuple(row))))
    return rows


def _estimated_bytes(value: Any) -> int:
    return len(json.dumps(value, default=str, ensure_ascii=False, sort_keys=True).encode("utf-8"))


def _required(value: str | None, field: str) -> str:
    if not value:
        raise ValueError(f"Safe-sample SQL client credentials require {field}")
    return value


def _clickhouse_default_port(credentials: CredentialsConfig) -> int:
    return 9440 if credentials.secure else 9000


__all__ = [
    "ClickHouseConnectionSafeSampleSqlClient",
    "ClickHouseCredentialsConnectionProviderFactory",
    "ClickHouseCredentialsSafeSampleSqlClientFactory",
    "ConnectionProvider",
    "MssqlCredentialsSafeSampleSqlClientFactory",
    "MssqlDbApiSafeSampleSqlClient",
]

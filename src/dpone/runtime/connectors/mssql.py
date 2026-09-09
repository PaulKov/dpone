"""SQL Server connector built on pyodbc plus native bcp bulk operations."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import replace
from typing import Any

from dpone.contracts.mssql_object_name import MSSQLObjectName
from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
from dpone.runtime.connectors.base import AbstractConnector
from dpone.runtime.connectors.mssql_bulk import BcpCredentials, BcpDsnOptions, BcpOptions, BcpRunner
from dpone.runtime.connectors.mssql_datetimeoffset import (
    SQL_SS_TIMESTAMPOFFSET as _SQL_SS_TIMESTAMPOFFSET,
)
from dpone.runtime.connectors.mssql_datetimeoffset import (
    MSSQLDatetimeOffsetDecodeError,
)
from dpone.runtime.connectors.mssql_datetimeoffset import (
    decode_datetimeoffset as _decode_datetimeoffset,
)
from dpone.runtime.connectors.mssql_query_timeout import MssqlBoundedQueryTimeoutMixin
from dpone.runtime.connectors.mssql_sql import MSSQLSqlRenderer
from dpone.runtime.connectors.mssql_support import (
    build_mssql_connection_string,
    normalize_odbc_options,
)
from dpone.runtime.connectors.mssql_support import (
    first_rowset as _first_rowset,
)
from dpone.runtime.connectors.mssql_support import (
    require_pyodbc as _require_pyodbc,
)

_SQL_RENDERER_DELEGATES = {
    "quote_identifier": "quote_identifier",
    "qualified_name": "qualified_name",
    "build_max_query": "build_max_query",
}
_BULK_OPERATION_DELEGATES = frozenset({"bcp_import", "bcp_import_format", "bcp_queryout"})


class _MssqlConnectorBulkOperations:
    """Connector-facing facade over injectable native BCP runners."""

    def __init__(
        self,
        runner_factory: Callable[[BcpOptions | None], BcpRunner],
        qualified_name: Callable[..., str],
    ) -> None:
        self._runner_factory = runner_factory
        self._qualified_name = qualified_name

    def bcp_queryout(self, query: str, output_path: str, *, options: BcpOptions | None = None) -> int:
        return self._runner_factory(options).queryout(query, output_path).rows_copied or 0

    def bcp_import(
        self,
        schema: str,
        table: str,
        file_path: str,
        *,
        options: BcpOptions | None = None,
        database: str | None = None,
    ) -> int:
        target = self._qualified_name(schema, table, database=database)
        return self._runner_factory(options).import_file(target, file_path).rows_copied or 0

    def bcp_import_format(
        self,
        schema: str,
        table: str,
        file_path: str,
        format_path: str,
        *,
        options: BcpOptions | None = None,
        database: str | None = None,
    ) -> int:
        target = self._qualified_name(schema, table, database=database)
        result = self._runner_factory(options).import_format_file(target, file_path, format_path)
        return result.rows_copied or 0


class MSSQLConnector(MssqlBoundedQueryTimeoutMixin, AbstractConnector):
    """Focused SQL Server connector.

    The connector keeps row-oriented SQL operations in pyodbc and delegates bulk
    import/export to ``BcpRunner``. This split makes the native path explicit and
    keeps tests independent from local ODBC/bcp installations.
    """

    dialect = "mssql"

    def __init__(
        self,
        host: str,
        port: int | None,
        database: str,
        user: str | None = None,
        password: str | None = None,
        *,
        driver: str = "ODBC Driver 18 for SQL Server",
        encrypt: str | bool = "yes",
        trust_server_certificate: str | bool = "no",
        connect_timeout: int = 10,
        query_timeout: int = 0,
        autocommit: bool = True,
        application_name: str = "dpone-mssql",
        bcp_path: str = "bcp",
        odbc_options: Mapping[str, Any] | None = None,
        bcp_runner_cls: type[BcpRunner] = BcpRunner,
        sql_renderer: MSSQLSqlRenderer | None = None,
    ) -> None:
        self._sql = sql_renderer or MSSQLSqlRenderer()
        self.host = host
        self.port = int(port or 1433)
        self.database = database
        self.user = user
        self.password = password
        self.driver = driver
        self.encrypt = self._sql.bool_option(encrypt)
        self.trust_server_certificate = self._sql.bool_option(trust_server_certificate)
        self.connect_timeout = int(connect_timeout or 0)
        self.query_timeout = query_timeout
        self.autocommit = autocommit
        self.application_name = application_name
        self.bcp_path = bcp_path
        self.odbc_options = normalize_odbc_options(dict(odbc_options or {}))
        self._connection: Any | None = None
        self._bcp_runner_cls = bcp_runner_cls
        self._bulk_operations = _MssqlConnectorBulkOperations(self._bcp_runner, self.qualified_name)

    def __getattr__(self, name: str) -> Any:
        if name in _SQL_RENDERER_DELEGATES:
            return getattr(self._sql, _SQL_RENDERER_DELEGATES[name])
        if name in _BULK_OPERATION_DELEGATES:
            return getattr(self._bulk_operations, name)
        raise AttributeError(f"{self.__class__.__name__!s} has no attribute {name!r}")

    def open_session(self, *, application_name: str) -> MSSQLConnector:
        """Create an independent ODBC session with the same resolved credentials.

        Session-scoped application locks must not share the target transaction
        connection: commit-ack recovery is allowed to close that connection.
        """

        if not application_name or len(application_name) > 128:
            raise ValueError("MSSQL session application_name must contain 1..128 characters")
        return MSSQLConnector(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            driver=self.driver,
            encrypt=self.encrypt,
            trust_server_certificate=self.trust_server_certificate,
            connect_timeout=self.connect_timeout,
            query_timeout=self.query_timeout,
            autocommit=self.autocommit,
            application_name=application_name,
            bcp_path=self.bcp_path,
            odbc_options=self.odbc_options,
            bcp_runner_cls=self._bcp_runner_cls,
            sql_renderer=self._sql,
        )

    @property
    def connection(self) -> Any:
        if self._connection is None:
            pyodbc = _require_pyodbc()
            kwargs: dict[str, Any] = {"autocommit": self.autocommit}
            if self.connect_timeout:
                kwargs["timeout"] = self.connect_timeout
            self._connection = pyodbc.connect(self._connection_string(), **kwargs)
            add_output_converter = getattr(self._connection, "add_output_converter", None)
            if callable(add_output_converter):
                add_output_converter(_SQL_SS_TIMESTAMPOFFSET, _decode_datetimeoffset)
            if self.query_timeout:
                self._connection.timeout = self.query_timeout
        return self._connection

    @property
    def bcp_runner(self) -> BcpRunner:
        return self._bcp_runner()

    def execute_query(self, query: Any, params: Iterable[Any] | None = None) -> int:
        cursor = self.connection.cursor()
        try:
            cursor.execute(str(query), tuple(params or ()))
            return max(cursor.rowcount or 0, 0)
        finally:
            cursor.close()

    def get_records(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        as_dict: bool = False,
    ) -> list[Any]:
        cursor = self.connection.cursor()
        try:
            cursor.execute(str(query), tuple(params or ()))
            rows, columns = _first_rowset(cursor)
            if not as_dict:
                return [tuple(row) for row in rows]
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()

    def get_records_streaming(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        batch_size: int = 10000,
        as_dict: bool = False,
    ):
        cursor = self.connection.cursor()
        try:
            cursor.execute(str(query), tuple(params or ()))
            columns = [column[0] for column in cursor.description or []]
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                if as_dict:
                    yield [dict(zip(columns, row)) for row in rows]
                else:
                    yield [tuple(row) for row in rows]
        finally:
            cursor.close()

    def get_records_iterator(self, query: Any, params: Iterable[Any] | None = None):
        for batch in self.get_records_streaming(query, params=params, batch_size=10000, as_dict=True):
            yield from batch

    def begin(self) -> None:
        self.connection.autocommit = False

    def commit_transaction(self) -> None:
        self.connection.commit()
        self.connection.autocommit = self.autocommit

    def rollback(self) -> None:
        self.connection.rollback()
        self.connection.autocommit = self.autocommit

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def build_select_query(
        self,
        schema: str,
        table: str,
        columns: Sequence[str],
        limit: int | None = None,
        offset: int | None = None,
        database: str | None = None,
    ) -> str:
        return self._sql.build_select_query(schema, table, columns, limit=limit, offset=offset, database=database)

    def get_max_column_value(self, schema: str, table: str, column: str, *, database: str | None = None) -> Any | None:
        rows = self.get_records(self.build_max_query(schema, table, column, database=database))
        return rows[0][0] if rows else None

    def build_max_query(self, schema: str, table: str, column: str, *, database: str | None = None) -> str:
        return self._sql.build_max_query(schema, table, column, database=database)

    def table_exists(self, schema: str, table: str, *, database: str | None = None) -> bool:
        name = MSSQLObjectName.from_parts(schema=schema, table=table, database=database)
        rows = self.get_records(
            f"""
            SELECT 1
            FROM {name.information_schema_table("TABLES")}
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """,
            (name.schema, name.table),
        )
        return bool(rows)

    def get_table_column_types(self, schema: str, table: str, *, database: str | None = None) -> dict[str, str]:
        name = MSSQLObjectName.from_parts(schema=schema, table=table, database=database)
        rows = self.get_records(
            f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {name.information_schema_table("COLUMNS")}
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """,
            (name.schema, name.table),
            as_dict=True,
        )
        return {str(row["COLUMN_NAME"]): str(row["DATA_TYPE"]) for row in rows}

    def fetch_schema(self, schema: str, table: str, *, database: str | None = None) -> list[tuple[str, str]]:
        name = MSSQLObjectName.from_parts(schema=schema, table=table, database=database)
        rows = self.get_records(
            f"""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
                   DATETIME_PRECISION, IS_NULLABLE
            FROM {name.information_schema_table("COLUMNS")}
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """,
            (name.schema, name.table),
            as_dict=True,
        )
        return [(str(row["COLUMN_NAME"]), self._sql.format_type(row)) for row in rows]

    def fetch_schema_columns(
        self,
        schema: str,
        table: str,
        *,
        database: str | None = None,
    ) -> list[MssqlCatalogColumn]:
        """Return exact names, types, nullability, and collation for evolution."""

        name = MSSQLObjectName.from_parts(schema=schema, table=table, database=database)
        rows = self.get_records(
            f"""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION,
                   NUMERIC_SCALE, DATETIME_PRECISION, IS_NULLABLE, COLLATION_NAME
            FROM {name.information_schema_table("COLUMNS")}
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """,
            (name.schema, name.table),
            as_dict=True,
        )
        return [
            MssqlCatalogColumn(
                name=str(row["COLUMN_NAME"]),
                dtype=self._sql.format_type(row).removesuffix(" nullable"),
                nullable=str(row["IS_NULLABLE"]).strip().upper() == "YES",
                collation=str(row["COLLATION_NAME"]) if row.get("COLLATION_NAME") else None,
            )
            for row in rows
        ]

    def _bcp_runner(self, options: BcpOptions | None = None) -> BcpRunner:
        effective = options or BcpOptions(bcp_path=self.bcp_path)
        if effective.bcp_path == "bcp" and self.bcp_path != "bcp":
            effective = replace(effective, bcp_path=self.bcp_path)
        if self.trust_server_certificate == "yes" and not effective.trust_server_certificate:
            effective = replace(effective, trust_server_certificate=True)
        if effective.connection_dsn is None and self.odbc_options.get("MultiSubnetFailover") == "Yes":
            effective = replace(
                effective,
                connection_dsn=BcpDsnOptions(
                    driver=self.driver,
                    encrypt=self.encrypt,
                    trust_server_certificate=self.trust_server_certificate,
                    login_timeout_seconds=self.connect_timeout or 60,
                ),
            )
        return self._bcp_runner_cls(
            BcpCredentials(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                trusted_connection=not bool(self.user),
            ),
            effective,
        )

    def _connection_string(self) -> str:
        return build_mssql_connection_string(
            driver=self.driver,
            host=self.host,
            port=self.port,
            database=self.database,
            application_name=self.application_name,
            encrypt=self.encrypt,
            trust_server_certificate=self.trust_server_certificate,
            odbc_options=self.odbc_options,
            user=self.user,
            password=self.password,
        )


__all__ = ["MSSQLConnector", "MSSQLDatetimeOffsetDecodeError"]

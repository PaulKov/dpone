"""MySQL connector backed by optional PyMySQL."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from dpone.runtime.connectors.base import AbstractConnector
from dpone.runtime.connectors.mysql_export import MySQLExportMixin
from dpone.type_system.source_sink.mysql_mssql import MySQLMssqlTypeMapper
from dpone.type_system.source_sink.mysql_postgres import MySQLPostgresTypeMapper


def _require_pymysql():
    try:
        import pymysql  # type: ignore[import-untyped]
        from pymysql.cursors import SSDictCursor  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - exercised when extra missing
        raise ImportError(
            'MySQL support requires optional dependency PyMySQL. Install with: pip install "dpone[mysql]"'
        ) from exc
    return pymysql, SSDictCursor


class MySQLConnector(MySQLExportMixin, AbstractConnector):
    """Thin MySQL connector used by MySQLSource extraction strategies."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        *,
        charset: str = "utf8mb4",
        connect_timeout: int = 10,
        autocommit: bool = True,
        ssl: Mapping[str, Any] | bool | None = None,
    ) -> None:
        self.host = host
        self.port = port or 3306
        self.database = database
        self.user = user
        self.password = password
        self.charset = charset
        self.connect_timeout = connect_timeout
        self.autocommit = autocommit
        self.ssl = ssl
        self._type_mapper = MySQLMssqlTypeMapper()
        self._postgres_type_mapper = MySQLPostgresTypeMapper()
        self._connection = None

    def _connect_kwargs(self, *, cursorclass: Any) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "charset": self.charset,
            "connect_timeout": self.connect_timeout,
            "autocommit": self.autocommit,
            "cursorclass": cursorclass,
        }
        if self.ssl is not None:
            kwargs["ssl"] = self.ssl
        return kwargs

    @property
    def connection(self):
        if self._connection is None or not self._connection.open:
            pymysql, _ = _require_pymysql()
            self._connection = pymysql.connect(**self._connect_kwargs(cursorclass=pymysql.cursors.DictCursor))
        return self._connection

    def clone_for_partition(self, partition_index: int) -> MySQLConnector:
        del partition_index
        return MySQLConnector(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            charset=self.charset,
            connect_timeout=self.connect_timeout,
            autocommit=self.autocommit,
            ssl=self.ssl,
        )

    def execute_query(self, query: Any, params: Iterable[Any] | None = None) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(query, tuple(params) if params is not None else None)
            return int(cursor.rowcount or 0)

    def get_records(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        as_dict: bool = False,
    ) -> list[Any]:
        with self.connection.cursor() as cursor:
            cursor.execute(query, tuple(params) if params is not None else None)
            rows = cursor.fetchall()
            if as_dict:
                return list(rows)
            return [tuple(row.values()) for row in rows]

    def get_records_streaming(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        *,
        as_dict: bool = True,
        batch_size: int = 10000,
    ) -> Iterable[list[Any]]:
        pymysql, ss_dict = _require_pymysql()
        kwargs = self._connect_kwargs(cursorclass=ss_dict)
        kwargs["autocommit"] = True
        connection = pymysql.connect(**kwargs)
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, tuple(params) if params is not None else None)
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    if as_dict:
                        yield list(rows)
                    else:
                        yield [tuple(row.values()) for row in rows]
        finally:
            connection.close()

    def get_records_iterator(self, query: Any, params: Iterable[Any] | None = None):
        for batch in self.get_records_streaming(query, params=params, as_dict=True, batch_size=10000):
            yield from batch

    def begin(self) -> None:
        self.connection.autocommit(False)

    def commit_transaction(self) -> None:
        self.connection.commit()
        self.connection.autocommit(self.autocommit)

    def rollback(self) -> None:
        self.connection.rollback()
        self.connection.autocommit(self.autocommit)

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    @staticmethod
    def quote_identifier(name: str) -> str:
        escaped = str(name).replace("`", "``")
        return f"`{escaped}`"

    def build_select_query(
        self,
        schema: str,
        table: str,
        columns: Sequence[str],
        limit: int | None = None,
        offset: int | None = None,
        database: str | None = None,
    ) -> str:
        db = database or schema or self.database
        qualified = f"{self.quote_identifier(db)}.{self.quote_identifier(table)}"
        projection = ", ".join(self.quote_identifier(column) for column in columns) if columns else "*"
        query = f"SELECT {projection} FROM {qualified}"
        if limit is not None:
            query += f" LIMIT {int(limit)}"
        if offset is not None:
            query += f" OFFSET {int(offset)}"
        return query

    def get_table_column_types(
        self,
        schema: str,
        table: str,
        *,
        database: str | None = None,
    ) -> dict[str, str]:
        db = database or schema or self.database
        rows = self.get_records(
            """
            SELECT COLUMN_NAME, COLUMN_TYPE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (db, table),
            as_dict=True,
        )
        return {str(row["COLUMN_NAME"]): str(row["COLUMN_TYPE"]) for row in rows}

    def get_max_column_value(
        self,
        schema: str,
        table: str,
        column: str,
        *,
        database: str | None = None,
    ) -> Any:
        db = database or schema or self.database
        query = (
            f"SELECT MAX({self.quote_identifier(column)}) AS max_value "
            f"FROM {self.quote_identifier(db)}.{self.quote_identifier(table)}"
        )
        rows = self.get_records(query, as_dict=True)
        if not rows:
            return None
        return rows[0].get("max_value")


__all__ = ["MySQLConnector"]

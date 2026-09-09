"""SQL store implementations for vendor-live Route Conformance adapters."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.ops.routes.conformance_models import RouteConformanceColumn, RouteConformanceSnapshot
from dpone.ops.routes.conformance_vendor_sql_values import (
    clickhouse_type,
    clickhouse_value,
    executemany_insert,
    mssql_type,
    mssql_value,
    normalize_db_rows,
    postgres_type,
    postgres_value,
)


@dataclass(frozen=True, slots=True)
class _DatasetName:
    namespace: str
    table: str


class SqlConformanceDialect(Protocol):
    """SQL dialect contract used by the lazy conformance store."""

    def reset(self, connector: Any, dataset_id: str, columns: Sequence[RouteConformanceColumn]) -> None: ...

    def insert_rows(
        self,
        connector: Any,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
        rows: Sequence[Mapping[str, object]],
    ) -> int: ...

    def add_column(
        self, connector: Any, dataset_id: str, column: RouteConformanceColumn, default_value: object
    ) -> None: ...

    def read_rows(
        self,
        connector: Any,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
    ) -> tuple[Mapping[str, object], ...]: ...


class SqlRouteConformanceLiveStore:
    """Live store backed by a lazily-created runtime connector."""

    def __init__(
        self,
        *,
        backend: str,
        connector_factory: Callable[[], Any],
        dialect: SqlConformanceDialect,
    ) -> None:
        self._backend = backend
        self._connector_factory = connector_factory
        self._dialect = dialect
        self._connector: Any | None = None

    @property
    def backend(self) -> str:
        return self._backend

    @property
    def connector(self) -> Any:
        if self._connector is None:
            self._connector = self._connector_factory()
        return self._connector

    def reset(self, *, dataset_id: str, columns: Sequence[RouteConformanceColumn]) -> None:
        self._dialect.reset(self.connector, dataset_id, columns)

    def seed(
        self,
        *,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
        rows: Sequence[Mapping[str, object]],
    ) -> int:
        self.reset(dataset_id=dataset_id, columns=columns)
        return self._dialect.insert_rows(self.connector, dataset_id, columns, rows)

    def add_column(self, *, dataset_id: str, column: RouteConformanceColumn, default_value: object) -> None:
        self._dialect.add_column(self.connector, dataset_id, column, default_value)

    def read_rows(
        self,
        *,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
    ) -> tuple[Mapping[str, object], ...]:
        return self._dialect.read_rows(self.connector, dataset_id, columns)

    def replace_rows(
        self,
        *,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
        rows: Sequence[Mapping[str, object]],
    ) -> int:
        self.reset(dataset_id=dataset_id, columns=columns)
        return self._dialect.insert_rows(self.connector, dataset_id, columns, rows)

    def read_snapshot(
        self,
        *,
        dataset_id: str,
        kind: str,
        columns: Sequence[RouteConformanceColumn],
    ) -> RouteConformanceSnapshot:
        rows = self.read_rows(dataset_id=dataset_id, columns=columns)
        return RouteConformanceSnapshot(
            kind=kind,
            columns=tuple(columns),
            rows=rows,
            fingerprint=_fingerprint(columns=columns, rows=rows),
        )

    def close(self) -> None:
        if self._connector is not None and hasattr(self._connector, "close"):
            self._connector.close()
        self._connector = None


class PostgresConformanceDialect:
    """Postgres DDL/DML for route conformance live stores."""

    def reset(self, connector: Any, dataset_id: str, columns: Sequence[RouteConformanceColumn]) -> None:
        name = _split_dataset_id(dataset_id)
        connector.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{name.namespace}"')
        connector.execute_query(f'DROP TABLE IF EXISTS "{name.namespace}"."{name.table}"')
        ddl = ", ".join(f'"{column.name}" {postgres_type(column)}' for column in columns)
        connector.execute_query(f'CREATE UNLOGGED TABLE "{name.namespace}"."{name.table}" ({ddl})')

    def insert_rows(
        self,
        connector: Any,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
        rows: Sequence[Mapping[str, object]],
    ) -> int:
        return executemany_insert(
            connector,
            qualified=self._qualified(dataset_id),
            columns=columns,
            rows=rows,
            placeholder="%s",
            prepare_value=postgres_value,
        )

    def add_column(
        self, connector: Any, dataset_id: str, column: RouteConformanceColumn, default_value: object
    ) -> None:
        qualified = self._qualified(dataset_id)
        connector.execute_query(
            f'ALTER TABLE {qualified} ADD COLUMN IF NOT EXISTS "{column.name}" {postgres_type(column)}'
        )
        if default_value is not None:
            connector.execute_query(
                f'UPDATE {qualified} SET "{column.name}" = %s', (postgres_value(column, default_value),)
            )

    def read_rows(
        self,
        connector: Any,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
    ) -> tuple[Mapping[str, object], ...]:
        select_list = ", ".join(f'"{column.name}"' for column in columns)
        rows = connector.get_records(
            f'SELECT {select_list} FROM {self._qualified(dataset_id)} ORDER BY "id"',
            as_dict=True,
        )
        return normalize_db_rows(rows, columns)

    def _qualified(self, dataset_id: str) -> str:
        name = _split_dataset_id(dataset_id)
        return f'"{name.namespace}"."{name.table}"'


class MssqlConformanceDialect:
    """MSSQL DDL/DML for route conformance live stores."""

    def reset(self, connector: Any, dataset_id: str, columns: Sequence[RouteConformanceColumn]) -> None:
        name = _split_dataset_id(dataset_id)
        connector.execute_query(f"IF SCHEMA_ID('{name.namespace}') IS NULL EXEC('CREATE SCHEMA [{name.namespace}]')")
        connector.execute_query(f"DROP TABLE IF EXISTS [{name.namespace}].[{name.table}]")
        ddl = ", ".join(f"[{column.name}] {mssql_type(column)}" for column in columns)
        connector.execute_query(f"CREATE TABLE [{name.namespace}].[{name.table}] ({ddl})")

    def insert_rows(
        self,
        connector: Any,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
        rows: Sequence[Mapping[str, object]],
    ) -> int:
        return executemany_insert(
            connector,
            qualified=self._qualified(dataset_id),
            columns=columns,
            rows=rows,
            placeholder="?",
            prepare_value=mssql_value,
            quote_column=lambda name: f"[{name}]",
        )

    def add_column(
        self, connector: Any, dataset_id: str, column: RouteConformanceColumn, default_value: object
    ) -> None:
        qualified = self._qualified(dataset_id)
        connector.execute_query(f"ALTER TABLE {qualified} ADD [{column.name}] {mssql_type(column)}")
        if default_value is not None:
            connector.execute_query(
                f"UPDATE {qualified} SET [{column.name}] = ?", (mssql_value(column, default_value),)
            )

    def read_rows(
        self,
        connector: Any,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
    ) -> tuple[Mapping[str, object], ...]:
        select_list = ", ".join(f"[{column.name}]" for column in columns)
        rows = connector.get_records(
            f"SELECT {select_list} FROM {self._qualified(dataset_id)} ORDER BY [id]", as_dict=True
        )
        return normalize_db_rows(rows, columns)

    def _qualified(self, dataset_id: str) -> str:
        name = _split_dataset_id(dataset_id)
        return f"[{name.namespace}].[{name.table}]"


class ClickHouseConformanceDialect:
    """ClickHouse DDL/DML for route conformance live stores."""

    def reset(self, connector: Any, dataset_id: str, columns: Sequence[RouteConformanceColumn]) -> None:
        name = _split_dataset_id(dataset_id)
        connector.execute_query(f"CREATE DATABASE IF NOT EXISTS `{name.namespace}`")
        connector.execute_query(f"DROP TABLE IF EXISTS `{name.namespace}`.`{name.table}`")
        ddl = ", ".join(f"`{column.name}` {clickhouse_type(column)}" for column in columns)
        connector.execute_query(
            f"CREATE TABLE `{name.namespace}`.`{name.table}` ({ddl}) ENGINE = MergeTree ORDER BY `id`"
        )

    def insert_rows(
        self,
        connector: Any,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
        rows: Sequence[Mapping[str, object]],
    ) -> int:
        values = [tuple(clickhouse_value(column, row.get(column.name)) for column in columns) for row in rows]
        if not values:
            return 0
        column_list = ", ".join(f"`{column.name}`" for column in columns)
        connector.connection.execute(f"INSERT INTO {self._qualified(dataset_id)} ({column_list}) VALUES", values)
        return len(values)

    def add_column(
        self, connector: Any, dataset_id: str, column: RouteConformanceColumn, default_value: object
    ) -> None:
        connector.execute_query(
            f"ALTER TABLE {self._qualified(dataset_id)} ADD COLUMN IF NOT EXISTS `{column.name}` {clickhouse_type(column)}"
        )
        del default_value

    def read_rows(
        self,
        connector: Any,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
    ) -> tuple[Mapping[str, object], ...]:
        select_list = ", ".join(f"`{column.name}`" for column in columns)
        rows = connector.get_records(
            f"SELECT {select_list} FROM {self._qualified(dataset_id)} ORDER BY `id`",
            as_dict=True,
        )
        return normalize_db_rows(rows, columns)

    def _qualified(self, dataset_id: str) -> str:
        name = _split_dataset_id(dataset_id)
        return f"`{name.namespace}`.`{name.table}`"


def _split_dataset_id(dataset_id: str) -> _DatasetName:
    if "." in dataset_id:
        namespace, table = dataset_id.split(".", 1)
    else:
        namespace, table = "dpone_rc", dataset_id
    return _DatasetName(namespace=namespace, table=table)


def _fingerprint(*, columns: Sequence[RouteConformanceColumn], rows: Sequence[Mapping[str, object]]) -> str:
    payload = {
        "columns": [column.to_dict() for column in columns],
        "rows": [dict(row) for row in rows],
    }
    data = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(data.encode()).hexdigest()


__all__ = [
    "ClickHouseConformanceDialect",
    "MssqlConformanceDialect",
    "PostgresConformanceDialect",
    "SqlConformanceDialect",
    "SqlRouteConformanceLiveStore",
]

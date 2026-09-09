"""DB connector port (interface).

This module is part of the **ports** layer.

Why here?
---------
Core / DAG / manifest analysis must not depend on runtime implementations.
Keeping the connector interface in `dpone.ports` allows:

- `dpone.core.*` to depend on the interface only (DIP)
- `dpone.runtime.connectors.*` to provide implementations

`AbstractConnector` includes a few convenience default methods that are
implementation-agnostic (they use only other abstract methods).
"""

from __future__ import annotations

import csv
import os
import shutil
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from typing import Any

from dpone.lib.utils import log_query_with_params
from dpone.ports.sql_renderer import SqlQueryRenderer
from dpone.ports.sql_result_probe import header_probe_sql


class AbstractConnector(ABC):
    """Базовый интерфейс коннектора к СУБД."""

    @property
    @abstractmethod
    def connection(self):
        """Возвращает низкоуровневое соединение."""

    @abstractmethod
    def execute_query(self, query: Any, params: Iterable[Any] | None = None) -> int:
        """Выполняет запрос (INSERT/UPDATE/DELETE) и возвращает количество строк."""

    @abstractmethod
    def get_records(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        as_dict: bool = False,
    ) -> list[Any]:
        """Выполняет SELECT и возвращает результат."""

    @abstractmethod
    def commit_transaction(self) -> None:
        """Фиксирует транзакцию."""

    @abstractmethod
    def rollback(self) -> None:
        """Откатывает транзакцию."""

    @abstractmethod
    def begin(self) -> None:
        """Начинает транзакцию."""

    @abstractmethod
    def build_select_query(
        self,
        schema: str,
        table: str,
        columns: list[str],
        limit: int | None = None,
        offset: int | None = None,
    ) -> Any:
        """Build SELECT query in connector-specific dialect."""

    def print_query(self, query: Any, params: Iterable[Any] | None = None) -> None:
        """Log query/params with automatic masking of sensitive fields."""

        log_query_with_params(query, params)

    def get_max_column_value(
        self,
        schema: str,
        table: str,
        column: str,
    ) -> Any | None:
        """Get MAX(column) from a table."""

        query = self.build_max_query(schema, table, column)
        result = self.get_records(query, as_dict=True)
        if result and result[0].get("max_val"):
            return result[0]["max_val"]
        return None

    @property
    def sql_renderer(self) -> SqlQueryRenderer:
        """Dialect-specific SQL renderer for convenience query builders.

        Base connectors intentionally do not ship generic SQL defaults for
        vendor-sensitive queries. Concrete database connectors should either
        provide this renderer through DI or override individual builder methods.
        """

        raise NotImplementedError(
            f"{self.__class__.__name__} must provide a SqlQueryRenderer or override build_max_query()."
        )

    def build_max_query(self, schema: str, table: str, column: str) -> Any:
        """Build MAX(column) query in connector-specific dialect."""

        return self.sql_renderer.build_max_query(schema, table, column)

    def get_records_iterator(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
    ) -> Iterator[Any]:
        """Streaming iterator over query results.

        Default implementation loads all rows via :meth:`get_records`.
        Implementations with server-side cursors should override.
        """

        rows = self.get_records(query, params, as_dict=True)
        yield from rows

    def describe_result_columns(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
    ) -> list[str]:
        """Resolve SELECT result column names without requiring data rows.

        Default implementation probes with ``LIMIT 0`` and reads keys from
        the first sample row. Connectors that expose column metadata for empty
        results (for example ClickHouse ``with_column_types``) SHOULD override
        this method so empty tables still export headers correctly.
        """

        sample = self.get_records(header_probe_sql(query), params, as_dict=True)
        if not sample:
            return []
        return list(sample[0].keys())

    def export_to_csv(
        self,
        query: str,
        output_dir: str,
        params: Iterable[Any] | None = None,
        *,
        chunk_size: int = 500_000,
        file_prefix: str = "export",
        clean_output_dir: bool = False,
    ) -> list[str]:
        """Export query results to chunked CSV files."""

        self.print_query(query, params)

        if clean_output_dir and os.path.exists(output_dir):
            shutil.rmtree(output_dir)

        os.makedirs(output_dir, exist_ok=True)

        headers = self.describe_result_columns(query, params)
        if not headers:
            return []

        # 2) Stream rows
        file_paths: list[str] = []
        file_index = 1
        row_in_file = 0

        def open_new_writer():
            nonlocal file_index, row_in_file
            file_path = os.path.join(output_dir, f"{file_prefix}_{file_index:04d}.csv")
            f = open(file_path, "w", newline="", encoding="utf-8")
            writer = csv.writer(f)
            writer.writerow(headers)
            file_paths.append(file_path)
            row_in_file = 0
            file_index += 1
            return writer, f

        writer, f = open_new_writer()
        try:
            for row in self.get_records_iterator(query, params):
                writer.writerow(row)
                row_in_file += 1
                if row_in_file >= chunk_size:
                    f.close()
                    writer, f = open_new_writer()

            return file_paths

        except Exception as e:
            raise RuntimeError(f"CSV export error: {e}") from e
        finally:
            if not f.closed:
                f.close()

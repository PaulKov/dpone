"""Low-level query and streaming operations for ClickHouseConnector."""

from __future__ import annotations

import csv
import os
import time
from collections.abc import Iterable, Iterator
from decimal import Decimal
from typing import Any

from dpone.ports.sql_result_probe import header_probe_sql
from dpone.runtime.connectors.clickhouse_result_mapping import (
    column_names_from_types,
    row_as_dict,
    rows_as_dicts,
)

_DDL_RETRYABLE_CODES = ("Code: 517",)
_DDL_RETRYABLE_MARKERS = ("Metadata on replica is not up to date",)
_DDL_PREFIXES = (
    "ALTER ",
    "ATTACH ",
    "CREATE ",
    "DETACH ",
    "DROP ",
    "EXCHANGE ",
    "OPTIMIZE ",
    "RENAME ",
    "TRUNCATE ",
)
_DEFAULT_DDL_RETRY_ATTEMPTS = 5
_DEFAULT_DDL_RETRY_BACKOFF_SECONDS = 2.0
_MAX_DDL_RETRY_BACKOFF_SECONDS = 30.0


class ClickHouseQueryOps:
    """Encapsulates low-level query execution and streaming helpers."""

    def __init__(self, connector: Any):
        self.connector = connector

    @property
    def logger(self):
        return self.connector.logger

    def execute_query(self, query: Any, params: Iterable[Any] | None = None) -> int:
        self.connector.print_query(query, params)
        attempts = _ddl_retry_attempts()
        settings = _execute_settings(self.connector, query)
        for attempt in range(1, attempts + 1):
            try:
                self.connector.connection.execute(query, params, settings=settings)
                return 0
            except Exception as e:
                if not _should_retry_ddl(query, e, attempt=attempt, attempts=attempts):
                    raise RuntimeError(f"ClickHouse execute_query error: {e}") from e
                delay = _ddl_retry_delay(attempt)
                self._log_retryable_ddl_lag(query, attempt=attempt, attempts=attempts, delay=delay, exc=e)
                if delay > 0:
                    time.sleep(delay)
        return 0

    def _log_retryable_ddl_lag(
        self,
        query: Any,
        *,
        attempt: int,
        attempts: int,
        delay: float,
        exc: Exception,
    ) -> None:
        if hasattr(self.logger, "warning"):
            self.logger.warning(
                "retryable ClickHouse metadata lag; retrying DDL query",
                {
                    "attempt": attempt,
                    "max_attempts": attempts,
                    "delay_seconds": delay,
                    "error": _short_error(exc),
                    "statement_kind": _statement_kind(query),
                },
            )

    def get_records(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        as_dict: bool = False,
    ) -> list[Any]:
        self.connector.print_query(query, params)
        settings = _execute_settings(self.connector, query)
        try:
            if as_dict:
                rows, cols = self.connector.connection.execute(query, params, with_column_types=True, settings=settings)
                return rows_as_dicts(column_names_from_types(cols), rows)
            return self.connector.connection.execute(query, params, settings=settings)
        except Exception as e:  # pragma: no cover - passthrough
            raise RuntimeError(f"ClickHouse get_records error: {e}") from e

    def describe_result_columns(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
    ) -> list[str]:
        """Resolve result column names without requiring data rows."""

        self.connector.print_query(query, params)
        settings = _execute_settings(self.connector, query)
        try:
            _rows, cols = self.connector.connection.execute(
                header_probe_sql(query),
                params,
                with_column_types=True,
                settings=settings,
            )
            return column_names_from_types(cols)
        except Exception as e:  # pragma: no cover - passthrough
            raise RuntimeError(f"ClickHouse describe_result_columns error: {e}") from e

    def get_records_iterator(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
    ) -> Iterator[Any]:
        return self.connector.connection.execute_iter(query, params)

    def import_from_csv(
        self,
        table_schema: str,
        table_name: str,
        csv_file: str,
        delimiter: str = ",",
        batch_rows: int = 100_000,
    ) -> None:
        full_table = f"{table_schema}.{table_name}" if table_schema else table_name
        column_types = self._get_table_column_types(table_schema=table_schema, table_name=table_name)

        with open(csv_file, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            columns = reader.fieldnames or []
            if not columns:
                raise ValueError("CSV must contain header row with column names")

            insert_sql = f"INSERT INTO {full_table} ({', '.join(columns)}) VALUES"
            batch: list[tuple[Any, ...]] = []

            for row in reader:
                batch.append(tuple(self._coerce_csv_value(row.get(col), column_types.get(col)) for col in columns))
                if len(batch) >= batch_rows:
                    self.connector.connection.execute(insert_sql, batch)
                    batch.clear()

            if batch:
                self.connector.connection.execute(insert_sql, batch)

    def _get_table_column_types(self, *, table_schema: str, table_name: str) -> dict[str, str]:
        database = table_schema or self.connector.database
        rows = self.connector.connection.execute(
            """
            SELECT name, type
            FROM system.columns
            WHERE database = %(database)s AND table = %(table)s
            ORDER BY position
            """,
            {"database": database, "table": table_name},
        )
        return {str(name): str(column_type) for name, column_type in rows}

    @staticmethod
    def _unwrap_clickhouse_type(column_type: str) -> tuple[str, bool]:
        base_type = column_type.strip()
        nullable = False
        while True:
            if base_type.startswith("Nullable(") and base_type.endswith(")"):
                nullable = True
                base_type = base_type[len("Nullable(") : -1].strip()
                continue
            if base_type.startswith("LowCardinality(") and base_type.endswith(")"):
                base_type = base_type[len("LowCardinality(") : -1].strip()
                continue
            return base_type, nullable

    def _coerce_csv_value(self, value: str | None, column_type: str | None) -> Any:
        if value is None or column_type is None:
            return value

        base_type, nullable = self._unwrap_clickhouse_type(column_type)
        if value == "":
            return None if nullable else value

        if base_type.startswith(("Int", "UInt")):
            return int(value)
        if base_type.startswith("Float"):
            return float(value)
        if base_type.startswith("Decimal"):
            return Decimal(value)
        return value

    def query_max_column(self, table_name: str, column_name: str) -> Any:
        sql = f"SELECT max({column_name}) FROM {table_name}"
        res = self.get_records(sql)
        return res[0][0] if res else None

    def get_records_streaming(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        batch_size: int = 10000,
        as_dict: bool = False,
    ) -> Iterable[list[Any]]:
        self.connector.print_query(query, params)
        try:
            if as_dict:
                resolved_names = self.describe_result_columns(query, params)
                batch: list[dict[str, Any]] = []
                for row in self.connector.connection.execute_iter(query, params):
                    batch.append(row_as_dict(resolved_names, row))
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                if batch:
                    yield batch
                return

            batch_tuples: list[Any] = []
            for row in self.connector.connection.execute_iter(query, params):
                batch_tuples.append(row)
                if len(batch_tuples) >= batch_size:
                    yield batch_tuples
                    batch_tuples = []
            if batch_tuples:
                yield batch_tuples
        except Exception as e:  # pragma: no cover - passthrough
            raise RuntimeError(f"ClickHouse get_records_streaming error: {e}") from e

    def build_select_query(
        self,
        schema: str,
        table: str,
        columns: list[str],
        limit: int | None = None,
        offset: int | None = None,
    ) -> str:
        columns_str = ", ".join(f"`{col}`" for col in columns)
        query = f"SELECT {columns_str} FROM `{schema}`.`{table}`"
        if limit is not None:
            query += f" LIMIT {limit}"
        if offset is not None:
            query += f" OFFSET {offset}"
        return query


def _should_retry_ddl(query: Any, exc: Exception, *, attempt: int, attempts: int) -> bool:
    return attempt < attempts and _is_ddl_like(query) and _is_retryable_cluster_metadata_error(exc)


def _is_ddl_like(query: Any) -> bool:
    text = str(query).lstrip()
    return any(text.upper().startswith(prefix) for prefix in _DDL_PREFIXES)


def _is_on_cluster_ddl(query: Any) -> bool:
    text = " ".join(str(query).split())
    return _is_ddl_like(query) and " ON CLUSTER " in text.upper()


def _execute_settings(connector: Any, query: Any) -> dict[str, Any] | None:
    """Merge connector settings with cluster-topology tolerance defaults.

    Intentionally isolated replicas stay Inactive in system.distributed_ddl_queue
    and refuse native TCP. Default throw/180s blocks staging CREATE/DROP ON
    CLUSTER (Code 159); Distributed/remote reads then fail with Code 279 while
    probing the dead host. Prefer null_status_on_timeout for ON CLUSTER DDL and
    skip_unavailable_shards for query fan-out so live replicas can finish.
    """

    settings = dict(getattr(connector, "settings", None) or {})
    skip_raw = os.getenv("DPONE_CLICKHOUSE_SKIP_UNAVAILABLE_SHARDS", "1")
    try:
        skip_unavailable = 1 if int(skip_raw) else 0
    except ValueError:
        skip_unavailable = 1
    settings.setdefault("skip_unavailable_shards", skip_unavailable)
    if _is_on_cluster_ddl(query):
        settings.setdefault(
            "distributed_ddl_output_mode",
            os.getenv("DPONE_CLICKHOUSE_DISTRIBUTED_DDL_OUTPUT_MODE", "null_status_on_timeout"),
        )
        raw_timeout = os.getenv("DPONE_CLICKHOUSE_DISTRIBUTED_DDL_TASK_TIMEOUT", "30")
        try:
            timeout = max(1, int(raw_timeout))
        except ValueError:
            timeout = 30
        settings.setdefault("distributed_ddl_task_timeout", timeout)
    return settings or None


def _is_retryable_cluster_metadata_error(exc: Exception) -> bool:
    text = str(exc)
    return any(code in text for code in _DDL_RETRYABLE_CODES) or any(
        marker in text for marker in _DDL_RETRYABLE_MARKERS
    )


def _ddl_retry_attempts() -> int:
    raw = os.getenv("DPONE_CLICKHOUSE_DDL_RETRY_ATTEMPTS")
    if not raw:
        return _DEFAULT_DDL_RETRY_ATTEMPTS
    try:
        return max(1, int(raw))
    except ValueError:
        return _DEFAULT_DDL_RETRY_ATTEMPTS


def _ddl_retry_delay(attempt: int) -> float:
    raw = os.getenv("DPONE_CLICKHOUSE_DDL_RETRY_BACKOFF_SECONDS")
    try:
        base = float(raw) if raw is not None else _DEFAULT_DDL_RETRY_BACKOFF_SECONDS
    except ValueError:
        base = _DEFAULT_DDL_RETRY_BACKOFF_SECONDS
    return max(0.0, min(_MAX_DDL_RETRY_BACKOFF_SECONDS, base * (2 ** (attempt - 1))))


def _statement_kind(query: Any) -> str:
    parts = str(query).lstrip().split(None, 1)
    return parts[0].upper() if parts else "UNKNOWN"


def _short_error(exc: Exception) -> str:
    return str(exc).splitlines()[0][:500]

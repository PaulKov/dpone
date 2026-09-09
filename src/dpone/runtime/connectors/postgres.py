"""PostgreSQL коннектор."""

from __future__ import annotations

import hashlib
import time
from collections.abc import Iterable, Iterator
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from dpone.runtime.connectors.base import AbstractConnector


class PostgresConnector(AbstractConnector):
    """Коннектор для PostgreSQL, оборачивает psycopg3."""

    dialect = "postgres"

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        application_name: str = "dpone",
        autocommit: bool = True,
    ):
        self.host = host
        self.port = port or 5432
        self.database = database
        self.user = user
        self.password = password
        self.application_name = application_name
        self.autocommit = autocommit
        self._connection: psycopg.Connection[dict[str, Any]] | None = None

    @property
    def connection(self):
        if not self._connection:
            conninfo = (
                f"host={self.host} port={self.port} dbname={self.database} "
                f"user={self.user} password={self.password} application_name={self.application_name}"
            )
            self._connection = psycopg.connect(conninfo=conninfo, row_factory=dict_row, autocommit=self.autocommit)
        return self._connection

    def clone_for_partition(self, partition_index: int) -> PostgresConnector:
        """Create an isolated connection for one parallel partition worker."""

        return PostgresConnector(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            application_name=f"{self.application_name}-partition-{partition_index}",
            autocommit=self.autocommit,
        )

    def execute_query(self, query: Any, params: Iterable[Any] | None = None) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.rowcount or 0

    def get_records(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        as_dict: bool = False,
    ) -> list[Any]:
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            if as_dict:
                return rows
            return [tuple(row.values()) for row in rows]

    def begin(self):
        """Начинает явную транзакцию."""
        self.connection.autocommit = False

    def commit_transaction(self) -> None:
        """Фиксирует транзакцию и возвращает autocommit."""
        self.connection.commit()
        self.connection.autocommit = self.autocommit

    def rollback(self) -> None:
        """Откатывает транзакцию и возвращает autocommit."""
        self.connection.rollback()
        self.connection.autocommit = self.autocommit

    def copy_from_iter(
        self,
        schema: str,
        table: str,
        columns: Iterable[str],
        rows: Iterable[str],
    ) -> int:
        copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(col) for col in columns),
        )

        processed = 0
        with self.connection.cursor() as cursor:
            with cursor.copy(copy_sql) as copy:
                for row in rows:
                    copy.write(row)
                    processed += 1
        return processed

    def get_records_streaming(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
        batch_size: int = 10000,
        as_dict: bool = False,
    ) -> Iterable[list[Any]]:
        """Streaming версия get_records с fetchmany."""
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                if as_dict:
                    yield batch
                else:
                    yield [tuple(row.values()) for row in batch]

    def get_records_iterator(
        self,
        query: Any,
        params: Iterable[Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield rows one-by-one for incremental source strategies.

        XMin and column-cursor strategies consume a row iterator so small deltas
        can stream into staging without loading the full result set in memory.
        """

        for batch in self.get_records_streaming(query, params=params, batch_size=10000, as_dict=True):
            yield from batch

    def copy_query_to_table_csv(
        self,
        source_query: Any,
        target_schema: str,
        target_table: str,
        columns: Iterable[str],
        logger=None,
    ) -> None:
        """Прямой COPY из query в таблицу через INSERT INTO ... SELECT."""
        # Преобразуем source_query в строку если это SQL объект
        if hasattr(source_query, "as_string"):
            query_string = source_query.as_string(self.connection)
        else:
            query_string = str(source_query)

        if logger:
            logger.log_etl_progress(
                "COPY_START",
                {
                    "Source Query": query_string[:100] + "..." if len(query_string) > 100 else query_string,
                    "Target": f"{target_schema}.{target_table}",
                    "Columns": len(list(columns)),
                },
            )

        insert_sql = f"""
        INSERT INTO {target_schema}.{target_table} ({", ".join(f'"{col}"' for col in columns)})
        {query_string}
        """

        start_time = time.time()
        self.execute_query(insert_sql)

        if logger:
            duration = time.time() - start_time
            logger.log_etl_progress(
                "COPY_COMPLETE", {"Duration": f"{duration:.2f}s", "Target": f"{target_schema}.{target_table}"}
            )

    def copy_to_file(
        self,
        query_sql,
        output_path: str,
        format: str = "CSV",
        compress: bool = True,
        compress_level: int = 1,
        buffer_size: int = 16 * 1024 * 1024,
        logger=None,
        params: tuple[object, ...] = (),
    ) -> dict[str, Any]:
        """
        Выполняет COPY TO STDOUT с записью в файл.

        Args:
            query_sql: SELECT запрос для экспорта
            output_path: Путь к выходному файлу
            format: Формат COPY (CSV или BINARY)
            compress: Использовать gzip сжатие
            compress_level: Уровень сжатия gzip (0-9)
            buffer_size: Размер буфера для записи (байты)
            logger: Логгер для прогресса
        """
        import gzip

        from psycopg import sql

        format_upper = format.upper().replace("-", "_")
        export_sql: Any

        if isinstance(query_sql, sql.Composable | sql.Composed):
            if format_upper == "CSV":
                export_sql = sql.Composed(
                    [sql.SQL("COPY ("), query_sql, sql.SQL(") TO STDOUT WITH (FORMAT CSV, FORCE_QUOTE *)")]
                )
            elif format_upper == "MSSQL_DELIMITED":
                export_sql = sql.Composed(
                    [
                        sql.SQL("COPY ("),
                        query_sql,
                        sql.SQL(
                            ") TO STDOUT WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE E'\\x1f', ESCAPE E'\\x1f', NULL '')"
                        ),
                    ]
                )
            else:
                export_sql = sql.Composed(
                    [sql.SQL("COPY ("), query_sql, sql.SQL(") TO STDOUT WITH (FORMAT "), sql.SQL(format), sql.SQL(")")]
                )
        else:
            if format_upper == "CSV":
                export_sql = f"COPY ({query_sql}) TO STDOUT WITH (FORMAT CSV, FORCE_QUOTE *)"
            elif format_upper == "MSSQL_DELIMITED":
                export_sql = (
                    f"COPY ({query_sql}) TO STDOUT WITH "
                    "(FORMAT CSV, DELIMITER E'\\t', QUOTE E'\\x1f', ESCAPE E'\\x1f', NULL '')"
                )
            else:
                export_sql = f"COPY ({query_sql}) TO STDOUT WITH (FORMAT {format})"

        start_time = time.time()
        total_bytes = 0
        copy_read_count = 0
        rows_exported = 0 if format_upper == "MSSQL_DELIMITED" else None
        output_digest = hashlib.sha256() if rows_exported is not None and not compress else None

        output_file: Any
        if compress:
            output_file = gzip.open(output_path, "wb", compresslevel=compress_level)
        else:
            output_file = open(output_path, "wb", buffering=buffer_size)

        try:
            with self.connection.cursor() as cursor:
                with cursor.copy(export_sql, params) as copy:
                    while True:
                        chunk = copy.read()
                        if not chunk:
                            break
                        copy_read_count += 1
                        chunk_bytes = len(chunk)
                        total_bytes += chunk_bytes

                        payload = chunk.tobytes() if isinstance(chunk, memoryview) else bytes(chunk)
                        output_file.write(payload)
                        if rows_exported is not None:
                            # BulkTextCodec removes literal row terminators
                            # from fields, and PostgreSQL COPY terminates every
                            # MSSQL-delimited record with one newline.
                            rows_exported += payload.count(b"\n")
                        if output_digest is not None:
                            output_digest.update(payload)

                        # Логируем прогресс каждые 100MB
                        if logger and total_bytes // (100 * 1024 * 1024) > (total_bytes - chunk_bytes) // (
                            100 * 1024 * 1024
                        ):
                            elapsed = time.time() - start_time
                            throughput_mbps = (total_bytes / 1024 / 1024) / elapsed if elapsed > 0 else 0
                            logger.log_etl_progress(
                                "POSTGRES_COPY_PROGRESS",
                                {
                                    "Bytes Read": f"{total_bytes / 1024 / 1024:.1f}MB",
                                    "COPY Reads": copy_read_count,
                                    "Elapsed": f"{elapsed:.1f}s",
                                    "Throughput": f"{throughput_mbps:.1f}MB/s",
                                },
                            )
        finally:
            output_file.close()

        elapsed_total = time.time() - start_time
        throughput_final = (total_bytes / 1024 / 1024) / elapsed_total if elapsed_total > 0 else 0

        return {
            "total_bytes": total_bytes,
            "copy_read_count": copy_read_count,
            # Deprecated compatibility alias.  A psycopg COPY read is not a
            # logical/backfill chunk and must never be presented as one.
            "chunk_count": copy_read_count,
            "elapsed": elapsed_total,
            "throughput": throughput_final,
            "rows_exported": rows_exported,
            "sha256": output_digest.hexdigest() if output_digest is not None else None,
        }

    def copy_to_stream(
        self,
        query_sql,
        *,
        columns: Iterable[str],
        format: str = "CSV",
        estimated_rows: int | None = None,
    ):
        from dpone.runtime.connectors.postgres_copy_stream import PostgresCopyStreamExporter

        return PostgresCopyStreamExporter(self.connection).export(
            query_sql,
            columns=tuple(columns),
            format=format,
            estimated_rows=estimated_rows,
        )

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None

    def build_select_query(
        self,
        schema: str,
        table: str,
        columns: list[str],
        limit: int | None = None,
        offset: int | None = None,
    ) -> sql.Composed:
        """
        Строит SELECT запрос для PostgreSQL используя psycopg.sql.

        Args:
            schema: Имя схемы
            table: Имя таблицы
            columns: Список колонок
            limit: LIMIT clause (optional)
            offset: OFFSET clause (optional)

        Returns:
            psycopg.sql.Composed объект
        """
        # SELECT columns FROM schema.table
        query = sql.SQL("SELECT {} FROM {}.{}").format(
            sql.SQL(", ").join(sql.Identifier(col) for col in columns),
            sql.Identifier(schema),
            sql.Identifier(table),
        )

        # Добавляем LIMIT/OFFSET если указаны
        if limit is not None:
            query = query + sql.SQL(" LIMIT {}").format(sql.Literal(limit))

        if offset is not None:
            query = query + sql.SQL(" OFFSET {}").format(sql.Literal(offset))

        return query

    def build_max_query(self, schema: str, table: str, column: str) -> sql.Composed:
        """Строит MAX запрос для PostgreSQL с sql.Identifier."""
        return sql.SQL("SELECT MAX({}) as max_val FROM {}.{}").format(
            sql.Identifier(column),
            sql.Identifier(schema),
            sql.Identifier(table),
        )

    def get_table_column_types(self, schema: str, table: str) -> dict[str, str]:
        """
        Возвращает словарь {column_name: data_type} для таблицы.

        Используется для explicit type casting при INSERT из staging в target,
        когда типы данных в staging (из API/JSON) не совпадают с типами target.

        Args:
            schema: Имя схемы
            table: Имя таблицы

        Returns:
            dict: {"request_id": "integer", "case_id": "integer", ...}
            Пустой dict если таблица не существует.
        """
        query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """
        rows = self.get_records(query, (schema, table), as_dict=True)
        return {row["column_name"]: row["data_type"] for row in rows}

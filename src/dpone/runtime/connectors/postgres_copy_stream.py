"""PostgreSQL COPY transport with distinct lazy-stream and owned-file strategies.

The stream retains its cursor only while consumed. The file strategy closes its
output exactly once and preserves primary COPY/write failures during cleanup.
"""

from __future__ import annotations

import gzip
import hashlib
import time
from collections.abc import Sequence
from typing import Any

from psycopg import sql

from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact


class PostgresCopyStreamExporter:
    """Build zero-file PostgreSQL COPY TO STDOUT byte streams."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def export(
        self,
        query_sql: Any,
        *,
        columns: Sequence[str],
        format: str = "CSV",
        chunk_size: int = 16 * 1024 * 1024,
        estimated_rows: int | None = None,
    ) -> ByteStreamArtifact:
        copy_sql = build_copy_to_stdout_sql(query_sql, format=format)

        def chunks():
            with self._connection.cursor() as cursor:
                with cursor.copy(copy_sql) as copy:
                    while chunk := copy.read():
                        if isinstance(chunk, memoryview):
                            yield chunk.tobytes()
                        else:
                            yield bytes(chunk)

        del chunk_size
        return ByteStreamArtifact(
            chunks,
            columns=tuple(columns),
            format=_artifact_format(format),
            estimated_rows=estimated_rows,
        )


def build_copy_to_stdout_sql(query_sql: Any, *, format: str = "CSV") -> Any:
    format_upper = format.upper().replace("-", "_")
    if isinstance(query_sql, sql.Composable | sql.Composed):
        if format_upper == "CSV":
            return sql.Composed([sql.SQL("COPY ("), query_sql, sql.SQL(") TO STDOUT WITH (FORMAT CSV, FORCE_QUOTE *)")])
        if format_upper == "MSSQL_DELIMITED":
            return sql.Composed(
                [
                    sql.SQL("COPY ("),
                    query_sql,
                    sql.SQL(
                        ") TO STDOUT WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE E'\\x1f', ESCAPE E'\\x1f', NULL '')"
                    ),
                ]
            )
        return sql.Composed(
            [sql.SQL("COPY ("), query_sql, sql.SQL(") TO STDOUT WITH (FORMAT "), sql.SQL(format), sql.SQL(")")]
        )
    if format_upper == "CSV":
        return f"COPY ({query_sql}) TO STDOUT WITH (FORMAT CSV, FORCE_QUOTE *)"
    if format_upper == "MSSQL_DELIMITED":
        return (
            f"COPY ({query_sql}) TO STDOUT WITH "
            "(FORMAT CSV, DELIMITER E'\\t', QUOTE E'\\x1f', ESCAPE E'\\x1f', NULL '')"
        )
    return f"COPY ({query_sql}) TO STDOUT WITH (FORMAT {format})"


def _artifact_format(format_name: str) -> str:
    if format_name.upper().replace("-", "_") == "MSSQL_DELIMITED":
        return "mssql-delimited"
    return format_name.lower()


__all__ = ["PostgresCopyStreamExporter", "build_copy_to_stdout_sql", "PostgresCopyFileExporter"]


class PostgresCopyFileExporter:
    """Export through an injected connection and close the output exactly once."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def export(
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
        format_upper = format.upper().replace("-", "_")
        export_sql = build_copy_to_stdout_sql(query_sql, format=format)

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

        primary: BaseException | None = None
        try:
            with self._connection.cursor() as cursor:
                with cursor.copy(export_sql, params) as copy:
                    while True:
                        chunk = copy.read()
                        if not chunk:
                            break
                        chunk_bytes = len(chunk)

                        payload = chunk.tobytes() if isinstance(chunk, memoryview) else bytes(chunk)
                        _write_complete_chunk(output_file, payload)
                        copy_read_count += 1
                        total_bytes += chunk_bytes
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
        except BaseException as failure:
            primary = failure
        try:
            output_file.close()
        except BaseException as close_failure:
            if primary is None:
                primary = close_failure
        if primary is not None:
            primary.__cause__ = primary.__context__ = None
            raise primary from None

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


def _write_complete_chunk(output_file: Any, payload: bytes) -> None:
    """Require positive, bounded progress until all bytes reach the writer."""
    remaining = memoryview(payload)
    while remaining:
        written = output_file.write(remaining)
        if type(written) is not int or written <= 0 or written > len(remaining):
            raise OSError("postgres_copy_file.write_progress_invalid")
        remaining = remaining[written:]

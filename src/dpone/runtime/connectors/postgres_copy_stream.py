from __future__ import annotations

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


__all__ = ["PostgresCopyStreamExporter", "build_copy_to_stdout_sql"]

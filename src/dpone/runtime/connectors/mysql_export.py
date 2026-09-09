"""MySQL SELECT export helpers for MSSQL BCP, Postgres CSV, and ClickHouse TSV."""

from __future__ import annotations

import csv
import gzip
import io
import json
import time
from collections.abc import Iterable, Sequence
from datetime import date, datetime
from datetime import time as dt_time
from decimal import Decimal
from typing import Any

from dpone.runtime.support.bulk_text_codec import BulkTextCodec
from dpone.runtime.support.mssql_bcp_values import format_mssql_bcp_scalar
from dpone.runtime.support.mysql_clickhouse_tsv import format_clickhouse_tsv_scalar


class MySQLExportMixin:
    """File export methods mixed into ``MySQLConnector``."""

    _type_mapper: Any
    _postgres_type_mapper: Any
    get_records_streaming: Any

    def export_mssql_delimited_to_file(
        self,
        query: str,
        output_path: str,
        schema: Sequence[tuple[str, str]],
        *,
        params: Iterable[Any] | None = None,
        compress: bool = False,
        batch_size: int = 10000,
        bulk_text_codec: Any | None = None,
        field_terminator: str | None = None,
    ) -> dict[str, Any]:
        terminator = field_terminator or getattr(bulk_text_codec, "field_terminator", None) or "\t"
        codec = bulk_text_codec or BulkTextCodec(field_terminator=terminator)
        if codec.field_terminator != terminator:
            codec = BulkTextCodec(field_terminator=terminator)
        start = time.time()
        total_bytes = 0
        row_count = 0
        opener = gzip.open if compress else open
        with opener(output_path, "wb") as handle:
            for batch in self.get_records_streaming(query, params, as_dict=True, batch_size=batch_size):
                for row in batch:
                    line = self._format_mssql_delimited_row(row, schema, codec)
                    payload = (line + codec.row_terminator).encode("utf-8")
                    handle.write(payload)
                    total_bytes += len(payload)
                    row_count += 1
        elapsed = time.time() - start
        return {
            "total_bytes": total_bytes,
            "row_count": row_count,
            "elapsed": elapsed,
            "throughput": (total_bytes / 1024 / 1024) / elapsed if elapsed > 0 else 0,
        }

    def export_csv_to_file(
        self,
        query: str,
        output_path: str,
        schema: Sequence[tuple[str, str]],
        *,
        params: Iterable[Any] | None = None,
        compress: bool = False,
        batch_size: int = 10000,
    ) -> dict[str, Any]:
        """Export SELECT results as Postgres-safe CSV (not MSSQL BCP text)."""

        start = time.time()
        total_bytes = 0
        row_count = 0
        opener = gzip.open if compress else open
        mode = "wt" if compress else "w"
        with opener(output_path, mode, encoding="utf-8", newline="") as handle:
            writer = csv.writer(
                handle,
                delimiter=",",
                quotechar='"',
                doublequote=True,
                lineterminator="\n",
                quoting=csv.QUOTE_MINIMAL,
            )
            for batch in self.get_records_streaming(query, params, as_dict=True, batch_size=batch_size):
                for row in batch:
                    writer.writerow(self._csv_fields(row, schema))
                    row_count += 1
            handle.flush()
            if hasattr(handle, "tell"):
                try:
                    total_bytes = int(handle.tell())
                except OSError:
                    total_bytes = 0
        if total_bytes <= 0:
            try:
                import os

                total_bytes = int(os.path.getsize(output_path))
            except OSError:
                total_bytes = 0
        elapsed = time.time() - start
        return {
            "total_bytes": total_bytes,
            "row_count": row_count,
            "elapsed": elapsed,
            "throughput": (total_bytes / 1024 / 1024) / elapsed if elapsed > 0 else 0,
        }

    def export_clickhouse_tsv_to_file(
        self,
        query: str,
        output_path: str,
        schema: Sequence[tuple[str, str]],
        *,
        params: Iterable[Any] | None = None,
        compress: bool = False,
        batch_size: int = 10000,
    ) -> dict[str, Any]:
        """Export SELECT results as ClickHouse TabSeparated text (``\\N`` nulls)."""

        start = time.time()
        total_bytes = 0
        row_count = 0
        opener = gzip.open if compress else open
        with opener(output_path, "wb") as handle:
            for batch in self.get_records_streaming(query, params, as_dict=True, batch_size=batch_size):
                for row in batch:
                    line = self._format_clickhouse_tsv_row(row, schema) + "\n"
                    payload = line.encode("utf-8")
                    handle.write(payload)
                    total_bytes += len(payload)
                    row_count += 1
        elapsed = time.time() - start
        return {
            "total_bytes": total_bytes,
            "row_count": row_count,
            "elapsed": elapsed,
            "throughput": (total_bytes / 1024 / 1024) / elapsed if elapsed > 0 else 0,
        }

    def _format_mssql_delimited_row(
        self,
        row: dict[str, Any],
        schema: Sequence[tuple[str, str]],
        codec: BulkTextCodec,
    ) -> str:
        fields: list[str] = []
        for column, dtype in schema:
            value = row.get(column)
            if value is None:
                fields.append("")
                continue
            if isinstance(value, bytes | bytearray | memoryview):
                # Hex character wire; BulkTextCodec preserves empty blob vs NULL.
                fields.append(codec.encode(bytes(value).hex()))
                continue
            mssql_type = self._type_mapper.resolve(dtype).target_type
            fields.append(
                format_mssql_bcp_scalar(
                    value,
                    mssql_type=mssql_type,
                    text_codec=codec,
                    field_terminator=codec.field_terminator,
                )
            )
        return codec.field_terminator.join(fields)

    def _format_csv_row(self, row: dict[str, Any], schema: Sequence[tuple[str, str]]) -> str:
        buffer = io.StringIO()
        csv.writer(
            buffer,
            delimiter=",",
            quotechar='"',
            doublequote=True,
            lineterminator="",
            quoting=csv.QUOTE_MINIMAL,
        ).writerow(self._csv_fields(row, schema))
        return buffer.getvalue()

    def _csv_fields(self, row: dict[str, Any], schema: Sequence[tuple[str, str]]) -> list[str]:
        fields: list[str] = []
        for column, dtype in schema:
            value = row.get(column)
            if value is None:
                fields.append("")
                continue
            # Keep postgres mapper in the hot path so unknown MySQL types stay explainable.
            _ = self._postgres_type_mapper.resolve(dtype)
            fields.append(_format_postgres_csv_scalar(value))
        return fields

    def _format_clickhouse_tsv_row(self, row: dict[str, Any], schema: Sequence[tuple[str, str]]) -> str:
        return "\t".join(format_clickhouse_tsv_scalar(row.get(column)) for column, _ in schema)


def _format_postgres_csv_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float | Decimal):
        return str(value)
    if isinstance(value, datetime):
        if value.microsecond:
            return value.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dt_time):
        return value.isoformat()
    if isinstance(value, bytes | bytearray | memoryview):
        return bytes(value).hex()
    if isinstance(value, dict | list):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


__all__ = ["MySQLExportMixin"]

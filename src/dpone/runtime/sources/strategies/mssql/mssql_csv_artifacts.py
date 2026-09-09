"""CSV extraction artifacts for MSSQL → Postgres / Kafka / BigQuery routes."""

from __future__ import annotations

import base64
import csv
import gzip
import os
import tempfile
import time
from collections.abc import Sequence
from datetime import date, datetime
from datetime import time as dt_time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

from dpone.runtime.file_artifacts import FileExportArtifact

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig

BinaryEncoding = Literal["postgres_hex", "base64"]


def build_csv_file_artifact(
    connector: Any,
    load_config: LoadConfig,
    query: str,
    schema: Sequence[tuple[str, str]],
    *,
    params: tuple[Any, ...] | None = None,
    binary_encoding: BinaryEncoding = "postgres_hex",
) -> FileExportArtifact:
    """Stream a SELECT into a CSV file artifact for Postgres/Kafka/BigQuery sinks."""

    compress = bool(load_config.options.get("compress_export", False) or load_config.compress_export)
    suffix = ".csv.gz" if compress else ".csv"
    fd, path = tempfile.mkstemp(prefix="dpone-mssql-", suffix=suffix)
    os.close(fd)
    stats = export_csv_to_file(
        connector,
        query,
        path,
        schema,
        params=params,
        compress=compress,
        batch_size=int(load_config.batch_size or 10000),
        binary_encoding=binary_encoding,
    )
    artifact = FileExportArtifact(
        file_path=path,
        columns=[name for name, _ in schema],
        compressed=compress,
        format="csv",
        estimated_rows=stats["row_count"],
        rows_exported=stats["row_count"],
    )
    return artifact


def export_csv_to_file(
    connector: Any,
    query: str,
    output_path: str,
    schema: Sequence[tuple[str, str]],
    *,
    params: tuple[Any, ...] | None = None,
    compress: bool = False,
    batch_size: int = 10000,
    binary_encoding: BinaryEncoding = "postgres_hex",
) -> dict[str, Any]:
    """Export SELECT results as comma-quoted UTF-8 CSV."""

    start = time.time()
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
        for batch in connector.get_records_streaming(query, params, as_dict=True, batch_size=batch_size):
            for row in batch:
                writer.writerow(_csv_fields(row, schema, binary_encoding=binary_encoding))
                row_count += 1
        handle.flush()
    elapsed = time.time() - start
    try:
        total_bytes = int(os.path.getsize(output_path))
    except OSError:
        total_bytes = 0
    return {
        "total_bytes": total_bytes,
        "row_count": row_count,
        "elapsed": elapsed,
        "throughput": (total_bytes / 1024 / 1024) / elapsed if elapsed > 0 else 0,
    }


def _csv_fields(
    row: dict[str, Any],
    schema: Sequence[tuple[str, str]],
    *,
    binary_encoding: BinaryEncoding,
) -> list[str]:
    return [_format_csv_scalar(row.get(column), binary_encoding=binary_encoding) for column, _ in schema]


def _format_csv_scalar(value: Any, *, binary_encoding: BinaryEncoding) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float | Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        if value.microsecond:
            return value.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dt_time):
        return value.isoformat(timespec="seconds")
    if isinstance(value, bytes | bytearray | memoryview):
        raw = bytes(value)
        if binary_encoding == "base64":
            return base64.b64encode(raw).decode("ascii")
        return "\\x" + raw.hex()
    return str(value)


__all__ = ["build_csv_file_artifact", "export_csv_to_file"]

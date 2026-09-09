"""Wire encode/decode helpers for strategy-metadata file enrichment."""

from __future__ import annotations

import gzip
import hashlib
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path

from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.support.mssql_bcp_values import format_mssql_bcp_scalar
from dpone.runtime.support.mysql_clickhouse_tsv import format_clickhouse_tsv_scalar

_STRATEGY_COLUMN_PREFIX = "__dpone__"


def assert_columns_append_only(input_columns: Sequence[str], output_columns: Sequence[str]) -> None:
    if list(output_columns[: len(input_columns)]) != list(input_columns):
        raise ValueError(
            "strategy metadata enrichment expects strategy columns to be appended; "
            f"got input={list(input_columns)!r} output={list(output_columns)!r}"
        )


def open_text(artifact: FileExportArtifact, *, mode: str):
    return open_text_path(Path(artifact.file_path), compressed=artifact.compressed, mode=mode)


def open_text_path(path: Path, *, compressed: bool, mode: str):
    if compressed or path.suffix == ".gz":
        return gzip.open(path, mode=mode, encoding="utf-8", newline="")
    return path.open(mode=mode, encoding="utf-8", newline="")


def is_header(record: Sequence[str], columns: Sequence[str]) -> bool:
    return [cell.strip().lower() for cell in record] == [column.strip().lower() for column in columns]


def decode_tab_cell(value: str, *, file_format: str) -> object:
    """Decode a tab field for hashing only; business wire text is preserved separately.

    ClickHouse ``\\N`` is null. Empty MSSQL/BCP fields stay empty strings so
    ``""`` vs SQL NULL hashing does not invent nulls for every blank cell.
    """

    if file_format == "clickhouse-tsv" and value == "\\N":
        return None
    return value


def encode_csv_cell(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def encode_strategy_tab_cell(
    value: object,
    *,
    column: str,
    file_format: str,
    text_codec: BulkTextCodec,
) -> str:
    """Encode only newly appended strategy columns for native tab wires."""

    if file_format == "clickhouse-tsv":
        return format_clickhouse_tsv_scalar(_coerce_temporal_value(value, column=column))
    if file_format == "mssql-delimited":
        return format_mssql_bcp_scalar(
            _coerce_temporal_value(value, column=column),
            mssql_type=mssql_type_for_strategy_column(column),
            text_codec=text_codec,
            field_terminator=text_codec.field_terminator,
        )
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    coerced = _coerce_temporal_value(value, column=column)
    if isinstance(coerced, datetime):
        return coerced.strftime("%Y-%m-%d %H:%M:%S")
    return str(coerced).replace("\t", " ").replace("\r", " ").replace("\n", " ")


def _coerce_temporal_value(value: object, *, column: str) -> object:
    """Parse lineage ISO timestamps so native formatters do not emit ``T``/offsets."""

    if value is None or isinstance(value, (bool, int, float, datetime, date)):
        return value
    name = column.lower()
    if not (name.endswith("_at") or "valid_" in name):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return value
    if parsed.tzinfo is not None:
        return parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed


def mssql_type_for_strategy_column(column: str) -> str:
    name = column.lower()
    if name.endswith("is_current") or name == "__dpone__is_current":
        return "bit"
    if "hash" in name:
        return "varchar(64)"
    if name.startswith(_STRATEGY_COLUMN_PREFIX) and name.endswith("_at"):
        return "datetime2(7)"
    return "nvarchar(max)"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "assert_columns_append_only",
    "decode_tab_cell",
    "encode_csv_cell",
    "encode_strategy_tab_cell",
    "is_header",
    "mssql_type_for_strategy_column",
    "open_text",
    "open_text_path",
    "sha256_file",
]

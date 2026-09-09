"""Lazy Parquet writer for columnar snapshot providers."""

from __future__ import annotations

import importlib.util
import re
from collections.abc import Iterable, Sequence
from datetime import datetime, time
from pathlib import Path
from typing import Any, Protocol


class ParquetChunkWriter(Protocol):
    def is_available(self) -> bool:
        """Return whether the writer dependencies are installed."""

    def write_chunk(
        self,
        *,
        local_path: Path,
        schema: Sequence[tuple[str, str]],
        rows: Iterable[tuple[object, ...]],
        compression: str,
    ) -> int:
        """Write rows to one Parquet file and return the row count."""


class PyArrowParquetChunkWriter:
    """Write Parquet chunks through pyarrow without importing it at module import time."""

    def is_available(self) -> bool:
        return (
            importlib.util.find_spec("pyarrow") is not None and importlib.util.find_spec("pyarrow.parquet") is not None
        )

    def write_chunk(
        self,
        *,
        local_path: Path,
        schema: Sequence[tuple[str, str]],
        rows: Iterable[tuple[object, ...]],
        compression: str,
    ) -> int:
        pa = _pyarrow()
        pq = _pyarrow_parquet()
        materialized = list(rows)
        arrays = {
            column: pa.array(
                _column_values(materialized, index=index, source_type=source_type),
                type=_arrow_type(pa, source_type),
            )
            for index, (column, source_type) in enumerate(schema)
        }
        table = pa.table(arrays)
        parquet_compression = None if compression.lower() == "none" else compression.lower()
        pq.write_table(table, local_path, compression=parquet_compression)
        return len(materialized)


def _arrow_type(pa: Any, source_type: str) -> Any:
    normalized = _normalize_source_type(source_type)
    kind = _arrow_kind(normalized)
    if kind is None:
        raise ValueError(f"Unsupported MSSQL columnar type: {source_type}")
    if kind == "bool":
        return pa.bool_()
    if kind == "uint8":
        return pa.uint8()
    if kind == "int16":
        return pa.int16()
    if kind == "int32":
        return pa.int32()
    if kind == "int64":
        return pa.int64()
    if kind == "float32":
        return pa.float32()
    if kind == "float64":
        return pa.float64()
    decimal = _decimal_precision_scale(normalized)
    if decimal:
        precision, scale = decimal
        return pa.decimal256(precision, scale) if precision > 38 else pa.decimal128(precision, scale)
    if kind == "date":
        return pa.date32()
    if kind == "datetime":
        return pa.timestamp("us")
    if kind in {"temporal_text", "time"}:
        return pa.string()
    if kind == "binary":
        return pa.binary()
    return pa.string()


def _column_values(
    rows: Sequence[tuple[object, ...]],
    *,
    index: int,
    source_type: str,
) -> list[object | None]:
    values = [row[index] for row in rows]
    normalized = _normalize_source_type(source_type)
    kind = _arrow_kind(normalized)
    if kind == "temporal_text":
        return [_canonical_temporal_text(value, source_type) for value in values]
    if kind != "time":
        return values
    return [_canonical_time(value, source_type) for value in values]


def _canonical_temporal_text(value: object, source_type: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        return str(value).strip()
    scale = _time_scale(source_type)
    base = value.strftime("%Y-%m-%d %H:%M:%S")
    if scale == 0:
        return base
    fraction = f"{value.microsecond:06d}"
    if scale <= 6:
        fraction = fraction[:scale]
    else:
        fraction += "0" * (scale - 6)
    suffix = value.strftime("%z")
    if suffix:
        suffix = f"{suffix[:3]}:{suffix[3:]}"
    return f"{base}.{fraction}{suffix}"


def _canonical_time(value: object, source_type: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, time):
        text = str(value).strip()
        if "T" in text or " " in text or text.count(":") != 2:
            raise ValueError(f"Invalid MSSQL time value for Parquet: {value!r}")
        return text
    scale = _time_scale(source_type)
    base = value.strftime("%H:%M:%S")
    if scale == 0:
        return base
    fraction = f"{value.microsecond:06d}"
    if scale <= 6:
        fraction = fraction[:scale]
    else:
        fraction += "0" * (scale - 6)
    return f"{base}.{fraction}"


def _time_scale(source_type: str) -> int:
    match = re.search(r"\((\d+)\)", _normalize_source_type(source_type))
    return min(max(int(match.group(1)), 0), 7) if match else 7


def mssql_source_type_supported(source_type: str) -> bool:
    """Return whether the default Parquet writer has a certified MSSQL type mapping."""

    return _arrow_kind(_normalize_source_type(source_type)) is not None


def _arrow_kind(normalized: str) -> str | None:
    base = normalized.split("(", 1)[0].strip()
    if normalized in {"bit", "bool", "boolean"}:
        return "bool"
    if base == "tinyint":
        return "uint8"
    if base == "smallint":
        return "int16"
    if base in {"int", "integer"}:
        return "int32"
    if base == "bigint":
        return "int64"
    if normalized in {"real", "float(24)"}:
        return "float32"
    if base in {"float", "double"} or normalized == "double precision":
        return "float64"
    if _decimal_precision_scale(normalized) is not None:
        return "decimal"
    if base == "date":
        return "date"
    if base in {"datetime", "smalldatetime"}:
        return "datetime"
    if base in {"datetime2", "datetimeoffset"}:
        return "temporal_text"
    if base == "time":
        return "time"
    if base in {"binary", "varbinary", "image", "rowversion", "timestamp"}:
        return "binary"
    if base in {"char", "varchar", "nchar", "nvarchar", "text", "ntext", "xml", "uniqueidentifier", "sysname"}:
        return "string"
    return None


def _decimal_precision_scale(source_type: str) -> tuple[int, int] | None:
    if "money" in source_type:
        return (19, 4)
    if not source_type.startswith(("decimal", "numeric")):
        return None
    match = re.search(r"\((\d+)\s*,\s*(\d+)\)", source_type)
    if not match:
        return (38, 9)
    return int(match.group(1)), int(match.group(2))


def _normalize_source_type(source_type: str) -> str:
    normalized = source_type.strip().lower()
    if normalized.startswith("nullable(") and normalized.endswith(")"):
        return normalized.removeprefix("nullable(").removesuffix(")").strip()
    return normalized.replace(" nullable", "").strip()


def _pyarrow() -> Any:
    try:
        import pyarrow as pa
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency guard
        raise ModuleNotFoundError(
            "pyarrow is required for columnar Parquet snapshots. Install 'dpone[columnar]'."
        ) from exc
    return pa


def _pyarrow_parquet() -> Any:
    try:
        import pyarrow.parquet as pq
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency guard
        raise ModuleNotFoundError("pyarrow.parquet is required for columnar Parquet snapshots.") from exc
    return pq


__all__ = ["ParquetChunkWriter", "PyArrowParquetChunkWriter", "mssql_source_type_supported"]

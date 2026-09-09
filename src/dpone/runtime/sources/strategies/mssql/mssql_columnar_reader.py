from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any


def iter_columnar_batches(
    connector: Any,
    *,
    query: str,
    schema: Sequence[tuple[str, str]],
    batch_size: int,
):
    """Stream values after SQL-side projection of ODBC-lossy temporal types."""

    yield from connector.get_records_streaming(
        parquet_read_query(query, schema),
        batch_size=batch_size,
        as_dict=False,
    )


def parquet_read_query(query: str, schema: Sequence[tuple[str, str]]) -> str:
    """Project 100ns MSSQL values to exact text before pyodbc truncates them."""

    if not any(_requires_text_projection(source_type) for _, source_type in schema):
        return query
    source_query = query.strip().removesuffix(";").strip()
    columns = ",\n    ".join(_projection(column, source_type) for column, source_type in schema)
    return f"SELECT\n    {columns}\nFROM (\n{source_query}\n) AS dpone_columnar_src"


def _projection(column: str, source_type: str) -> str:
    quoted = _quote_identifier(column)
    source = f"dpone_columnar_src.{quoted}"
    normalized = _normalize_source_type(source_type)
    base = normalized.split("(", 1)[0]
    if base == "datetime2":
        return f"CONVERT(VARCHAR(33), {source}, 121) AS {quoted}"
    if base == "time":
        return f"CONVERT(VARCHAR(16), {source}) AS {quoted}"
    if base == "datetimeoffset":
        utc = f"CAST(SWITCHOFFSET(CAST({source} AS datetimeoffset), '+00:00') AS datetime2(7))"
        return f"CONVERT(VARCHAR(33), {utc}, 121) AS {quoted}"
    return f"{source} AS {quoted}"


def _requires_text_projection(source_type: str) -> bool:
    base = _normalize_source_type(source_type).split("(", 1)[0]
    return base in {"datetime2", "datetimeoffset", "time"}


def _normalize_source_type(source_type: str) -> str:
    normalized = str(source_type).strip().lower().replace(" nullable", "").strip()
    if normalized.startswith("nullable(") and normalized.endswith(")"):
        normalized = normalized.removeprefix("nullable(").removesuffix(")").strip()
    return re.sub(r"\s+", "", normalized)


def _quote_identifier(value: str) -> str:
    return "[" + str(value).replace("]", "]]") + "]"


__all__ = ["iter_columnar_batches", "parquet_read_query"]

"""Compare reader adapters for CDC ops services."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.ops.cdc.runtime_adapters import required_path
from dpone.runtime.cdc.compare import InMemoryCdcCompareReader
from dpone.runtime.cdc.compare_models import CdcCompareRow
from dpone.runtime.cdc.compare_readers import ClickHouseCdcLogCompareReader, MssqlCdcCompareReader


def local_compare_reader(path: str | Path | None, *, unique_key: Sequence[str]) -> InMemoryCdcCompareReader:
    return InMemoryCdcCompareReader(
        rows_from_json(
            required_path(path, "--source-rows-json", mode="local CDC compare mode"),
            unique_key=unique_key,
        )
    )


def local_target_compare_reader(path: str | Path | None, *, unique_key: Sequence[str]) -> InMemoryCdcCompareReader:
    return InMemoryCdcCompareReader(
        rows_from_json(
            required_path(path, "--target-rows-json", mode="local CDC compare mode"),
            unique_key=unique_key,
        )
    )


def mssql_compare_reader(
    *,
    connector: Any,
    source_schema: str,
    source_table: str,
    columns: tuple[str, ...],
    unique_key: tuple[str, ...],
    max_rows: int,
) -> MssqlCdcCompareReader:
    return MssqlCdcCompareReader(
        connector=connector,
        source_schema=source_schema,
        source_table=source_table,
        columns=selected_columns(columns, unique_key),
        unique_key=unique_key,
        max_rows=max_rows,
    )


def clickhouse_compare_reader(
    *,
    connector: Any,
    cdc_dataset: str,
    stream_id: str,
    unique_key: tuple[str, ...],
    max_rows: int,
) -> ClickHouseCdcLogCompareReader:
    return ClickHouseCdcLogCompareReader(
        connector=connector,
        cdc_dataset=cdc_dataset,
        stream_id=stream_id,
        unique_key=unique_key,
        max_rows=max_rows,
    )


def rows_from_json(path: str | Path, *, unique_key: Sequence[str]) -> tuple[CdcCompareRow, ...]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = payload.get("rows") if isinstance(payload, Mapping) else payload
    if not isinstance(rows, list | tuple):
        raise ValueError("CDC compare row JSON must be a list or an object with rows")
    return tuple(CdcCompareRow.from_payload(unique_key=unique_key, payload=_mapping(row)) for row in rows)


def selected_columns(columns: tuple[str, ...], unique_key: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(column for column in columns if column)
    return normalized or unique_key


def _mapping(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return value
    raise ValueError("CDC compare rows must be objects")


__all__ = [
    "clickhouse_compare_reader",
    "local_compare_reader",
    "local_target_compare_reader",
    "mssql_compare_reader",
    "rows_from_json",
    "selected_columns",
]

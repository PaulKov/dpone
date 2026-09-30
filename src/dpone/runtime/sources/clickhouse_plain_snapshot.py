"""Plain MergeTree admission and one lazy, lossless native query stream.

The caller owns the schema guard, artifact, and extraction lifecycle. This
strategy admits metadata and renders the legacy plain query before returning a
lazy row iterator; consuming that iterator owns only the driver's query stream.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from dpone.manifest.mssql_native_policy import native_window
from dpone.runtime.sources.clickhouse_native_guard import admit_source_type as _admit_type
from dpone.runtime.sources.clickhouse_native_guard import source_identifier as _identifier
from dpone.runtime.sources.clickhouse_native_values import projection, restore, temporal


@dataclass(frozen=True)
class PlainSnapshot:
    """Admitted relation identity and lazy query, without guard ownership."""

    schema: tuple[tuple[str, str], ...]
    source_uuid: str
    query_id: str
    rows: Iterator[Mapping[str, object]]


def prepare_plain_snapshot(connector: Any, config: Any, query_id: str | None) -> PlainSnapshot:
    """Admit metadata under the caller's guard without starting the data query."""
    database, table = config.source_schema, config.source_table
    relation = f"{_identifier(database)}.{_identifier(table)}"
    params = {"database": database, "table": table}
    tables = connector.get_records(
        "SELECT t.engine, toString(t.uuid) AS uuid, d.engine AS database_engine FROM system.tables t "
        "INNER JOIN system.databases d ON t.database = d.name WHERE t.database = %(database)s AND t.name = %(table)s SETTINGS use_query_cache=0, read_overflow_mode='throw', result_overflow_mode='throw'",
        params,
        as_dict=True,
    )
    if len(tables) != 1 or tables[0]["engine"] != "MergeTree" or tables[0]["database_engine"] != "Atomic":
        raise ValueError("mssql_native.plain_mergetree_atomic_required")
    source_uuid = str(UUID(tables[0]["uuid"]))
    if UUID(source_uuid).int == 0:
        raise ValueError("mssql_native.source_uuid_required")
    columns = connector.get_records(
        "SELECT name, type, default_kind FROM system.columns WHERE database = %(database)s AND table = %(table)s ORDER BY position SETTINGS use_query_cache=0, read_overflow_mode='throw', result_overflow_mode='throw'",
        params,
        as_dict=True,
    )
    if not columns or any(column.get("default_kind") not in ("", None) for column in columns):
        raise ValueError("mssql_native.ordinary_columns_required")
    schema = tuple((str(item["name"]), str(item["type"])) for item in columns)
    if len({name for name, _ in schema}) != len(schema):
        raise ValueError("mssql_native.duplicate_columns")
    for name, dtype in schema:
        _identifier(name)
        _admit_type(dtype)
    query = "SELECT " + ", ".join(projection(name, dtype)[0] for name, dtype in schema) + " FROM " + relation
    window = native_window(config)
    query_params = {}
    if window is not None:
        if window.column not in dict(schema) or not temporal(dict(schema)[window.column]):
            raise ValueError("mssql_native.window_column_missing")
        # Qualify the physical column: ClickHouse can otherwise substitute
        # its SELECT alias, which contains integer microseconds, into WHERE.
        source_column = f"`__dpone_native_source`.{_identifier(window.column)}"
        query += (
            f" AS `__dpone_native_source` WHERE {source_column} >= toDateTime64(%(start)s, 6, 'UTC')"
            f" AND {source_column} < toDateTime64(%(end)s, 6, 'UTC')"
        )
        query_params = {
            "start": window.start.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "end": window.end.strftime("%Y-%m-%d %H:%M:%S.%f"),
        }
    query_id = query_id or "dpone-native-" + uuid4().hex
    rows = _rows(connector, query, query_params, query_id, schema)
    return PlainSnapshot(schema, source_uuid, query_id, rows)


def _rows(
    connector: Any, query: str, params: dict[str, Any], query_id: str, schema: tuple[tuple[str, str], ...]
) -> Iterator[Mapping[str, object]]:
    stream = connector.connection.execute_iter(
        query,
        params,
        query_id=query_id,
        with_column_types=True,
        settings={
            "strings_as_bytes": True,
            "max_block_size": 65536,
            "skip_unavailable_shards": 0,
            "read_overflow_mode": "throw",
            "result_overflow_mode": "throw",
            "timeout_overflow_mode": "throw",
            "use_query_cache": 0,
        },
    )
    try:
        wire_schema = tuple((name, projection(name, dtype)[1]) for name, dtype in schema)
        if tuple(next(stream)) != wire_schema:
            raise ValueError("mssql_native.source_schema_changed")
        for row in stream:
            if len(row) != len(schema):
                raise ValueError("mssql_native.source_row_arity")
            yield {name: restore(value, dtype) for (name, dtype), value in zip(schema, row, strict=True)}
    finally:
        close = getattr(stream, "close", None)
        if callable(close):
            primary = sys.exc_info()[1]
            try:
                close()
            except BaseException as cleanup_error:
                if primary is None:
                    raise
                primary.add_note(f"native source iterator cleanup failed: {type(cleanup_error).__name__}")

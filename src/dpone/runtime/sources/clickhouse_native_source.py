"""One plain-MergeTree query snapshot for bounded native MSSQL extraction.

The injected connector owns a dedicated native-driver session. No table count,
header probe, offset restart or independent partition query is executed.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager
from typing import Any
from uuid import UUID, uuid4

from dpone.manifest.mssql_native_policy import native_window, validate_native_config
from dpone.runtime.sources.clickhouse_native_values import projection, restore, temporal
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.streaming_rows import StreamingRowsArtifact
from dpone.type_system.source_sink.provenance import SourceRelationDialect


class NativeQueryArtifact(StreamingRowsArtifact):
    """Owned source iterator whose EOF is distinct from verified staging."""

    def __init__(
        self,
        rows: Iterator[Mapping[str, object]],
        *,
        query_id: str,
        cleanup: Any,
        source_relation_uuid: str | None = None,
    ) -> None:
        super().__init__(rows, cleanup_callback=cleanup)
        self.source_query_id = query_id
        self.source_relation_uuid = source_relation_uuid
        self._native_started = False

    def iter_native_rows(self) -> Iterator[Mapping[str, object]]:
        """Consume once, including existing payload iterator transformations."""
        if self._native_started:
            raise ValueError("mssql_native.source_reextract_required")
        self._native_started = True
        count = 0
        try:
            for row in self._iterator:
                yield row
                count += 1
            self.rows_exported = self.row_count = count
            if self.extraction_lifecycle is not None:
                self.extraction_lifecycle.complete()
        finally:
            primary = sys.exc_info()[1]
            close = getattr(self._iterator, "close", None)
            if callable(close):
                try:
                    close()
                except BaseException as error:
                    if primary is None:
                        raise
                    primary.add_note(f"native source cleanup failed: {type(error).__name__}")


class ClickHouseNativeSource:
    """Metadata admission followed by exactly one explicitly identified SELECT."""

    def __init__(self, connector: Any, *, schema_guard_factory: Any = None) -> None:
        self.connector = connector
        self.schema_guard_factory = schema_guard_factory

    def extract(self, config: Any, *, query_id: str | None = None) -> ExtractResult:
        validate_native_config(config)
        if getattr(self.connector, "driver", None) != "native":
            raise ValueError("mssql_native.native_clickhouse_driver_required")
        if self.schema_guard_factory is None:
            raise ValueError("mssql_native.source_ddl_guard_required")
        guard = self.schema_guard_factory(config)
        guard.__enter__()
        try:
            return self._extract_guarded(config, guard, query_id)
        except BaseException as primary:
            try:
                self.connector.connection.disconnect()
            except BaseException as cleanup_error:
                primary.add_note(f"native source disconnect failed: {type(cleanup_error).__name__}")
            try:
                guard.__exit__(*sys.exc_info())
            except BaseException as cleanup_error:
                primary.add_note(f"native source guard cleanup failed: {type(cleanup_error).__name__}")
            raise

    def _extract_guarded(self, config: Any, guard: AbstractContextManager[Any], query_id: str | None) -> ExtractResult:
        database, table = config.source_schema, config.source_table
        relation = f"{_identifier(database)}.{_identifier(table)}"
        params = {"database": database, "table": table}
        tables = self.connector.get_records(
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
        columns = self.connector.get_records(
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
            query += f" WHERE {_identifier(window.column)} >= toDateTime64(%(start)s, 6, 'UTC') AND {_identifier(window.column)} < toDateTime64(%(end)s, 6, 'UTC')"
            query_params = {
                "start": window.start.strftime("%Y-%m-%d %H:%M:%S.%f"),
                "end": window.end.strftime("%Y-%m-%d %H:%M:%S.%f"),
            }
        query_id = query_id or "dpone-native-" + uuid4().hex
        rows = self._rows(query, query_params, query_id, schema)

        def cleanup() -> None:
            try:
                self.connector.connection.disconnect()
            finally:
                primary = sys.exc_info()[1]
                try:
                    guard.__exit__(None, None, None)
                except BaseException as cleanup_error:
                    if primary is None:
                        raise
                    primary.add_note(f"native source guard cleanup failed: {type(cleanup_error).__name__}")

        artifact = NativeQueryArtifact(rows, query_id=query_id, cleanup=cleanup, source_relation_uuid=source_uuid)
        return ExtractResult(
            artifact, schema, relation_schema=schema, relation_dialect=SourceRelationDialect.CLICKHOUSE
        )

    def _rows(
        self, query: str, params: dict[str, Any], query_id: str, schema: tuple[tuple[str, str], ...]
    ) -> Iterator[Mapping[str, object]]:
        stream = self.connector.connection.execute_iter(
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


def _identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("mssql_native.source_identifier_invalid")
    return f"`{value}`"


def _admit_type(value: str) -> None:
    normalized = value
    if normalized.startswith("Nullable(") and normalized.endswith(")"):
        normalized = normalized[9:-1]
    if re.fullmatch(r"(?:U?Int(?:8|16|32|64)|Float(?:32|64)|String|UUID|Date|Date32|Bool)", normalized):
        return
    if re.fullmatch(r"Decimal\((?:[1-9]|[12][0-9]|3[0-8]),\s*\d+\)", normalized):
        return
    if re.fullmatch(r"DateTime(?:\('UTC'\))?|DateTime64\([0-6](?:,\s*'UTC')?\)", normalized):
        return
    raise ValueError("mssql_native.source_type_unsupported")

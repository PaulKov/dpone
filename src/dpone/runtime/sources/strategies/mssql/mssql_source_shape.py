"""MSSQL source-shape inspection for native transfer planning."""

from __future__ import annotations

from typing import Any

from dpone.contracts.mssql_object_name import MSSQLObjectName
from dpone.runtime.source_scan import SourceShape


class MSSQLSourceShapeInspector:
    """Thin MSSQL adapter that reports heap/index/view confidence."""

    def __init__(self, connector: Any) -> None:
        self.connector = connector

    def inspect(self, load_config: Any, *, boundary_column: str | None) -> SourceShape:
        if not load_config.source_schema or not load_config.source_table:
            return SourceShape(table_kind="unknown", has_seekable_boundary=False, stats_confidence="low")
        query = self._query(load_config, boundary_column)
        try:
            rows = self.connector.get_records(query, as_dict=True)
        except Exception:
            return SourceShape(table_kind="unknown", has_seekable_boundary=False, stats_confidence="low")
        if not rows:
            return SourceShape(table_kind="unknown", has_seekable_boundary=False, stats_confidence="low")
        row = rows[0]
        return SourceShape(
            table_kind=str(row.get("table_kind") or "unknown").lower(),
            has_seekable_boundary=bool(row.get("has_seekable_boundary")),
            stats_confidence=str(row.get("stats_confidence") or "unknown").lower(),
        )

    def _query(self, load_config: Any, boundary_column: str | None) -> str:
        name = MSSQLObjectName.from_parts(
            database=load_config.source_database,
            schema=load_config.source_schema,
            table=load_config.source_table,
        )
        qualified = name.quoted().replace("'", "''")
        catalog = name.execution_scope_prefix
        column = str(boundary_column or "").replace("'", "''")
        return f"""
        DECLARE @dpone_object_id int = OBJECT_ID(N'{qualified}');
        DECLARE @dpone_boundary_column sysname = N'{column}';
        SELECT
            CASE
                WHEN @dpone_object_id IS NULL THEN 'unknown'
                WHEN EXISTS (
                    SELECT 1 FROM {catalog}sys.views WHERE object_id = @dpone_object_id
                ) THEN 'view'
                WHEN EXISTS (
                    SELECT 1 FROM {catalog}sys.indexes
                    WHERE object_id = @dpone_object_id AND index_id = 1
                ) THEN 'clustered'
                WHEN EXISTS (
                    SELECT 1 FROM {catalog}sys.indexes
                    WHERE object_id = @dpone_object_id AND index_id = 0
                ) THEN 'heap'
                ELSE 'indexed'
            END AS table_kind,
            CASE
                WHEN @dpone_boundary_column = N'' THEN 0
                WHEN EXISTS (
                    SELECT 1
                    FROM {catalog}sys.indexes i
                    JOIN {catalog}sys.index_columns ic
                      ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                    JOIN {catalog}sys.columns c
                      ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    WHERE i.object_id = @dpone_object_id
                      AND i.is_disabled = 0
                      AND ic.key_ordinal > 0
                      AND c.name = @dpone_boundary_column
                ) THEN 1
                ELSE 0
            END AS has_seekable_boundary,
            CASE
                WHEN @dpone_object_id IS NULL THEN 'low'
                WHEN EXISTS (
                    SELECT 1
                    FROM {catalog}sys.stats s
                    WHERE s.object_id = @dpone_object_id
                      AND s.has_filter = 0
                ) THEN 'high'
                ELSE 'low'
            END AS stats_confidence,
            'dpone_source_shape' AS dpone_source_shape
        """


__all__ = ["MSSQLSourceShapeInspector"]

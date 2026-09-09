"""Artifact format labels accepted as ClickHouse TabSeparated bulk wire."""

from __future__ import annotations

CLICKHOUSE_TSV_ARTIFACT_FORMATS: frozenset[str] = frozenset({"mssql-delimited", "clickhouse-tsv"})

__all__ = ["CLICKHOUSE_TSV_ARTIFACT_FORMATS"]

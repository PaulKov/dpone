"""ClickHouse server-side SQL query extraction."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sql_query_artifact import SqlQueryArtifact
from dpone.runtime.sql_query_resolver import ResolvedSqlQuery, SqlQueryResolver


class ClickHouseQueryExtractService:
    """Build a SQL query artifact without materializing rows in Python."""

    def __init__(self, connector: Any, logger: Any) -> None:
        self._connector = connector
        self._logger = logger

    def extract(self, load_config: Any) -> ExtractResult:
        resolved = self._resolve(load_config)
        schema = self._describe(resolved.sql)
        # Certified SELECT count() — independent source authority for quality gates.
        # estimated_rows alone never certifies source_target_count; publish rows_exported.
        row_count = self._count(resolved.sql)
        self._logger.log_etl_progress(
            "CH_SQL_QUERY_ARTIFACT",
            {"SqlHash": resolved.sql_hash, "Columns": len(schema), "Rows": row_count},
        )
        artifact = SqlQueryArtifact(
            sql=resolved.sql,
            dialect="clickhouse",
            sql_hash=resolved.sql_hash,
            source_path=str(resolved.source_path) if resolved.source_path else None,
            evidence=resolved.evidence,
            estimated_rows=row_count,
        )
        if row_count is not None:
            artifact.rows_exported = row_count
            artifact.row_count = row_count
        return ExtractResult(
            artifact=artifact,
            schema=schema,
            state=None,
        )

    def describe(self, load_config: Any) -> list[tuple[str, str]]:
        """Return the schema of the rendered query, not a placeholder table."""

        return self._describe(self._resolve(load_config).sql)

    @staticmethod
    def _resolve(load_config: Any) -> ResolvedSqlQuery:
        options = getattr(load_config, "options", {}) or {}
        query_config = dict(options.get("query") or {})
        manifest_dir = Path(str(options.get("manifest_dir") or "."))
        repo_root = Path(str(options.get("repo_root") or manifest_dir))
        return SqlQueryResolver(repo_root=repo_root).resolve(
            query_config,
            manifest_dir=manifest_dir,
            dialect="clickhouse",
        )

    def _describe(self, sql: str) -> list[tuple[str, str]]:
        rows = self._connector.get_records(
            f"DESCRIBE SELECT * FROM ({sql}) AS dpone_sql_query",
            as_dict=True,
        )
        return [(str(row["name"]), str(row["type"])) for row in rows]

    def _count(self, sql: str) -> int | None:
        try:
            rows = self._connector.get_records(f"SELECT count() FROM ({sql}) AS dpone_sql_query")
        except Exception:
            return None
        if not rows:
            return None
        first = rows[0]
        return int(first[0] if isinstance(first, (tuple, list)) else first)


__all__ = ["ClickHouseQueryExtractService"]

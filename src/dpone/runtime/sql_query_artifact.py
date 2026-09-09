"""Connector-neutral SQL query extraction artifact."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.runtime.artifact_models import BaseExtractionArtifact, StagingTableArtifact


class SqlQueryArtifact(BaseExtractionArtifact):
    """A read-only SQL query that must be staged by a compatible sink."""

    extraction_completion_mode = "lazy"

    def __init__(
        self,
        *,
        sql: str,
        dialect: str,
        sql_hash: str,
        source_path: str | None = None,
        schema_type_dialect: str | None = None,
        evidence: Mapping[str, Any] | None = None,
        estimated_rows: int | None = None,
    ) -> None:
        BaseExtractionArtifact.__init__(self, estimated_rows=estimated_rows)
        self.sql = sql
        self.dialect = dialect
        self.sql_hash = sql_hash
        self.source_path = source_path
        self.schema_type_dialect = schema_type_dialect or dialect
        self.evidence = dict(evidence or {})

    def materialize(
        self,
        staging_manager: Any,
        load_config: Any,
        schema: Sequence[tuple[str, str]],
    ) -> StagingTableArtifact:
        del staging_manager, load_config, schema
        raise RuntimeError("sql_query_artifact_requires_sink_side_staging")

    def cleanup(self) -> None:
        """SQL query artifacts do not own external temporary resources."""


__all__ = ["SqlQueryArtifact"]

"""Full refresh SQL Server source extraction strategy."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.mssql.mssql_base import MSSQLBaseExtractStrategy
from dpone.runtime.sources.strategies.mssql.mssql_schema_access import prune_schema
from dpone.type_system.source_sink.provenance import SourceRelationDialect

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class MSSQLFullExtractStrategy(MSSQLBaseExtractStrategy):
    """Full extract for SQL Server tables."""

    def get_state(self, load_config: LoadConfig) -> None:
        return None

    def extract(self, load_config: LoadConfig, last_state: Any | None) -> ExtractResult:
        del last_state
        schema = prune_schema(self.fetch_schema(load_config), load_config.options.get("columns"))
        predicate = load_config.options.get("source_custom_predicate") or load_config.custom_predicate
        query = self._select_query(load_config, [column for column, _ in schema], predicate)
        artifact = self._artifact_for_query(load_config, query, schema)
        return ExtractResult(
            artifact=artifact,
            schema=self._output_schema(load_config, schema),
            relation_schema=schema,
            relation_dialect=SourceRelationDialect.MSSQL,
            state=None,
        )


__all__ = ["MSSQLFullExtractStrategy"]

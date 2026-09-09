"""Full refresh MySQL source extraction."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.mysql.mysql_base import MySQLBaseExtractStrategy

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class MySQLFullExtractStrategy(MySQLBaseExtractStrategy):
    """Full extract for MySQL tables (also used by replace / partition_replace)."""

    def get_state(self, load_config: LoadConfig) -> None:
        del load_config
        return None

    def extract(self, load_config: LoadConfig, last_state: Any | None) -> ExtractResult:
        del last_state
        schema = self.fetch_schema(load_config)
        predicate = load_config.options.get("source_custom_predicate") or load_config.custom_predicate
        query = self._select_query(load_config, [column for column, _ in schema], predicate)
        artifact = self._artifact_for_query(load_config, query, schema)
        return ExtractResult(artifact=artifact, schema=schema, state=None)


__all__ = ["MySQLFullExtractStrategy"]

"""Generic REST API source."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.sources.api.base import AbstractAPISource
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.streaming_rows import StreamingRowsArtifact

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class GenericRestSource(AbstractAPISource):
    """Self-service JSON REST pull source."""

    def __init__(self, connector, sink_connector=None, logger=None):
        super().__init__(connector=connector, sink_connector=sink_connector, logger=logger)
        self._strategy_map = {
            LoadStrategy.FULL_REFRESH: self,
            LoadStrategy.INCREMENTAL_APPEND: self,
            LoadStrategy.INCREMENTAL_MERGE: self,
            LoadStrategy.REPLACE: self,
            LoadStrategy.PARTITION_REPLACE: self,
        }

    def health_check(self) -> bool:
        return True

    def get_state(self, load_config: LoadConfig) -> dict[str, Any] | None:
        incremental_column = load_config.options.get("incremental_column")
        if (
            not incremental_column
            or not self.sink_connector
            or not hasattr(self.sink_connector, "get_max_column_value")
        ):
            return None
        max_value = self.sink_connector.get_max_column_value(
            load_config.target_schema, load_config.target_table, incremental_column
        )
        return {"last_value": max_value, "column": incremental_column} if max_value is not None else None

    def extract(self, load_config: LoadConfig, last_state: Mapping[str, Any] | None) -> ExtractResult:
        rows = self.connector.iter_rows(load_config.options, last_state=last_state)
        artifact = StreamingRowsArtifact(rows, batch_size=load_config.batch_size)
        schema = self._schema_from_options(load_config.options)
        return ExtractResult(artifact=artifact, schema=schema, state=last_state)

    @staticmethod
    def _schema_from_options(options: Mapping[str, Any]) -> list[tuple[str, str]]:
        configured = options.get("schema") or options.get("columns")
        if isinstance(configured, list):
            result = []
            for item in configured:
                if isinstance(item, Mapping):
                    result.append((str(item["name"]), str(item.get("type", "nvarchar(max)"))))
                else:
                    result.append((str(item), "nvarchar(max)"))
            return result
        return [("payload", "nvarchar(max)")]

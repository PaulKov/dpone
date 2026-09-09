from __future__ import annotations

from typing import Any

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.connectors.api.fasttrack_resources import get_fasttrack_resource
from dpone.runtime.sources.api.base import AbstractAPISource
from dpone.runtime.sources.strategies.api.fasttrack import (
    FasttrackFullExtractStrategy,
    FasttrackIncrementalMergeExtractStrategy,
)


class FasttrackSource(AbstractAPISource):
    """Fasttrack pull API source.

    Landing semantics stay close to the provider payload:
    provider fields are preserved and column names are only sanitized to be
    warehouse-safe. Temporal parsing is the only provider-aware transform and
    can be disabled via runtime/source options.
    """

    def __init__(self, connector, sink_connector, logger):
        super().__init__(connector, sink_connector, logger)
        self._full_extract = FasttrackFullExtractStrategy(
            connector=self.connector,
            sink_connector=self.sink_connector,
            logger=self.logger,
        )
        self._incremental_merge = FasttrackIncrementalMergeExtractStrategy(
            connector=self.connector,
            sink_connector=self.sink_connector,
            logger=self.logger,
        )
        self._strategy_map = {
            LoadStrategy.FULL_REFRESH: self._full_extract,
            LoadStrategy.INCREMENTAL_MERGE: self._incremental_merge,
        }

    def _resolve_strategy(self, load_config: Any):
        spec = get_fasttrack_resource(
            (getattr(load_config, "options", {}) or {}).get("resource") or getattr(load_config, "source_table", None)
        )
        strategy = super()._resolve_strategy(load_config)
        if hasattr(self.logger, "log_etl_progress"):
            self.logger.log_etl_progress(
                "FASTTRACK_SOURCE_STRATEGY",
                {
                    "Resource": spec.name,
                    "Requested": getattr(load_config.load_strategy, "value", load_config.load_strategy),
                    "Default": spec.default_load_strategy,
                },
            )
        return strategy

    def health_check(self) -> bool:
        return self.connector.health_check()

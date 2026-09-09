from __future__ import annotations

from typing import Any

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.sources.api.base import AbstractAPISource
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.appsflyer import (
    AppsflyerFullExtractStrategy,
    AppsflyerIncrementalAppendExtractStrategy,
)


class AppsflyerSource(AbstractAPISource):
    """AppsFlyer API source integrated into canonical dpone runtime."""

    def __init__(self, connector, sink_connector, logger):
        super().__init__(connector, sink_connector, logger)
        self._full_extract = AppsflyerFullExtractStrategy(
            connector=self.connector,
            sink_connector=self.sink_connector,
            logger=self.logger,
        )
        self._incremental_append = AppsflyerIncrementalAppendExtractStrategy(
            connector=self.connector,
            sink_connector=self.sink_connector,
            logger=self.logger,
        )
        self._strategy_map = {
            LoadStrategy.FULL_REFRESH: self._full_extract,
            LoadStrategy.INCREMENTAL_APPEND: self._incremental_append,
        }

    def get_incremental_state(self, load_config: Any) -> dict[str, Any] | None:
        return self._resolve_strategy(load_config).get_state(load_config)

    def extract(self, load_config: Any, last_state: dict[str, Any] | None) -> ExtractResult:
        return self._resolve_strategy(load_config).extract(load_config, last_state)

    def health_check(self) -> bool:
        return self.connector.health_check()

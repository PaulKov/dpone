from __future__ import annotations

from typing import Any

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.connectors.api.cbr import CbrConnector
from dpone.runtime.sources.api.base import AbstractAPISource
from dpone.runtime.sources.strategies.api.cbr import (
    CbrFullExtractStrategy,
    CbrIncrementalMergeExtractStrategy,
)


class CbrSource(AbstractAPISource):
    """Public API source for the Central Bank of Russia daily FX rates."""

    def __init__(self, connector: CbrConnector | None = None, sink_connector: Any = None, logger: Any = None):
        resolved_connector = connector if connector is not None else CbrConnector()
        super().__init__(connector=resolved_connector, sink_connector=sink_connector, logger=logger)
        self._strategy_map = {
            LoadStrategy.FULL_REFRESH: CbrFullExtractStrategy(
                connector=self.connector,
                sink_connector=self.sink_connector,
                logger=self.logger,
            ),
            LoadStrategy.INCREMENTAL_MERGE: CbrIncrementalMergeExtractStrategy(
                connector=self.connector,
                sink_connector=self.sink_connector,
                logger=self.logger,
            ),
        }

    def health_check(self) -> bool:
        return self.connector.health_check()

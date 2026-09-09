from __future__ import annotations

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.connectors.api.openexchangerates import OpenExchangeRatesConnector
from dpone.runtime.sources.api.base import AbstractAPISource
from dpone.runtime.sources.strategies.api.openexchangerates import (
    OpenExchangeRatesFullExtractStrategy,
    OpenExchangeRatesIncrementalMergeExtractStrategy,
)


class OpenExchangeRatesSource(AbstractAPISource):
    """OpenExchangeRates daily historical FX rates source."""

    def __init__(self, connector: OpenExchangeRatesConnector, sink_connector=None, logger=None):
        super().__init__(connector=connector, sink_connector=sink_connector, logger=logger)
        self._strategy_map = {
            LoadStrategy.FULL_REFRESH: OpenExchangeRatesFullExtractStrategy(
                connector=self.connector,
                sink_connector=self.sink_connector,
                logger=self.logger,
            ),
            LoadStrategy.INCREMENTAL_MERGE: OpenExchangeRatesIncrementalMergeExtractStrategy(
                connector=self.connector,
                sink_connector=self.sink_connector,
                logger=self.logger,
            ),
        }

    def health_check(self) -> bool:
        return self.connector.health_check()

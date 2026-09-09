from __future__ import annotations

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.sources.api.base import AbstractAPISource
from dpone.runtime.sources.strategies.api.google_sheets import GoogleSheetsFullExtractStrategy


class GoogleSheetsSource(AbstractAPISource):
    """Google Sheets source for canonical dpone runtime."""

    def __init__(self, connector, sink_connector, logger):
        super().__init__(connector, sink_connector, logger)
        self._strategy_map = {
            LoadStrategy.FULL_REFRESH: GoogleSheetsFullExtractStrategy(
                connector=self.connector,
                sink_connector=self.sink_connector,
                logger=self.logger,
            ),
        }

    def health_check(self) -> bool:
        try:
            return self.connector.health_check()
        except Exception:
            return False

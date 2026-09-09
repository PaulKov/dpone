from __future__ import annotations

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.sources.api.base import AbstractAPISource
from dpone.runtime.sources.strategies.api.similarweb import SimilarwebKeywordsIncrementalAppendStrategy


class SimilarwebSource(AbstractAPISource):
    """SimilarWeb Website Keywords source for canonical dpone runtime."""

    def __init__(self, connector, sink_connector, logger):
        super().__init__(connector, sink_connector, logger)
        self._strategy_map = {
            LoadStrategy.INCREMENTAL_APPEND: SimilarwebKeywordsIncrementalAppendStrategy(
                connector=self.connector,
                sink_connector=self.sink_connector,
                logger=self.logger,
            ),
        }

    def health_check(self) -> bool:
        return self.connector.health_check()

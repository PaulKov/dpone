from __future__ import annotations

from typing import Any

from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.sources.api.base import AbstractAPISource
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.omnidesk import (
    OmnideskDictionaryExtractStrategy,
    OmnideskFullExtractStrategy,
    OmnideskIncrementalAppendExtractStrategy,
    OmnideskIncrementalMergeExtractStrategy,
)


class OmnideskSource(AbstractAPISource):
    """
    Источник данных Omnidesk.

    """

    # Ресурсы-справочники (без временных фильтров)
    DICTIONARY_RESOURCES = {"groups", "staff", "labels"}

    def __init__(
        self,
        connector,
        sink_connector,
        logger,
        resource: str = "cases",
    ):
        """
        Args:
            connector: OmnideskConnector
            sink_connector: Коннектор к sink (PG/BQ) для получения MAX()
            logger: ETL logger
            resource: Тип ресурса (cases, messages)
        """
        super().__init__(connector, sink_connector, logger)
        self.resource = resource

        # Инициализируем стратегии
        self._full_extract = OmnideskFullExtractStrategy(
            connector=self.connector,
            sink_connector=self.sink_connector,
            logger=self.logger,
        )

        self._cases_incremental = OmnideskIncrementalMergeExtractStrategy(
            connector=self.connector,
            sink_connector=self.sink_connector,
            logger=self.logger,
        )

        self._messages_incremental = OmnideskIncrementalAppendExtractStrategy(
            connector=self.connector,
            sink_connector=self.sink_connector,
            logger=self.logger,
        )

        self._dictionary_extract = OmnideskDictionaryExtractStrategy(
            connector=self.connector,
            sink_connector=self.sink_connector,
            logger=self.logger,
        )

        # Маппинг стратегий для cases/messages
        self._strategy_map = {
            LoadStrategy.FULL_REFRESH: self._full_extract,
            LoadStrategy.INCREMENTAL_MERGE: self._cases_incremental,
            LoadStrategy.INCREMENTAL_APPEND: self._messages_incremental,
        }

    def _get_strategy(self, load_config: Any):
        """
        Выбирает стратегию на основе resource и load_strategy.

        Для справочников (groups, staff, labels) всегда используется
        OmnideskDictionaryExtractStrategy независимо от load_strategy.
        """
        options = getattr(load_config, "options", {}) or {}
        resource = options.get("resource", self.resource)

        # Справочники — специальная стратегия
        if resource in self.DICTIONARY_RESOURCES:
            return self._dictionary_extract

        # Для cases/messages — используем маппинг по load_strategy
        load_strategy = getattr(load_config, "load_strategy", LoadStrategy.FULL_REFRESH)
        return self._strategy_map.get(load_strategy, self._full_extract)

    def get_state(self, load_config: Any) -> dict[str, Any] | None:
        """Получает инкрементальное состояние через выбранную стратегию."""
        strategy = self._get_strategy(load_config)
        return strategy.get_state(load_config)

    def extract(
        self,
        load_config: Any,
        last_state: dict[str, Any] | None,
    ) -> ExtractResult:
        """Извлекает данные через выбранную стратегию."""
        strategy = self._get_strategy(load_config)
        return strategy.extract(load_config, last_state)

    def health_check(self) -> bool:
        """Проверяет доступность Omnidesk API."""
        return self.connector.health_check()

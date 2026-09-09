"""
Базовый класс для API источников данных.

Предоставляет абстракцию для работы с REST API:
- Интеграция со стратегиями извлечения
- Управление API коннекторами
- Единый интерфейс для ETL pipeline
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.config.load_strategy import LoadStrategy
from abc import abstractmethod
from typing import Any

from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.source_protocol import AbstractSource
from dpone.runtime.sources.strategies.api.base import APIBaseStrategy


class AbstractAPISource(AbstractSource):
    """
    Абстрактный источник данных API.

    Наследники должны:
    - Инициализировать connector
    - Настроить маппинг стратегий
    - Опционально: переопределить логику выбора стратегии
    """

    def __init__(
        self,
        connector,
        sink_connector,
        logger,
    ):
        """
        Args:
            connector: API коннектор (OmnideskConnector, etc.)
            sink_connector: Коннектор к sink для получения MAX()
            logger: ETL logger
        """
        self.connector = connector
        self.sink_connector = sink_connector
        self.logger = logger

        self._strategy_map: dict[LoadStrategy, APIBaseStrategy] = {}

    def get_incremental_state(self, load_config: LoadConfig) -> Any | None:
        """Получает состояние для инкрементальной загрузки."""
        strategy = self._resolve_strategy(load_config)
        return strategy.get_state(load_config)

    def extract(self, load_config: LoadConfig, last_state: Any | None) -> ExtractResult:
        """Извлекает данные из API используя выбранную стратегию."""
        strategy = self._resolve_strategy(load_config)
        return strategy.extract(load_config, last_state)

    def _resolve_strategy(self, load_config: LoadConfig) -> APIBaseStrategy:
        """
        Выбирает стратегию на основе load_strategy.

        Наследники могут переопределить для более сложной логики.
        """
        strategy = self._strategy_map.get(load_config.load_strategy)
        if strategy is None:
            supported = ", ".join(s.value for s in self._strategy_map.keys())
            raise ValueError(
                f"Неподдерживаемая стратегия: {load_config.load_strategy.value}. Поддерживаемые: {supported}"
            )
        return strategy

    @abstractmethod
    def health_check(self) -> bool:
        """Проверяет доступность API."""

    def close(self) -> None:
        """Закрывает соединения."""
        if hasattr(self.connector, "close"):
            self.connector.close()

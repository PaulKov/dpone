"""Базовые стратегии загрузки для Sinks."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult


class SinkStrategy(ABC):
    @abstractmethod
    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        """Применяет стратегию к целевой системе."""

    def save_state(self, load_config: Any, state: Any) -> None:
        """Опциональная запись состояния."""
        return None

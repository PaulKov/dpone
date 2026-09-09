"""Source interface contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from dpone.runtime.sources.extract_result import ExtractResult


class AbstractSource(ABC):
    """Abstract runtime data source."""

    @abstractmethod
    def extract(self, load_config: Any, last_state: Any | None) -> ExtractResult:
        """Extract rows from a source system."""

    @abstractmethod
    def get_incremental_state(self, load_config: Any) -> Any | None:
        """Return saved incremental state when the source supports it."""


__all__ = ["AbstractSource"]

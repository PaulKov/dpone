"""Sink interface contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult


class AbstractSink(ABC):
    """Abstract runtime data sink."""

    @abstractmethod
    def load(self, load_config: Any, payload: LoadPayload) -> LoadResult:
        """Load a payload into the target system."""

    def save_state(self, load_config: Any, state: Any) -> None:
        """Persist incremental state when the sink supports it."""
        return None


__all__ = ["AbstractSink"]

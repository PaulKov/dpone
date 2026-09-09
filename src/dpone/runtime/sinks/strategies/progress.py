"""Shared structured progress logging for sink/source strategies."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.runtime.sink_logging import etl_logger


class ProgressLoggerProtocol(Protocol):
    """Minimal runtime logger contract used by strategies."""

    def log_etl_progress(self, event: str, payload: dict[str, Any]) -> None:  # pragma: no cover - protocol
        ...


@dataclass(frozen=True, slots=True)
class ProgressEvent:
    """A normalized structured progress event."""

    code: str
    payload: Mapping[str, Any]

    def normalized_payload(self) -> dict[str, Any]:
        return dict(self.payload)


class StrategyProgressLogger:
    """Thin standardized adapter over ETLLogger progress events.

    Vendor-specific strategy modules should define semantic event names and
    payloads, but should not each invent their own logging transport contract.
    """

    def __init__(self, logger: ProgressLoggerProtocol | None = None) -> None:
        self._logger = logger or etl_logger

    def emit(self, event: ProgressEvent | str, payload: Mapping[str, Any] | None = None) -> None:
        if isinstance(event, ProgressEvent):
            self._logger.log_etl_progress(event.code, event.normalized_payload())
            return
        self._logger.log_etl_progress(event, dict(payload or {}))

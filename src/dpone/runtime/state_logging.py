"""State-storage logging port."""

from __future__ import annotations

from dpone.runtime.logging_core import RuntimeLogger, etl_logger

ETLLogger = RuntimeLogger
StateLogger = RuntimeLogger

__all__ = ["ETLLogger", "StateLogger", "etl_logger"]

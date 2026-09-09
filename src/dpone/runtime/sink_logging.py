"""Sink and staging logging port."""

from __future__ import annotations

from dpone.runtime.logging_core import RuntimeLogger, etl_logger

ETLLogger = RuntimeLogger
SinkLogger = RuntimeLogger

__all__ = ["ETLLogger", "SinkLogger", "etl_logger"]

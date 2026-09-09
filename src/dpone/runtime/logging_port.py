"""Backward-compatible runtime logging facade."""

from __future__ import annotations

from dpone.runtime.logging_core import RuntimeLogger, create_etl_logger, etl_logger

ETLLogger = RuntimeLogger

__all__ = ["ETLLogger", "RuntimeLogger", "create_etl_logger", "etl_logger"]

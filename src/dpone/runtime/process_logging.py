"""ETL process logging port."""

from __future__ import annotations

from dpone.runtime.logging_core import RuntimeLogger, create_etl_logger, etl_logger

ETLLogger = RuntimeLogger
ProcessLogger = RuntimeLogger

__all__ = ["ETLLogger", "ProcessLogger", "create_etl_logger", "etl_logger"]

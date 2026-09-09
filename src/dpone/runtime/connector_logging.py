"""Connector and credential logging port."""

from __future__ import annotations

from dpone.runtime.logging_core import RuntimeLogger, etl_logger

ConnectorLogger = RuntimeLogger
ETLLogger = RuntimeLogger

__all__ = ["ConnectorLogger", "ETLLogger", "etl_logger"]

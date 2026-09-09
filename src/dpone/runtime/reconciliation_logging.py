"""Reconciliation logging port."""

from __future__ import annotations

from dpone.runtime.logging_core import RuntimeLogger, etl_logger

ETLLogger = RuntimeLogger
ReconciliationLogger = RuntimeLogger

__all__ = ["ETLLogger", "ReconciliationLogger", "etl_logger"]

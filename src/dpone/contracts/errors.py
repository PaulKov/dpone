"""Backward-compatible error facade."""

from __future__ import annotations

from dpone.contracts.configuration_errors import ETLConfigurationError, RuntimeConfigurationError
from dpone.contracts.process_errors import ETLProcessError

__all__ = ["ETLConfigurationError", "ETLProcessError", "RuntimeConfigurationError"]

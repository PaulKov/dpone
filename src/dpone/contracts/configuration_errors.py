"""Configuration error contracts."""

from __future__ import annotations


class ETLConfigurationError(Exception):
    """Raised when manifest, CLI, or runtime configuration is invalid."""


class RuntimeConfigurationError(ETLConfigurationError):
    """Raised when runtime connectors, sources, sinks, or hydration inputs are invalid."""


__all__ = ["ETLConfigurationError", "RuntimeConfigurationError"]

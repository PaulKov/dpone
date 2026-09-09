"""DAG configuration errors."""

from __future__ import annotations

from dpone.contracts.configuration_errors import ETLConfigurationError


class DagConfigurationError(ETLConfigurationError):
    """Raised when DAG process config, dependencies, or graph selection are invalid."""


__all__ = ["DagConfigurationError"]

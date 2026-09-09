"""Documentation maintenance errors."""

from __future__ import annotations

from dpone.contracts.configuration_errors import ETLConfigurationError


class DocsConfigurationError(ETLConfigurationError):
    """Raised when docs self-service commands receive invalid inputs."""


__all__ = ["DocsConfigurationError"]

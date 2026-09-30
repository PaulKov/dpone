"""Manifest configuration errors."""

from __future__ import annotations

from dpone.contracts.configuration_errors import ETLConfigurationError


class ManifestConfigurationError(ETLConfigurationError):
    """Raised when manifest files, registry data, or manifest selectors are invalid."""

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code


class LegacySingleManifestConfigurationError(ManifestConfigurationError):
    """A legacy single-manifest config cannot expose selector-aware metadata."""


__all__ = ["LegacySingleManifestConfigurationError", "ManifestConfigurationError"]

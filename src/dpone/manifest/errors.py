"""Manifest configuration errors."""

from __future__ import annotations

from dpone.contracts.configuration_errors import ETLConfigurationError


class ManifestConfigurationError(ETLConfigurationError):
    """Raised when manifest files, registry data, or manifest selectors are invalid."""


class LegacySingleManifestConfigurationError(ManifestConfigurationError):
    """A legacy single-manifest config cannot expose selector-aware metadata."""


__all__ = ["LegacySingleManifestConfigurationError", "ManifestConfigurationError"]

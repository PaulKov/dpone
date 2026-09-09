"""Runtime configuration errors."""

from __future__ import annotations

from typing import NoReturn

from dpone.contracts.configuration_errors import ETLConfigurationError

LEGACY_RUNTIME_DEFAULTS_DISABLED_CODE = "DPONE_LEGACY_RUNTIME_DEFAULTS_DISABLED"
LEGACY_RUNTIME_DEFAULTS_DISABLED_MESSAGE = "Implicit runtime defaults are disabled; configure a logical connection_ref."


class RuntimeConfigurationError(ETLConfigurationError):
    """Raised when runtime connectors, sources, sinks, or hydration inputs are invalid."""


class LegacyRuntimeDefaultsDisabledError(RuntimeConfigurationError):
    """Raised when removed tenant-specific runtime defaults would have been inferred."""

    def __init__(self, *, detail: str | None = None) -> None:
        message = LEGACY_RUNTIME_DEFAULTS_DISABLED_MESSAGE
        if detail:
            message = f"{message} {detail}"
        super().__init__(message)
        self.code = LEGACY_RUNTIME_DEFAULTS_DISABLED_CODE


def raise_legacy_runtime_defaults_disabled(*, detail: str | None = None) -> NoReturn:
    raise LegacyRuntimeDefaultsDisabledError(detail=detail)


__all__ = [
    "LEGACY_RUNTIME_DEFAULTS_DISABLED_CODE",
    "LEGACY_RUNTIME_DEFAULTS_DISABLED_MESSAGE",
    "LegacyRuntimeDefaultsDisabledError",
    "RuntimeConfigurationError",
    "raise_legacy_runtime_defaults_disabled",
]

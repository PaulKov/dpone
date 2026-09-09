"""Schema control-plane configuration errors."""

from __future__ import annotations

from dpone.contracts.configuration_errors import ETLConfigurationError


class SchemaMigrationConfigurationError(ETLConfigurationError):
    """Raised when schema migration packs, ledgers, or actual-state inputs are invalid."""


__all__ = ["SchemaMigrationConfigurationError"]

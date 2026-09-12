"""Read-only capability boundaries for PostgreSQL to MSSQL R1 resolution."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.contracts.postgres_mssql_correctness_profile import (
        PostgresMssqlCorrectnessProfile,
        PostgresMssqlCorrectnessRouteRequest,
    )


class PostgresMssqlCorrectnessProfileProvider(Protocol):
    """Load one environment-owned profile without reading business data."""

    def load(self, profile_id: str) -> PostgresMssqlCorrectnessProfile:
        """Return the exact signed deployment profile or fail closed."""


class PostgresMssqlCorrectnessEvidencePort(Protocol):
    """Resolve current implementation and certification status."""

    def bind_current_evidence(
        self,
        profile: PostgresMssqlCorrectnessProfile,
    ) -> PostgresMssqlCorrectnessProfile:
        """Return an immutable profile copy bound to current exact evidence."""


class PostgresMssqlCorrectnessProfileSelector(Protocol):
    """Select an explicitly registered profile for exact route coordinates."""

    def select_profile_id(self, request: PostgresMssqlCorrectnessRouteRequest) -> str | None:
        """Return a platform-owned profile ID or ``None`` for compatibility."""


__all__ = [
    "PostgresMssqlCorrectnessEvidencePort",
    "PostgresMssqlCorrectnessProfileProvider",
    "PostgresMssqlCorrectnessProfileSelector",
]

"""Reusable fail-closed binding for signed SQL Server database authority."""

from __future__ import annotations

from typing import Any


class MssqlDatabaseAuthorityBindingMixin:
    """Give target-atomic state adapters one explicit verifier boundary.

    Construction alone never grants authority. Strict hydration or an
    explicitly composed caller must bind a typed verifier before route
    preflight. This prevents direct and legacy construction from bypassing
    deployment-owned database pins.
    """

    connector: Any
    _database_authority_verifier: Any | None

    def _initialize_database_authority_binding(self) -> None:
        self._database_authority_verifier = None

    def bind_database_authority(self, verifier: Any) -> Any:
        """Bind a verifier issued from an immutable connection registry."""

        if verifier is None or not callable(getattr(verifier, "verify", None)):
            raise ValueError("mssql_transaction.database_authority_verifier_required")
        self._database_authority_verifier = verifier
        return self

    @property
    def database_authority_bound(self) -> bool:
        """Whether an explicit composition root supplied signed authority."""

        return self._database_authority_verifier is not None

    def require_database_authority_binding(self) -> None:
        """Assert signed authority composition without performing vendor I/O."""

        self._require_database_authority_verifier()

    def verify_database_authority(self, target_connector: Any) -> None:
        """Verify target, staging, and state before catalog or source work."""

        verifier = self._require_database_authority_verifier()
        verifier.verify(
            target_connector=target_connector,
            state_connector=self.connector,
        )

    def bounded_database_authority_query_timeout(self, seconds: int) -> Any:
        """Open the verifier-owned factory-session timeout capability."""

        verifier = self._require_database_authority_verifier()
        scope = getattr(verifier, "bounded_query_timeout", None)
        if not callable(scope):
            raise RuntimeError("mssql_transaction.database_authority_query_timeout_scope_required")
        return scope(seconds)

    def acquire_staging_database_authority_lease(self, target_connector: Any) -> Any:
        """Acquire a live, signed session lease for staging operations."""

        verifier = self._require_database_authority_verifier()
        acquire = getattr(verifier, "acquire_staging_lease", None)
        if not callable(acquire):
            from dpone.runtime.state.mssql_database_authority import (
                MssqlDatabaseAuthorityVerificationError,
            )

            raise MssqlDatabaseAuthorityVerificationError(
                "mssql_transaction.staging_database_authority_verifier_required"
            )
        return acquire(target_connector=target_connector)

    def _require_database_authority_verifier(self) -> Any:
        verifier = self._database_authority_verifier
        if verifier is None:
            from dpone.runtime.state.mssql_database_authority import (
                MssqlDatabaseAuthorityVerificationError,
            )

            raise MssqlDatabaseAuthorityVerificationError("mssql_transaction.database_authority_verifier_required")
        return verifier


__all__ = ["MssqlDatabaseAuthorityBindingMixin"]

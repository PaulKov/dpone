"""Prepared PostgreSQL source boundary for governed MSSQL admission.

The boundary keeps one branded repeatable-read session, its signed relation
lock, and the schema projection together from post-receipt admission through
COPY.  It is intentionally a runtime object and must never be serialized into
authoring or evidence contracts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.runtime.extraction_lifecycle import ExtractionLifecycleStateError
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_failure import (
    rollback_preserving_primary,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    PostgresRepeatableReadSnapshotLease,
)

MSSQL_PREPARED_POSTGRES_SOURCE_BOUNDARY_OPTION = "__dpone_mssql_prepared_postgres_source_boundary"


@dataclass(frozen=True, slots=True)
class PreparedPostgresSourceBoundary:
    """One active RR lease with verified identity and cached source schema."""

    connector: Any
    snapshot_lease: PostgresRepeatableReadSnapshotLease
    source_identity: Any
    schema_projection: Any

    def require_active(self, connector: Any) -> None:
        """Prove the same session still owns the in-progress RR boundary."""

        if connector is not self.connector:
            raise RuntimeError("postgres_prepared_source_boundary.connector_mismatch")
        self.snapshot_lease.require_for(connector)

    def complete(self) -> None:
        """Commit the RR after eager artifact materialization."""

        self.require_active(self.connector)
        self.snapshot_lease.lifecycle.complete()
        self.connector.commit_transaction()

    def abort_preserving(self, primary: BaseException) -> None:
        """Release an active boundary without replacing the primary failure."""

        if not self._is_active():
            return
        rollback_preserving_primary(self.connector, primary)

    def abort_if_active(self) -> None:
        """Release an abandoned boundary during deterministic finalization."""

        if self._is_active():
            self.connector.rollback()

    def _is_active(self) -> bool:
        try:
            self.snapshot_lease.require_for(self.connector)
        except ExtractionLifecycleStateError:
            return False
        return True


def prepared_postgres_source_boundary(load_config: Any) -> PreparedPostgresSourceBoundary | None:
    """Return only the typed runtime boundary stored by admission."""

    options = getattr(load_config, "options", {}) or {}
    boundary = options.get(MSSQL_PREPARED_POSTGRES_SOURCE_BOUNDARY_OPTION)
    if boundary is None:
        return None
    if not isinstance(boundary, PreparedPostgresSourceBoundary):
        raise RuntimeError("postgres_prepared_source_boundary.invalid")
    return boundary


__all__ = [
    "MSSQL_PREPARED_POSTGRES_SOURCE_BOUNDARY_OPTION",
    "PreparedPostgresSourceBoundary",
    "prepared_postgres_source_boundary",
]

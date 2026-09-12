"""Prepared PostgreSQL source boundary for governed MSSQL admission.

The boundary keeps one branded repeatable-read session, its signed relation
lock, and the schema projection together from post-receipt admission through
COPY.  It is intentionally a runtime object and must never be serialized into
authoring or evidence contracts.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.runtime.extraction_lifecycle import ExtractionLifecycleStateError
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sources.postgres_mssql_source_schema_types import (
    PostgresMssqlSelectedRelationSchemaAuthorityV1,
    PostgresMssqlSourceSchemaAuthorityErrorV1,
)
from dpone.runtime.sources.postgres_verified_relation_snapshot import PostgresVerifiedRelationSnapshotScopeV1
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_failure import (
    rollback_preserving_primary,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    PostgresRepeatableReadSnapshotLease,
)

if TYPE_CHECKING:
    from dpone.runtime.sources.strategies.postgres.postgres_schema_metadata import PostgresFetchedSchema
    from dpone.runtime.support.postgres_mssql_projection_models import PostgresMssqlSchemaProjection
    from dpone.type_system.source_sink.provenance import SourceColumnProvenance

MSSQL_PREPARED_POSTGRES_SOURCE_BOUNDARY_OPTION = "__dpone_mssql_prepared_postgres_source_boundary"
_R1_BOUNDARY_ISSUER_TOKEN = object()


@dataclass(frozen=True, slots=True, init=False)
class PreparedPostgresSourceBoundary:
    """One active RR lease with verified identity and cached source schema."""

    connector: Any
    snapshot_lease: PostgresRepeatableReadSnapshotLease
    source_identity: Any
    schema_projection: Any

    _scope: PostgresVerifiedRelationSnapshotScopeV1 | None
    _source_schema_authority: PostgresMssqlSelectedRelationSchemaAuthorityV1 | None
    _runtime_issued: bool

    def __init__(
        self,
        connector: object,
        snapshot_lease: PostgresRepeatableReadSnapshotLease,
        source_identity: object,
        schema_projection: object,
        *,
        _scope: PostgresVerifiedRelationSnapshotScopeV1 | None = None,
        _source_schema_authority: PostgresMssqlSelectedRelationSchemaAuthorityV1 | None = None,
        _issuer_token: object | None = None,
    ) -> None:
        if _issuer_token is _R1_BOUNDARY_ISSUER_TOKEN and (
            type(snapshot_lease) is not PostgresRepeatableReadSnapshotLease
            or snapshot_lease._connector is not connector
        ):
            raise TypeError("invalid prepared PostgreSQL source boundary")
        object.__setattr__(self, "connector", connector)
        object.__setattr__(self, "snapshot_lease", snapshot_lease)
        object.__setattr__(self, "source_identity", source_identity)
        object.__setattr__(self, "schema_projection", schema_projection)
        runtime_issued = _issuer_token is _R1_BOUNDARY_ISSUER_TOKEN
        if runtime_issued != (_scope is not None and _source_schema_authority is not None):
            raise TypeError("invalid prepared PostgreSQL source boundary authority")
        object.__setattr__(self, "_scope", _scope)
        object.__setattr__(self, "_source_schema_authority", _source_schema_authority)
        object.__setattr__(self, "_runtime_issued", runtime_issued)

    @property
    def source_schema_authority(self) -> PostgresMssqlSelectedRelationSchemaAuthorityV1 | None:
        """Return the sealed runtime authority; legacy boundaries expose none."""

        return self._source_schema_authority

    def require_active(self, connector: Any) -> None:
        """Prove the same session still owns the in-progress RR boundary."""

        if self._runtime_issued and type(self) is not PreparedPostgresSourceBoundary:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation")
        if connector is not self.connector:
            raise RuntimeError("postgres_prepared_source_boundary.connector_mismatch")
        if self._runtime_issued:
            assert self._scope is not None
            try:
                self._scope.require_active(connector)
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit):
                raise
            except BaseException as failure:
                raise PostgresMssqlSourceSchemaAuthorityErrorV1.from_internal_reason(
                    getattr(failure, "reason", "internal_invariant_violation")
                ) from None
        else:
            self.snapshot_lease.require_for(connector)

    def require_active_for_copy(self, connector: object) -> None:
        """Revalidate the exact hidden R1 scope immediately before COPY."""

        if type(self) is not PreparedPostgresSourceBoundary or not getattr(self, "_runtime_issued", False):
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation")
        self.require_active(connector)

    def lifecycle_for_copy(self):
        """Return lifecycle authority without exposing the hidden scope."""

        if type(self) is not PreparedPostgresSourceBoundary or not getattr(self, "_runtime_issued", False):
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation")
        assert self._scope is not None
        return self._scope.lifecycle

    @property
    def lifecycle(self):
        """Compatibility view of the boundary-owned extraction lifecycle."""

        if self._runtime_issued:
            assert self._scope is not None
            return self._scope.lifecycle
        return self.snapshot_lease.lifecycle

    def require_completed(self) -> None:
        """Require successful terminal cleanup without altering lifecycle or receipt."""
        terminal = self.terminal_receipt
        if terminal is None or terminal.outcome != "completed" or not terminal.cleanup_succeeded:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("snapshot_cleanup_failed")

    def complete(self, artifact: FileExportArtifact | None = None) -> None:
        """Commit only after a verified artifact and successful terminal cleanup."""
        if not self._runtime_issued:
            if artifact is not None:
                raise TypeError("legacy complete accepts no artifact")
            self.require_active(self.connector)
            self.snapshot_lease.lifecycle.complete()
            self.connector.commit_transaction()
            return
        assert self._scope is not None
        stage_reason = "internal_invariant_violation"
        try:
            if type(artifact) is not FileExportArtifact:
                raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation")
            receipt = artifact.require_integrity_receipt()
            if receipt.rows_exported is None:
                raise PostgresMssqlSourceSchemaAuthorityErrorV1("internal_invariant_violation")
            stage_reason = "snapshot_cleanup_failed"
            terminal = self._scope.complete_after_artifact_seal()
            if not terminal.cleanup_succeeded:
                raise PostgresMssqlSourceSchemaAuthorityErrorV1("snapshot_cleanup_failed")
        except BaseException as original:
            if not isinstance(original, Exception) or isinstance(original, PostgresMssqlSourceSchemaAuthorityErrorV1):
                failure = original
            else:
                failure = PostgresMssqlSourceSchemaAuthorityErrorV1.from_internal_reason(
                    getattr(original, "reason", stage_reason)
                )
            self._abort_runtime_preserving(failure)

    def _abort_runtime_preserving(self, primary: BaseException) -> None:
        """Attempt scope cleanup without replacing the primary failure."""
        assert self._scope is not None
        try:
            self._scope.abort_preserving(primary)
        except BaseException:
            pass
        primary.__cause__ = primary.__context__ = None
        raise primary from None

    def abort_preserving(self, primary: BaseException) -> None:
        """Release an active boundary while preserving its primary failure."""
        if self._runtime_issued:
            self._abort_runtime_preserving(primary)
        elif self._is_active():
            rollback_preserving_primary(self.connector, primary)

    def close_if_active(self):
        """Report current cleanup failure; repeated close does not retry cleanup."""
        if self._runtime_issued:
            assert self._scope is not None
            try:
                terminal = self._scope.close_if_active()
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit):
                raise
            except PostgresMssqlSourceSchemaAuthorityErrorV1:
                raise
            except Exception:
                raise PostgresMssqlSourceSchemaAuthorityErrorV1("snapshot_cleanup_failed") from None
            if terminal is not None and not terminal.cleanup_succeeded:
                raise PostgresMssqlSourceSchemaAuthorityErrorV1("snapshot_cleanup_failed")
        elif self._is_active():
            self.connector.rollback()
        return None

    def abort_if_active(self) -> None:
        """Compatibility spelling for legacy finalizers."""

        self.close_if_active()

    @property
    def terminal_receipt(self):
        if not getattr(self, "_runtime_issued", False):
            return None
        assert self._scope is not None
        return self._scope.terminal_receipt

    def _is_active(self) -> bool:
        if self._runtime_issued:
            assert self._scope is not None
            return self._scope.terminal_receipt is None
        try:
            self.snapshot_lease.require_for(self.connector)
        except ExtractionLifecycleStateError:
            return False
        return True


def build_r1_postgres_fetched_schema(
    *,
    relation_schema: tuple[tuple[str, str], ...],
    projected_schema: tuple[tuple[str, str], ...],
    relation_metadata: tuple[SourceColumnProvenance, ...],
    target_projection: PostgresMssqlSchemaProjection,
) -> PostgresFetchedSchema:
    """Construct the exact compatibility DTO at its owning boundary."""

    from dpone.runtime.sources.strategies.postgres.postgres_schema_metadata import PostgresFetchedSchema

    result = PostgresFetchedSchema(relation_schema, projected_schema, relation_metadata, target_projection)
    if type(result) is not PostgresFetchedSchema:
        raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation")
    return result


def issue_r1_prepared_postgres_source_boundary(
    *,
    connector: object,
    scope: PostgresVerifiedRelationSnapshotScopeV1,
    source_schema_authority: PostgresMssqlSelectedRelationSchemaAuthorityV1,
    schema_projection: PostgresFetchedSchema,
) -> PreparedPostgresSourceBoundary:
    """Issue the only exact R1 boundary admitted to the COPY path."""

    from dpone.runtime.sources.strategies.postgres.postgres_schema_metadata import PostgresFetchedSchema

    if (
        type(scope) is not PostgresVerifiedRelationSnapshotScopeV1
        or type(source_schema_authority) is not PostgresMssqlSelectedRelationSchemaAuthorityV1
        or type(schema_projection) is not PostgresFetchedSchema
    ):
        raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation")
    selected = source_schema_authority.selected_source_document
    result = PreparedPostgresSourceBoundary(
        connector,
        scope._verified.snapshot_lease,
        selected,
        schema_projection,
        _scope=scope,
        _source_schema_authority=source_schema_authority,
        _issuer_token=_R1_BOUNDARY_ISSUER_TOKEN,
    )
    return result


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
    "build_r1_postgres_fetched_schema",
    "issue_r1_prepared_postgres_source_boundary",
    "prepared_postgres_source_boundary",
]

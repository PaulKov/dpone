"""Exact composition bundle for activation-blocked PostgreSQL R1 schema authority."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, cast

from dpone.runtime.sources.postgres_mssql_source_schema_issuer import PostgresMssqlRelationSchemaAuthorityIssuerV1
from dpone.runtime.sources.postgres_mssql_source_schema_projection import PostgresMssqlSourceSchemaProjectionAdapterV1
from dpone.runtime.sources.postgres_source_authority import PostgresSourceAuthorityVerifier
from dpone.runtime.sources.postgres_verified_relation_snapshot import PostgresVerifiedRelationSnapshotIssuerV1
from dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary import (
    PreparedPostgresSourceBoundary,
    build_r1_postgres_fetched_schema,
    issue_r1_prepared_postgres_source_boundary,
)

if TYPE_CHECKING:
    from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
    from dpone.runtime.sources.strategies.postgres.postgres_schema_metadata import PostgresFetchedSchema


class PostgresMssqlSourceSchemaRuntimeV1:
    """Immutable exact bundle selected by the private composition root."""

    verifier: PostgresSourceAuthorityVerifier
    snapshot_scope_issuer: PostgresVerifiedRelationSnapshotIssuerV1
    schema_authority_issuer: PostgresMssqlRelationSchemaAuthorityIssuerV1
    projection_adapter: PostgresMssqlSourceSchemaProjectionAdapterV1

    __slots__ = (
        "verifier",
        "snapshot_scope_issuer",
        "schema_authority_issuer",
        "projection_adapter",
    )

    def __init__(
        self,
        *,
        verifier: PostgresSourceAuthorityVerifier,
        snapshot_scope_issuer: PostgresVerifiedRelationSnapshotIssuerV1,
        schema_authority_issuer: PostgresMssqlRelationSchemaAuthorityIssuerV1,
        projection_adapter: PostgresMssqlSourceSchemaProjectionAdapterV1,
    ) -> None:
        object.__setattr__(self, "verifier", verifier)
        object.__setattr__(self, "snapshot_scope_issuer", snapshot_scope_issuer)
        object.__setattr__(self, "schema_authority_issuer", schema_authority_issuer)
        object.__setattr__(self, "projection_adapter", projection_adapter)
        self.require_exact_bundle()

    def __setattr__(self, _name: str, _value: object) -> None:
        raise AttributeError("PostgresMssqlSourceSchemaRuntimeV1 is immutable")

    def require_exact_bundle(self) -> None:
        """Reject forged or substituted composition objects before endpoint I/O."""

        exact_bundle = (
            type(self.verifier) is not PostgresSourceAuthorityVerifier
            or type(self.snapshot_scope_issuer) is not PostgresVerifiedRelationSnapshotIssuerV1
            or type(self.schema_authority_issuer) is not PostgresMssqlRelationSchemaAuthorityIssuerV1
            or type(self.projection_adapter) is not PostgresMssqlSourceSchemaProjectionAdapterV1
            or self.projection_adapter.fetched_schema_factory is not build_r1_postgres_fetched_schema
        )
        if exact_bundle:
            raise TypeError("exact PostgreSQL source-schema runtime bundle required")

    def prepare_boundary(
        self,
        *,
        connector: object,
        lifecycle: ExtractionLifecycleAuthority,
        load_config: Any,
    ) -> PreparedPostgresSourceBoundary:
        scope = None
        try:
            selected = self.verifier.select_for(load_config)
            profile = self.schema_authority_issuer.bind_query_profile(selected_source_authority=selected)
            scope = self.snapshot_scope_issuer.open(
                connector=connector,
                lifecycle=lifecycle,
                selected_source_authority=selected,
                query_profile=profile.generic_relation_profile,
            )
            authority = self.schema_authority_issuer.issue(
                connector=connector,
                verified_relation=scope.require_active(connector),
                query_profile=profile,
            )
            projection = cast("PostgresFetchedSchema", self.projection_adapter.project(authority))
            return issue_r1_prepared_postgres_source_boundary(
                connector=connector,
                scope=scope,
                source_schema_authority=authority,
                schema_projection=projection,
            )
        except asyncio.CancelledError as cancellation:
            if scope is not None:
                scope.abort_preserving(cancellation)
            cancellation.__cause__ = cancellation.__context__ = None
            raise
        except BaseException as failure:
            if scope is not None:
                scope.abort_preserving(failure)
            failure.__cause__ = failure.__context__ = None
            raise


__all__ = ["PostgresMssqlSourceSchemaRuntimeV1"]

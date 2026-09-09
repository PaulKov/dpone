"""Composition root for the protected semantic-refresh model publication path."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from dpone.adapters.semantic_refresh_artifact_reader import (
    AuthorityBoundVersionPinnedSealedArtifactReader,
)
from dpone.adapters.semantic_refresh_clickhouse_authority import (
    MssqlProtectedSemanticRefreshClickHouseAuthority,
)
from dpone.adapters.semantic_refresh_clickhouse_http import (
    ClickHouseHttpSemanticRefreshGateway,
)
from dpone.adapters.semantic_refresh_mssql_after_image import (
    MssqlSemanticRefreshAfterImageReader,
)
from dpone.adapters.semantic_refresh_mssql_authority import (
    MssqlSemanticRefreshCanonicalAuthorityLoader,
)
from dpone.adapters.semantic_refresh_mssql_evidence import (
    MssqlSemanticRefreshEvidenceReader,
)
from dpone.adapters.semantic_refresh_mssql_prerequisites import (
    MssqlSemanticRefreshPrerequisiteAuthority,
)
from dpone.adapters.semantic_refresh_mssql_protected_authority import (
    MssqlSemanticRefreshProtectedOperationState,
)
from dpone.adapters.semantic_refresh_mssql_publication import (
    MssqlSemanticRefreshPublicationState,
)
from dpone.adapters.semantic_refresh_mssql_resource_authority import (
    MssqlSemanticRefreshProtectedResourceLedger,
)
from dpone.adapters.semantic_refresh_mssql_seal import (
    MssqlSemanticRefreshSealAuthorizationStore,
)
from dpone.runtime.semantic_refresh_clickhouse_authority_state import (
    AuthorityBoundSemanticRefreshPublicationState,
)
from dpone.runtime.semantic_refresh_clickhouse_plan_factory import (
    ProtectedClickHousePreparePlanFactory,
)
from dpone.runtime.semantic_refresh_clickhouse_prepared import (
    AuthenticatedClickHousePreparedPublicationLoader,
)
from dpone.runtime.semantic_refresh_clickhouse_service import (
    ClickHousePublicationService,
)
from dpone.runtime.semantic_refresh_model_publication import (
    SemanticRefreshModelPublicationService,
)
from dpone.runtime.semantic_refresh_parquet_authority import (
    DponeParquetV1SealCodecAuthority,
)
from dpone.runtime.semantic_refresh_parquet_codec import DponeParquetV1Codec
from dpone.services.semantic_refresh_artifact_mssql import (
    MssqlCommittedAfterImageArtifactPublisher,
)
from dpone.services.semantic_refresh_mssql_authority import (
    SemanticRefreshMssqlProtectedAuthorityService,
)
from dpone.services.semantic_refresh_mssql_failure import SemanticRefreshMssqlOutcomeService
from dpone.services.semantic_refresh_seal_authorization import (
    SemanticRefreshSealAuthorizationIssuer,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_artifact_store import (
        SemanticRefreshArtifactStoreResolver,
    )
    from dpone.ports.semantic_refresh_clickhouse_connection import (
        ClickHouseClusterConnectionAuthority,
        SemanticRefreshClickHouseHttpClientPort,
    )
    from dpone.ports.semantic_refresh_seal_policy import (
        SemanticRefreshSealPolicyAuthorityPort,
    )


@dataclass(frozen=True, slots=True)
class SemanticRefreshPublicationRuntime:
    """Concrete protected services shared by Airflow model tasks."""

    models: SemanticRefreshModelPublicationService
    protected_operation: SemanticRefreshMssqlProtectedAuthorityService
    publication_authority: MssqlProtectedSemanticRefreshClickHouseAuthority
    publication_state: MssqlSemanticRefreshPublicationState
    seal_authorization: MssqlSemanticRefreshSealAuthorizationStore


def build_semantic_refresh_publication_runtime(
    *,
    mssql_connection_factory: Callable[[], Any],
    mssql_connection_authority_id: str,
    artifact_stores: SemanticRefreshArtifactStoreResolver,
    seal_policy: SemanticRefreshSealPolicyAuthorityPort,
    clickhouse_http_client: SemanticRefreshClickHouseHttpClientPort,
    clickhouse_connection_authority: ClickHouseClusterConnectionAuthority,
    now: Callable[[], datetime],
    control_schema: str = "dpone_control",
) -> SemanticRefreshPublicationRuntime:
    """Wire the sole production-capable seal/PREPARE/COMMIT dependency graph.

    Credentials and vendor clients are supplied explicitly by the deployment
    composition. The graph contains no static publication authority, callback
    evidence adapter, environment lookup, or Airflow/XCom state authority.
    """

    protected_state = MssqlSemanticRefreshProtectedOperationState(
        mssql_connection_factory,
        control_schema=control_schema,
    )
    protected_operation = SemanticRefreshMssqlProtectedAuthorityService(protected_state)
    canonical = MssqlSemanticRefreshCanonicalAuthorityLoader(
        mssql_connection_factory,
        control_schema=control_schema,
    )
    prerequisites = MssqlSemanticRefreshPrerequisiteAuthority(
        mssql_connection_factory,
        control_schema=control_schema,
    )
    evidence = MssqlSemanticRefreshEvidenceReader(
        mssql_connection_factory,
        control_schema=control_schema,
    )
    seal_authorization = MssqlSemanticRefreshSealAuthorizationStore(
        mssql_connection_factory,
        control_schema=control_schema,
    )
    parquet_codec = DponeParquetV1Codec()
    seal_issuer = SemanticRefreshSealAuthorizationIssuer(
        protected_operation=protected_operation,
        prerequisites=prerequisites,
        evidence=evidence,
        outcome=SemanticRefreshMssqlOutcomeService(),
        codec=DponeParquetV1SealCodecAuthority(parquet_codec),
        policy=seal_policy,
        store=seal_authorization,
        now=now,
    )
    artifact_publisher = MssqlCommittedAfterImageArtifactPublisher(
        protected_operation=protected_operation,
        prerequisites=prerequisites,
        seal_authorization=seal_authorization,
        after_image=MssqlSemanticRefreshAfterImageReader(
            mssql_connection_factory,
            mssql_connection_authority_id=mssql_connection_authority_id,
        ),
        artifact_stores=artifact_stores,
        codec=parquet_codec,
    )
    publication_authority = MssqlProtectedSemanticRefreshClickHouseAuthority(
        protected=protected_operation,
        canonical=canonical,
        prerequisites=prerequisites,
    )
    publication_state = MssqlSemanticRefreshPublicationState(
        mssql_connection_factory,
        control_schema=control_schema,
        require_protected_authority=True,
    )
    protected_publication_state = AuthorityBoundSemanticRefreshPublicationState(
        delegate=publication_state,
        authority=publication_authority,
    )
    gateway = ClickHouseHttpSemanticRefreshGateway(
        client=clickhouse_http_client,
        authority=publication_authority,
        artifact_reader=AuthorityBoundVersionPinnedSealedArtifactReader(
            protected_operation=protected_operation,
            artifact_stores=artifact_stores,
        ),
        seal_authorization=seal_authorization,
        connection_authority=clickhouse_connection_authority,
    )
    publication = ClickHousePublicationService(
        gateway=gateway,
        state=protected_publication_state,
        authority=publication_authority,
    )
    resources = MssqlSemanticRefreshProtectedResourceLedger(
        mssql_connection_factory,
        control_schema=control_schema,
        clock=now,
    )
    models = SemanticRefreshModelPublicationService(
        seal_issuer=seal_issuer,
        artifact_publisher=artifact_publisher,
        prepare_plan_factory=ProtectedClickHousePreparePlanFactory(
            authority=publication_authority,
            seal_authorization=seal_authorization,
        ),
        prepared_loader=AuthenticatedClickHousePreparedPublicationLoader(
            durable=publication_state,
            authority=publication_authority,
            seal_authorization=seal_authorization,
        ),
        publication_authority=publication_authority,
        publication=publication,
        resources=resources,
    )
    return SemanticRefreshPublicationRuntime(
        models=models,
        protected_operation=protected_operation,
        publication_authority=publication_authority,
        publication_state=publication_state,
        seal_authorization=seal_authorization,
    )


__all__ = [
    "SemanticRefreshPublicationRuntime",
    "build_semantic_refresh_publication_runtime",
]

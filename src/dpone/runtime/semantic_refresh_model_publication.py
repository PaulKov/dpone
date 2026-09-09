"""Runtime orchestration for one protected semantic-refresh publication."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from dpone.runtime.semantic_refresh_clickhouse_plan_factory import (
    ClickHouseHeadPublicationPlanFactory,
    ProtectedClickHousePreparePlanFactory,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_artifact_seal import SealedArtifactReceipt
    from dpone.ports.semantic_refresh_clickhouse_authority import (
        SemanticRefreshClickHousePublicationAuthorityPort,
    )
    from dpone.ports.semantic_refresh_clickhouse_resources import (
        ClickHouseFailedScratchCleanupReceipt,
        SemanticRefreshPublicationResourcePort,
    )
    from dpone.ports.semantic_refresh_mssql_cleanup_ack import (
        MssqlFailedPrecommitCleanupAck,
        SemanticRefreshMssqlFailedPrecommitCleanupAckPort,
    )
    from dpone.ports.semantic_refresh_mssql_replacement import (
        MssqlFailedScratchCleanupAuthority,
    )
    from dpone.ports.semantic_refresh_mssql_resources import (
        SemanticRefreshMssqlReleasedResourceClosurePort,
    )
    from dpone.runtime.semantic_refresh_clickhouse_models import (
        ClickHousePreparedReceipt,
        ClickHousePublicationReceipt,
    )
    from dpone.runtime.semantic_refresh_clickhouse_prepared import (
        AuthenticatedClickHousePreparedPublicationLoader,
    )
    from dpone.runtime.semantic_refresh_clickhouse_service import (
        ClickHousePublicationService,
    )


class _SealAuthorizationIssuer(Protocol):
    def issue(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> object: ...


class _ArtifactPublisher(Protocol):
    def seal(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> SealedArtifactReceipt: ...


class _FailedScratchAuthority(Protocol):
    def load(
        self,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlFailedScratchCleanupAuthority: ...


class _FailedScratchCleaner(Protocol):
    def cleanup(
        self,
        authority: MssqlFailedScratchCleanupAuthority,
    ) -> ClickHouseFailedScratchCleanupReceipt: ...


class SemanticRefreshModelPublicationService:
    """Seal, prepare, and commit one model through protected durable authorities.

    Airflow may invoke :meth:`prepare` and :meth:`commit` in different tasks.
    Their handoff is the durable SQL Server PREPARED document loaded by
    ``prepared_loader``; task return values and XCom are never commit authority.
    """

    def __init__(
        self,
        *,
        seal_issuer: _SealAuthorizationIssuer,
        artifact_publisher: _ArtifactPublisher,
        prepare_plan_factory: ProtectedClickHousePreparePlanFactory,
        prepared_loader: AuthenticatedClickHousePreparedPublicationLoader,
        publication_authority: SemanticRefreshClickHousePublicationAuthorityPort,
        publication: ClickHousePublicationService,
        resources: SemanticRefreshPublicationResourcePort,
        head_plan_factory: ClickHouseHeadPublicationPlanFactory | None = None,
    ) -> None:
        self._seal_issuer = seal_issuer
        self._artifact_publisher = artifact_publisher
        self._prepare_plan_factory = prepare_plan_factory
        self._prepared_loader = prepared_loader
        self._publication_authority = publication_authority
        self._publication = publication
        self._resources = resources
        self._head_plan_factory = head_plan_factory or ClickHouseHeadPublicationPlanFactory()

    def prepare(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> ClickHousePreparedReceipt:
        """Issue exact seal authority, seal committed bytes, and persist PREPARED."""

        prepared = self._prepared_loader.load_if_prepared(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )
        if prepared is not None:
            return prepared.receipt

        self._resources.reserve_operation(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )
        self._seal_issuer.issue(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )
        artifact = self._artifact_publisher.seal(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )
        plan = self._prepare_plan_factory.build(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
            sealed_artifact=artifact,
        )
        return self._publication.prepare(plan)

    def commit(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> ClickHousePublicationReceipt:
        """Load exact durable PREPARED authority and reconcile one commit."""

        prepared = self._prepared_loader.load(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )
        authority = self._publication_authority.load(
            workflow_execution_binding_sha256,
            operation_id,
        )
        heads = self._head_plan_factory.build(authority, prepared.receipt)
        receipt = self._publication.commit(
            prepared.plan,
            prepared=prepared.receipt,
            heads=heads,
        )
        self._resources.release_operation(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )
        return receipt


class SemanticRefreshFailedPrecommitCleanupService:
    """Clean exact FAILED_PRE_COMMIT scratch, then release its aggregate budget."""

    def __init__(
        self,
        *,
        authority: _FailedScratchAuthority,
        cleaner: _FailedScratchCleaner,
        resources: SemanticRefreshPublicationResourcePort,
        released_resources: SemanticRefreshMssqlReleasedResourceClosurePort,
        cleanup_ack: SemanticRefreshMssqlFailedPrecommitCleanupAckPort,
    ) -> None:
        self._authority = authority
        self._cleaner = cleaner
        self._resources = resources
        self._released_resources = released_resources
        self._cleanup_ack = cleanup_ack

    def cleanup(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlFailedPrecommitCleanupAck:
        """Persist and return the exact cleanup ack after both typed proofs."""

        authority = self._authority.load(workflow_execution_binding_sha256, operation_id)
        receipt = self._cleaner.cleanup(authority)
        self._resources.release_operation(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )
        closure = self._released_resources.assert_released(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )
        return self._cleanup_ack.persist_exact(scratch=receipt, resources=closure)


__all__ = [
    "SemanticRefreshFailedPrecommitCleanupService",
    "SemanticRefreshModelPublicationService",
]

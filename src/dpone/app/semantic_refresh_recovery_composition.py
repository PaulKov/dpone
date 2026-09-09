"""Composition root for semantic-refresh failure recovery and successor admission."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from dpone.adapters.semantic_refresh_clickhouse_cleanup import (
    ClickHouseFailedPrecommitScratchCleaner,
)
from dpone.adapters.semantic_refresh_clickhouse_connection import (
    ClickHouseConnectionAuthorityVerifier,
    ClickHouseEndpointClient,
)
from dpone.adapters.semantic_refresh_mssql_authority import MssqlSemanticRefreshCanonicalAuthorityLoader
from dpone.adapters.semantic_refresh_mssql_cleanup_ack import (
    MssqlSemanticRefreshFailedPrecommitCleanupAckStore,
)
from dpone.adapters.semantic_refresh_mssql_evidence import MssqlSemanticRefreshEvidenceReader
from dpone.adapters.semantic_refresh_mssql_failure import MssqlSemanticRefreshWorkflowFailureState
from dpone.adapters.semantic_refresh_mssql_recovery_heads import MssqlSemanticRefreshRecoveryHeadReader
from dpone.adapters.semantic_refresh_mssql_replacement import MssqlSemanticRefreshPredecessorStateReader
from dpone.adapters.semantic_refresh_mssql_resource_authority import (
    MssqlSemanticRefreshProtectedResourceLedger,
)
from dpone.adapters.semantic_refresh_mssql_scratch_cleanup import (
    MssqlSemanticRefreshFailedScratchCleanupReader,
)
from dpone.adapters.semantic_refresh_mssql_state import MssqlSemanticRefreshStateAdapter
from dpone.app.semantic_refresh_recovery_authority import SemanticRefreshMssqlRecoveryAuthorityVerifier
from dpone.contracts.semantic_refresh_types import SqlServerModelOutcome
from dpone.runtime.semantic_refresh_model_publication import (
    SemanticRefreshFailedPrecommitCleanupService,
)
from dpone.services.semantic_refresh_mssql_authority import SemanticRefreshMssqlAuthorityAdmissionService
from dpone.services.semantic_refresh_mssql_failure import SemanticRefreshMssqlFailureTerminalizationService
from dpone.services.semantic_refresh_mssql_recovery_heads import SemanticRefreshMssqlRecoveryHeadService
from dpone.services.semantic_refresh_mssql_replacement import (
    SemanticRefreshMssqlFailedScratchCleanupService,
    SemanticRefreshMssqlReplacementService,
)

if TYPE_CHECKING:
    from dpone.contracts.semantic_refresh_failure_summary import SemanticRefreshFailedWorkflowSummary
    from dpone.contracts.semantic_refresh_plan_refs import ReplacementActionBinding
    from dpone.ports.semantic_refresh_clickhouse_connection import (
        ClickHouseClusterConnectionAuthority,
    )
    from dpone.ports.semantic_refresh_mssql import MssqlAdmissionReceipt
    from dpone.ports.semantic_refresh_mssql_cleanup_ack import (
        MssqlFailedPrecommitCleanupAck,
    )


@dataclass(frozen=True, slots=True)
class SemanticRefreshRecoveryDecision:
    """Evidence-derived predecessor summary and its only safe action map."""

    summary: SemanticRefreshFailedWorkflowSummary
    replacement_actions: tuple[ReplacementActionBinding, ...]


@dataclass(frozen=True, slots=True)
class SemanticRefreshRecoveryRuntime:
    """Supported controller boundary for failed-precommit recovery."""

    failure: SemanticRefreshMssqlFailureTerminalizationService
    replacement: SemanticRefreshMssqlReplacementService
    predecessor: MssqlSemanticRefreshPredecessorStateReader
    admission: SemanticRefreshMssqlAuthorityAdmissionService
    plan_verifier: SemanticRefreshMssqlRecoveryAuthorityVerifier

    def reconcile(self, workflow_execution_id: str) -> SemanticRefreshRecoveryDecision:
        """Persist exact outcomes and return a caller-immutable recovery decision."""

        summary = self.failure.terminalize(workflow_execution_id)
        predecessor = self.predecessor.load_predecessor(workflow_execution_id)
        if (
            predecessor.workflow_execution_binding_sha256 != summary.workflow_execution_binding_sha256
            or predecessor.terminal_summary_sha256 != summary.terminal_summary_sha256
        ):
            raise ValueError("reconciled predecessor differs from the durable failure summary")
        outcomes: dict[str, SqlServerModelOutcome] = {}
        for model in predecessor.models:
            if model.model_unique_id in outcomes:
                raise ValueError("predecessor journal contains a duplicate model identity")
            try:
                outcomes[model.model_unique_id] = SqlServerModelOutcome(model.mssql_outcome)
            except ValueError as exc:
                raise ValueError("predecessor journal outcome is unsupported") from exc
        return SemanticRefreshRecoveryDecision(
            summary=summary,
            replacement_actions=self.replacement.propose(outcomes),
        )

    def admit_successor(self, workflow_execution_binding_sha256: str) -> MssqlAdmissionReceipt:
        """Apply one protected successor whose replacement plan is already authenticated."""

        return self.admission.admit(workflow_execution_binding_sha256)


@dataclass(frozen=True, slots=True)
class SemanticRefreshFailedScratchCleanupRuntime:
    """Explicit operator boundary for authenticated FAILED_PRE_COMMIT cleanup."""

    service: SemanticRefreshFailedPrecommitCleanupService

    def cleanup(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlFailedPrecommitCleanupAck:
        """Persist exact scratch-absence and released-allocation proof."""

        return self.service.cleanup(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )


def build_semantic_refresh_recovery_runtime(
    *,
    mssql_connection_factory: Callable[[], Any],
    control_schema: str = "dpone_control",
) -> SemanticRefreshRecoveryRuntime:
    """Wire evidence reconciliation, replacement policy and successor CAS."""

    failure_state = MssqlSemanticRefreshWorkflowFailureState(
        mssql_connection_factory,
        control_schema=control_schema,
    )
    evidence = MssqlSemanticRefreshEvidenceReader(
        mssql_connection_factory,
        control_schema=control_schema,
    )
    predecessor = MssqlSemanticRefreshPredecessorStateReader(
        mssql_connection_factory,
        control_schema=control_schema,
    )
    canonical_authority = MssqlSemanticRefreshCanonicalAuthorityLoader(
        mssql_connection_factory,
        control_schema=control_schema,
    )
    replacement = SemanticRefreshMssqlReplacementService(predecessor)
    return SemanticRefreshRecoveryRuntime(
        failure=SemanticRefreshMssqlFailureTerminalizationService(
            contexts=failure_state,
            evidence=evidence,
            state=failure_state,
        ),
        replacement=replacement,
        predecessor=predecessor,
        admission=SemanticRefreshMssqlAuthorityAdmissionService(
            authority=canonical_authority,
            admission=MssqlSemanticRefreshStateAdapter(
                mssql_connection_factory,
                control_schema=control_schema,
            ),
            replacement=replacement,
        ),
        plan_verifier=SemanticRefreshMssqlRecoveryAuthorityVerifier(
            SemanticRefreshMssqlRecoveryHeadService(
                authority=canonical_authority,
                state=MssqlSemanticRefreshRecoveryHeadReader(
                    mssql_connection_factory,
                    control_schema=control_schema,
                ),
            ),
            failed_authority=canonical_authority,
            failed_state=predecessor,
        ),
    )


def build_semantic_refresh_failed_scratch_cleanup_runtime(
    *,
    mssql_connection_factory: Callable[[], Any],
    clickhouse_http_client: ClickHouseEndpointClient,
    clickhouse_connection_authority: ClickHouseClusterConnectionAuthority,
    now: Callable[[], datetime],
    control_schema: str = "dpone_control",
) -> SemanticRefreshFailedScratchCleanupRuntime:
    """Wire the only supported ClickHouse cleanup path for FAILED_PRE_COMMIT."""

    resources = MssqlSemanticRefreshProtectedResourceLedger(
        mssql_connection_factory,
        control_schema=control_schema,
        clock=now,
    )
    cleanup_ack = MssqlSemanticRefreshFailedPrecommitCleanupAckStore(
        mssql_connection_factory,
        control_schema=control_schema,
    )
    return SemanticRefreshFailedScratchCleanupRuntime(
        service=SemanticRefreshFailedPrecommitCleanupService(
            authority=SemanticRefreshMssqlFailedScratchCleanupService(
                MssqlSemanticRefreshFailedScratchCleanupReader(
                    mssql_connection_factory,
                    control_schema=control_schema,
                )
            ),
            cleaner=ClickHouseFailedPrecommitScratchCleaner(
                client=clickhouse_http_client,
                connection=ClickHouseConnectionAuthorityVerifier(
                    client=clickhouse_http_client,
                    authority=clickhouse_connection_authority,
                ),
            ),
            resources=resources,
            released_resources=resources,
            cleanup_ack=cleanup_ack,
        ),
    )


__all__ = [
    "SemanticRefreshFailedScratchCleanupRuntime",
    "SemanticRefreshRecoveryDecision",
    "SemanticRefreshRecoveryRuntime",
    "build_semantic_refresh_failed_scratch_cleanup_runtime",
    "build_semantic_refresh_recovery_runtime",
]

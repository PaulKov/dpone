"""Composition root for semantic-refresh deployment and run activation."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from dpone.adapters.semantic_refresh_mssql_activation import MssqlSemanticRefreshActivationStore
from dpone.adapters.semantic_refresh_mssql_activation_authority import (
    MssqlSemanticRefreshActivationAuthorityStore,
)
from dpone.ports.semantic_refresh_production_activation import (
    SemanticRefreshProductionActivationGuardPort,
    SemanticRefreshProductionActivationUnavailableError,
    UnavailableSemanticRefreshProductionActivationGuard,
)
from dpone.services.dbt_semantic_refresh_activation import (
    SemanticRefreshDeploymentAuthorityService,
)
from dpone.services.semantic_refresh_mssql_activation import SemanticRefreshMssqlActivationService

if TYPE_CHECKING:
    from dpone.contracts.dbt_semantic_refresh_activation import (
        SemanticRefreshActivationAuthorityReceipt,
        SemanticRefreshActivationAuthoritySet,
        SemanticRefreshReleaseTemplateVerifierPort,
    )
    from dpone.contracts.dbt_semantic_refresh_plan_contracts import (
        SemanticRefreshDeploymentAuthoritySubject,
        SemanticRefreshPlanBundle,
        SemanticRefreshReleaseDeploymentVerifierPort,
    )
    from dpone.contracts.semantic_refresh_route_certification import (
        SemanticRefreshRouteLiveCertificationReceipt,
    )
    from dpone.contracts.semantic_refresh_runtime_assurance import (
        SemanticRefreshRuntimeAssuranceReceipt,
    )


@dataclass(frozen=True, slots=True)
class SemanticRefreshActivationRuntime:
    """Preview-only planner; production persistence stays fail-closed in 0.74."""

    deployment: SemanticRefreshDeploymentAuthorityService
    mssql: SemanticRefreshMssqlActivationService
    activation_guard: SemanticRefreshProductionActivationGuardPort = field(
        default_factory=UnavailableSemanticRefreshProductionActivationGuard,
        compare=False,
        repr=False,
    )

    def persist_deployment_authorities(
        self,
        *,
        template_pack: Mapping[str, Any],
        deployment_subject: SemanticRefreshDeploymentAuthoritySubject,
        plan_bundle: SemanticRefreshPlanBundle,
        route_certification: SemanticRefreshRouteLiveCertificationReceipt,
        runtime_assurances: tuple[SemanticRefreshRuntimeAssuranceReceipt, ...],
        persisted_at: str,
    ) -> SemanticRefreshActivationAuthorityReceipt:
        """Reject production activation before any authority or target mutation."""

        self.activation_guard.authorize(subject=deployment_subject, plan_bundle=plan_bundle)
        receipt = self.deployment.persist_deployment_authorities(
            template_pack=template_pack,
            plan_bundle=plan_bundle,
            route_certification=route_certification,
            runtime_assurances=runtime_assurances,
            persisted_at=persisted_at,
        )
        self.mssql.activate_deployment(
            subject=deployment_subject,
            plan_bundle=plan_bundle,
        )
        return receipt

    def plan_deployment_authorities(
        self,
        *,
        template_pack: Mapping[str, Any],
        plan_bundle: SemanticRefreshPlanBundle,
        route_certification: SemanticRefreshRouteLiveCertificationReceipt,
        runtime_assurances: tuple[SemanticRefreshRuntimeAssuranceReceipt, ...],
        persisted_at: str,
    ) -> SemanticRefreshActivationAuthoritySet:
        """Freeze retry-stable authority bytes without activating physical state."""

        return self.deployment.plan_deployment_authorities(
            template_pack=template_pack,
            plan_bundle=plan_bundle,
            route_certification=route_certification,
            runtime_assurances=runtime_assurances,
            persisted_at=persisted_at,
        )

    def persist_planned_deployment_authorities(
        self,
        *,
        template_pack: Mapping[str, Any],
        plan_bundle: SemanticRefreshPlanBundle,
        authority: SemanticRefreshActivationAuthoritySet,
    ) -> SemanticRefreshActivationAuthorityReceipt:
        """Reject durable authority persistence while production stays unavailable."""

        self.activation_guard.authorize(
            subject=plan_bundle,
            plan_bundle=plan_bundle,
        )
        return self.deployment.persist_planned_deployment_authorities(
            template_pack=template_pack,
            plan_bundle=plan_bundle,
            authority=authority,
        )


def build_semantic_refresh_activation_runtime(
    *,
    mssql_connection_factory: Callable[[], Any],
    release_deployment_verifier: SemanticRefreshReleaseDeploymentVerifierPort,
    release_template_verifier: SemanticRefreshReleaseTemplateVerifierPort,
    authority_store_ref: str,
    control_schema: str = "dpone_control",
) -> SemanticRefreshActivationRuntime:
    """Wire full typed authority persistence and initial durable control rows."""

    activation_store = MssqlSemanticRefreshActivationStore(
        mssql_connection_factory,
        control_schema=control_schema,
    )
    activation_guard = UnavailableSemanticRefreshProductionActivationGuard()
    return SemanticRefreshActivationRuntime(
        deployment=SemanticRefreshDeploymentAuthorityService(
            MssqlSemanticRefreshActivationAuthorityStore(
                mssql_connection_factory,
                authority_store_ref=authority_store_ref,
                control_schema=control_schema,
            ),
            release_template_verifier,
            persistence_guard=activation_guard,
        ),
        mssql=SemanticRefreshMssqlActivationService(
            activation=activation_store,
            deployment_verifier=release_deployment_verifier,
            activation_guard=activation_guard,
        ),
        activation_guard=activation_guard,
    )


__all__ = [
    "SemanticRefreshActivationRuntime",
    "SemanticRefreshProductionActivationUnavailableError",
    "build_semantic_refresh_activation_runtime",
]

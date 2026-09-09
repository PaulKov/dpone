"""Persist complete protected authority before activating a semantic-refresh pack."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from dpone.contracts.dbt_semantic_refresh_activation import (
    SemanticRefreshActivationAuthorityReceipt,
    SemanticRefreshActivationAuthoritySet,
    SemanticRefreshActivationAuthorityStorePort,
    SemanticRefreshDeploymentAuthorityLoaderPort,
    SemanticRefreshReleaseTemplateSubject,
    SemanticRefreshReleaseTemplateVerifierPort,
)
from dpone.ports.semantic_refresh_production_activation import (
    SemanticRefreshProductionActivationGuardPort,
    UnavailableSemanticRefreshProductionActivationGuard,
)
from dpone.readiness.dbt_semantic_refresh_airflow_pack import (
    validate_semantic_refresh_deployment_readiness,
)

if TYPE_CHECKING:
    from dpone.contracts.dbt_semantic_refresh_plan_contracts import SemanticRefreshPlanBundle
    from dpone.contracts.semantic_refresh_route_certification import (
        SemanticRefreshRouteLiveCertificationReceipt,
    )
    from dpone.contracts.semantic_refresh_runtime_assurance import (
        SemanticRefreshRuntimeAssuranceReceipt,
    )


class SemanticRefreshDeploymentAuthorityService:
    """Persist exact deployment authorities before any logical DagRun exists."""

    def __init__(
        self,
        store: SemanticRefreshActivationAuthorityStorePort,
        template_verifier: SemanticRefreshReleaseTemplateVerifierPort,
        persistence_guard: SemanticRefreshProductionActivationGuardPort | None = None,
    ) -> None:
        self._store = store
        self._template_verifier = template_verifier
        self._persistence_guard = persistence_guard or UnavailableSemanticRefreshProductionActivationGuard()

    def persist_deployment_authorities(
        self,
        *,
        template_pack: Mapping[str, Any],
        plan_bundle: SemanticRefreshPlanBundle,
        route_certification: SemanticRefreshRouteLiveCertificationReceipt,
        runtime_assurances: tuple[SemanticRefreshRuntimeAssuranceReceipt, ...],
        persisted_at: str,
    ) -> SemanticRefreshActivationAuthorityReceipt:
        """Compatibility wrapper that plans and immediately persists authority."""

        self._authorize_persistence(plan_bundle)
        planned = self.plan_deployment_authorities(
            template_pack=template_pack,
            plan_bundle=plan_bundle,
            route_certification=route_certification,
            runtime_assurances=runtime_assurances,
            persisted_at=persisted_at,
        )
        return self._persist_authority(plan_bundle, planned)

    def plan_deployment_authorities(
        self,
        *,
        template_pack: Mapping[str, Any],
        plan_bundle: SemanticRefreshPlanBundle,
        route_certification: SemanticRefreshRouteLiveCertificationReceipt,
        runtime_assurances: tuple[SemanticRefreshRuntimeAssuranceReceipt, ...],
        persisted_at: str,
    ) -> SemanticRefreshActivationAuthoritySet:
        """Freeze one retry-stable full authority set before create-only persistence."""

        authority = plan_bundle.release_deployment_authority
        self._require_release_template(template_pack, plan_bundle)
        authority_set = SemanticRefreshActivationAuthoritySet(
            release_id=authority.release_id,
            deployment_id=authority.deployment_id,
            plan_bundle_sha256=plan_bundle.plan_bundle_sha256,
            baselines=tuple(item.baseline_receipt for item in plan_bundle.targets),
            route_certification=route_certification,
            runtime_assurances=runtime_assurances,
            persisted_at=persisted_at,
        )
        _validate_authority_set(plan_bundle, authority_set)
        return authority_set

    def persist_planned_deployment_authorities(
        self,
        *,
        template_pack: Mapping[str, Any],
        plan_bundle: SemanticRefreshPlanBundle,
        authority: SemanticRefreshActivationAuthoritySet,
    ) -> SemanticRefreshActivationAuthorityReceipt:
        """Persist/replay the same frozen plan without sampling a new timestamp."""

        self._authorize_persistence(plan_bundle)
        if not isinstance(authority, SemanticRefreshActivationAuthoritySet):
            raise TypeError("authority must be a typed activation authority set")
        self._require_release_template(template_pack, plan_bundle)
        return self._persist_authority(plan_bundle, authority)

    def _persist_authority(
        self,
        plan_bundle: SemanticRefreshPlanBundle,
        authority: SemanticRefreshActivationAuthoritySet,
    ) -> SemanticRefreshActivationAuthorityReceipt:
        _validate_authority_set(plan_bundle, authority)
        receipt = self._store.persist_exact(authority)
        _validate_store_receipt(authority, receipt)
        return receipt

    def _require_release_template(
        self,
        template_pack: Mapping[str, Any],
        plan_bundle: SemanticRefreshPlanBundle,
    ) -> None:
        authority = plan_bundle.release_deployment_authority
        validate_semantic_refresh_deployment_readiness(
            template_pack=template_pack,
            plan_bundle=plan_bundle,
        )
        subject = _template_subject(template_pack, release_id=authority.release_id)
        if self._template_verifier.verify(subject) is not True:
            raise ValueError("semantic-refresh template is not protected by the exact release")

    def _authorize_persistence(self, plan_bundle: SemanticRefreshPlanBundle) -> None:
        self._persistence_guard.authorize(
            subject=plan_bundle.release_deployment_authority,
            plan_bundle=plan_bundle,
        )


def load_exact_deployment_authority_receipt(
    loader: SemanticRefreshDeploymentAuthorityLoaderPort,
    *,
    plan_bundle: SemanticRefreshPlanBundle,
) -> SemanticRefreshActivationAuthorityReceipt:
    """Resolve and revalidate the deployment receipt before worker admission."""

    authority = plan_bundle.release_deployment_authority
    receipt = loader.load_exact(
        release_id=authority.release_id,
        deployment_id=authority.deployment_id,
        plan_bundle_sha256=plan_bundle.plan_bundle_sha256,
    )
    if not isinstance(receipt, SemanticRefreshActivationAuthorityReceipt):
        raise TypeError("deployment authority loader must return a typed receipt")
    expected_baselines = tuple(
        sorted((target.model_unique_id, target.baseline_receipt_sha256) for target in plan_bundle.targets)
    )
    route_receipts = {target.route_certification_receipt_sha256 for target in plan_bundle.targets}
    if (
        receipt.release_id != authority.release_id
        or receipt.deployment_id != authority.deployment_id
        or receipt.plan_bundle_sha256 != plan_bundle.plan_bundle_sha256
        or receipt.baseline_receipts != expected_baselines
        or route_receipts != {receipt.route_certification_receipt_sha256}
        or receipt.runtime_assurance_receipts != _plan_assurance_receipts(plan_bundle)
    ):
        raise ValueError("persisted deployment authority differs from the exact plan")
    return receipt


def _validate_authority_set(
    plan: SemanticRefreshPlanBundle,
    authority: SemanticRefreshActivationAuthoritySet,
) -> None:
    release_deployment = plan.release_deployment_authority
    targets = {item.model_unique_id: item for item in plan.targets}
    baselines = {item.model_unique_id: item for item in authority.baselines}
    expected_assurances = _plan_assurance_receipts(plan)
    if (
        authority.release_id != release_deployment.release_id
        or authority.deployment_id != release_deployment.deployment_id
        or authority.plan_bundle_sha256 != plan.plan_bundle_sha256
        or set(targets) != set(baselines)
        or any(baselines[model] != target.baseline_receipt for model, target in targets.items())
        or {item.route_certification_receipt_sha256 for item in plan.targets}
        != {authority.route_certification.route_certification_receipt_sha256}
        or authority.runtime_assurance_receipts != expected_assurances
    ):
        raise ValueError("full protected authority differs from the exact semantic-refresh plan")


def _validate_store_receipt(
    authority: SemanticRefreshActivationAuthoritySet,
    receipt: SemanticRefreshActivationAuthorityReceipt,
) -> None:
    if not isinstance(receipt, SemanticRefreshActivationAuthorityReceipt) or (
        receipt.release_id,
        receipt.deployment_id,
        receipt.plan_bundle_sha256,
        receipt.baseline_receipts,
        receipt.route_certification_receipt_sha256,
        receipt.runtime_assurance_receipts,
        receipt.persisted_at,
    ) != (
        authority.release_id,
        authority.deployment_id,
        authority.plan_bundle_sha256,
        authority.baseline_receipts,
        authority.route_certification.route_certification_receipt_sha256,
        authority.runtime_assurance_receipts,
        authority.persisted_at,
    ):
        raise ValueError("protected authority store acknowledgement differs")


def _plan_assurance_receipts(
    plan: SemanticRefreshPlanBundle,
) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        sorted(
            (target.model_unique_id, kind, digest)
            for target in plan.targets
            for kind, digest in (
                ("ddl_freeze", target.ddl_freeze_assurance_receipt_sha256),
                ("utc_semantics", target.utc_semantics_assurance_receipt_sha256),
                (
                    "writer_exclusivity",
                    target.writer_exclusivity_assurance_receipt_sha256,
                ),
            )
            if digest is not None
        )
    )


def _template_subject(
    template_pack: Mapping[str, Any],
    *,
    release_id: str,
) -> SemanticRefreshReleaseTemplateSubject:
    semantic = template_pack.get("semantic_refresh")
    if not isinstance(semantic, Mapping):
        raise ValueError("semantic-refresh template authority is absent")
    return SemanticRefreshReleaseTemplateSubject(
        release_id=release_id,
        template_pack_fingerprint=str(template_pack.get("pack_fingerprint") or ""),
        pre_release_bundle_sha256=str(semantic.get("pre_release_bundle_sha256") or ""),
        package_artifacts_sha256=str(semantic.get("package_artifacts_sha256") or ""),
    )


__all__ = [
    "SemanticRefreshDeploymentAuthorityService",
    "load_exact_deployment_authority_receipt",
]

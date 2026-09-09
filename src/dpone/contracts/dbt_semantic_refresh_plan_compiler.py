"""Post-release/deployment canonical semantic-refresh plan compiler."""

from __future__ import annotations

from datetime import datetime

from dpone.contracts.dbt_semantic_refresh_activation import (
    SemanticRefreshProtectedAssuranceVerifierPort,
)
from dpone.contracts.dbt_semantic_refresh_plan_assurance import (
    deployment_model_index,
    runtime_assurance_index,
    validate_baseline_authority,
    validate_model_assurances,
    validate_route_certification,
)
from dpone.contracts.dbt_semantic_refresh_plan_contracts import (
    SemanticRefreshDeploymentAuthoritySubject,
    SemanticRefreshDeploymentModelAuthority,
    SemanticRefreshPlanBundle,
    SemanticRefreshPreReleaseModelInput,
    SemanticRefreshPreReleaseProofBundle,
    SemanticRefreshReleaseDeploymentAuthority,
    SemanticRefreshReleaseDeploymentVerifierPort,
    plan_payload,
)
from dpone.contracts.dbt_semantic_refresh_plan_recovery import (
    SemanticRefreshPlanModeContext,
    semantic_refresh_plan_mode_context,
    semantic_refresh_replacement_plan,
)
from dpone.contracts.dbt_semantic_refresh_plan_target import plan_target as _plan_target
from dpone.contracts.dbt_semantic_refresh_recovery_authority import (
    SemanticRefreshPlanRecoveryAuthority,
    SemanticRefreshPlanRecoveryVerifierPort,
)
from dpone.contracts.dbt_semantic_refresh_run_guard import SemanticRefreshRunGuardClosure
from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_daily_scope,
    require_text,
    semantic_refresh_sha256,
)
from dpone.contracts.semantic_refresh_operation_plan import SemanticRefreshOperationPlan
from dpone.contracts.semantic_refresh_plan_refs import OperationPlanReference
from dpone.contracts.semantic_refresh_route_certification import (
    SemanticRefreshRouteLiveCertificationReceipt,
)
from dpone.contracts.semantic_refresh_runtime_assurance import (
    RuntimeAssuranceKind,
    SemanticRefreshRuntimeAssuranceReceipt,
)
from dpone.contracts.semantic_refresh_types import EffectiveKeyColumn
from dpone.contracts.semantic_refresh_workflow_plan import SemanticRefreshWorkflowPlan


class SemanticRefreshPostDeploymentPlanCompiler:
    """Create final plans only after protected deployment and receipt verification."""

    def __init__(
        self,
        verifier: SemanticRefreshReleaseDeploymentVerifierPort,
        assurance_verifier: SemanticRefreshProtectedAssuranceVerifierPort,
        recovery_verifier: SemanticRefreshPlanRecoveryVerifierPort | None = None,
    ) -> None:
        self._verifier = verifier
        self._assurance_verifier = assurance_verifier
        self._recovery_verifier = recovery_verifier

    def compile(
        self,
        *,
        pre_release: SemanticRefreshPreReleaseProofBundle,
        authority: SemanticRefreshReleaseDeploymentAuthority,
        workflow_id: str,
        scope_start: str,
        scope_end: str,
        deployment_models: tuple[SemanticRefreshDeploymentModelAuthority, ...],
        route_certification: SemanticRefreshRouteLiveCertificationReceipt,
        runtime_assurances: tuple[SemanticRefreshRuntimeAssuranceReceipt, ...],
        verification_time: datetime,
        recovery_authority: SemanticRefreshPlanRecoveryAuthority | None = None,
    ) -> SemanticRefreshPlanBundle:
        require_text(workflow_id, "workflow_id")
        require_daily_scope(scope_start, scope_end)
        if verification_time.tzinfo is None or verification_time.utcoffset() is None:
            raise SemanticRefreshContractError("verification_time must be timezone-aware")
        dynamic = deployment_model_index(deployment_models, pre_release.models)
        for deployment in dynamic.values():
            validate_baseline_authority(authority, deployment)
        subject = SemanticRefreshDeploymentAuthoritySubject.build(authority, tuple(dynamic.values()))
        if self._verifier.verify(subject) is not True:
            raise SemanticRefreshContractError("release/deployment/model authority is not protected and exact")
        validate_route_certification(
            pre_release,
            route_certification,
            verification_time,
            self._assurance_verifier,
        )
        assurances = runtime_assurance_index(runtime_assurances, pre_release.models)
        model_ids = tuple(item.model_unique_id for item in pre_release.models)
        mode = semantic_refresh_plan_mode_context(
            recovery=recovery_authority,
            verifier=self._recovery_verifier,
            pre_release_bundle_sha256=pre_release.pre_release_bundle_sha256,
            package_artifacts_sha256=pre_release.lifecycle_policy.package_artifacts_digest,
            release_id=authority.release_id,
            deployment_id=authority.deployment_id,
            model_unique_ids=model_ids,
            scope_start=scope_start,
            scope_end=scope_end,
        )
        final_keys: dict[str, tuple[EffectiveKeyColumn, ...]] = {}
        for model in pre_release.models:
            deployment = dynamic[model.model_unique_id]
            final_keys[model.model_unique_id] = validate_model_assurances(
                pre_release=pre_release,
                authority=authority,
                model=model,
                deployment=deployment,
                assurances=assurances,
                verification_time=verification_time,
                verifier=self._assurance_verifier,
            )
        operations = tuple(
            _operation_plan(
                pre_release,
                authority,
                workflow_id,
                scope_start,
                scope_end,
                model,
                dynamic[model.model_unique_id],
                final_keys[model.model_unique_id],
                assurances[(model.model_unique_id, RuntimeAssuranceKind.WRITER_EXCLUSIVITY)],
                mode,
            )
            for model in pre_release.models
        )
        workflow = _workflow_plan(pre_release, scope_start, scope_end, operations, mode)
        replacement = semantic_refresh_replacement_plan(
            workflow_plan_sha256=workflow.workflow_plan_sha256,
            model_unique_ids=model_ids,
            context=mode,
        )
        models = {item.model_unique_id: item for item in pre_release.models}
        targets = tuple(
            _plan_target(
                operation,
                models[operation.model_unique_id],
                dynamic[operation.model_unique_id],
                scope_start,
                scope_end,
                pre_release,
                route_certification,
                assurances,
                mode,
            )
            for operation in operations
        )
        run_guards = SemanticRefreshRunGuardClosure.build(
            deployment_id=authority.deployment_id,
            workflow_name=workflow.workflow_name,
            resource_guard_ids=tuple(item.target_resource_id for item in targets),
        )
        payload = plan_payload(
            pre_release.pre_release_bundle_sha256,
            pre_release.lifecycle_policy.package_artifacts_digest,
            authority,
            operations,
            workflow,
            targets,
            run_guards,
            replacement,
        )
        return SemanticRefreshPlanBundle(
            pre_release_bundle_sha256=pre_release.pre_release_bundle_sha256,
            package_artifacts_sha256=pre_release.lifecycle_policy.package_artifacts_digest,
            release_deployment_authority=authority,
            operation_plans=operations,
            workflow_plan=workflow,
            targets=targets,
            run_guard_closure=run_guards,
            plan_bundle_sha256=semantic_refresh_sha256(payload),
            workflow_replacement_plan=replacement,
        )


def _workflow_plan(
    pre_release: SemanticRefreshPreReleaseProofBundle,
    scope_start: str,
    scope_end: str,
    operations: tuple[SemanticRefreshOperationPlan, ...],
    mode: SemanticRefreshPlanModeContext,
) -> SemanticRefreshWorkflowPlan:
    model_ids = tuple(item.model_unique_id for item in pre_release.models)
    return SemanticRefreshWorkflowPlan.build(
        workflow_name=pre_release.workflow_name,
        workflow_mode=mode.workflow_mode,
        scope_start=scope_start,
        scope_end=scope_end,
        scope_revision=mode.scope_revision,
        mutation_closure_sha256=pre_release.mutation_closure.mutation_closure_sha256,
        selected_mutating_node_ids=model_ids,
        model_operation_plan_ids=model_ids,
        expected_model_outcome_ids=model_ids,
        replacement_action_ids=mode.replacement_action_ids,
        operation_plan_refs=tuple(
            OperationPlanReference(item.model_unique_id, item.operation_id, item.operation_plan_sha256)
            for item in operations
        ),
    )


def _operation_plan(
    pre_release: SemanticRefreshPreReleaseProofBundle,
    authority: SemanticRefreshReleaseDeploymentAuthority,
    workflow_id: str,
    scope_start: str,
    scope_end: str,
    model: SemanticRefreshPreReleaseModelInput,
    deployment: SemanticRefreshDeploymentModelAuthority,
    effective_keys: tuple[EffectiveKeyColumn, ...],
    writer_assurance: SemanticRefreshRuntimeAssuranceReceipt,
    mode: SemanticRefreshPlanModeContext,
) -> SemanticRefreshOperationPlan:
    lineage = mode.lineage_for(model.model_unique_id) if mode.lineages else None
    target_predecessor_generation_id = deployment.target_predecessor_generation_id
    if lineage is not None:
        if lineage.target_predecessor_generation_id is None:
            raise SemanticRefreshContractError("recovery target predecessor generation is absent")
        target_predecessor_generation_id = lineage.target_predecessor_generation_id
    return SemanticRefreshOperationPlan.build(
        model_unique_id=model.model_unique_id,
        release_id=authority.release_id,
        deployment_id=authority.deployment_id,
        environment=pre_release.environment,
        workflow_id=workflow_id,
        scope_family_id=model.scope_family_id,
        scope_revision=mode.scope_revision,
        target_predecessor_generation_id=target_predecessor_generation_id,
        owner_generation=deployment.owner_generation,
        platform_policy_digest=pre_release.platform_policy_digest,
        resource_policy_digest=pre_release.resource_policy_digest,
        writer_assurance_digest=writer_assurance.runtime_assurance_receipt_sha256,
        operation_kind=mode.workflow_mode,
        model_definition_proof_sha256=model.model_definition_proof.model_definition_proof_sha256,
        mutation_closure_sha256=pre_release.mutation_closure.mutation_closure_sha256,
        sqlserver_lifecycle_policy_sha256=pre_release.lifecycle_policy.sqlserver_lifecycle_policy_sha256,
        read_dependency_proof_sha256=model.read_dependency_proof.read_dependency_proof_sha256,
        scope_start=scope_start,
        scope_end=scope_end,
        event_time_column=model.event_time_column,
        effective_key_columns=effective_keys,
        scope_predecessor_operation_id=(None if lineage is None else lineage.scope_predecessor_operation_id),
        replaces_failed_operation_id=(None if lineage is None else lineage.replaces_failed_operation_id),
        replacement_reason=(None if lineage is None else lineage.replacement_reason),
        replacement_ordinal=(None if lineage is None else lineage.replacement_ordinal),
    )


__all__ = ["SemanticRefreshPostDeploymentPlanCompiler"]

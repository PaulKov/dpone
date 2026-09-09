"""Mode and lineage derivation for protected semantic-refresh recovery plans."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.dbt_semantic_refresh_recovery_authority import (
    SemanticRefreshCompleteScopeReplayAuthority,
    SemanticRefreshFailedPrecommitReplacementAuthority,
    SemanticRefreshPlanRecoveryAuthority,
    SemanticRefreshPlanRecoveryVerifierPort,
)
from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    semantic_refresh_sha256,
)
from dpone.contracts.semantic_refresh_types import WorkflowMode
from dpone.contracts.semantic_refresh_workflow_replacement import (
    SemanticRefreshWorkflowReplacementPlan,
)


@dataclass(frozen=True, slots=True)
class SemanticRefreshModelRecoveryLineage:
    """Derived per-model lineage accepted by the canonical operation builder."""

    model_unique_id: str
    scope_predecessor_operation_id: str | None = None
    replaces_failed_operation_id: str | None = None
    replacement_reason: str | None = None
    replacement_ordinal: int | None = None
    clickhouse_target_authority_id: str | None = None
    target_predecessor_generation: int | None = None
    target_predecessor_generation_id: str | None = None
    target_predecessor_uuid: str | None = None
    target_predecessor_operation_id: str | None = None
    target_head_terminal_receipt_sha256: str | None = None
    target_head_authority_receipt_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class SemanticRefreshPlanModeContext:
    """Validated mode closure consumed by plan and run compilers."""

    workflow_mode: WorkflowMode
    scope_revision: int
    lineages: tuple[SemanticRefreshModelRecoveryLineage, ...]
    replacement_source: SemanticRefreshFailedPrecommitReplacementAuthority | None = None

    @property
    def replacement_action_ids(self) -> tuple[str, ...]:
        source = self.replacement_source
        return () if source is None else tuple(item.action_id for item in source.replacement_actions)

    def lineage_for(self, model_unique_id: str) -> SemanticRefreshModelRecoveryLineage:
        try:
            return next(item for item in self.lineages if item.model_unique_id == model_unique_id)
        except StopIteration as exc:
            raise SemanticRefreshContractError("recovery lineage omitted an exact selected model") from exc


def semantic_refresh_plan_mode_context(
    *,
    recovery: SemanticRefreshPlanRecoveryAuthority | None,
    verifier: SemanticRefreshPlanRecoveryVerifierPort | None,
    pre_release_bundle_sha256: str,
    package_artifacts_sha256: str,
    release_id: str,
    deployment_id: str,
    model_unique_ids: tuple[str, ...],
    scope_start: str,
    scope_end: str,
) -> SemanticRefreshPlanModeContext:
    """Derive lineage only from a protected exact predecessor authority."""

    if recovery is None:
        return SemanticRefreshPlanModeContext(WorkflowMode.NORMAL, 1, ())
    if verifier is None or verifier.verify(recovery) is not True:
        raise SemanticRefreshContractError("recovery authority is not protected and exact")
    predecessor = recovery.predecessor_plan
    workflow = predecessor.workflow_plan
    predecessor_models = tuple(item.model_unique_id for item in predecessor.operation_plans)
    if (
        predecessor.pre_release_bundle_sha256 != pre_release_bundle_sha256
        or predecessor.package_artifacts_sha256 != package_artifacts_sha256
        or predecessor.release_deployment_authority.release_id != release_id
        or predecessor.release_deployment_authority.deployment_id != deployment_id
        or predecessor_models != model_unique_ids
        or workflow.scope_start != scope_start
        or workflow.scope_end != scope_end
    ):
        raise SemanticRefreshContractError("recovery predecessor differs from the exact plan subject")
    previous = {item.model_unique_id: item for item in predecessor.operation_plans}
    if isinstance(recovery, SemanticRefreshCompleteScopeReplayAuthority):
        heads = {item.model_unique_id: item for item in recovery.target_heads}
        return SemanticRefreshPlanModeContext(
            WorkflowMode.COMPLETE_SCOPE_REPLAY,
            workflow.scope_revision + 1,
            tuple(
                SemanticRefreshModelRecoveryLineage(
                    model_unique_id=model_id,
                    scope_predecessor_operation_id=previous[model_id].operation_id,
                    clickhouse_target_authority_id=heads[model_id].clickhouse_target_authority_id,
                    target_predecessor_generation=heads[model_id].target_generation,
                    target_predecessor_generation_id=heads[model_id].target_generation_id,
                    target_predecessor_uuid=heads[model_id].target_uuid,
                    target_predecessor_operation_id=heads[model_id].owner_operation_id,
                    target_head_terminal_receipt_sha256=heads[model_id].terminal_receipt_sha256,
                    target_head_authority_receipt_sha256=heads[model_id].head_authority_receipt_sha256,
                )
                for model_id in model_unique_ids
            ),
        )
    targets = {item.model_unique_id: item for item in predecessor.targets}
    return SemanticRefreshPlanModeContext(
        WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT,
        workflow.scope_revision,
        tuple(
            SemanticRefreshModelRecoveryLineage(
                model_unique_id=model_id,
                replaces_failed_operation_id=previous[model_id].operation_id,
                replacement_reason="failed_precommit_replacement",
                replacement_ordinal=(previous[model_id].replacement_ordinal or 0) + 1,
                clickhouse_target_authority_id=targets[model_id].clickhouse_target_authority_id,
                target_predecessor_generation=targets[model_id].target_predecessor_generation,
                target_predecessor_generation_id=targets[model_id].target_predecessor_generation_id,
                target_predecessor_uuid=targets[model_id].clickhouse_target_uuid,
                target_predecessor_operation_id=targets[model_id].target_predecessor_operation_id,
                target_head_terminal_receipt_sha256=targets[model_id].target_head_terminal_receipt_sha256,
                target_head_authority_receipt_sha256=targets[model_id].target_head_authority_receipt_sha256,
            )
            for model_id in model_unique_ids
        ),
        recovery,
    )


def semantic_refresh_replacement_plan(
    *,
    workflow_plan_sha256: str,
    model_unique_ids: tuple[str, ...],
    context: SemanticRefreshPlanModeContext,
) -> SemanticRefreshWorkflowReplacementPlan | None:
    """Build the optional canonical replacement document after the workflow digest exists."""

    source = context.replacement_source
    if source is None:
        return None
    recovery_plan_digest = semantic_refresh_sha256(
        {
            "predecessor_plan_bundle_sha256": source.predecessor_plan.plan_bundle_sha256,
            "predecessor_workflow_summary_sha256": source.predecessor_summary.terminal_summary_sha256,
            "replacement_actions": [item.to_dict() for item in source.replacement_actions],
            "schema": "dpone.dbt-semantic-refresh-recovery-decision.v1",
        }
    )
    return SemanticRefreshWorkflowReplacementPlan.build(
        workflow_plan_sha256=workflow_plan_sha256,
        predecessor_workflow_execution_id=source.predecessor_workflow_execution_id,
        predecessor_workflow_execution_binding_sha256=(source.predecessor_summary.workflow_execution_binding_sha256),
        predecessor_workflow_summary_sha256=source.predecessor_summary.terminal_summary_sha256,
        recovery_plan_digest=recovery_plan_digest,
        selected_mutating_node_ids=model_unique_ids,
        model_operation_plan_ids=model_unique_ids,
        expected_model_outcome_ids=model_unique_ids,
        replacement_action_ids=context.replacement_action_ids,
        replacement_actions=source.replacement_actions,
    )


__all__ = [
    "SemanticRefreshPlanModeContext",
    "semantic_refresh_plan_mode_context",
    "semantic_refresh_replacement_plan",
]

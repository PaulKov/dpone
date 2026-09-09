"""Closed predecessor evidence authorities for semantic-refresh recovery."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, TypeAlias, runtime_checkable

from dpone.contracts.dbt_semantic_refresh_recovery_head import (
    SemanticRefreshRecoveryTargetHead,
)
from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_digest,
    require_text,
    semantic_refresh_sha256,
)
from dpone.contracts.semantic_refresh_failure_summary import SemanticRefreshFailedWorkflowSummary
from dpone.contracts.semantic_refresh_plan_refs import ReplacementActionBinding
from dpone.contracts.semantic_refresh_workflow_summary import SemanticRefreshDurableWorkflowSummary


class _ReleaseDeploymentAuthority(Protocol):
    @property
    def release_id(self) -> str: ...

    @property
    def deployment_id(self) -> str: ...


class _WorkflowPlan(Protocol):
    @property
    def workflow_plan_sha256(self) -> str: ...

    @property
    def scope_start(self) -> str: ...

    @property
    def scope_end(self) -> str: ...

    @property
    def scope_revision(self) -> int: ...


class _OperationPlan(Protocol):
    @property
    def model_unique_id(self) -> str: ...

    @property
    def operation_id(self) -> str: ...

    @property
    def operation_plan_sha256(self) -> str: ...

    @property
    def replacement_ordinal(self) -> int | None: ...


class _PlanTarget(Protocol):
    @property
    def model_unique_id(self) -> str: ...

    @property
    def clickhouse_target_authority_id(self) -> str: ...

    @property
    def clickhouse_target_uuid(self) -> str: ...

    @property
    def target_predecessor_generation(self) -> int: ...

    @property
    def target_predecessor_generation_id(self) -> str: ...

    @property
    def target_predecessor_operation_id(self) -> str: ...

    @property
    def target_head_terminal_receipt_sha256(self) -> str | None: ...

    @property
    def target_head_authority_receipt_sha256(self) -> str: ...


@runtime_checkable
class SemanticRefreshPredecessorPlan(Protocol):
    """Minimal immutable plan authority needed for recovery compilation."""

    @property
    def pre_release_bundle_sha256(self) -> str: ...

    @property
    def package_artifacts_sha256(self) -> str: ...

    @property
    def plan_bundle_sha256(self) -> str: ...

    @property
    def release_deployment_authority(self) -> _ReleaseDeploymentAuthority: ...

    @property
    def operation_plans(self) -> tuple[_OperationPlan, ...]: ...

    @property
    def targets(self) -> tuple[_PlanTarget, ...]: ...

    @property
    def workflow_plan(self) -> _WorkflowPlan: ...

    def to_dict(self) -> Mapping[str, object]: ...


@dataclass(frozen=True, slots=True)
class SemanticRefreshCompleteScopeReplayAuthority:
    """Protected complete predecessor that authorizes revision n+1."""

    predecessor_plan: SemanticRefreshPredecessorPlan
    predecessor_summary: SemanticRefreshDurableWorkflowSummary
    predecessor_workflow_execution_id: str
    target_heads: tuple[SemanticRefreshRecoveryTargetHead, ...]
    authority_receipt_sha256: str

    @classmethod
    def build(
        cls,
        *,
        predecessor_plan: SemanticRefreshPredecessorPlan,
        predecessor_summary: SemanticRefreshDurableWorkflowSummary,
        predecessor_workflow_execution_id: str,
        target_heads: tuple[SemanticRefreshRecoveryTargetHead, ...],
    ) -> SemanticRefreshCompleteScopeReplayAuthority:
        """Build the closed replay authority receipt before protected verification."""

        return cls(
            predecessor_plan=predecessor_plan,
            predecessor_summary=predecessor_summary,
            predecessor_workflow_execution_id=predecessor_workflow_execution_id,
            target_heads=target_heads,
            authority_receipt_sha256=_replay_authority_sha256(
                predecessor_plan=predecessor_plan,
                predecessor_summary=predecessor_summary,
                predecessor_workflow_execution_id=predecessor_workflow_execution_id,
                target_heads=target_heads,
            ),
        )

    def __post_init__(self) -> None:
        _require_plan(self.predecessor_plan)
        if not isinstance(self.predecessor_summary, SemanticRefreshDurableWorkflowSummary):
            raise SemanticRefreshContractError("replay predecessor summary must be canonical and typed")
        if self.predecessor_workflow_execution_id != semantic_refresh_predecessor_workflow_execution_id(
            self.predecessor_summary
        ):
            raise SemanticRefreshContractError("replay workflow execution identity differs from its summary")
        require_digest(self.authority_receipt_sha256, "recovery authority receipt")
        _validate_predecessor(self.predecessor_plan, self.predecessor_summary)
        _validate_replay_heads(self.predecessor_plan, self.predecessor_summary, self.target_heads)
        if self.authority_receipt_sha256 != _replay_authority_sha256(
            predecessor_plan=self.predecessor_plan,
            predecessor_summary=self.predecessor_summary,
            predecessor_workflow_execution_id=self.predecessor_workflow_execution_id,
            target_heads=self.target_heads,
        ):
            raise SemanticRefreshContractError("replay authority receipt differs")


@dataclass(frozen=True, slots=True)
class SemanticRefreshFailedPrecommitReplacementAuthority:
    """Protected failed predecessor and its evidence-derived action closure."""

    predecessor_plan: SemanticRefreshPredecessorPlan
    predecessor_summary: SemanticRefreshFailedWorkflowSummary
    predecessor_workflow_execution_id: str
    replacement_actions: tuple[ReplacementActionBinding, ...]
    authority_receipt_sha256: str

    @classmethod
    def build(
        cls,
        *,
        predecessor_plan: SemanticRefreshPredecessorPlan,
        predecessor_summary: SemanticRefreshFailedWorkflowSummary,
        predecessor_workflow_execution_id: str,
        replacement_actions: tuple[ReplacementActionBinding, ...],
    ) -> SemanticRefreshFailedPrecommitReplacementAuthority:
        """Build the closed failed-precommit replacement authority receipt."""

        return cls(
            predecessor_plan=predecessor_plan,
            predecessor_summary=predecessor_summary,
            predecessor_workflow_execution_id=predecessor_workflow_execution_id,
            replacement_actions=replacement_actions,
            authority_receipt_sha256=_replacement_authority_sha256(
                predecessor_plan=predecessor_plan,
                predecessor_summary=predecessor_summary,
                predecessor_workflow_execution_id=predecessor_workflow_execution_id,
                replacement_actions=replacement_actions,
            ),
        )

    def __post_init__(self) -> None:
        _require_plan(self.predecessor_plan)
        if not isinstance(self.predecessor_summary, SemanticRefreshFailedWorkflowSummary):
            raise SemanticRefreshContractError("replacement predecessor summary must be canonical and typed")
        if self.predecessor_workflow_execution_id != semantic_refresh_predecessor_workflow_execution_id(
            self.predecessor_summary
        ):
            raise SemanticRefreshContractError("replacement workflow execution identity differs from its summary")
        require_digest(self.authority_receipt_sha256, "recovery authority receipt")
        actions = _canonical_actions(self.replacement_actions)
        if actions != self.replacement_actions:
            raise SemanticRefreshContractError("replacement actions must be canonically ordered")
        _validate_predecessor(self.predecessor_plan, self.predecessor_summary)
        _validate_replacement_outcomes(self.predecessor_plan, self.predecessor_summary, actions)
        if self.authority_receipt_sha256 != _replacement_authority_sha256(
            predecessor_plan=self.predecessor_plan,
            predecessor_summary=self.predecessor_summary,
            predecessor_workflow_execution_id=self.predecessor_workflow_execution_id,
            replacement_actions=self.replacement_actions,
        ):
            raise SemanticRefreshContractError("replacement authority receipt differs")


SemanticRefreshPlanRecoveryAuthority: TypeAlias = (
    SemanticRefreshCompleteScopeReplayAuthority | SemanticRefreshFailedPrecommitReplacementAuthority
)


class SemanticRefreshPlanRecoveryVerifierPort(Protocol):
    """Authenticate the complete predecessor plan, summary, and action authority."""

    def verify(self, authority: SemanticRefreshPlanRecoveryAuthority) -> bool: ...


def semantic_refresh_predecessor_workflow_execution_id(
    summary: SemanticRefreshDurableWorkflowSummary | SemanticRefreshFailedWorkflowSummary,
) -> str:
    """Project the authenticated logical execution locator from either summary variant."""

    if isinstance(summary, SemanticRefreshDurableWorkflowSummary):
        return require_text(summary.workflow_execution_id, "predecessor workflow execution identity")
    if isinstance(summary, SemanticRefreshFailedWorkflowSummary):
        return require_text(summary.workflow_id, "predecessor workflow execution identity")
    raise SemanticRefreshContractError("predecessor workflow summary must be canonical and typed")


def _require_plan(value: SemanticRefreshPredecessorPlan) -> None:
    if not isinstance(value, SemanticRefreshPredecessorPlan):
        raise SemanticRefreshContractError("recovery predecessor plan must be canonical and typed")
    payload = dict(value.to_dict())
    supplied = payload.pop("plan_bundle_sha256", None)
    if supplied != value.plan_bundle_sha256 or semantic_refresh_sha256(payload) != supplied:
        raise SemanticRefreshContractError("recovery predecessor plan digest differs")


def _validate_predecessor(
    plan: SemanticRefreshPredecessorPlan,
    summary: SemanticRefreshDurableWorkflowSummary | SemanticRefreshFailedWorkflowSummary,
) -> None:
    operation_ids = tuple(sorted(item.operation_id for item in plan.operation_plans))
    if (
        summary.workflow_plan_sha256 != plan.workflow_plan.workflow_plan_sha256
        or summary.expected_operation_ids != operation_ids
    ):
        raise SemanticRefreshContractError("recovery summary differs from the predecessor plan closure")
    if isinstance(summary, SemanticRefreshDurableWorkflowSummary):
        plans = {item.operation_id: item.operation_plan_sha256 for item in plan.operation_plans}
        if any(plans[item.operation_id] != item.operation_plan_sha256 for item in summary.publications):
            raise SemanticRefreshContractError("replay publication differs from predecessor operation plans")


def _canonical_actions(values: tuple[ReplacementActionBinding, ...]) -> tuple[ReplacementActionBinding, ...]:
    if not values or any(not isinstance(item, ReplacementActionBinding) for item in values):
        raise SemanticRefreshContractError("replacement actions must be a non-empty typed tuple")
    ordered = tuple(sorted(values))
    if len({item.action_id for item in ordered}) != len(ordered):
        raise SemanticRefreshContractError("replacement actions contain duplicate model identities")
    return ordered


def _validate_replacement_outcomes(
    plan: SemanticRefreshPredecessorPlan,
    summary: SemanticRefreshFailedWorkflowSummary,
    actions: tuple[ReplacementActionBinding, ...],
) -> None:
    operation_by_model = {item.model_unique_id: item.operation_id for item in plan.operation_plans}
    outcome_by_operation = {item.operation_id: item.mssql_outcome for item in summary.models}
    if tuple(item.action_id for item in actions) != tuple(sorted(operation_by_model)) or any(
        action.outcome is not outcome_by_operation[operation_by_model[action.action_id]] for action in actions
    ):
        raise SemanticRefreshContractError("replacement actions differ from durable predecessor outcomes")


def _validate_replay_heads(
    plan: SemanticRefreshPredecessorPlan,
    summary: SemanticRefreshDurableWorkflowSummary,
    values: tuple[SemanticRefreshRecoveryTargetHead, ...],
) -> None:
    if not values or any(not isinstance(item, SemanticRefreshRecoveryTargetHead) for item in values):
        raise SemanticRefreshContractError("replay target heads must be a non-empty typed tuple")
    ordered = tuple(sorted(values))
    if ordered != values or len({item.model_unique_id for item in ordered}) != len(ordered):
        raise SemanticRefreshContractError("replay target heads must be canonical and unique")
    operations = {item.model_unique_id: item for item in plan.operation_plans}
    targets = {item.model_unique_id: item for item in plan.targets}
    if tuple(item.model_unique_id for item in ordered) != tuple(sorted(operations)):
        raise SemanticRefreshContractError("replay target heads differ from predecessor model closure")
    for head in ordered:
        target = targets[head.model_unique_id]
        if head.clickhouse_target_authority_id != target.clickhouse_target_authority_id:
            raise SemanticRefreshContractError("replay target head differs from the predecessor physical target")


def _replay_authority_sha256(
    *,
    predecessor_plan: SemanticRefreshPredecessorPlan,
    predecessor_summary: SemanticRefreshDurableWorkflowSummary,
    predecessor_workflow_execution_id: str,
    target_heads: tuple[SemanticRefreshRecoveryTargetHead, ...],
) -> str:
    return semantic_refresh_sha256(
        {
            "predecessor_plan_bundle_sha256": predecessor_plan.plan_bundle_sha256,
            "predecessor_workflow_execution_id": predecessor_workflow_execution_id,
            "predecessor_workflow_summary_sha256": predecessor_summary.terminal_summary_sha256,
            "schema": "dpone.dbt-semantic-refresh-complete-replay-authority.v1",
            "target_head_authority_receipts": [
                [item.model_unique_id, item.head_authority_receipt_sha256] for item in target_heads
            ],
        }
    )


def _replacement_authority_sha256(
    *,
    predecessor_plan: SemanticRefreshPredecessorPlan,
    predecessor_summary: SemanticRefreshFailedWorkflowSummary,
    predecessor_workflow_execution_id: str,
    replacement_actions: tuple[ReplacementActionBinding, ...],
) -> str:
    return semantic_refresh_sha256(
        {
            "predecessor_plan_bundle_sha256": predecessor_plan.plan_bundle_sha256,
            "predecessor_workflow_execution_id": predecessor_workflow_execution_id,
            "predecessor_workflow_summary_sha256": predecessor_summary.terminal_summary_sha256,
            "replacement_actions": [item.to_dict() for item in replacement_actions],
            "schema": "dpone.dbt-semantic-refresh-failed-precommit-authority.v1",
        }
    )


__all__ = [
    "SemanticRefreshCompleteScopeReplayAuthority",
    "SemanticRefreshFailedPrecommitReplacementAuthority",
    "SemanticRefreshPlanRecoveryAuthority",
    "SemanticRefreshPlanRecoveryVerifierPort",
    "SemanticRefreshPredecessorPlan",
    "semantic_refresh_predecessor_workflow_execution_id",
]

"""Canonical post-deployment semantic-refresh plan bundle."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.dbt_semantic_refresh_deployment_contracts import (
    SemanticRefreshPlanTarget,
    SemanticRefreshReleaseDeploymentAuthority,
)
from dpone.contracts.dbt_semantic_refresh_run_guard import SemanticRefreshRunGuardClosure
from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_digest,
    semantic_refresh_sha256,
)
from dpone.contracts.semantic_refresh_operation_plan import SemanticRefreshOperationPlan
from dpone.contracts.semantic_refresh_workflow_plan import SemanticRefreshWorkflowPlan
from dpone.contracts.semantic_refresh_workflow_replacement import SemanticRefreshWorkflowReplacementPlan

PLAN_BUNDLE_SCHEMA = "dpone.dbt-semantic-refresh-plan-bundle.v1"


@dataclass(frozen=True, slots=True)
class SemanticRefreshPlanBundle:
    """Canonical post-deployment plans; run admission adds execution identity."""

    pre_release_bundle_sha256: str
    package_artifacts_sha256: str
    release_deployment_authority: SemanticRefreshReleaseDeploymentAuthority
    operation_plans: tuple[SemanticRefreshOperationPlan, ...]
    workflow_plan: SemanticRefreshWorkflowPlan
    targets: tuple[SemanticRefreshPlanTarget, ...]
    run_guard_closure: SemanticRefreshRunGuardClosure
    plan_bundle_sha256: str
    workflow_replacement_plan: SemanticRefreshWorkflowReplacementPlan | None = None
    schema: str = PLAN_BUNDLE_SCHEMA

    def __post_init__(self) -> None:
        require_digest(self.pre_release_bundle_sha256, "pre_release_bundle_sha256")
        require_digest(self.package_artifacts_sha256, "package_artifacts_sha256")
        require_digest(self.plan_bundle_sha256, "plan_bundle_sha256")
        if not isinstance(self.run_guard_closure, SemanticRefreshRunGuardClosure):
            raise SemanticRefreshContractError("run_guard_closure must be canonical and typed")
        if set(self.run_guard_closure.resource_guard_ids) != {item.target_resource_id for item in self.targets}:
            raise SemanticRefreshContractError("run guard closure differs from the exact plan target topology")
        if self.workflow_plan.workflow_mode.value == "failed_precommit_replacement":
            if (
                not isinstance(
                    self.workflow_replacement_plan,
                    SemanticRefreshWorkflowReplacementPlan,
                )
                or self.workflow_replacement_plan.workflow_plan_sha256 != self.workflow_plan.workflow_plan_sha256
            ):
                raise SemanticRefreshContractError("replacement workflow requires its canonical replacement plan")
        elif self.workflow_replacement_plan is not None:
            raise SemanticRefreshContractError("normal/replay plan bundle cannot contain a replacement plan")
        expected = semantic_refresh_sha256(
            plan_payload(
                self.pre_release_bundle_sha256,
                self.package_artifacts_sha256,
                self.release_deployment_authority,
                self.operation_plans,
                self.workflow_plan,
                self.targets,
                self.run_guard_closure,
                self.workflow_replacement_plan,
            )
        )
        if self.plan_bundle_sha256 != expected:
            raise SemanticRefreshContractError("plan bundle digest differs from its canonical content")

    def to_dict(self) -> dict[str, object]:
        """Return the exact canonical plan bundle projection."""

        return {
            **plan_payload(
                self.pre_release_bundle_sha256,
                self.package_artifacts_sha256,
                self.release_deployment_authority,
                self.operation_plans,
                self.workflow_plan,
                self.targets,
                self.run_guard_closure,
                self.workflow_replacement_plan,
            ),
            "plan_bundle_sha256": self.plan_bundle_sha256,
        }


def plan_payload(
    pre_release_bundle_sha256: str,
    package_artifacts_sha256: str,
    authority: SemanticRefreshReleaseDeploymentAuthority,
    operations: tuple[SemanticRefreshOperationPlan, ...],
    workflow: SemanticRefreshWorkflowPlan,
    targets: tuple[SemanticRefreshPlanTarget, ...],
    run_guard_closure: SemanticRefreshRunGuardClosure,
    replacement: SemanticRefreshWorkflowReplacementPlan | None = None,
) -> dict[str, object]:
    """Return the unsigned canonical plan payload."""

    payload: dict[str, object] = {
        "operation_plans": [item.to_dict() for item in operations],
        "package_artifacts_sha256": package_artifacts_sha256,
        "pre_release_bundle_sha256": pre_release_bundle_sha256,
        "release_deployment_authority": authority.to_dict(),
        "run_guard_closure": run_guard_closure.to_dict(),
        "schema": PLAN_BUNDLE_SCHEMA,
        "targets": [item.to_dict() for item in targets],
        "workflow_plan": workflow.to_dict(),
    }
    if replacement is not None:
        payload["workflow_replacement_plan"] = replacement.to_dict()
    return payload

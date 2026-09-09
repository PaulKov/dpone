"""Protected logical-run authority and execution-binding handoff."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from dpone.contracts.dbt_semantic_refresh_plan_contracts import SemanticRefreshPlanBundle
from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_digest,
    require_text,
    semantic_refresh_sha256,
)
from dpone.contracts.semantic_refresh_execution_binding import SemanticRefreshWorkflowExecutionBinding

RUN_EXECUTION_BUNDLE_SCHEMA = "dpone.dbt-semantic-refresh-run-execution-bundle.v1"


@dataclass(frozen=True, slots=True)
class SemanticRefreshRunAdmissionAuthority:
    """Protected logical DagRun identity bound to one deployment plan bundle."""

    workflow_execution_id: str
    plan_bundle_sha256: str
    authority_receipt_sha256: str

    def __post_init__(self) -> None:
        require_text(self.workflow_execution_id, "workflow_execution_id")
        require_digest(self.plan_bundle_sha256, "plan_bundle_sha256")
        require_digest(self.authority_receipt_sha256, "authority_receipt_sha256")

    def to_dict(self) -> dict[str, object]:
        return {
            "authority_receipt_sha256": self.authority_receipt_sha256,
            "plan_bundle_sha256": self.plan_bundle_sha256,
            "workflow_execution_id": self.workflow_execution_id,
        }


class SemanticRefreshRunAdmissionVerifierPort(Protocol):
    """Verify the logical DagRun identity in protected orchestrator state."""

    def verify(self, authority: SemanticRefreshRunAdmissionAuthority) -> bool: ...


@dataclass(frozen=True, slots=True)
class SemanticRefreshRunExecutionBundle:
    """Run-bound execution handoff; attempt/fence authority remains absent."""

    plan_bundle_sha256: str
    run_authority_receipt_sha256: str
    workflow_execution_binding: SemanticRefreshWorkflowExecutionBinding
    run_execution_bundle_sha256: str
    schema: str = RUN_EXECUTION_BUNDLE_SCHEMA

    @classmethod
    def build(
        cls,
        *,
        plan_bundle_sha256: str,
        run_authority_receipt_sha256: str,
        workflow_execution_binding: SemanticRefreshWorkflowExecutionBinding,
    ) -> SemanticRefreshRunExecutionBundle:
        values = {
            "plan_bundle_sha256": require_digest(plan_bundle_sha256, "plan_bundle_sha256"),
            "run_authority_receipt_sha256": require_digest(
                run_authority_receipt_sha256,
                "run_authority_receipt_sha256",
            ),
            "schema": RUN_EXECUTION_BUNDLE_SCHEMA,
            "workflow_execution_binding": workflow_execution_binding.to_dict(),
        }
        return cls(
            plan_bundle_sha256,
            run_authority_receipt_sha256,
            workflow_execution_binding,
            semantic_refresh_sha256(values),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "plan_bundle_sha256": self.plan_bundle_sha256,
            "run_authority_receipt_sha256": self.run_authority_receipt_sha256,
            "run_execution_bundle_sha256": self.run_execution_bundle_sha256,
            "schema": self.schema,
            "workflow_execution_binding": self.workflow_execution_binding.to_dict(),
        }


class SemanticRefreshRunAdmissionCompiler:
    """Bind one trusted logical DagRun without inventing attempt authority."""

    def __init__(self, verifier: SemanticRefreshRunAdmissionVerifierPort) -> None:
        self._verifier = verifier

    def compile(
        self,
        *,
        plan_bundle: SemanticRefreshPlanBundle,
        authority: SemanticRefreshRunAdmissionAuthority,
    ) -> SemanticRefreshRunExecutionBundle:
        if authority.plan_bundle_sha256 != plan_bundle.plan_bundle_sha256:
            raise SemanticRefreshContractError("run authority differs from the deployment plan bundle")
        if self._verifier.verify(authority) is not True:
            raise SemanticRefreshContractError("logical workflow execution authority is not protected and exact")
        workflow = plan_bundle.workflow_plan
        deployment = plan_bundle.release_deployment_authority
        replacement = plan_bundle.workflow_replacement_plan
        binding = SemanticRefreshWorkflowExecutionBinding.build(
            workflow_execution_id=authority.workflow_execution_id,
            workflow_mode=workflow.workflow_mode,
            workflow_plan_sha256=workflow.workflow_plan_sha256,
            selected_mutating_node_ids=workflow.selected_mutating_node_ids,
            model_operation_plan_ids=workflow.model_operation_plan_ids,
            expected_model_outcome_ids=workflow.expected_model_outcome_ids,
            replacement_action_ids=workflow.replacement_action_ids,
            deployment_id=deployment.deployment_id,
            binding_set_ref=deployment.binding_set_ref,
            connection_registry_ref=deployment.connection_registry_ref,
            credential_runtime_ref=deployment.credential_runtime_ref,
            workflow_replacement_plan_sha256=(
                None if replacement is None else replacement.workflow_replacement_plan_sha256
            ),
            recovery_plan_digest=(None if replacement is None else replacement.recovery_plan_digest),
        )
        return SemanticRefreshRunExecutionBundle.build(
            plan_bundle_sha256=plan_bundle.plan_bundle_sha256,
            run_authority_receipt_sha256=authority.authority_receipt_sha256,
            workflow_execution_binding=binding,
        )


__all__ = [
    "RUN_EXECUTION_BUNDLE_SCHEMA",
    "SemanticRefreshRunAdmissionCompiler",
    "SemanticRefreshRunAdmissionAuthority",
    "SemanticRefreshRunAdmissionVerifierPort",
    "SemanticRefreshRunExecutionBundle",
]

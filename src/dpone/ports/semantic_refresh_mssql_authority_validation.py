"""Fail-closed validation for canonical and admitted MSSQL authority."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, TypeVar

from dpone.contracts.semantic_refresh_execution_binding import (
    SemanticRefreshWorkflowExecutionBinding,
)
from dpone.contracts.semantic_refresh_types import WorkflowMode
from dpone.contracts.semantic_refresh_workflow_plan import SemanticRefreshWorkflowPlan

if TYPE_CHECKING:
    from dpone.contracts.semantic_refresh_attempt_binding import SemanticRefreshAttemptBinding
    from dpone.contracts.semantic_refresh_operation_plan import SemanticRefreshOperationPlan
    from dpone.ports.semantic_refresh_mssql_authority import MssqlProtectedOperationStateRecord
    from dpone.ports.semantic_refresh_mssql_authority_models import (
        MssqlCanonicalAdmissionBundle,
        MssqlModelResourceAuthority,
    )

_SHA256_PREFIX = "sha256:"
_T = TypeVar("_T")


def validate_canonical_bundle(bundle: MssqlCanonicalAdmissionBundle) -> None:
    """Require exact plan, execution, attempt, guard, target, and recovery closure."""

    if not isinstance(bundle.workflow_plan, SemanticRefreshWorkflowPlan):
        raise TypeError("workflow_plan must be a semantic workflow plan")
    if not isinstance(bundle.execution_binding, SemanticRefreshWorkflowExecutionBinding):
        raise TypeError("execution_binding must be a workflow execution binding")
    execution = bundle.execution_binding
    if execution.workflow_plan_sha256 != bundle.workflow_plan.workflow_plan_sha256:
        raise ValueError("execution binding does not reference the canonical workflow plan")
    if execution.workflow_execution_id != bundle.workflow_execution_id:
        raise ValueError("execution binding workflow execution identity differs from canonical authority")
    if execution.workflow_mode is not bundle.workflow_plan.workflow_mode:
        raise ValueError("execution binding mode differs from workflow plan mode")
    workflow_closure = (
        bundle.workflow_plan.selected_mutating_node_ids,
        bundle.workflow_plan.model_operation_plan_ids,
        bundle.workflow_plan.expected_model_outcome_ids,
        bundle.workflow_plan.replacement_action_ids,
    )
    execution_closure = (
        execution.selected_mutating_node_ids,
        execution.model_operation_plan_ids,
        execution.expected_model_outcome_ids,
        execution.replacement_action_ids,
    )
    if execution_closure != workflow_closure:
        raise ValueError("execution binding model closure differs from workflow plan")
    operations = _ordered_unique(
        bundle.operation_plans,
        "operation_plans",
        lambda item: item.model_unique_id,
    )
    model_ids = bundle.workflow_plan.model_operation_plan_ids
    if tuple(item.model_unique_id for item in operations) != model_ids:
        raise ValueError("operation plans do not cover the exact workflow model closure")
    _validate_operations(bundle, operations)
    attempts = _ordered_unique(
        bundle.attempt_bindings,
        "attempt_bindings",
        lambda item: item.operation_id,
    )
    if {item.operation_id for item in attempts} != {item.operation_id for item in operations}:
        raise ValueError("attempt bindings do not cover the exact operation closure")
    attempt_by_operation = {item.operation_id: item for item in attempts}
    for operation in operations:
        _validate_attempt(bundle, operation, attempt_by_operation[operation.operation_id])
    resources = _ordered_unique(
        bundle.resource_guards,
        "resource_guards",
        lambda item: item.resource_id,
    )
    models = _ordered_unique(
        bundle.model_resources,
        "model_resources",
        lambda item: item.model_unique_id,
    )
    if tuple(item.model_unique_id for item in models) != model_ids:
        raise ValueError("model resources do not cover the exact workflow model closure")
    guard_by_resource = {item.resource_id: item for item in resources}
    for operation, model_resource in zip(operations, models, strict=True):
        guard = guard_by_resource.get(model_resource.target_resource_id)
        if guard is None:
            raise ValueError("model target is absent from protected resource guard closure")
        if guard.fencing_epoch != attempt_by_operation[operation.operation_id].fencing_epoch:
            raise ValueError("attempt fence differs from protected target guard epoch")
        _validate_model_resource(operation, model_resource)
    _validate_replacement(bundle)


def validate_runtime_operation_state(
    bundle: MssqlCanonicalAdmissionBundle,
    operation: SemanticRefreshOperationPlan,
    attempt: SemanticRefreshAttemptBinding,
    resource: MssqlModelResourceAuthority,
    state: MssqlProtectedOperationStateRecord,
) -> None:
    """Require exact ACTIVE authority, held guard, journal, and predecessor state."""

    if state.authority.workflow_execution_binding_sha256 != (
        bundle.execution_binding.workflow_execution_binding_sha256
    ):
        raise ValueError("runtime authority lookup differs from canonical execution")
    expected = (
        bundle.workflow_execution_id,
        bundle.workflow_plan.workflow_plan_sha256,
        operation.operation_id,
        operation.operation_plan_sha256,
        attempt.attempt_binding_sha256,
        attempt.fencing_epoch,
        bundle.owner_id,
        resource.target_resource_id,
    )
    actual = (
        state.workflow_execution_id,
        state.workflow_plan_sha256,
        state.operation_id,
        state.operation_plan_sha256,
        state.attempt_binding_sha256,
        state.fencing_epoch,
        state.owner_id,
        state.guard_resource_id,
    )
    if actual != expected:
        raise ValueError("durable operation state differs from canonical authority")
    if state.guard_status != "HELD" or state.journal_status not in {
        "PREPARING",
        "PREPARED",
        "COMMITTING",
        "TARGET_COMMITTED",
        "COMPLETE",
        "COMMITTED_INCOMPLETE",
    }:
        raise ValueError("durable operation state is not active or publishable")
    if (
        state.target_predecessor_generation_id != operation.target_predecessor_generation_id
        or state.scope_predecessor_operation_id != operation.scope_predecessor_operation_id
    ):
        raise ValueError("durable predecessor identity differs from canonical operation")
    image_fields = (
        state.before_image_relation,
        state.before_image_sha256,
        state.after_image_relation,
        state.after_image_sha256,
    )
    if any(value is None for value in image_fields) and any(value is not None for value in image_fields):
        raise ValueError("durable image authority is incomplete")
    for digest in (state.before_image_sha256, state.after_image_sha256):
        if digest is not None:
            _require_digest(digest, "durable image digest")


def _validate_operations(
    bundle: MssqlCanonicalAdmissionBundle,
    operations: tuple[SemanticRefreshOperationPlan, ...],
) -> None:
    refs = {item.model_unique_id: item for item in bundle.workflow_plan.operation_plan_refs}
    for operation in operations:
        ref = refs[operation.model_unique_id]
        if (operation.operation_id, operation.operation_plan_sha256) != (
            ref.operation_id,
            ref.operation_plan_sha256,
        ):
            raise ValueError("operation plan differs from its canonical workflow reference")
        if operation.operation_kind is not bundle.workflow_plan.workflow_mode:
            raise ValueError("operation kind differs from workflow mode")
        if operation.deployment_id != bundle.execution_binding.deployment_id:
            raise ValueError("operation deployment differs from execution binding")
        if (
            operation.scope_start,
            operation.scope_end,
            operation.scope_revision,
            operation.mutation_closure_sha256,
        ) != (
            bundle.workflow_plan.scope_start,
            bundle.workflow_plan.scope_end,
            bundle.workflow_plan.scope_revision,
            bundle.workflow_plan.mutation_closure_sha256,
        ):
            raise ValueError("operation scope or mutation closure differs from workflow plan")
    if len({item.release_id for item in operations}) != 1:
        raise ValueError("workflow operations must use one release identity")
    if len({item.environment for item in operations}) != 1:
        raise ValueError("workflow operations must use one environment")


def _validate_attempt(
    bundle: MssqlCanonicalAdmissionBundle,
    operation: SemanticRefreshOperationPlan,
    attempt: SemanticRefreshAttemptBinding,
) -> None:
    if (
        attempt.operation_plan_sha256 != operation.operation_plan_sha256
        or attempt.workflow_execution_binding_sha256 != bundle.execution_binding.workflow_execution_binding_sha256
        or attempt.workflow_execution_id != bundle.workflow_execution_id
        or attempt.dag_run_id != bundle.workflow_execution_id
    ):
        raise ValueError("attempt binding differs from its canonical operation/execution")
    if attempt.owner_id != bundle.owner_id:
        raise ValueError("attempt owner differs from protected writer owner")


def _validate_model_resource(
    operation: SemanticRefreshOperationPlan,
    resource: MssqlModelResourceAuthority,
) -> None:
    if operation.model_definition_proof_sha256 != resource.model_definition_proof_sha256:
        raise ValueError("model definition proof differs from protected target authority")
    if (
        operation.effective_key_template_sha256 != resource.effective_key_template_sha256
        or operation.effective_key_mapping_sha256 != resource.effective_key_mapping_sha256
        or operation.resource_policy_digest != resource.resource_policy.resource_policy_sha256
        or operation.writer_assurance_digest != resource.writer_exclusivity_assurance_receipt_sha256
    ):
        raise ValueError("operation key/resource/assurance identity differs from target authority")


def _validate_replacement(bundle: MssqlCanonicalAdmissionBundle) -> None:
    is_replacement = bundle.workflow_plan.workflow_mode is WorkflowMode.FAILED_PRECOMMIT_REPLACEMENT
    if not is_replacement:
        if bundle.replacement_plan is not None:
            raise ValueError("normal/replay workflow cannot carry a replacement plan")
        return
    if bundle.replacement_plan is None:
        raise ValueError("failed-precommit workflow requires a protected replacement plan")
    replacement = bundle.replacement_plan
    if (
        replacement.workflow_plan_sha256 != bundle.workflow_plan.workflow_plan_sha256
        or bundle.execution_binding.workflow_replacement_plan_sha256 != replacement.workflow_replacement_plan_sha256
        or bundle.execution_binding.recovery_plan_digest != replacement.recovery_plan_digest
    ):
        raise ValueError("replacement plan differs from execution recovery authority")


def _ordered_unique(
    values: tuple[_T, ...],
    field_name: str,
    key: Callable[[_T], str],
) -> tuple[_T, ...]:
    if not isinstance(values, tuple) or not values:
        raise ValueError(f"{field_name} must be a non-empty tuple")
    identities = tuple(key(item) for item in values)
    if identities != tuple(sorted(identities)) or len(identities) != len(set(identities)):
        raise ValueError(f"{field_name} must be unique and canonically ordered")
    return values


def _require_digest(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value.startswith(_SHA256_PREFIX)
        or len(value) != len(_SHA256_PREFIX) + 64
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{field_name} must be a lowercase sha256 digest")


__all__ = ["validate_canonical_bundle", "validate_runtime_operation_state"]

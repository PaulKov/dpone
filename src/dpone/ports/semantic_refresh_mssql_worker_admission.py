"""Protected worker-attempt inputs and canonical MSSQL run materialization."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from dpone.contracts.dbt_semantic_refresh_plan_contracts import (
    SemanticRefreshPlanBundle,
)
from dpone.contracts.dbt_semantic_refresh_run_contracts import (
    SemanticRefreshRunExecutionBundle,
)
from dpone.contracts.semantic_refresh_attempt_binding import (
    SemanticRefreshAttemptBinding,
)
from dpone.ports.semantic_refresh_mssql_authority_models import (
    MssqlCanonicalAdmissionBundle,
    mssql_model_resource_authority_from_plan_target,
)
from dpone.ports.semantic_refresh_mssql_primitives import (
    MssqlGuardClaim,
    MssqlWorkflowResourceBudget,
    _require_text,
)

if TYPE_CHECKING:
    from dpone.contracts.semantic_refresh_workflow_replacement import (
        SemanticRefreshWorkflowReplacementPlan,
    )
    from dpone.ports.semantic_refresh_mssql import (
        MssqlAdmissionReceipt,
        MssqlAdmissionRequest,
    )
    from dpone.ports.semantic_refresh_mssql_activation import MssqlActivatedPackRegistration


@dataclass(frozen=True, order=True, slots=True)
class MssqlTrustedAttemptCoordinate:
    """Airflow worker identity for one planned operation, before fence allocation."""

    operation_id: str
    task_id: str
    try_number: int
    pod_uid: str

    def __post_init__(self) -> None:
        _require_text(self.operation_id, "operation_id")
        _require_text(self.task_id, "task_id")
        _require_text(self.pod_uid, "pod_uid")
        if isinstance(self.try_number, bool) or not isinstance(self.try_number, int) or self.try_number <= 0:
            raise ValueError("try_number must be positive")


@dataclass(frozen=True, slots=True)
class MssqlWorkerAdmissionCoordinates:
    """Protected worker attempts; MSSQL derives every run-wide authority value."""

    attempts: tuple[MssqlTrustedAttemptCoordinate, ...]

    def __post_init__(self) -> None:
        operation_ids = tuple(item.operation_id for item in self.attempts)
        if (
            not operation_ids
            or operation_ids != tuple(sorted(set(operation_ids)))
            or any(not isinstance(item, MssqlTrustedAttemptCoordinate) for item in self.attempts)
        ):
            raise ValueError("attempts must be a non-empty canonical operation closure")


class SemanticRefreshMssqlWorkerAttemptAuthorityPort(Protocol):
    """Resolve actual task/pod coordinates from protected worker runtime state."""

    def load_attempts(
        self,
        *,
        plan_bundle_sha256: str,
        workflow_execution_id: str,
        operation_ids: tuple[str, ...],
    ) -> MssqlWorkerAdmissionCoordinates:
        """Return the exact actual worker attempt closure or fail closed."""


@dataclass(frozen=True, order=True, slots=True)
class MssqlGuardEpochSnapshot:
    """One locked AVAILABLE/RELEASED guard predecessor observed by MSSQL."""

    resource_id: str
    predecessor_epoch: int

    def __post_init__(self) -> None:
        _require_text(self.resource_id, "resource_id")
        if (
            isinstance(self.predecessor_epoch, bool)
            or not isinstance(self.predecessor_epoch, int)
            or self.predecessor_epoch < 0
        ):
            raise ValueError("predecessor_epoch must be non-negative")


class SemanticRefreshMssqlAtomicWorkerAdmissionPort(Protocol):
    """Allocate fences, persist authority, and admit in one SQL transaction."""

    def admit_run(
        self,
        *,
        activated_pack: MssqlActivatedPackRegistration,
        plan_bundle: SemanticRefreshPlanBundle,
        run_execution: SemanticRefreshRunExecutionBundle,
    ) -> MssqlAdmissionReceipt:
        """Create/replay the run pack and admission without caller guard epochs."""


class SemanticRefreshMssqlReplacementAdmissionBinderPort(Protocol):
    """Bind protected failed-precommit predecessor state into admission."""

    def bind_successor(
        self,
        *,
        admission: MssqlAdmissionRequest,
        replacement_plan: SemanticRefreshWorkflowReplacementPlan,
    ) -> MssqlAdmissionRequest:
        """Return admission bound to exact durable predecessor state."""


def compose_worker_admission_bundle(
    *,
    plan_bundle: SemanticRefreshPlanBundle,
    run_execution: SemanticRefreshRunExecutionBundle,
    coordinates: MssqlWorkerAdmissionCoordinates,
    guard_epochs: tuple[MssqlGuardEpochSnapshot, ...],
) -> MssqlCanonicalAdmissionBundle:
    """Build canonical attempt/fence authority only from a locked guard snapshot."""

    _validate_run_inputs(plan_bundle, run_execution, coordinates)
    controller_id, owner_id, reservation_id = _worker_run_ids(run_execution)
    resource_budget = mssql_workflow_resource_budget(plan_bundle)
    closure = plan_bundle.run_guard_closure
    expected_resources = (closure.workflow_guard_resource_id, *closure.resource_guard_ids)
    if tuple(item.resource_id for item in guard_epochs) != tuple(sorted(expected_resources)):
        raise ValueError("locked guard epochs differ from the canonical plan closure")
    epoch_by_resource = {item.resource_id: item.predecessor_epoch for item in guard_epochs}
    target_by_model = {item.model_unique_id: item for item in plan_bundle.targets}
    coordinate_by_operation = {item.operation_id: item for item in coordinates.attempts}
    attempts = tuple(
        sorted(
            (
                SemanticRefreshAttemptBinding.build(
                    workflow_execution_id=run_execution.workflow_execution_binding.workflow_execution_id,
                    workflow_execution_binding_sha256=(
                        run_execution.workflow_execution_binding.workflow_execution_binding_sha256
                    ),
                    operation_id=operation.operation_id,
                    operation_plan_sha256=operation.operation_plan_sha256,
                    dag_run_id=run_execution.workflow_execution_binding.workflow_execution_id,
                    task_id=coordinate_by_operation[operation.operation_id].task_id,
                    try_number=coordinate_by_operation[operation.operation_id].try_number,
                    pod_uid=coordinate_by_operation[operation.operation_id].pod_uid,
                    fencing_epoch=(
                        epoch_by_resource[target_by_model[operation.model_unique_id].target_resource_id] + 1
                    ),
                    owner_id=owner_id,
                )
                for operation in plan_bundle.operation_plans
            ),
            key=lambda item: item.operation_id,
        )
    )
    resource_guards = tuple(
        MssqlGuardClaim(resource_id, epoch_by_resource[resource_id], epoch_by_resource[resource_id] + 1)
        for resource_id in closure.resource_guard_ids
    )
    workflow_epoch = epoch_by_resource[closure.workflow_guard_resource_id]
    return MssqlCanonicalAdmissionBundle(
        workflow_execution_id=run_execution.workflow_execution_binding.workflow_execution_id,
        workflow_plan=plan_bundle.workflow_plan,
        execution_binding=run_execution.workflow_execution_binding,
        operation_plans=plan_bundle.operation_plans,
        attempt_bindings=attempts,
        workflow_guard=MssqlGuardClaim(
            closure.workflow_guard_resource_id,
            workflow_epoch,
            workflow_epoch + 1,
        ),
        resource_guards=resource_guards,
        model_resources=tuple(
            mssql_model_resource_authority_from_plan_target(item)
            for item in sorted(plan_bundle.targets, key=lambda target: target.model_unique_id)
        ),
        controller_id=controller_id,
        owner_id=owner_id,
        reservation_id=reservation_id,
        resource_budget=resource_budget,
        replacement_plan=plan_bundle.workflow_replacement_plan,
    )


def validate_worker_admission_bundle(
    bundle: MssqlCanonicalAdmissionBundle,
    *,
    plan_bundle: SemanticRefreshPlanBundle,
    run_execution: SemanticRefreshRunExecutionBundle,
    coordinates: MssqlWorkerAdmissionCoordinates,
) -> None:
    """Reject replay authority that differs from the canonical worker inputs."""

    _validate_run_inputs(plan_bundle, run_execution, coordinates)
    controller_id, owner_id, reservation_id = _worker_run_ids(run_execution)
    resource_budget = mssql_workflow_resource_budget(plan_bundle)
    coordinate_by_operation = {item.operation_id: item for item in coordinates.attempts}
    if (
        bundle.workflow_execution_id != run_execution.workflow_execution_binding.workflow_execution_id
        or bundle.workflow_plan != plan_bundle.workflow_plan
        or bundle.execution_binding != run_execution.workflow_execution_binding
        or bundle.operation_plans != plan_bundle.operation_plans
        or bundle.model_resources
        != tuple(
            mssql_model_resource_authority_from_plan_target(item)
            for item in sorted(plan_bundle.targets, key=lambda target: target.model_unique_id)
        )
        or bundle.replacement_plan != plan_bundle.workflow_replacement_plan
        or bundle.controller_id != controller_id
        or bundle.owner_id != owner_id
        or bundle.reservation_id != reservation_id
        or bundle.resource_budget != resource_budget
        or bundle.workflow_guard.resource_id != plan_bundle.run_guard_closure.workflow_guard_resource_id
        or tuple(item.resource_id for item in bundle.resource_guards)
        != plan_bundle.run_guard_closure.resource_guard_ids
    ):
        raise ValueError("existing canonical admission differs from worker inputs")
    for attempt in bundle.attempt_bindings:
        coordinate = coordinate_by_operation.get(attempt.operation_id)
        if coordinate is None or (
            attempt.task_id,
            attempt.try_number,
            attempt.pod_uid,
            attempt.owner_id,
        ) != (
            coordinate.task_id,
            coordinate.try_number,
            coordinate.pod_uid,
            owner_id,
        ):
            raise ValueError("existing attempt authority differs from worker coordinates")


def _validate_run_inputs(
    plan_bundle: SemanticRefreshPlanBundle,
    run_execution: SemanticRefreshRunExecutionBundle,
    coordinates: MssqlWorkerAdmissionCoordinates,
) -> None:
    if not isinstance(plan_bundle, SemanticRefreshPlanBundle):
        raise TypeError("plan_bundle must be canonical")
    if not isinstance(run_execution, SemanticRefreshRunExecutionBundle):
        raise TypeError("run_execution must be canonical")
    if not isinstance(coordinates, MssqlWorkerAdmissionCoordinates):
        raise TypeError("coordinates must be trusted worker coordinates")
    binding = run_execution.workflow_execution_binding
    if (
        run_execution.plan_bundle_sha256 != plan_bundle.plan_bundle_sha256
        or binding.workflow_plan_sha256 != plan_bundle.workflow_plan.workflow_plan_sha256
        or binding.workflow_mode is not plan_bundle.workflow_plan.workflow_mode
    ):
        raise ValueError("run execution differs from the canonical plan bundle")
    operation_ids = tuple(item.operation_id for item in plan_bundle.operation_plans)
    if tuple(item.operation_id for item in coordinates.attempts) != operation_ids:
        raise ValueError("worker attempt coordinates differ from the operation closure")


def mssql_workflow_resource_budget(plan_bundle: SemanticRefreshPlanBundle) -> MssqlWorkflowResourceBudget:
    """Aggregate exact workflow limits only from digest-bound model policies."""

    if not isinstance(plan_bundle, SemanticRefreshPlanBundle) or not plan_bundle.targets:
        raise TypeError("plan_bundle must be a non-empty canonical plan")
    return MssqlWorkflowResourceBudget(
        max_workflow_prepared_models=len(plan_bundle.targets),
        max_workflow_sealed_extract_bytes=sum(
            item.artifact_authority.max_artifact_bytes for item in plan_bundle.targets
        ),
        max_workflow_clickhouse_staging_bytes=sum(
            item.resource_policy.max_clickhouse_staging_bytes for item in plan_bundle.targets
        ),
        max_workflow_shadow_bytes=sum(item.resource_policy.max_clickhouse_shadow_bytes for item in plan_bundle.targets),
        max_workflow_peak_bytes=sum(
            item.artifact_authority.max_artifact_bytes + item.resource_policy.max_clickhouse_total_transient_bytes
            for item in plan_bundle.targets
        ),
    )


def _worker_run_ids(run_execution: SemanticRefreshRunExecutionBundle) -> tuple[str, str, str]:
    binding = run_execution.workflow_execution_binding.workflow_execution_binding_sha256
    suffix = binding.removeprefix("sha256:")
    reservation_payload = json.dumps(
        {
            "schema": "dpone.semantic-refresh-mssql-workflow-reservation.v1",
            "workflow_execution_binding_sha256": binding,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return (
        f"semantic-refresh-controller:{suffix}",
        f"semantic-refresh-owner:{suffix}",
        "sha256:" + hashlib.sha256(reservation_payload.encode()).hexdigest(),
    )


__all__ = [
    "MssqlGuardEpochSnapshot",
    "MssqlTrustedAttemptCoordinate",
    "MssqlWorkerAdmissionCoordinates",
    "SemanticRefreshMssqlAtomicWorkerAdmissionPort",
    "SemanticRefreshMssqlReplacementAdmissionBinderPort",
    "SemanticRefreshMssqlWorkerAttemptAuthorityPort",
    "compose_worker_admission_bundle",
    "mssql_workflow_resource_budget",
    "validate_worker_admission_bundle",
]

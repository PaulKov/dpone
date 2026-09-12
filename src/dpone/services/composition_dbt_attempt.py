"""Exact composition identity and fresh parent fencing before actual dbt build.

This capability replaces native-v2 workspace admission only in an explicitly
composed parent executor. The outer CompositionWorker owns credentials and
terminal observations; this pre-build fence never creates a second attempt.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Protocol

from dpone.contracts.composition_activation import CompositionActivationOccurrence, CompositionAdmissionError
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    CompositionAttemptReceipt,
    require_composition_attempt_scope,
)
from dpone.contracts.dbt_relation_writes import selected_relation_writes
from dpone.contracts.dbt_runtime import AirflowAttemptCorrelation, AirflowRunIdentity, DbtExecutionPack
from dpone.contracts.dbt_workspace_control import dbt_relation_write_subject


class CompositionDbtAttemptReader(Protocol):
    """Fresh protected audit; readback confers no new executor permit."""

    def read_exact(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptReceipt: ...


def build_composition_dbt_attempt(
    occurrence: CompositionActivationOccurrence,
    *,
    pack: DbtExecutionPack,
    run_identity: AirflowRunIdentity,
    airflow_attempt: AirflowAttemptCorrelation,
) -> CompositionAttemptIdentity:
    """Bind workload pack and its verified execution plan as separate hashes.

    The source workload pack digest belongs to the parent membership. The dbt
    execution-pack digest binds its immutable invocation/selection plan. Both
    inputs must already have passed the existing source/artifact verifiers.
    """
    occurrence.require_state("ACTIVE")
    pack = DbtExecutionPack.from_mapping(pack.to_dict())
    run_identity = AirflowRunIdentity.from_mapping(run_identity.to_dict())
    scheduler = AirflowAttemptCorrelation.from_mapping(airflow_attempt.to_dict())
    if (run_identity.release_id, run_identity.deployment_id) != (
        occurrence.request.release_id,
        occurrence.request.deployment_id,
    ):
        raise CompositionAdmissionError("worker_parent_identity")
    workload_id = f"dbt__{pack.workflow_id}"
    workload = next((row for row in occurrence.request.workloads if row.workload_id == workload_id), None)
    if (
        workload is None
        or workload.constituent_id != "native"
        or workload.execution_cell != "sqlserver_dbt_v1"
        or run_identity.workload_pack.id != workload_id
        or run_identity.workload_pack.sha256 != workload.pack_sha256
    ):
        raise CompositionAdmissionError("worker_dbt_membership")
    guards = {
        row.guard_id
        for row in occurrence.request.resources
        if set(workload.write_subjects).intersection(row.write_subjects)
    }
    result = CompositionAttemptIdentity(
        occurrence.request.request_sha256,
        workload_id,
        "native",
        workload.pack_sha256,
        pack.pack_sha256,
        scheduler.run_id,
        scheduler.task_id,
        scheduler.try_number,
        scheduler.map_index,
        tuple(pair for pair in occurrence.receipt.guard_epochs if pair[0] in guards),
    )
    require_composition_attempt_scope(occurrence, result)
    return result


class CompositionDbtAttemptLifecycle:
    """Revalidate the real preflight writes, then independently reopen SQL fences."""

    def __init__(
        self,
        *,
        attempt: CompositionAttemptIdentity,
        occurrence: CompositionActivationOccurrence,
        pack: DbtExecutionPack,
        attempts: CompositionDbtAttemptReader,
        read_active: Callable[[], CompositionActivationOccurrence],
        read_preflight_manifest: Callable[[CompositionAttemptIdentity], Mapping[str, object]],
    ) -> None:
        require_composition_attempt_scope(occurrence, attempt)
        if attempt.plan_sha256 != pack.pack_sha256:
            raise CompositionAdmissionError("worker_plan_identity")
        self._attempt = attempt
        self._occurrence = occurrence
        self._pack = pack
        self._attempts = attempts
        self._read_active = read_active
        self._read_preflight_manifest = read_preflight_manifest

    def verify_before_build(
        self,
        *,
        pack: DbtExecutionPack,
        run_identity: AirflowRunIdentity,
        airflow_attempt: AirflowAttemptCorrelation,
    ) -> None:
        """Run after existing graph/macro preflight, immediately before dbt build."""
        if (
            pack != self._pack
            or build_composition_dbt_attempt(
                self._occurrence, pack=pack, run_identity=run_identity, airflow_attempt=airflow_attempt
            )
            != self._attempt
        ):
            raise CompositionAdmissionError("worker_plan_identity")
        manifest = self._read_preflight_manifest(self._attempt)
        writes = selected_relation_writes(project_path=pack.project_subdir, execution=pack, manifest=manifest)
        observed = tuple(sorted(dbt_relation_write_subject(write) for write in writes))
        workload = next(
            row for row in self._occurrence.request.workloads if row.workload_id == self._attempt.workload_id
        )
        if observed != workload.write_subjects:
            raise CompositionAdmissionError("worker_preflight_writes")
        active = self._read_active()
        active.require_state("ACTIVE")
        if active != self._occurrence:
            raise CompositionAdmissionError("worker_active_readback")
        require_composition_attempt_scope(active, self._attempt)
        receipt = self._attempts.read_exact(self._attempt)
        receipt.__post_init__()
        if receipt.attempt != self._attempt or receipt.state != "RUNNING":
            raise CompositionAdmissionError("worker_running_readback")

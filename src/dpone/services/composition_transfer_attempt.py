"""Exact composition identity for one ordinary PostgreSQL→MSSQL parent attempt.

This capability replaces generic `dpone run` admission only inside an explicitly
composed transfer root. The outer CompositionWorker owns credentials and
terminal observations; this helper never creates a second attempt or reads
PostgreSQL. A RUNNING replay is a rejection, never a second executor permit.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import AirflowRunIdentity
from dpone.contracts.composition_activation import CompositionActivationOccurrence, CompositionAdmissionError
from dpone.contracts.composition_execution import composition_transfer_cell
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    require_composition_attempt_scope,
)

TRANSFER_CELL = "postgres_mssql_full_refresh_v1"


def build_composition_transfer_attempt(
    occurrence: CompositionActivationOccurrence,
    *,
    manifest: Mapping[str, Any],
    plan_sha256: str,
    run_identity: AirflowRunIdentity,
    airflow_attempt: AirflowAttemptCorrelation,
) -> CompositionAttemptIdentity:
    """Bind the verified ordinary manifest to the ACTIVE parent membership."""

    occurrence.require_state("ACTIVE")
    cell = composition_transfer_cell(manifest)
    if cell != TRANSFER_CELL:
        raise CompositionAdmissionError("worker_transfer_cell")
    workload_id = str(manifest.get("name") or "")
    if (run_identity.release_id, run_identity.deployment_id) != (
        occurrence.request.release_id,
        occurrence.request.deployment_id,
    ):
        raise CompositionAdmissionError("worker_parent_identity")
    workload = next((row for row in occurrence.request.workloads if row.workload_id == workload_id), None)
    if (
        workload is None
        or workload.constituent_id != "standalone"
        or workload.execution_cell != cell
        or run_identity.workload_pack.id != workload_id
        or run_identity.workload_pack.sha256 != workload.pack_sha256
    ):
        raise CompositionAdmissionError("worker_transfer_membership")
    guards = {
        row.guard_id
        for row in occurrence.request.resources
        if set(workload.write_subjects).intersection(row.write_subjects)
    }
    result = CompositionAttemptIdentity(
        occurrence.request.request_sha256,
        workload_id,
        "standalone",
        workload.pack_sha256,
        plan_sha256,
        airflow_attempt.run_id,
        airflow_attempt.task_id,
        airflow_attempt.try_number,
        airflow_attempt.map_index,
        tuple(pair for pair in occurrence.receipt.guard_epochs if pair[0] in guards),
    )
    require_composition_attempt_scope(occurrence, result)
    return result


__all__ = ["TRANSFER_CELL", "build_composition_transfer_attempt"]

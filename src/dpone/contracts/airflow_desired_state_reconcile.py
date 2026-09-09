"""Stable evidence and activation contracts for Airflow desired-state reconcile."""

from __future__ import annotations

from dpone.contracts.airflow_desired_state_activation import (
    AIRFLOW_DESIRED_STATE_CHECKPOINT_SCHEMA,
    AIRFLOW_DESIRED_STATE_RECOVERY_SCHEMA,
    MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES,
    DesiredStateCheckpoint,
    DesiredStateRecoveryRecord,
    activated_evidence,
)
from dpone.contracts.airflow_desired_state_reconcile_evidence import (
    AIRFLOW_DESIRED_STATE_RECONCILE_SCHEMA,
    DesiredStateReconcileEvidence,
)


def activation_receipt_matches_checkpoint(
    receipt: DesiredStateReconcileEvidence,
    checkpoint: DesiredStateCheckpoint,
) -> bool:
    """Return whether one immutable receipt proves the checkpoint activation."""

    return receipt.status == "activated" and _receipt_identity(receipt) == _checkpoint_identity(checkpoint)


def _checkpoint_identity(checkpoint: DesiredStateCheckpoint) -> tuple[object, ...]:
    return (
        checkpoint.environment,
        checkpoint.desired_state_sha256,
        checkpoint.registry_scope_id,
        checkpoint.source_project,
        checkpoint.source_ref,
        checkpoint.release_id,
        checkpoint.deployment_id,
        checkpoint.occurrence_id,
        checkpoint.source_git_sha,
        checkpoint.airflow_index_sha256,
        checkpoint.runtime_image_digest,
        checkpoint.expected_dag_ids,
        checkpoint.activation_id,
    )


def _receipt_identity(receipt: DesiredStateReconcileEvidence) -> tuple[object, ...]:
    return (
        receipt.environment,
        receipt.desired_state_sha256,
        receipt.registry_scope_id,
        receipt.source_project,
        receipt.source_ref,
        receipt.release_id,
        receipt.deployment_id,
        receipt.occurrence_id,
        receipt.source_git_sha,
        receipt.airflow_index_sha256,
        receipt.runtime_image_digest,
        receipt.expected_dag_ids,
        receipt.activation_id,
    )


def unchanged_evidence(checkpoint: DesiredStateCheckpoint) -> DesiredStateReconcileEvidence:
    return _checkpoint_evidence(
        checkpoint,
        status="unchanged",
        predecessor="unchanged",
        previous_deployment_id=checkpoint.deployment_id,
    )


def recovered_evidence(checkpoint: DesiredStateCheckpoint) -> DesiredStateReconcileEvidence:
    return _checkpoint_evidence(
        checkpoint,
        status="recovered",
        predecessor="recovered",
        previous_deployment_id=None,
    )


def reconstructed_activation_receipt(
    checkpoint: DesiredStateCheckpoint,
) -> DesiredStateReconcileEvidence:
    return _checkpoint_evidence(
        checkpoint,
        status="activated",
        predecessor="recovered",
        previous_deployment_id=None,
        materialized=True,
        activated=True,
    )


def _checkpoint_evidence(
    checkpoint: DesiredStateCheckpoint,
    *,
    status: str,
    predecessor: str,
    previous_deployment_id: str | None,
    materialized: bool = False,
    activated: bool = False,
) -> DesiredStateReconcileEvidence:
    return DesiredStateReconcileEvidence(
        status=status,
        environment=checkpoint.environment,
        observed_revision=checkpoint.observed_revision.value,
        desired_state_sha256=checkpoint.desired_state_sha256,
        registry_scope_id=checkpoint.registry_scope_id,
        source_project=checkpoint.source_project,
        source_ref=checkpoint.source_ref,
        release_id=checkpoint.release_id,
        deployment_id=checkpoint.deployment_id,
        occurrence_id=checkpoint.occurrence_id,
        source_git_sha=checkpoint.source_git_sha,
        airflow_index_sha256=checkpoint.airflow_index_sha256,
        runtime_image_digest=checkpoint.runtime_image_digest,
        expected_dag_ids=checkpoint.expected_dag_ids,
        activation_id=checkpoint.activation_id,
        previous_deployment_id=previous_deployment_id,
        predecessor_status=predecessor,
        materialized=materialized,
        activated=activated,
    )


__all__ = [
    "AIRFLOW_DESIRED_STATE_CHECKPOINT_SCHEMA",
    "AIRFLOW_DESIRED_STATE_RECOVERY_SCHEMA",
    "AIRFLOW_DESIRED_STATE_RECONCILE_SCHEMA",
    "MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES",
    "DesiredStateCheckpoint",
    "DesiredStateReconcileEvidence",
    "DesiredStateRecoveryRecord",
    "activated_evidence",
    "activation_receipt_matches_checkpoint",
    "reconstructed_activation_receipt",
    "recovered_evidence",
    "unchanged_evidence",
]

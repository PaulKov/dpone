"""Durable ordering and crash recovery for Airflow desired-state activation."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.airflow_desired_state import (
    MAX_AIRFLOW_DESIRED_STATE_BYTES,
    AirflowDesiredDeployment,
    AirflowDesiredStateError,
)
from dpone.contracts.airflow_desired_state_reconcile import (
    MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES,
    DesiredStateCheckpoint,
    DesiredStateReconcileEvidence,
    DesiredStateRecoveryRecord,
    activated_evidence,
    activation_receipt_matches_checkpoint,
    reconstructed_activation_receipt,
    recovered_evidence,
)
from dpone.ports.airflow_desired_state import (
    ActiveDesiredDeployment,
    AirflowDesiredStateReader,
    AirflowDesiredStateSnapshotStore,
    DesiredDeploymentActivator,
    DesiredDeploymentMaterializer,
    DesiredStateActivationReceiptStore,
    DesiredStateCheckpointStore,
    DesiredStatePortError,
    DesiredStateReadResult,
    DesiredStateReadStatus,
    DesiredStateReconcilePortError,
    DesiredStateRecoveryRecordStore,
)


class DesiredStateReconcileError(RuntimeError):
    """A watcher cycle failed without exposing backend or credential details."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        state_may_have_changed: bool = False,
    ) -> None:
        self.code = code
        self.state_may_have_changed = state_may_have_changed
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True, slots=True)
class DesiredStateReconcileRequest:
    """Trusted authority selected for one watcher reconciliation cycle."""

    environment: str
    registry_scope_id: str
    source_project: str
    source_ref: str


class DesiredStateControlPlane:
    """Coordinate durable control records around one cache activation."""

    def __init__(
        self,
        *,
        snapshot_store: AirflowDesiredStateSnapshotStore,
        checkpoint_store: DesiredStateCheckpointStore,
        activation_receipt_store: DesiredStateActivationReceiptStore,
        recovery_record_store: DesiredStateRecoveryRecordStore,
    ) -> None:
        self._snapshot_store = snapshot_store
        self._checkpoint_store = checkpoint_store
        self._activation_receipt_store = activation_receipt_store
        self._recovery_record_store = recovery_record_store

    def read_checkpoint(
        self,
        request: DesiredStateReconcileRequest,
    ) -> DesiredStateCheckpoint | None:
        try:
            checkpoint = self._checkpoint_store.read()
        except (OSError, ValueError, DesiredStateReconcilePortError) as exc:
            raise DesiredStateReconcileError(
                getattr(
                    exc,
                    "code",
                    "DPONE_AIRFLOW_DESIRED_STATE_CHECKPOINT_INVALID",
                ),
                "desired-state checkpoint is unreadable or invalid",
            ) from exc
        if checkpoint is not None and checkpoint.environment != request.environment:
            raise DesiredStateReconcileError(
                "DPONE_AIRFLOW_DESIRED_STATE_ENVIRONMENT_MISMATCH",
                "desired-state checkpoint belongs to another environment",
            )
        if checkpoint is not None and checkpoint.registry_scope_id != request.registry_scope_id:
            raise DesiredStateReconcileError(
                "DPONE_ARTIFACT_REGISTRY_SCOPE_MISMATCH",
                "desired-state checkpoint belongs to another registry authority",
            )
        if checkpoint is not None and (
            checkpoint.source_project != request.source_project or checkpoint.source_ref != request.source_ref
        ):
            raise DesiredStateReconcileError(
                "DPONE_AIRFLOW_DESIRED_STATE_SOURCE_MISMATCH",
                "desired-state checkpoint belongs to another protected source",
            )
        return checkpoint

    def recover(
        self,
        *,
        request: DesiredStateReconcileRequest,
        checkpoint: DesiredStateCheckpoint | None,
        current: ActiveDesiredDeployment | None,
    ) -> DesiredStateReconcileEvidence | None:
        if current is None:
            return None
        if checkpoint is not None and checkpoint_is_current(checkpoint, current):
            receipt = self.read_receipt(current.activation_id)
            if receipt is not None:
                require_receipt_matches_checkpoint(receipt, checkpoint)
                return None
            if not self.snapshot_matches(checkpoint):
                return None
            self.commit_receipt(
                reconstructed_activation_receipt(checkpoint),
                state_may_have_changed=False,
            )
            return recovered_evidence(checkpoint)
        record = self._read_recovery_record()
        if record is None:
            raise DesiredStateReconcileError(
                "DPONE_AIRFLOW_DESIRED_STATE_RECOVERY_RECORD_MISSING",
                "active deployment has no durable pre-activation recovery record",
                state_may_have_changed=True,
            )
        desired = record.desired
        if not _recovery_matches(request=request, current=current, record=record):
            raise DesiredStateReconcileError(
                "DPONE_AIRFLOW_DESIRED_STATE_RECOVERY_MISMATCH",
                "pre-activation recovery record does not prove the active deployment",
                state_may_have_changed=True,
            )
        recovered = DesiredStateCheckpoint.from_desired(
            desired,
            observed_revision=record.observed_revision,
            activation_id=current.activation_id,
        )
        self.commit_snapshot(desired.to_json_bytes())
        receipt = self.read_receipt(current.activation_id)
        if receipt is None:
            self.commit_receipt(
                reconstructed_activation_receipt(recovered),
                state_may_have_changed=False,
            )
        else:
            require_receipt_matches_checkpoint(receipt, recovered)
        self.commit_checkpoint(recovered, state_may_have_changed=False)
        return recovered_evidence(recovered)

    def stage(
        self,
        record: DesiredStateRecoveryRecord,
        body: bytes,
    ) -> None:
        require_control_records_fit(record)
        try:
            self._recovery_record_store.commit(record)
        except (OSError, ValueError, DesiredStateReconcilePortError) as exc:
            raise DesiredStateReconcileError(
                getattr(
                    exc,
                    "code",
                    "DPONE_AIRFLOW_DESIRED_STATE_RECOVERY_RECORD_FAILED",
                ),
                "pre-activation recovery record could not be committed",
            ) from exc
        self.commit_snapshot(body)

    def snapshot_matches(self, checkpoint: DesiredStateCheckpoint) -> bool:
        try:
            body = self._snapshot_store.read(max_bytes=MAX_AIRFLOW_DESIRED_STATE_BYTES)
        except (OSError, ValueError, DesiredStateReconcilePortError) as exc:
            raise DesiredStateReconcileError(
                getattr(
                    exc,
                    "code",
                    "DPONE_AIRFLOW_DESIRED_STATE_SNAPSHOT_INVALID",
                ),
                "local desired-state snapshot could not be validated",
            ) from exc
        if body is None:
            return False
        try:
            desired = AirflowDesiredDeployment.from_json(body)
        except AirflowDesiredStateError:
            return False
        return desired.sha256 == checkpoint.desired_state_sha256

    def commit_snapshot(self, body: bytes) -> None:
        try:
            self._snapshot_store.commit(body)
        except (OSError, ValueError, DesiredStateReconcilePortError) as exc:
            raise DesiredStateReconcileError(
                getattr(
                    exc,
                    "code",
                    "DPONE_AIRFLOW_DESIRED_STATE_LOCAL_COMMIT_FAILED",
                ),
                "verified desired state could not be committed locally",
            ) from exc

    def commit_checkpoint(
        self,
        checkpoint: DesiredStateCheckpoint,
        *,
        state_may_have_changed: bool,
    ) -> None:
        try:
            self._checkpoint_store.commit(checkpoint)
        except (OSError, ValueError, DesiredStateReconcilePortError) as exc:
            raise DesiredStateReconcileError(
                getattr(
                    exc,
                    "code",
                    "DPONE_AIRFLOW_DESIRED_STATE_CHECKPOINT_FAILED",
                ),
                "active desired deployment checkpoint could not be committed",
                state_may_have_changed=state_may_have_changed,
            ) from exc

    def commit_receipt(
        self,
        evidence: DesiredStateReconcileEvidence,
        *,
        state_may_have_changed: bool,
    ) -> None:
        try:
            self._activation_receipt_store.commit(evidence)
        except (OSError, ValueError, DesiredStateReconcilePortError) as exc:
            raise DesiredStateReconcileError(
                getattr(
                    exc,
                    "code",
                    "DPONE_AIRFLOW_DESIRED_STATE_RECEIPT_FAILED",
                ),
                "active deployment receipt could not be committed",
                state_may_have_changed=state_may_have_changed,
            ) from exc

    def read_receipt(
        self,
        activation_id: str,
    ) -> DesiredStateReconcileEvidence | None:
        try:
            return self._activation_receipt_store.read(activation_id)
        except (OSError, ValueError, DesiredStateReconcilePortError) as exc:
            raise DesiredStateReconcileError(
                getattr(
                    exc,
                    "code",
                    "DPONE_AIRFLOW_DESIRED_STATE_RECEIPT_INVALID",
                ),
                "active deployment receipt could not be validated",
            ) from exc

    def receipt_matches(self, checkpoint: DesiredStateCheckpoint) -> bool:
        receipt = self.read_receipt(checkpoint.activation_id)
        if receipt is None:
            return False
        require_receipt_matches_checkpoint(receipt, checkpoint)
        return True

    def _read_recovery_record(self) -> DesiredStateRecoveryRecord | None:
        try:
            return self._recovery_record_store.read()
        except (OSError, ValueError, DesiredStateReconcilePortError) as exc:
            raise DesiredStateReconcileError(
                getattr(
                    exc,
                    "code",
                    "DPONE_AIRFLOW_DESIRED_STATE_RECOVERY_RECORD_INVALID",
                ),
                "pre-activation recovery record could not be validated",
            ) from exc


def current_is_desired(
    current: ActiveDesiredDeployment | None,
    *,
    release_id: str,
    deployment_id: str,
    attestation_ref: str,
) -> bool:
    return (
        current is not None
        and current.release_id == release_id
        and current.deployment_id == deployment_id
        and current.attestation_ref == attestation_ref
    )


def checkpoint_is_current(
    checkpoint: DesiredStateCheckpoint,
    current: ActiveDesiredDeployment | None,
) -> bool:
    return (
        current is not None
        and current.release_id == checkpoint.release_id
        and current.deployment_id == checkpoint.deployment_id
        and current.activation_id == checkpoint.activation_id
        and current.attestation_ref == checkpoint.desired_state_sha256
    )


def require_receipt_matches_checkpoint(
    receipt: DesiredStateReconcileEvidence,
    checkpoint: DesiredStateCheckpoint,
) -> None:
    if not activation_receipt_matches_checkpoint(receipt, checkpoint):
        raise DesiredStateReconcileError(
            "DPONE_AIRFLOW_DESIRED_STATE_RECEIPT_MISMATCH",
            "activation receipt does not match the active deployment checkpoint",
        )


def require_control_records_fit(
    record: DesiredStateRecoveryRecord,
) -> None:
    desired = record.desired
    revision = record.observed_revision
    checkpoint = DesiredStateCheckpoint.from_desired(
        desired,
        observed_revision=revision,
        activation_id="00000000-0000-4000-8000-000000000000",
    )
    receipt = activated_evidence(
        desired,
        revision=revision,
        activation_id=checkpoint.activation_id,
        previous_deployment_id=desired.promotion.deployment_id,
        predecessor="continuous",
    )
    records = (
        record.to_json_bytes(),
        checkpoint.to_json_bytes(),
        receipt.to_json_bytes(),
    )
    if max(map(len, records)) > MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES:
        raise DesiredStateReconcileError(
            "DPONE_AIRFLOW_DESIRED_STATE_CONTROL_TOO_LARGE",
            "desired state cannot produce bounded recovery evidence",
        )


def _recovery_matches(
    *,
    request: DesiredStateReconcileRequest,
    current: ActiveDesiredDeployment,
    record: DesiredStateRecoveryRecord,
) -> bool:
    desired = record.desired
    return (
        desired.environment == request.environment
        and desired.promotion.registry_scope_id == request.registry_scope_id
        and desired.source.project == request.source_project
        and desired.source.ref == request.source_ref
        and current_is_desired(
            current,
            release_id=desired.promotion.release_id,
            deployment_id=desired.promotion.deployment_id,
            attestation_ref=desired.sha256,
        )
    )


__all__ = [
    "DesiredStateControlPlane",
    "DesiredStateReconcileError",
    "DesiredStateReconcileRequest",
    "ActiveDesiredDeployment",
    "AirflowDesiredStateReader",
    "AirflowDesiredStateSnapshotStore",
    "DesiredDeploymentActivator",
    "DesiredDeploymentMaterializer",
    "DesiredStateActivationReceiptStore",
    "DesiredStateCheckpointStore",
    "DesiredStatePortError",
    "DesiredStateReadResult",
    "DesiredStateReadStatus",
    "DesiredStateReconcilePortError",
    "DesiredStateRecoveryRecordStore",
    "checkpoint_is_current",
    "current_is_desired",
    "require_receipt_matches_checkpoint",
]

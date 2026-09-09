"""One locked, connector-neutral Airflow desired-state reconciliation cycle."""

from __future__ import annotations

from collections.abc import Callable

from dpone.contracts.airflow_desired_state import (
    MAX_AIRFLOW_DESIRED_STATE_BYTES,
    AirflowDesiredDeployment,
    AirflowDesiredStateError,
    DesiredStateRevision,
)
from dpone.contracts.airflow_desired_state_reconcile import (
    DesiredStateCheckpoint,
    DesiredStateReconcileEvidence,
    DesiredStateRecoveryRecord,
    activated_evidence,
    reconstructed_activation_receipt,
    recovered_evidence,
    unchanged_evidence,
)
from dpone.ports.airflow_desired_state import DesiredStateActivationResult
from dpone.services.airflow_desired_state_control import (
    ActiveDesiredDeployment,
    AirflowDesiredStateReader,
    AirflowDesiredStateSnapshotStore,
    DesiredDeploymentActivator,
    DesiredDeploymentMaterializer,
    DesiredStateActivationReceiptStore,
    DesiredStateCheckpointStore,
    DesiredStateControlPlane,
    DesiredStatePortError,
    DesiredStateReadResult,
    DesiredStateReadStatus,
    DesiredStateReconcileError,
    DesiredStateReconcilePortError,
    DesiredStateReconcileRequest,
    DesiredStateRecoveryRecordStore,
    checkpoint_is_current,
    current_is_desired,
    require_receipt_matches_checkpoint,
)


class DesiredStateTransitionError(RuntimeError):
    """A desired-state transition violates one deterministic contract."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


class AirflowDesiredStateReconciler:
    """Fetch, materialize and activate one exact desired deployment."""

    def __init__(
        self,
        *,
        reader: AirflowDesiredStateReader,
        snapshot_writer: AirflowDesiredStateSnapshotStore,
        checkpoint_store: DesiredStateCheckpointStore,
        activation_receipt_store: DesiredStateActivationReceiptStore,
        recovery_record_store: DesiredStateRecoveryRecordStore,
        materializer: DesiredDeploymentMaterializer,
        activator: DesiredDeploymentActivator,
        success_status_committer: Callable[[DesiredStateReconcileEvidence], None] | None = None,
    ) -> None:
        self._reader = reader
        self._control = DesiredStateControlPlane(
            snapshot_store=snapshot_writer,
            checkpoint_store=checkpoint_store,
            activation_receipt_store=activation_receipt_store,
            recovery_record_store=recovery_record_store,
        )
        self._materializer = materializer
        self._activator = activator
        self._success_status_committer = success_status_committer

    def reconcile(
        self,
        request: DesiredStateReconcileRequest,
    ) -> DesiredStateReconcileEvidence:
        checkpoint = self._control.read_checkpoint(request)
        current = self._current(request.environment)
        recovered = self._recover_under_current_fence(
            request=request,
            checkpoint=checkpoint,
            current=current,
        )
        if recovered is not None:
            return recovered
        remote = self._read_remote(None if checkpoint is None else checkpoint.observed_revision)
        if remote.status is DesiredStateReadStatus.UNCHANGED:
            assert checkpoint is not None
            if (
                checkpoint_is_current(checkpoint, current)
                and self._control.receipt_matches(checkpoint)
                and self._control.snapshot_matches(checkpoint)
            ):
                assert current is not None
                return self._commit_current_success(
                    current,
                    environment=request.environment,
                    evidence=unchanged_evidence(checkpoint),
                    commit_control_records=lambda: None,
                )
            remote = self._read_remote(None)
        try:
            desired, revision, body = validated_desired(
                remote,
                environment=request.environment,
                registry_scope_id=request.registry_scope_id,
                source_project=request.source_project,
                source_ref=request.source_ref,
            )
        except DesiredStateTransitionError as exc:
            raise DesiredStateReconcileError(exc.code, str(exc)) from exc
        self._control.stage(
            DesiredStateRecoveryRecord(desired, revision),
            body,
        )
        try:
            if current_is_desired(
                current,
                release_id=desired.promotion.release_id,
                deployment_id=desired.promotion.deployment_id,
                attestation_ref=desired.sha256,
            ):
                assert current is not None
                recovered_checkpoint = DesiredStateCheckpoint.from_desired(
                    desired,
                    observed_revision=revision,
                    activation_id=current.activation_id,
                )
                receipt = self._control.read_receipt(current.activation_id)
                recovered = recovered_evidence(recovered_checkpoint)

                def commit_recovered_records() -> None:
                    recovered_receipt = receipt
                    if recovered_receipt is None:
                        recovered_receipt = reconstructed_activation_receipt(recovered_checkpoint)
                        self._control.commit_receipt(
                            recovered_receipt,
                            state_may_have_changed=False,
                        )
                    require_receipt_matches_checkpoint(recovered_receipt, recovered_checkpoint)
                    self._control.commit_checkpoint(
                        recovered_checkpoint,
                        state_may_have_changed=False,
                    )

                return self._commit_current_success(
                    current,
                    environment=request.environment,
                    evidence=recovered,
                    commit_control_records=commit_recovered_records,
                )
            predecessor = predecessor_status(checkpoint, current, desired)
            self._materializer.materialize(desired)
            committed: list[DesiredStateReconcileEvidence] = []

            def commit_activation(activation: DesiredStateActivationResult) -> None:
                activated_checkpoint = DesiredStateCheckpoint.from_desired(
                    desired,
                    observed_revision=revision,
                    activation_id=activation.activation_id,
                )
                evidence = activated_evidence(
                    desired,
                    revision=revision,
                    activation_id=activation.activation_id,
                    previous_deployment_id=activation.previous_deployment_id,
                    predecessor=predecessor,
                )
                self._control.commit_receipt(evidence, state_may_have_changed=True)
                self._control.commit_checkpoint(activated_checkpoint, state_may_have_changed=True)
                if self._success_status_committer is not None:
                    self._success_status_committer(evidence)
                committed.append(evidence)

            activation = self._activator.activate(
                desired,
                expected_current_deployment_id=(None if current is None else current.deployment_id),
                remote_precondition=lambda: self._remote_revision_is_current(revision),
                post_activation_commit=commit_activation,
            )
        except DesiredStateReconcilePortError as exc:
            raise DesiredStateReconcileError(
                exc.code,
                "desired deployment could not be materialized or activated",
                state_may_have_changed=exc.state_may_have_changed,
            ) from exc
        if not committed or committed[0].activation_id != activation.activation_id:
            raise DesiredStateReconcileError(
                "DPONE_AIRFLOW_DESIRED_STATE_COMMIT_MISSING",
                "cache activation completed without its exact durable control records",
                state_may_have_changed=True,
            )
        return committed[0]

    def _recover_under_current_fence(
        self,
        *,
        request: DesiredStateReconcileRequest,
        checkpoint: DesiredStateCheckpoint | None,
        current: ActiveDesiredDeployment | None,
    ) -> DesiredStateReconcileEvidence | None:
        if current is None:
            return self._control.recover(request=request, checkpoint=checkpoint, current=None)
        recovered: list[DesiredStateReconcileEvidence] = []

        def commit_recovery() -> None:
            evidence = self._control.recover(
                request=request,
                checkpoint=checkpoint,
                current=current,
            )
            if evidence is None:
                return
            if self._success_status_committer is not None:
                self._success_status_committer(evidence)
            recovered.append(evidence)

        self._commit_if_current(current, environment=request.environment, commit=commit_recovery)
        return recovered[0] if recovered else None

    def _commit_current_success(
        self,
        current: ActiveDesiredDeployment,
        *,
        environment: str,
        evidence: DesiredStateReconcileEvidence,
        commit_control_records: Callable[[], None],
    ) -> DesiredStateReconcileEvidence:
        def commit() -> None:
            commit_control_records()
            if self._success_status_committer is not None:
                self._success_status_committer(evidence)

        self._commit_if_current(current, environment=environment, commit=commit)
        return evidence

    def _commit_if_current(
        self,
        current: ActiveDesiredDeployment,
        *,
        environment: str,
        commit: Callable[[], None],
    ) -> None:
        try:
            self._activator.commit_if_current(
                current,
                environment=environment,
                post_validation_commit=commit,
            )
        except DesiredStateReconcilePortError as exc:
            raise DesiredStateReconcileError(
                exc.code,
                "active deployment changed before control evidence commit",
                state_may_have_changed=exc.state_may_have_changed,
            ) from exc

    def _current(self, environment: str) -> ActiveDesiredDeployment | None:
        try:
            return self._activator.current(environment=environment)
        except DesiredStateReconcilePortError as exc:
            raise DesiredStateReconcileError(
                exc.code,
                "active desired deployment could not be validated",
                state_may_have_changed=exc.state_may_have_changed,
            ) from exc

    def _read_remote(
        self,
        revision: DesiredStateRevision | None,
    ) -> DesiredStateReadResult:
        try:
            result = self._reader.read(
                max_bytes=MAX_AIRFLOW_DESIRED_STATE_BYTES,
                if_changed_from=revision,
            )
        except DesiredStatePortError as exc:
            raise DesiredStateReconcileError(
                "DPONE_AIRFLOW_DESIRED_STATE_UNAVAILABLE",
                "desired state could not be fetched",
            ) from exc
        if result.status is DesiredStateReadStatus.ABSENT:
            raise DesiredStateReconcileError(
                "DPONE_AIRFLOW_DESIRED_STATE_NOT_FOUND",
                "desired state is not present",
            )
        return result

    def _remote_revision_is_current(self, revision: DesiredStateRevision) -> bool:
        try:
            result = self._reader.read(
                max_bytes=MAX_AIRFLOW_DESIRED_STATE_BYTES,
                if_changed_from=revision,
            )
        except DesiredStatePortError as exc:
            raise DesiredStateReconcileError(
                "DPONE_AIRFLOW_DESIRED_STATE_PRECOMMIT_UNAVAILABLE",
                "desired state could not be rechecked before activation",
                state_may_have_changed=True,
            ) from exc
        return result.status is DesiredStateReadStatus.UNCHANGED


def validated_desired(
    result: DesiredStateReadResult,
    *,
    environment: str,
    registry_scope_id: str,
    source_project: str,
    source_ref: str,
) -> tuple[AirflowDesiredDeployment, DesiredStateRevision, bytes]:
    if result.status is not DesiredStateReadStatus.PRESENT or result.body is None or result.revision is None:
        raise DesiredStateTransitionError(
            "DPONE_AIRFLOW_DESIRED_STATE_INTEGRITY",
            "desired-state read result is incomplete",
        )
    try:
        desired = AirflowDesiredDeployment.from_json(result.body)
    except AirflowDesiredStateError as exc:
        raise DesiredStateTransitionError(
            "DPONE_AIRFLOW_DESIRED_STATE_INTEGRITY",
            "desired state is invalid",
        ) from exc
    if desired.environment != environment:
        raise DesiredStateTransitionError(
            "DPONE_AIRFLOW_DESIRED_STATE_ENVIRONMENT_MISMATCH",
            "desired state belongs to another environment",
        )
    if desired.promotion.registry_scope_id != registry_scope_id:
        raise DesiredStateTransitionError(
            "DPONE_ARTIFACT_REGISTRY_SCOPE_MISMATCH",
            "desired state selects another artifact registry authority",
        )
    if desired.source.project != source_project or desired.source.ref != source_ref:
        raise DesiredStateTransitionError(
            "DPONE_AIRFLOW_DESIRED_STATE_SOURCE_MISMATCH",
            "desired state selects another protected source",
        )
    return desired, result.revision, result.body


def predecessor_status(
    checkpoint: DesiredStateCheckpoint | None,
    current: ActiveDesiredDeployment | None,
    desired: AirflowDesiredDeployment,
) -> str:
    if checkpoint is None:
        if current is not None:
            raise _predecessor_mismatch("active deployment has no trusted checkpoint")
        return "bootstrap"
    if current is None and desired.sha256 == checkpoint.desired_state_sha256:
        return "continuous"
    if not checkpoint_is_current(checkpoint, current):
        raise _predecessor_mismatch("active deployment and trusted checkpoint diverge")
    if desired.sha256 == checkpoint.desired_state_sha256:
        return "continuous"
    if (
        desired.previous.revision == checkpoint.observed_revision
        and desired.previous.deployment_id == checkpoint.deployment_id
    ):
        return "continuous"
    return "skipped"


def _predecessor_mismatch(message: str) -> DesiredStateReconcileError:
    return DesiredStateReconcileError(
        "DPONE_AIRFLOW_DESIRED_STATE_PREDECESSOR_MISMATCH",
        message,
    )


__all__ = [
    "AirflowDesiredStateReconciler",
    "DesiredStateReconcileError",
    "DesiredStateReconcileRequest",
    "activated_evidence",
    "reconstructed_activation_receipt",
    "recovered_evidence",
]

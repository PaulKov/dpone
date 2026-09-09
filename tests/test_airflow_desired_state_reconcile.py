from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path

import pytest

from dpone.adapters.airflow_desired_state_snapshot import (
    AtomicAirflowDesiredStateSnapshotWriter,
)
from dpone.contracts.airflow_desired_state import (
    AirflowDesiredDeployment,
    DesiredStateRevision,
)
from dpone.contracts.airflow_desired_state_reconcile import (
    DesiredStateCheckpoint,
    DesiredStateReconcileEvidence,
    DesiredStateRecoveryRecord,
)
from dpone.ports.airflow_desired_state import DesiredStatePortError, DesiredStateReadResult
from dpone.ports.airflow_desired_state_reconcile import (
    ActiveDesiredDeployment,
    DesiredStateActivationResult,
    DesiredStateReconcilePortError,
)
from dpone.services.airflow_desired_state_reconcile import (
    AirflowDesiredStateReconciler,
    DesiredStateReconcileError,
    DesiredStateReconcileRequest,
    reconstructed_activation_receipt,
)

_DIGESTS = tuple("sha256:" + char * 64 for char in "abcde")
_REVISION = DesiredStateRevision('"etag-1"')
_ACTIVATION_ID = "123e4567-e89b-42d3-a456-426614174001"
_ACTIVATION_IDS = (
    _ACTIVATION_ID,
    "223e4567-e89b-42d3-a456-426614174001",
    "323e4567-e89b-42d3-a456-426614174001",
)


def _desired() -> AirflowDesiredDeployment:
    return AirflowDesiredDeployment.from_mapping(
        {
            "schema": "dpone.airflow-desired-deployment.v1",
            "environment": "dev",
            "source": {
                "project": "group/repository",
                "ref": "master",
                "pipeline_id": "1",
                "job_id": "2",
                "occurrence_id": "123e4567-e89b-42d3-a456-426614174000",
                "git_sha": "1" * 40,
            },
            "promotion": {
                "registry_scope_id": _DIGESTS[4],
                "release_id": _DIGESTS[0],
                "deployment_id": _DIGESTS[1],
                "airflow_index_sha256": _DIGESTS[2],
                "runtime_image_digest": _DIGESTS[3],
                "expected_dag_ids": ["DAG__platform__smoke__run"],
                "publication_evidence_sha256": _DIGESTS[4],
            },
            "previous": {"revision": None, "deployment_id": None},
            "promoted_at": "2026-07-28T10:00:00Z",
        }
    )


def _successor(
    previous: AirflowDesiredDeployment,
    *,
    occurrence_id: str,
    deployment_id: str,
    previous_revision: DesiredStateRevision,
) -> AirflowDesiredDeployment:
    payload = previous.to_dict()
    payload["source"] = {
        **dict(payload["source"]),
        "occurrence_id": occurrence_id,
    }
    payload["promotion"] = {
        **dict(payload["promotion"]),
        "deployment_id": deployment_id,
    }
    payload["previous"] = {
        "revision": previous_revision.value,
        "deployment_id": previous.promotion.deployment_id,
    }
    return AirflowDesiredDeployment.from_mapping(payload)


class _Reader:
    def __init__(self, *results: DesiredStateReadResult | DesiredStatePortError) -> None:
        self.results = list(results)
        self.revisions: list[DesiredStateRevision | None] = []

    def read(
        self,
        *,
        if_changed_from: DesiredStateRevision | None = None,
        **_: object,
    ) -> DesiredStateReadResult:
        self.revisions.append(if_changed_from)
        result = self.results.pop(0)
        if isinstance(result, DesiredStatePortError):
            raise result
        return result


@dataclass
class _CheckpointStore:
    checkpoint: DesiredStateCheckpoint | None = None
    fail_commit: bool = False

    def read(self) -> DesiredStateCheckpoint | None:
        return self.checkpoint

    def commit(self, checkpoint: DesiredStateCheckpoint) -> None:
        if self.fail_commit:
            raise OSError("disk full")
        self.checkpoint = checkpoint


class _Materializer:
    def __init__(self) -> None:
        self.desired: list[AirflowDesiredDeployment] = []

    def materialize(self, desired: AirflowDesiredDeployment) -> None:
        self.desired.append(desired)


@dataclass
class _ReceiptStore:
    evidence: DesiredStateReconcileEvidence | None = None
    fail_commit: bool = False
    commits: int = 0
    receipts: dict[str, DesiredStateReconcileEvidence] | None = None

    def __post_init__(self) -> None:
        self.receipts = {}
        if self.evidence is not None:
            self.receipts[self.evidence.activation_id] = self.evidence

    def read(self, activation_id: str) -> DesiredStateReconcileEvidence | None:
        assert self.receipts is not None
        return self.receipts.get(activation_id)

    def commit(self, evidence: DesiredStateReconcileEvidence) -> None:
        if self.fail_commit:
            raise OSError("disk full")
        self.commits += 1
        self.evidence = evidence
        assert self.receipts is not None
        self.receipts[evidence.activation_id] = evidence


@dataclass
class _RecoveryStore:
    record: DesiredStateRecoveryRecord | None = None
    fail_commit: bool = False

    def read(self) -> DesiredStateRecoveryRecord | None:
        return self.record

    def commit(self, record: DesiredStateRecoveryRecord) -> None:
        if self.fail_commit:
            raise OSError("disk full")
        self.record = record


class _Activator:
    def __init__(
        self,
        current: ActiveDesiredDeployment | None = None,
    ) -> None:
        self.active = current
        self.activations = 0
        self._activation_offset = 0 if current is None else 1
        self.before_commit_fence = None

    def current(self, *, environment: str) -> ActiveDesiredDeployment | None:
        assert environment == "dev"
        return self.active

    def commit_if_current(
        self,
        expected: ActiveDesiredDeployment,
        *,
        environment: str,
        post_validation_commit,
    ) -> None:
        assert environment == "dev"
        if self.before_commit_fence is not None:
            self.before_commit_fence()
            self.before_commit_fence = None
        if self.active != expected:
            raise DesiredStateReconcilePortError(
                "DPONE_AIRFLOW_DESIRED_STATE_SUPERSEDED",
                "active deployment changed before control evidence commit",
                state_may_have_changed=True,
            )
        post_validation_commit()

    def activate(
        self,
        desired: AirflowDesiredDeployment,
        *,
        expected_current_deployment_id: str | None,
        remote_precondition,
        post_activation_commit,
    ) -> DesiredStateActivationResult:
        assert expected_current_deployment_id == (None if self.active is None else self.active.deployment_id)
        if not remote_precondition():
            raise DesiredStateReconcilePortError(
                "DPONE_AIRFLOW_DESIRED_STATE_SUPERSEDED",
                "desired state changed before activation",
            )
        previous = None if self.active is None else self.active.deployment_id
        activation_id = _ACTIVATION_IDS[self._activation_offset + self.activations]
        self.activations += 1
        self.active = ActiveDesiredDeployment(
            release_id=desired.promotion.release_id,
            deployment_id=desired.promotion.deployment_id,
            activation_id=activation_id,
            attestation_ref=desired.sha256,
        )
        result = DesiredStateActivationResult(
            activation_id=activation_id,
            previous_deployment_id=previous,
        )
        post_activation_commit(result)
        return result


def _service(
    tmp_path: Path,
    *,
    reader: _Reader,
    checkpoint_store: _CheckpointStore,
    materializer: _Materializer,
    activator: _Activator,
    receipt_store: _ReceiptStore | None = None,
    recovery_store: _RecoveryStore | None = None,
    success_status_committer=None,
) -> AirflowDesiredStateReconciler:
    return AirflowDesiredStateReconciler(
        reader=reader,
        snapshot_writer=AtomicAirflowDesiredStateSnapshotWriter(tmp_path / "desired.json"),
        checkpoint_store=checkpoint_store,
        activation_receipt_store=receipt_store or _ReceiptStore(),
        recovery_record_store=recovery_store or _RecoveryStore(),
        materializer=materializer,
        activator=activator,
        success_status_committer=success_status_committer,
    )


def test_reconcile_materializes_activates_and_checkpoints_exact_occurrence(
    tmp_path: Path,
) -> None:
    desired = _desired()
    reader = _Reader(
        DesiredStateReadResult.present(desired.to_json_bytes(), _REVISION),
        DesiredStateReadResult.unchanged(_REVISION),
    )
    checkpoints = _CheckpointStore()
    materializer = _Materializer()
    activator = _Activator()

    evidence = _service(
        tmp_path,
        reader=reader,
        checkpoint_store=checkpoints,
        materializer=materializer,
        activator=activator,
    ).reconcile(
        DesiredStateReconcileRequest(
            environment="dev",
            registry_scope_id=_DIGESTS[4],
            source_project="group/repository",
            source_ref="master",
        )
    )

    assert evidence.status == "activated"
    assert evidence.desired_state_sha256 == desired.sha256
    assert (
        DesiredStateReconcileEvidence.from_json_bytes(json.dumps(evidence.to_dict(), sort_keys=True).encode("utf-8"))
        == evidence
    )
    assert materializer.desired == [desired]
    assert activator.activations == 1
    assert checkpoints.checkpoint is not None
    assert checkpoints.checkpoint.observed_revision == _REVISION
    assert (tmp_path / "desired.json").read_bytes() == desired.to_json_bytes()


def test_precommit_read_failure_reports_possible_local_snapshot_mutation(
    tmp_path: Path,
) -> None:
    desired = _desired()
    reader = _Reader(
        DesiredStateReadResult.present(desired.to_json_bytes(), _REVISION),
        DesiredStatePortError("remote read failed"),
    )
    checkpoints = _CheckpointStore()
    materializer = _Materializer()
    activator = _Activator()

    with pytest.raises(DesiredStateReconcileError) as exc:
        _service(
            tmp_path,
            reader=reader,
            checkpoint_store=checkpoints,
            materializer=materializer,
            activator=activator,
        ).reconcile(
            DesiredStateReconcileRequest(
                environment="dev",
                registry_scope_id=_DIGESTS[4],
                source_project="group/repository",
                source_ref="master",
            )
        )

    assert exc.value.code == "DPONE_AIRFLOW_DESIRED_STATE_PRECOMMIT_UNAVAILABLE"
    assert exc.value.state_may_have_changed is True
    assert materializer.desired == [desired]
    assert activator.activations == 0
    assert checkpoints.checkpoint is None


def test_reconcile_skips_remote_payload_only_when_checkpoint_matches_current(
    tmp_path: Path,
) -> None:
    desired = _desired()
    AtomicAirflowDesiredStateSnapshotWriter(tmp_path / "desired.json").commit(desired.to_json_bytes())
    checkpoint = DesiredStateCheckpoint.from_desired(
        desired,
        observed_revision=_REVISION,
        activation_id=_ACTIVATION_ID,
    )
    reader = _Reader(DesiredStateReadResult.unchanged(_REVISION))
    materializer = _Materializer()
    activator = _Activator(
        ActiveDesiredDeployment(
            release_id=checkpoint.release_id,
            deployment_id=checkpoint.deployment_id,
            activation_id=checkpoint.activation_id,
            attestation_ref=checkpoint.desired_state_sha256,
        )
    )

    evidence = _service(
        tmp_path,
        reader=reader,
        checkpoint_store=_CheckpointStore(checkpoint),
        materializer=materializer,
        activator=activator,
        receipt_store=_ReceiptStore(
            reconstructed_activation_receipt(checkpoint),
        ),
    ).reconcile(
        DesiredStateReconcileRequest(
            environment="dev",
            registry_scope_id=_DIGESTS[4],
            source_project="group/repository",
            source_ref="master",
        )
    )

    assert evidence.status == "unchanged"
    assert materializer.desired == []
    assert reader.revisions == [_REVISION]


def test_reconcile_does_not_publish_unchanged_after_current_switch(tmp_path: Path) -> None:
    desired = _desired()
    AtomicAirflowDesiredStateSnapshotWriter(tmp_path / "desired.json").commit(desired.to_json_bytes())
    checkpoint = DesiredStateCheckpoint.from_desired(
        desired,
        observed_revision=_REVISION,
        activation_id=_ACTIVATION_ID,
    )
    current = ActiveDesiredDeployment(
        release_id=checkpoint.release_id,
        deployment_id=checkpoint.deployment_id,
        activation_id=checkpoint.activation_id,
        attestation_ref=checkpoint.desired_state_sha256,
    )
    activator = _Activator(current)
    activator.before_commit_fence = lambda: setattr(
        activator,
        "active",
        ActiveDesiredDeployment(
            release_id=_DIGESTS[1],
            deployment_id=_DIGESTS[2],
            activation_id=_ACTIVATION_IDS[2],
            attestation_ref=_DIGESTS[3],
        ),
    )
    statuses: list[DesiredStateReconcileEvidence] = []

    with pytest.raises(DesiredStateReconcileError) as exc:
        _service(
            tmp_path,
            reader=_Reader(DesiredStateReadResult.unchanged(_REVISION)),
            checkpoint_store=_CheckpointStore(checkpoint),
            materializer=_Materializer(),
            activator=activator,
            receipt_store=_ReceiptStore(reconstructed_activation_receipt(checkpoint)),
            success_status_committer=statuses.append,
        ).reconcile(
            DesiredStateReconcileRequest(
                environment="dev",
                registry_scope_id=_DIGESTS[4],
                source_project="group/repository",
                source_ref="master",
            )
        )

    assert exc.value.code == "DPONE_AIRFLOW_DESIRED_STATE_SUPERSEDED"
    assert exc.value.state_may_have_changed is True
    assert statuses == []


@pytest.mark.parametrize(
    "snapshot_kind",
    ["missing", "malformed", "stale"],
)
def test_reconcile_repairs_invalid_snapshot_before_reporting_success(
    tmp_path: Path,
    snapshot_kind: str,
) -> None:
    desired = _desired()
    snapshot_body = {
        "missing": None,
        "malformed": b"{}",
        "stale": _successor(
            desired,
            occurrence_id="223e4567-e89b-42d3-a456-426614174000",
            deployment_id=_DIGESTS[2],
            previous_revision=_REVISION,
        ).to_json_bytes(),
    }[snapshot_kind]
    if snapshot_body is not None:
        AtomicAirflowDesiredStateSnapshotWriter(tmp_path / "desired.json").commit(snapshot_body)
    checkpoint = DesiredStateCheckpoint.from_desired(
        desired,
        observed_revision=_REVISION,
        activation_id=_ACTIVATION_ID,
    )
    reader = _Reader(
        DesiredStateReadResult.unchanged(_REVISION),
        DesiredStateReadResult.present(desired.to_json_bytes(), _REVISION),
    )
    activator = _Activator(
        ActiveDesiredDeployment(
            release_id=checkpoint.release_id,
            deployment_id=checkpoint.deployment_id,
            activation_id=checkpoint.activation_id,
            attestation_ref=checkpoint.desired_state_sha256,
        )
    )

    evidence = _service(
        tmp_path,
        reader=reader,
        checkpoint_store=_CheckpointStore(checkpoint),
        materializer=_Materializer(),
        activator=activator,
        receipt_store=_ReceiptStore(
            reconstructed_activation_receipt(checkpoint),
        ),
    ).reconcile(
        DesiredStateReconcileRequest(
            environment="dev",
            registry_scope_id=_DIGESTS[4],
            source_project="group/repository",
            source_ref="master",
        )
    )

    assert evidence.status == "recovered"
    assert reader.revisions == [_REVISION, None]
    assert (tmp_path / "desired.json").read_bytes() == desired.to_json_bytes()


def test_reconcile_recovers_missing_receipt_before_returning_unchanged(
    tmp_path: Path,
) -> None:
    desired = _desired()
    checkpoint = DesiredStateCheckpoint.from_desired(
        desired,
        observed_revision=_REVISION,
        activation_id=_ACTIVATION_ID,
    )
    reader = _Reader(
        DesiredStateReadResult.unchanged(_REVISION),
        DesiredStateReadResult.present(desired.to_json_bytes(), _REVISION),
    )
    receipts = _ReceiptStore()
    activator = _Activator(
        ActiveDesiredDeployment(
            release_id=checkpoint.release_id,
            deployment_id=checkpoint.deployment_id,
            activation_id=checkpoint.activation_id,
            attestation_ref=checkpoint.desired_state_sha256,
        )
    )

    evidence = _service(
        tmp_path,
        reader=reader,
        checkpoint_store=_CheckpointStore(checkpoint),
        materializer=_Materializer(),
        activator=activator,
        receipt_store=receipts,
    ).reconcile(
        DesiredStateReconcileRequest(
            environment="dev",
            registry_scope_id=_DIGESTS[4],
            source_project="group/repository",
            source_ref="master",
        )
    )

    assert evidence.status == "recovered"
    assert receipts.evidence is not None
    assert receipts.evidence.status == "activated"
    assert receipts.commits == 1
    assert reader.revisions == [_REVISION, None]
    assert activator.activations == 0


def test_reconcile_rejects_mismatched_activation_receipt(
    tmp_path: Path,
) -> None:
    desired = _desired()
    checkpoint = DesiredStateCheckpoint.from_desired(
        desired,
        observed_revision=_REVISION,
        activation_id=_ACTIVATION_ID,
    )
    wrong_checkpoint = replace(checkpoint, deployment_id=_DIGESTS[2])

    with pytest.raises(DesiredStateReconcileError) as failed:
        _service(
            tmp_path,
            reader=_Reader(DesiredStateReadResult.unchanged(_REVISION)),
            checkpoint_store=_CheckpointStore(checkpoint),
            materializer=_Materializer(),
            activator=_Activator(
                ActiveDesiredDeployment(
                    release_id=checkpoint.release_id,
                    deployment_id=checkpoint.deployment_id,
                    activation_id=checkpoint.activation_id,
                    attestation_ref=checkpoint.desired_state_sha256,
                )
            ),
            receipt_store=_ReceiptStore(
                reconstructed_activation_receipt(wrong_checkpoint),
            ),
        ).reconcile(
            DesiredStateReconcileRequest(
                environment="dev",
                registry_scope_id=_DIGESTS[4],
                source_project="group/repository",
                source_ref="master",
            )
        )

    assert failed.value.code == "DPONE_AIRFLOW_DESIRED_STATE_RECEIPT_MISMATCH"


def test_reconcile_rejects_checkpoint_from_another_registry_authority(
    tmp_path: Path,
) -> None:
    checkpoint = DesiredStateCheckpoint.from_desired(
        _desired(),
        observed_revision=_REVISION,
        activation_id=_ACTIVATION_ID,
    )
    reader = _Reader(DesiredStateReadResult.unchanged(_REVISION))

    with pytest.raises(DesiredStateReconcileError) as failed:
        _service(
            tmp_path,
            reader=reader,
            checkpoint_store=_CheckpointStore(checkpoint),
            materializer=_Materializer(),
            activator=_Activator(),
        ).reconcile(
            DesiredStateReconcileRequest(
                environment="dev",
                registry_scope_id=_DIGESTS[3],
                source_project="group/repository",
                source_ref="master",
            )
        )

    assert failed.value.code == "DPONE_ARTIFACT_REGISTRY_SCOPE_MISMATCH"
    assert reader.revisions == []


def test_reconcile_rejects_checkpoint_from_another_protected_source(
    tmp_path: Path,
) -> None:
    checkpoint = DesiredStateCheckpoint.from_desired(
        _desired(),
        observed_revision=_REVISION,
        activation_id=_ACTIVATION_ID,
    )
    reader = _Reader(DesiredStateReadResult.unchanged(_REVISION))

    with pytest.raises(DesiredStateReconcileError) as failed:
        _service(
            tmp_path,
            reader=reader,
            checkpoint_store=_CheckpointStore(checkpoint),
            materializer=_Materializer(),
            activator=_Activator(),
        ).reconcile(
            DesiredStateReconcileRequest(
                environment="dev",
                registry_scope_id=_DIGESTS[4],
                source_project="group/other-repository",
                source_ref="master",
            )
        )

    assert failed.value.code == "DPONE_AIRFLOW_DESIRED_STATE_SOURCE_MISMATCH"
    assert reader.revisions == []


def test_reconcile_recovers_empty_cache_even_when_remote_revision_is_unchanged(
    tmp_path: Path,
) -> None:
    desired = _desired()
    checkpoint = DesiredStateCheckpoint.from_desired(
        desired,
        observed_revision=_REVISION,
        activation_id=_ACTIVATION_ID,
    )
    reader = _Reader(
        DesiredStateReadResult.unchanged(_REVISION),
        DesiredStateReadResult.present(desired.to_json_bytes(), _REVISION),
        DesiredStateReadResult.unchanged(_REVISION),
    )
    materializer = _Materializer()
    activator = _Activator()

    evidence = _service(
        tmp_path,
        reader=reader,
        checkpoint_store=_CheckpointStore(checkpoint),
        materializer=materializer,
        activator=activator,
    ).reconcile(
        DesiredStateReconcileRequest(
            environment="dev",
            registry_scope_id=_DIGESTS[4],
            source_project="group/repository",
            source_ref="master",
        )
    )

    assert evidence.status == "activated"
    assert materializer.desired == [desired]
    assert reader.revisions == [_REVISION, None, _REVISION]


def test_reconcile_recovers_missing_checkpoint_from_verified_current(
    tmp_path: Path,
) -> None:
    desired = _desired()
    checkpoints = _CheckpointStore()
    receipts = _ReceiptStore()
    activator = _Activator(
        ActiveDesiredDeployment(
            release_id=desired.promotion.release_id,
            deployment_id=desired.promotion.deployment_id,
            activation_id=_ACTIVATION_ID,
            attestation_ref=desired.sha256,
        )
    )

    evidence = _service(
        tmp_path,
        reader=_Reader(DesiredStateReadResult.present(desired.to_json_bytes(), _REVISION)),
        checkpoint_store=checkpoints,
        materializer=_Materializer(),
        activator=activator,
        receipt_store=receipts,
        recovery_store=_RecoveryStore(
            DesiredStateRecoveryRecord(desired, _REVISION),
        ),
    ).reconcile(
        DesiredStateReconcileRequest(
            environment="dev",
            registry_scope_id=_DIGESTS[4],
            source_project="group/repository",
            source_ref="master",
        )
    )

    assert evidence.status == "recovered"
    assert evidence.predecessor_status == "recovered"
    assert receipts.evidence is not None
    assert receipts.evidence.status == "activated"
    assert receipts.evidence.activation_id == evidence.activation_id
    assert receipts.commits == 1
    assert checkpoints.checkpoint is not None
    assert checkpoints.checkpoint.activation_id == _ACTIVATION_ID
    assert activator.activations == 0


def test_reconcile_reuses_existing_activation_receipt_during_recovery(
    tmp_path: Path,
) -> None:
    desired = _desired()
    checkpoint = DesiredStateCheckpoint.from_desired(
        desired,
        observed_revision=_REVISION,
        activation_id=_ACTIVATION_ID,
    )
    receipt = reconstructed_activation_receipt(checkpoint)
    receipts = _ReceiptStore(receipt)
    activator = _Activator(
        ActiveDesiredDeployment(
            release_id=checkpoint.release_id,
            deployment_id=checkpoint.deployment_id,
            activation_id=checkpoint.activation_id,
            attestation_ref=checkpoint.desired_state_sha256,
        )
    )

    evidence = _service(
        tmp_path,
        reader=_Reader(DesiredStateReadResult.present(desired.to_json_bytes(), _REVISION)),
        checkpoint_store=_CheckpointStore(),
        materializer=_Materializer(),
        activator=activator,
        receipt_store=receipts,
        recovery_store=_RecoveryStore(
            DesiredStateRecoveryRecord(desired, _REVISION),
        ),
    ).reconcile(
        DesiredStateReconcileRequest(
            environment="dev",
            registry_scope_id=_DIGESTS[4],
            source_project="group/repository",
            source_ref="master",
        )
    )

    assert evidence.status == "recovered"
    assert receipts.evidence == receipt
    assert receipts.commits == 0
    assert activator.activations == 0


def test_reconcile_never_checkpoints_recovery_before_its_receipt(
    tmp_path: Path,
) -> None:
    desired = _desired()
    checkpoints = _CheckpointStore()
    activator = _Activator(
        ActiveDesiredDeployment(
            release_id=desired.promotion.release_id,
            deployment_id=desired.promotion.deployment_id,
            activation_id=_ACTIVATION_ID,
            attestation_ref=desired.sha256,
        )
    )

    with pytest.raises(DesiredStateReconcileError) as failed:
        _service(
            tmp_path,
            reader=_Reader(DesiredStateReadResult.present(desired.to_json_bytes(), _REVISION)),
            checkpoint_store=checkpoints,
            materializer=_Materializer(),
            activator=activator,
            receipt_store=_ReceiptStore(fail_commit=True),
            recovery_store=_RecoveryStore(
                DesiredStateRecoveryRecord(desired, _REVISION),
            ),
        ).reconcile(
            DesiredStateReconcileRequest(
                environment="dev",
                registry_scope_id=_DIGESTS[4],
                source_project="group/repository",
                source_ref="master",
            )
        )

    assert failed.value.code == "DPONE_AIRFLOW_DESIRED_STATE_RECEIPT_FAILED"
    assert failed.value.state_may_have_changed is False
    assert checkpoints.checkpoint is None
    assert activator.activations == 0


def test_reconcile_repairs_activated_state_before_processing_newer_remote(
    tmp_path: Path,
) -> None:
    first = _desired()
    first_checkpoint = DesiredStateCheckpoint.from_desired(
        first,
        observed_revision=_REVISION,
        activation_id=_ACTIVATION_ID,
    )
    second_revision = DesiredStateRevision('"etag-2"')
    second = _successor(
        first,
        occurrence_id="223e4567-e89b-42d3-a456-426614174000",
        deployment_id=_DIGESTS[2],
        previous_revision=_REVISION,
    )
    third_revision = DesiredStateRevision('"etag-3"')
    third = _successor(
        second,
        occurrence_id="323e4567-e89b-42d3-a456-426614174000",
        deployment_id=_DIGESTS[3],
        previous_revision=second_revision,
    )
    checkpoints = _CheckpointStore(first_checkpoint)
    receipts = _ReceiptStore(reconstructed_activation_receipt(first_checkpoint))
    recovery = _RecoveryStore()
    activator = _Activator(
        ActiveDesiredDeployment(
            release_id=first_checkpoint.release_id,
            deployment_id=first_checkpoint.deployment_id,
            activation_id=first_checkpoint.activation_id,
            attestation_ref=first_checkpoint.desired_state_sha256,
        )
    )
    receipts.fail_commit = True

    with pytest.raises(DesiredStateReconcileError) as failed:
        _service(
            tmp_path,
            reader=_Reader(
                DesiredStateReadResult.present(second.to_json_bytes(), second_revision),
                DesiredStateReadResult.unchanged(second_revision),
            ),
            checkpoint_store=checkpoints,
            materializer=_Materializer(),
            activator=activator,
            receipt_store=receipts,
            recovery_store=recovery,
        ).reconcile(
            DesiredStateReconcileRequest(
                environment="dev",
                registry_scope_id=_DIGESTS[4],
                source_project="group/repository",
                source_ref="master",
            )
        )

    assert failed.value.code == "DPONE_AIRFLOW_DESIRED_STATE_RECEIPT_FAILED"
    assert activator.active is not None
    assert activator.active.deployment_id == second.promotion.deployment_id
    assert checkpoints.checkpoint == first_checkpoint
    assert recovery.record is not None
    assert recovery.record.desired == second

    receipts.fail_commit = False
    recovery_reader = _Reader()
    recovered = _service(
        tmp_path,
        reader=recovery_reader,
        checkpoint_store=checkpoints,
        materializer=_Materializer(),
        activator=activator,
        receipt_store=receipts,
        recovery_store=recovery,
    ).reconcile(
        DesiredStateReconcileRequest(
            environment="dev",
            registry_scope_id=_DIGESTS[4],
            source_project="group/repository",
            source_ref="master",
        )
    )

    assert recovered.status == "recovered"
    assert recovery_reader.revisions == []
    assert checkpoints.checkpoint is not None
    assert checkpoints.checkpoint.deployment_id == second.promotion.deployment_id

    activated = _service(
        tmp_path,
        reader=_Reader(
            DesiredStateReadResult.present(third.to_json_bytes(), third_revision),
            DesiredStateReadResult.unchanged(third_revision),
        ),
        checkpoint_store=checkpoints,
        materializer=_Materializer(),
        activator=activator,
        receipt_store=receipts,
        recovery_store=recovery,
    ).reconcile(
        DesiredStateReconcileRequest(
            environment="dev",
            registry_scope_id=_DIGESTS[4],
            source_project="group/repository",
            source_ref="master",
        )
    )

    assert activated.status == "activated"
    assert activated.deployment_id == third.promotion.deployment_id


def test_reconcile_converges_to_latest_authority_and_audits_a_skipped_predecessor(
    tmp_path: Path,
) -> None:
    previous = _desired()
    checkpoint = DesiredStateCheckpoint.from_desired(
        previous,
        observed_revision=_REVISION,
        activation_id=_ACTIVATION_ID,
    )
    payload = previous.to_dict()
    payload["source"] = {
        **dict(payload["source"]),
        "occurrence_id": "223e4567-e89b-42d3-a456-426614174000",
    }
    payload["promotion"] = {
        **dict(payload["promotion"]),
        "deployment_id": _DIGESTS[2],
    }
    payload["previous"] = {
        "revision": '"unrelated"',
        "deployment_id": _DIGESTS[3],
    }
    desired = AirflowDesiredDeployment.from_mapping(payload)

    evidence = _service(
        tmp_path,
        reader=_Reader(
            DesiredStateReadResult.present(
                desired.to_json_bytes(),
                DesiredStateRevision('"etag-2"'),
            ),
            DesiredStateReadResult.unchanged(DesiredStateRevision('"etag-2"')),
        ),
        checkpoint_store=_CheckpointStore(checkpoint),
        materializer=_Materializer(),
        activator=_Activator(
            ActiveDesiredDeployment(
                release_id=checkpoint.release_id,
                deployment_id=checkpoint.deployment_id,
                activation_id=checkpoint.activation_id,
                attestation_ref=checkpoint.desired_state_sha256,
            )
        ),
        receipt_store=_ReceiptStore(
            reconstructed_activation_receipt(checkpoint),
        ),
    ).reconcile(
        DesiredStateReconcileRequest(
            environment="dev",
            registry_scope_id=_DIGESTS[4],
            source_project="group/repository",
            source_ref="master",
        )
    )

    assert evidence.status == "activated"
    assert evidence.predecessor_status == "skipped"


def test_reconcile_rejects_skipped_predecessor_when_current_diverges(
    tmp_path: Path,
) -> None:
    previous = _desired()
    checkpoint = DesiredStateCheckpoint.from_desired(
        previous,
        observed_revision=_REVISION,
        activation_id=_ACTIVATION_ID,
    )
    payload = previous.to_dict()
    payload["source"] = {
        **dict(payload["source"]),
        "occurrence_id": "223e4567-e89b-42d3-a456-426614174000",
    }
    payload["promotion"] = {
        **dict(payload["promotion"]),
        "deployment_id": _DIGESTS[2],
    }
    payload["previous"] = {
        "revision": '"unrelated"',
        "deployment_id": _DIGESTS[3],
    }
    desired = AirflowDesiredDeployment.from_mapping(payload)

    with pytest.raises(DesiredStateReconcileError) as failed:
        _service(
            tmp_path,
            reader=_Reader(
                DesiredStateReadResult.present(
                    desired.to_json_bytes(),
                    DesiredStateRevision('"etag-2"'),
                )
            ),
            checkpoint_store=_CheckpointStore(checkpoint),
            materializer=_Materializer(),
            activator=_Activator(),
        ).reconcile(
            DesiredStateReconcileRequest(
                environment="dev",
                registry_scope_id=_DIGESTS[4],
                source_project="group/repository",
                source_ref="master",
            )
        )

    assert failed.value.code == "DPONE_AIRFLOW_DESIRED_STATE_PREDECESSOR_MISMATCH"


class _GuardedSnapshot:
    def commit(self, body: bytes) -> None:
        del body
        raise DesiredStateReconcilePortError(
            "DPONE_CACHE_PATH_ESCAPE",
            "unsafe control path",
        )


def test_reconcile_normalizes_snapshot_guard_failure(
    tmp_path: Path,
) -> None:
    desired = _desired()
    service = AirflowDesiredStateReconciler(
        reader=_Reader(DesiredStateReadResult.present(desired.to_json_bytes(), _REVISION)),
        snapshot_writer=_GuardedSnapshot(),
        checkpoint_store=_CheckpointStore(),
        activation_receipt_store=_ReceiptStore(),
        recovery_record_store=_RecoveryStore(),
        materializer=_Materializer(),
        activator=_Activator(),
    )

    with pytest.raises(DesiredStateReconcileError) as failed:
        service.reconcile(
            DesiredStateReconcileRequest(
                environment="dev",
                registry_scope_id=_DIGESTS[4],
                source_project="group/repository",
                source_ref="master",
            )
        )

    assert failed.value.code == "DPONE_CACHE_PATH_ESCAPE"
    assert failed.value.state_may_have_changed is False


def test_reconcile_never_activates_when_remote_desired_state_is_superseded(
    tmp_path: Path,
) -> None:
    desired = _desired()
    reader = _Reader(
        DesiredStateReadResult.present(desired.to_json_bytes(), _REVISION),
        DesiredStateReadResult.present(
            desired.to_json_bytes(),
            DesiredStateRevision('"etag-2"'),
        ),
    )
    activator = _Activator()

    with pytest.raises(DesiredStateReconcileError) as failed:
        _service(
            tmp_path,
            reader=reader,
            checkpoint_store=_CheckpointStore(),
            materializer=_Materializer(),
            activator=activator,
        ).reconcile(
            DesiredStateReconcileRequest(
                environment="dev",
                registry_scope_id=_DIGESTS[4],
                source_project="group/repository",
                source_ref="master",
            )
        )

    assert failed.value.code == "DPONE_AIRFLOW_DESIRED_STATE_SUPERSEDED"
    assert failed.value.state_may_have_changed is False
    assert activator.activations == 0


def test_reconcile_marks_checkpoint_failure_as_uncertain_after_activation(
    tmp_path: Path,
) -> None:
    desired = _desired()
    reader = _Reader(
        DesiredStateReadResult.present(desired.to_json_bytes(), _REVISION),
        DesiredStateReadResult.unchanged(_REVISION),
    )
    activator = _Activator()
    receipts = _ReceiptStore()

    with pytest.raises(DesiredStateReconcileError) as failed:
        _service(
            tmp_path,
            reader=reader,
            checkpoint_store=_CheckpointStore(fail_commit=True),
            materializer=_Materializer(),
            activator=activator,
            receipt_store=receipts,
        ).reconcile(
            DesiredStateReconcileRequest(
                environment="dev",
                registry_scope_id=_DIGESTS[4],
                source_project="group/repository",
                source_ref="master",
            )
        )

    assert failed.value.code == "DPONE_AIRFLOW_DESIRED_STATE_CHECKPOINT_FAILED"
    assert failed.value.state_may_have_changed is True
    assert activator.activations == 1
    assert receipts.evidence is not None
    assert receipts.evidence.status == "activated"


def test_reconcile_marks_receipt_failure_as_uncertain_after_activation(
    tmp_path: Path,
) -> None:
    desired = _desired()
    activator = _Activator()

    with pytest.raises(DesiredStateReconcileError) as failed:
        _service(
            tmp_path,
            reader=_Reader(
                DesiredStateReadResult.present(desired.to_json_bytes(), _REVISION),
                DesiredStateReadResult.unchanged(_REVISION),
            ),
            checkpoint_store=_CheckpointStore(),
            materializer=_Materializer(),
            activator=activator,
            receipt_store=_ReceiptStore(fail_commit=True),
        ).reconcile(
            DesiredStateReconcileRequest(
                environment="dev",
                registry_scope_id=_DIGESTS[4],
                source_project="group/repository",
                source_ref="master",
            )
        )

    assert failed.value.code == "DPONE_AIRFLOW_DESIRED_STATE_RECEIPT_FAILED"
    assert failed.value.state_may_have_changed is True
    assert activator.activations == 1

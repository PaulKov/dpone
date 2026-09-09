from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from dpone.contracts.airflow_desired_state import (
    AirflowDesiredDeployment,
    DesiredStateRevision,
)
from dpone.contracts.airflow_desired_state_reconcile import (
    DesiredStateCheckpoint,
    DesiredStateReconcileEvidence,
    DesiredStateRecoveryRecord,
)
from dpone.ports.airflow_desired_state import (
    ActiveDesiredDeployment,
    DesiredStateActivationResult,
    DesiredStateReadResult,
    DesiredStateReconcilePortError,
)
from dpone.services.airflow_desired_state_reconcile import (
    AirflowDesiredStateReconciler,
    DesiredStateReconcileError,
    DesiredStateReconcileRequest,
)

_DIGESTS = tuple("sha256:" + char * 64 for char in "abcdef")
_REVISION = DesiredStateRevision('"etag-successor"')
_ACTIVATION_ID = "123e4567-e89b-42d3-a456-426614174001"


def _successor_desired() -> AirflowDesiredDeployment:
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
                "registry_scope_id": _DIGESTS[0],
                "release_id": _DIGESTS[1],
                "deployment_id": _DIGESTS[2],
                "airflow_index_sha256": _DIGESTS[3],
                "runtime_image_digest": _DIGESTS[4],
                "expected_dag_ids": ["DAG__platform__smoke__run"],
                "publication_evidence_sha256": _DIGESTS[5],
            },
            "previous": {
                "revision": '"etag-previous"',
                "deployment_id": _DIGESTS[5],
            },
            "promoted_at": "2026-07-30T10:00:00Z",
        }
    )


def _request() -> DesiredStateReconcileRequest:
    return DesiredStateReconcileRequest(
        environment="dev",
        registry_scope_id=_DIGESTS[0],
        source_project="group/repository",
        source_ref="master",
    )


class _Reader:
    def __init__(self, *results: DesiredStateReadResult) -> None:
        self.results = list(results)
        self.revisions: list[DesiredStateRevision | None] = []

    def read(
        self,
        *,
        if_changed_from: DesiredStateRevision | None = None,
        **_: object,
    ) -> DesiredStateReadResult:
        self.revisions.append(if_changed_from)
        return self.results.pop(0)


@dataclass
class _SnapshotStore:
    body: bytes | None = None

    def read(self, **_: object) -> bytes | None:
        return self.body

    def commit(self, body: bytes) -> None:
        self.body = body


@dataclass
class _CheckpointStore:
    checkpoint: DesiredStateCheckpoint | None = None
    fail_commit: bool = False

    def read(self) -> DesiredStateCheckpoint | None:
        return self.checkpoint

    def commit(self, checkpoint: DesiredStateCheckpoint) -> None:
        if self.fail_commit:
            raise OSError("checkpoint unavailable")
        self.checkpoint = checkpoint


@dataclass
class _ReceiptStore:
    receipts: dict[str, DesiredStateReconcileEvidence] = field(default_factory=dict)

    def read(self, activation_id: str) -> DesiredStateReconcileEvidence | None:
        return self.receipts.get(activation_id)

    def commit(self, evidence: DesiredStateReconcileEvidence) -> None:
        self.receipts[evidence.activation_id] = evidence


@dataclass
class _RecoveryStore:
    record: DesiredStateRecoveryRecord | None = None

    def read(self) -> DesiredStateRecoveryRecord | None:
        return self.record

    def commit(self, record: DesiredStateRecoveryRecord) -> None:
        self.record = record


@dataclass
class _Materializer:
    desired: list[AirflowDesiredDeployment] = field(default_factory=list)
    fail: bool = False

    def materialize(self, desired: AirflowDesiredDeployment) -> None:
        self.desired.append(desired)
        if self.fail:
            raise DesiredStateReconcilePortError(
                "DPONE_AIRFLOW_DEPLOYMENT_MATERIALIZE_FAILED",
                "synthetic materialization failure",
            )


@dataclass
class _Activator:
    active: ActiveDesiredDeployment | None = None
    activations: int = 0

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
        self.activations += 1
        self.active = ActiveDesiredDeployment(
            release_id=desired.promotion.release_id,
            deployment_id=desired.promotion.deployment_id,
            activation_id=_ACTIVATION_ID,
            attestation_ref=desired.sha256,
        )
        result = DesiredStateActivationResult(_ACTIVATION_ID, previous)
        post_activation_commit(result)
        return result


@dataclass
class _Harness:
    reader: _Reader
    snapshot: _SnapshotStore = field(default_factory=_SnapshotStore)
    checkpoints: _CheckpointStore = field(default_factory=_CheckpointStore)
    receipts: _ReceiptStore = field(default_factory=_ReceiptStore)
    recovery: _RecoveryStore = field(default_factory=_RecoveryStore)
    materializer: _Materializer = field(default_factory=_Materializer)
    activator: _Activator = field(default_factory=_Activator)

    def service(self) -> AirflowDesiredStateReconciler:
        return AirflowDesiredStateReconciler(
            reader=self.reader,
            snapshot_writer=self.snapshot,
            checkpoint_store=self.checkpoints,
            activation_receipt_store=self.receipts,
            recovery_record_store=self.recovery,
            materializer=self.materializer,
            activator=self.activator,
        )


def _cold_reader(desired: AirflowDesiredDeployment) -> _Reader:
    return _Reader(
        DesiredStateReadResult.present(desired.to_json_bytes(), _REVISION),
        DesiredStateReadResult.unchanged(_REVISION),
    )


def test_empty_disposable_cache_activates_authoritative_successor() -> None:
    desired = _successor_desired()
    harness = _Harness(_cold_reader(desired))

    evidence = harness.service().reconcile(_request())

    assert evidence.status == "activated"
    assert evidence.predecessor_status == "bootstrap"
    assert evidence.deployment_id == desired.promotion.deployment_id
    assert harness.materializer.desired == [desired]
    assert harness.activator.activations == 1
    assert harness.snapshot.body == desired.to_json_bytes()
    assert harness.recovery.record == DesiredStateRecoveryRecord(desired, _REVISION)
    assert harness.checkpoints.checkpoint is not None
    assert harness.receipts.read(_ACTIVATION_ID) == evidence
    assert harness.reader.revisions == [None, _REVISION]


def test_cold_start_materialization_failure_never_activates() -> None:
    desired = _successor_desired()
    harness = _Harness(
        _cold_reader(desired),
        materializer=_Materializer(fail=True),
    )

    with pytest.raises(DesiredStateReconcileError) as failed:
        harness.service().reconcile(_request())

    assert failed.value.code == "DPONE_AIRFLOW_DEPLOYMENT_MATERIALIZE_FAILED"
    assert failed.value.state_may_have_changed is False
    assert harness.activator.activations == 0
    assert harness.checkpoints.checkpoint is None
    assert harness.receipts.receipts == {}


def test_cold_start_checkpoint_failure_recovers_without_second_activation() -> None:
    desired = _successor_desired()
    harness = _Harness(
        _cold_reader(desired),
        checkpoints=_CheckpointStore(fail_commit=True),
    )

    with pytest.raises(DesiredStateReconcileError) as failed:
        harness.service().reconcile(_request())

    assert failed.value.code == "DPONE_AIRFLOW_DESIRED_STATE_CHECKPOINT_FAILED"
    assert failed.value.state_may_have_changed is True
    assert harness.activator.activations == 1
    assert harness.recovery.record is not None
    assert harness.receipts.read(_ACTIVATION_ID) is not None

    harness.checkpoints.fail_commit = False
    harness.reader = _Reader()
    recovered = harness.service().reconcile(_request())

    assert recovered.status == "recovered"
    assert harness.activator.activations == 1
    assert harness.reader.revisions == []
    assert harness.checkpoints.checkpoint is not None


def test_surviving_current_without_recovery_record_remains_fail_closed() -> None:
    desired = _successor_desired()
    active = ActiveDesiredDeployment(
        release_id=desired.promotion.release_id,
        deployment_id=desired.promotion.deployment_id,
        activation_id=_ACTIVATION_ID,
        attestation_ref=desired.sha256,
    )
    harness = _Harness(_cold_reader(desired), activator=_Activator(active=active))

    with pytest.raises(DesiredStateReconcileError) as failed:
        harness.service().reconcile(_request())

    assert failed.value.code == "DPONE_AIRFLOW_DESIRED_STATE_RECOVERY_RECORD_MISSING"
    assert failed.value.state_may_have_changed is True
    assert harness.reader.revisions == []
    assert harness.materializer.desired == []
    assert harness.activator.activations == 0

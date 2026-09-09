from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

import dpone.adapters.airflow_desired_state_checkpoint as checkpoint_module
from dpone.adapters.airflow_desired_state_checkpoint import (
    AtomicAirflowDesiredStateSnapshotWriter,
    FileDesiredStateCheckpointStore,
    FileDesiredStateReconcileEvidenceStore,
    FileDesiredStateRecoveryRecordStore,
)
from dpone.adapters.object_storage_artifact_registry import (
    ObjectStorageArtifactRegistry,
)
from dpone.contracts.airflow_desired_state import (
    AirflowDesiredDeployment,
    DesiredStateRevision,
)
from dpone.contracts.airflow_desired_state_reconcile import (
    MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES,
    DesiredStateCheckpoint,
    DesiredStateReconcileEvidence,
    DesiredStateRecoveryRecord,
)
from dpone.ports.airflow_desired_state import (
    DesiredStateReconcilePortError,
)
from dpone.readiness.airflow_desired_state_reconcile import (
    ArtifactRegistryDesiredDeploymentMaterializer,
    DeploymentCacheDesiredDeploymentActivator,
)
from dpone.runtime.airflow_artifact_delivery import (
    AirflowArtifactPublisher,
    PublishRequest,
)
from dpone.runtime.deployment_cache import DeploymentCacheError
from dpone.services.airflow_desired_state_reconcile import (
    activated_evidence,
    reconstructed_activation_receipt,
)
from dpone.storage.local import LocalObjectStorageClient
from dpone.storage.models import ObjectStorageUri
from tests.support.airflow_artifact_projection import write_exact_test_projection

_DIGESTS = tuple("sha256:" + char * 64 for char in "abcde")
_REVISION = DesiredStateRevision('"etag-1"')
_ACTIVATION_ID = "123e4567-e89b-42d3-a456-426614174001"


@pytest.mark.parametrize(
    "code",
    (
        "DPONE_DEPLOYMENT_ACTIVATION_CONFLICT",
        "DPONE_DEPLOYMENT_ACTIVATION_FAILED",
    ),
)
def test_activation_terminal_errors_preserve_uncertain_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    code: str,
) -> None:
    activator = DeploymentCacheDesiredDeploymentActivator(
        cache_root=tmp_path / "cache",
        promoted_by="airflow-pack-watcher",
    )

    def fail_promotion(*args: object, **kwargs: object) -> None:
        raise DeploymentCacheError(
            code,
            "synthetic activation failure",
            details={"state_may_have_changed": True},
        )

    monkeypatch.setattr(activator._materializer, "promote", fail_promotion)

    with pytest.raises(DesiredStateReconcilePortError) as caught:
        activator.activate(
            _desired(),
            expected_current_deployment_id=None,
            remote_precondition=lambda: True,
        )

    assert caught.value.code == code
    assert caught.value.state_may_have_changed is True


def test_real_materializer_and_activator_bind_current_to_desired_evidence(
    tmp_path: Path,
) -> None:
    source_cache = tmp_path / "source"
    target_cache = tmp_path / "target"
    registry_root = tmp_path / "registry"
    release_id, deployment_id = write_exact_test_projection(source_cache)
    registry = ObjectStorageArtifactRegistry(
        client=LocalObjectStorageClient(registry_root),
        root=ObjectStorageUri.parse("s3://dpone-artifacts/airflow"),
    )
    AirflowArtifactPublisher(registry=registry).publish(
        PublishRequest(
            cache_root=source_cache,
            release_id=release_id,
            deployment_id=deployment_id,
            environment="dev",
            artifact_registry_ref="local-test",
        )
    )
    deployment_dir = source_cache / "deployments" / "dev" / deployment_id.replace(":", "-", 1)
    index_bytes = (deployment_dir / "airflow-index.json").read_bytes()
    index = json.loads(index_bytes)
    desired = _desired_for_projection(
        release_id=release_id,
        deployment_id=deployment_id,
        airflow_index_sha256="sha256:" + hashlib.sha256(index_bytes).hexdigest(),
        runtime_image_digest=str(index["runtime_image_digest"]),
        expected_dag_ids=tuple(sorted(str(item["id"]) for item in index["dag_specs"])),
    )
    ArtifactRegistryDesiredDeploymentMaterializer(
        cache_root=target_cache,
        artifact_registry_ref="local-test",
        registry=registry,
        max_object_bytes=64 * 1024 * 1024,
        max_total_bytes=512 * 1024 * 1024,
    ).materialize(desired)
    activator = DeploymentCacheDesiredDeploymentActivator(
        cache_root=target_cache,
        promoted_by="airflow-pack-watcher",
    )

    activation = activator.activate(
        desired,
        expected_current_deployment_id=None,
        remote_precondition=lambda: True,
    )
    current = activator.current(environment="dev")

    assert current is not None
    assert current.deployment_id == deployment_id
    assert current.release_id == release_id
    assert current.activation_id == activation.activation_id
    assert activation.activation_id == desired.source.occurrence_id
    assert current.attestation_ref == desired.sha256

    index_path = (target_cache / "current/airflow-index.json").resolve()
    index_path.chmod(0o600)
    index_path.write_text(
        '{"corrupted":true}',
        encoding="utf-8",
    )

    with pytest.raises(DesiredStateReconcilePortError):
        activator.current(environment="dev")


def test_real_activator_rejects_evidence_commit_after_atomic_pointer_switch(
    tmp_path: Path,
) -> None:
    source_cache = tmp_path / "source"
    target_cache = tmp_path / "target"
    registry_root = tmp_path / "registry"
    release_id, deployment_id = write_exact_test_projection(source_cache)
    registry = ObjectStorageArtifactRegistry(
        client=LocalObjectStorageClient(registry_root),
        root=ObjectStorageUri.parse("s3://dpone-artifacts/airflow"),
    )
    AirflowArtifactPublisher(registry=registry).publish(
        PublishRequest(
            cache_root=source_cache,
            release_id=release_id,
            deployment_id=deployment_id,
            environment="dev",
            artifact_registry_ref="local-test",
        )
    )
    deployment_dir = source_cache / "deployments" / "dev" / deployment_id.replace(":", "-", 1)
    index_bytes = (deployment_dir / "airflow-index.json").read_bytes()
    index = json.loads(index_bytes)
    desired = _desired_for_projection(
        release_id=release_id,
        deployment_id=deployment_id,
        airflow_index_sha256="sha256:" + hashlib.sha256(index_bytes).hexdigest(),
        runtime_image_digest=str(index["runtime_image_digest"]),
        expected_dag_ids=tuple(sorted(str(item["id"]) for item in index["dag_specs"])),
    )
    ArtifactRegistryDesiredDeploymentMaterializer(
        cache_root=target_cache,
        artifact_registry_ref="local-test",
        registry=registry,
        max_object_bytes=64 * 1024 * 1024,
        max_total_bytes=512 * 1024 * 1024,
    ).materialize(desired)
    activator = DeploymentCacheDesiredDeploymentActivator(
        cache_root=target_cache,
        promoted_by="airflow-pack-watcher",
    )
    activator.activate(
        desired,
        expected_current_deployment_id=None,
        remote_precondition=lambda: True,
    )
    expected = activator.current(environment="dev")
    assert expected is not None

    pointer_path = target_cache / "current-pointer.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["activation_id"] = "223e4567-e89b-42d3-a456-426614174000"
    replacement = target_cache / ".current-pointer.concurrent.tmp"
    replacement.write_text(json.dumps(pointer, sort_keys=True), encoding="utf-8")
    os.replace(replacement, pointer_path)
    committed = False

    def commit_evidence() -> None:
        nonlocal committed
        committed = True

    with pytest.raises(DesiredStateReconcilePortError) as caught:
        activator.commit_if_current(
            expected,
            environment="dev",
            post_validation_commit=commit_evidence,
        )

    assert caught.value.code == "DPONE_AIRFLOW_DESIRED_STATE_SUPERSEDED"
    assert caught.value.state_may_have_changed is True
    assert committed is False


def test_real_materializer_rejects_projection_from_another_source_commit(
    tmp_path: Path,
) -> None:
    source_cache = tmp_path / "source"
    registry_root = tmp_path / "registry"
    release_id, deployment_id = write_exact_test_projection(source_cache)
    registry = ObjectStorageArtifactRegistry(
        client=LocalObjectStorageClient(registry_root),
        root=ObjectStorageUri.parse("s3://dpone-artifacts/airflow"),
    )
    AirflowArtifactPublisher(registry=registry).publish(
        PublishRequest(
            cache_root=source_cache,
            release_id=release_id,
            deployment_id=deployment_id,
            environment="dev",
            artifact_registry_ref="local-test",
        )
    )
    deployment_dir = source_cache / "deployments" / "dev" / deployment_id.replace(":", "-", 1)
    index_bytes = (deployment_dir / "airflow-index.json").read_bytes()
    index = json.loads(index_bytes)
    desired = _desired_for_projection(
        release_id=release_id,
        deployment_id=deployment_id,
        airflow_index_sha256="sha256:" + hashlib.sha256(index_bytes).hexdigest(),
        runtime_image_digest=str(index["runtime_image_digest"]),
        expected_dag_ids=tuple(sorted(str(item["id"]) for item in index["dag_specs"])),
    )
    payload = desired.to_dict()
    payload["source"] = {**dict(payload["source"]), "git_sha": "8" * 40}

    with pytest.raises(
        DesiredStateReconcilePortError,
        match="does not match desired-state evidence",
    ):
        ArtifactRegistryDesiredDeploymentMaterializer(
            cache_root=tmp_path / "target",
            artifact_registry_ref="local-test",
            registry=registry,
            max_object_bytes=64 * 1024 * 1024,
            max_total_bytes=512 * 1024 * 1024,
        ).materialize(AirflowDesiredDeployment.from_mapping(payload))


def test_file_checkpoint_store_round_trips_canonical_checkpoint(
    tmp_path: Path,
) -> None:
    path = tmp_path / "control" / "checkpoint.json"
    checkpoint = DesiredStateCheckpoint.from_desired(
        _desired(),
        observed_revision=_REVISION,
        activation_id=_ACTIVATION_ID,
    )
    store = FileDesiredStateCheckpointStore(path)

    assert store.read() is None
    store.commit(checkpoint)

    assert store.read() == checkpoint
    assert path.read_bytes() == checkpoint.to_json_bytes()


def test_file_checkpoint_store_rejects_noncanonical_or_oversized_state(
    tmp_path: Path,
) -> None:
    path = tmp_path / "checkpoint.json"
    store = FileDesiredStateCheckpointStore(path)
    path.write_bytes(b"{}")
    with pytest.raises(ValueError, match="checkpoint"):
        store.read()

    path.write_bytes(b"x" * (MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES + 1))
    with pytest.raises(ValueError, match="size"):
        store.read()


def test_file_checkpoint_store_never_follows_a_symlink_outside_cache_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / "cache"
    status = root / "status"
    outside = tmp_path / "outside.json"
    status.mkdir(parents=True)
    outside.write_bytes(
        DesiredStateCheckpoint.from_desired(
            _desired(),
            observed_revision=_REVISION,
            activation_id=_ACTIVATION_ID,
        ).to_json_bytes()
    )
    checkpoint_path = status / "checkpoint.json"
    checkpoint_path.symlink_to(outside)

    with pytest.raises(ValueError, match="unsafe"):
        FileDesiredStateCheckpointStore(
            checkpoint_path,
            root=root,
        ).read()


def test_reconcile_evidence_store_keeps_immutable_receipt_and_latest_status(
    tmp_path: Path,
) -> None:
    root = tmp_path / "cache"
    store = FileDesiredStateReconcileEvidenceStore(root / "status", root=root)
    evidence = activated_evidence(
        _desired(),
        revision=_REVISION,
        activation_id=_ACTIVATION_ID,
        previous_deployment_id=None,
        predecessor="bootstrap",
    )

    store.commit(evidence)
    store.commit(evidence)
    store.commit_status(evidence.to_json_bytes())

    receipt = root / f"status/reconcile-receipts/{_ACTIVATION_ID}.json"
    assert DesiredStateReconcileEvidence.from_json_bytes(receipt.read_bytes()) == evidence
    assert (root / "status/last-reconcile-status.json").read_bytes() == evidence.to_json_bytes()


def test_reconcile_evidence_store_never_replaces_conflicting_receipt(
    tmp_path: Path,
) -> None:
    root = tmp_path / "cache"
    store = FileDesiredStateReconcileEvidenceStore(root / "status", root=root)
    original = activated_evidence(
        _desired(),
        revision=_REVISION,
        activation_id=_ACTIVATION_ID,
        previous_deployment_id=None,
        predecessor="bootstrap",
    )
    conflicting = DesiredStateReconcileEvidence.from_mapping(
        {
            **original.to_dict(),
            "predecessor_status": "continuous",
        }
    )
    store.commit(original)

    with pytest.raises(ValueError, match="different bytes"):
        store.commit(conflicting)

    assert store.read(_ACTIVATION_ID) == original


def test_reconcile_evidence_store_persists_reconstructed_activation_receipt(
    tmp_path: Path,
) -> None:
    root = tmp_path / "cache"
    store = FileDesiredStateReconcileEvidenceStore(root / "status", root=root)
    evidence = reconstructed_activation_receipt(
        DesiredStateCheckpoint.from_desired(
            _desired(),
            observed_revision=_REVISION,
            activation_id=_ACTIVATION_ID,
        )
    )

    store.commit(evidence)

    receipt = root / f"status/reconcile-receipts/{_ACTIVATION_ID}.json"
    assert DesiredStateReconcileEvidence.from_json_bytes(receipt.read_bytes()) == evidence
    assert store.read(_ACTIVATION_ID) == evidence


def test_recovery_record_store_round_trips_and_reports_its_own_path_errors(
    tmp_path: Path,
) -> None:
    root = tmp_path / "cache"
    path = root / "status" / "active-recovery-record.json"
    store = FileDesiredStateRecoveryRecordStore(path, root=root)
    record = DesiredStateRecoveryRecord(_desired(), _REVISION)

    assert store.read() is None
    store.commit(record)
    assert store.read() == record

    outside = tmp_path / "outside.json"
    outside.write_bytes(record.to_json_bytes())
    path.unlink()
    path.symlink_to(outside)
    with pytest.raises(
        DesiredStateReconcilePortError,
        match="RECOVERY_RECORD_INVALID",
    ):
        store.read()


def test_control_stores_support_a_two_thousand_dag_deployment(
    tmp_path: Path,
) -> None:
    payload = _desired().to_dict()
    payload["promotion"] = {
        **dict(payload["promotion"]),
        "expected_dag_ids": [f"DAG__domain__model_{index:04d}__refresh" for index in range(2_000)],
    }
    desired = AirflowDesiredDeployment.from_mapping(payload)
    checkpoint = DesiredStateCheckpoint.from_desired(
        desired,
        observed_revision=_REVISION,
        activation_id=_ACTIVATION_ID,
    )
    recovery = DesiredStateRecoveryRecord(desired, _REVISION)
    receipt = reconstructed_activation_receipt(checkpoint)
    root = tmp_path / "cache"
    checkpoint_store = FileDesiredStateCheckpointStore(
        root / "status/checkpoint.json",
        root=root,
    )
    recovery_store = FileDesiredStateRecoveryRecordStore(
        root / "status/recovery.json",
        root=root,
    )
    receipt_store = FileDesiredStateReconcileEvidenceStore(root / "status", root=root)

    checkpoint_store.commit(checkpoint)
    recovery_store.commit(recovery)
    receipt_store.commit(receipt)

    assert checkpoint_store.read() == checkpoint
    assert recovery_store.read() == recovery
    assert receipt_store.read(_ACTIVATION_ID) == receipt


def test_atomic_snapshot_fsyncs_parent_directory_after_replace(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = tmp_path / "control" / "desired.json"
    fsynced: list[Path] = []
    monkeypatch.setattr(
        checkpoint_module,
        "durable_fsync_directory",
        fsynced.append,
    )

    AtomicAirflowDesiredStateSnapshotWriter(output).commit(b"canonical")

    assert output.read_bytes() == b"canonical"
    assert fsynced == [output.parent]


def test_atomic_snapshot_enforces_symmetric_read_write_bound(
    tmp_path: Path,
) -> None:
    output = tmp_path / "control" / "desired.json"
    store = AtomicAirflowDesiredStateSnapshotWriter(output, max_bytes=8)

    with pytest.raises(ValueError, match="bounded size"):
        store.commit(b"123456789")

    store.commit(b"12345678")
    assert store.read(max_bytes=8) == b"12345678"


def test_reconcile_status_rejects_oversized_body(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    store = FileDesiredStateReconcileEvidenceStore(root / "status", root=root)

    with pytest.raises(ValueError, match="bounded size"):
        store.commit_status(b"x" * (MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES + 1))


def test_snapshot_reader_rejects_symlinked_control_file(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    outside = tmp_path / "outside.json"
    root.mkdir()
    outside.write_bytes(b"{}")
    snapshot = root / "desired.json"
    snapshot.symlink_to(outside)

    with pytest.raises(DesiredStateReconcilePortError, match="unsafe"):
        AtomicAirflowDesiredStateSnapshotWriter(
            snapshot,
            root=root,
        ).read(max_bytes=1024)


def test_confined_snapshot_rejects_symlinked_status_directory(
    tmp_path: Path,
) -> None:
    root = tmp_path / "cache"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "status").symlink_to(outside, target_is_directory=True)

    with pytest.raises(DesiredStateReconcilePortError, match="committed safely"):
        AtomicAirflowDesiredStateSnapshotWriter(
            root / "status" / "last-reconcile-status.json",
            root=root,
        ).commit(b"evidence")

    assert list(outside.iterdir()) == []


def _desired_for_projection(
    *,
    release_id: str,
    deployment_id: str,
    airflow_index_sha256: str,
    runtime_image_digest: str,
    expected_dag_ids: tuple[str, ...],
) -> AirflowDesiredDeployment:
    payload = _desired().to_dict()
    promotion = dict(payload["promotion"])
    promotion.update(
        {
            "release_id": release_id,
            "deployment_id": deployment_id,
            "airflow_index_sha256": airflow_index_sha256,
            "runtime_image_digest": runtime_image_digest,
            "expected_dag_ids": list(expected_dag_ids),
        }
    )
    payload["promotion"] = promotion
    return AirflowDesiredDeployment.from_mapping(payload)


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
                "git_sha": "7" * 40,
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

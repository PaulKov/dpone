from __future__ import annotations

import hashlib
import json
import os
import warnings
from pathlib import Path

import pytest
from dpone_airflow_pack import cache_activation_contract, cache_writer_coordination
from dpone_airflow_pack.cache_reconcile_status import read_reconcile_status
from dpone_airflow_pack.cache_status import airflow_pack_cache_dir, read_airflow_pack_cache_status
from dpone_airflow_pack.cache_status_ack import attach_loader_ack_status
from dpone_airflow_pack.cache_sync import AirflowPackSyncOptions
from dpone_airflow_pack.cache_sync_evidence import write_sync_warning
from dpone_airflow_pack.deployment_index import LoadReport
from dpone_airflow_pack.loader_ack import LoaderAcknowledgementError, write_dpone_loader_ack

from dpone.contracts.airflow_desired_state import DesiredStateRevision
from dpone.contracts.airflow_desired_state_activation import DesiredStateCheckpoint
from dpone.contracts.airflow_desired_state_reconcile import DesiredStateReconcileEvidence

RELEASE_ID = "sha256:" + "a" * 64
DEPLOYMENT_ID = "sha256:" + "b" * 64
ACTIVATION_ID = "123e4567-e89b-42d3-a456-426614174001"


def _write_canonical_reconcile_evidence(root: Path) -> tuple[DesiredStateReconcileEvidence, Path]:
    evidence = DesiredStateReconcileEvidence(
        status="activated",
        environment="dev",
        observed_revision='"etag"',
        desired_state_sha256="sha256:" + "d" * 64,
        registry_scope_id="sha256:" + "e" * 64,
        source_project="group/repository",
        source_ref="master",
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        occurrence_id="123e4567-e89b-42d3-a456-426614174000",
        source_git_sha="1" * 40,
        airflow_index_sha256="sha256:" + "f" * 64,
        runtime_image_digest="sha256:" + "1" * 64,
        expected_dag_ids=("DAG__platform__smoke__run",),
        activation_id=ACTIVATION_ID,
        previous_deployment_id=None,
        predecessor_status="bootstrap",
        materialized=True,
        activated=True,
    )
    status_dir = root / "status"
    status_dir.mkdir(parents=True)
    path = status_dir / "last-reconcile-status.json"
    path.write_bytes(evidence.to_json_bytes())
    checkpoint = DesiredStateCheckpoint(
        environment=evidence.environment,
        observed_revision=DesiredStateRevision(evidence.observed_revision),
        desired_state_sha256=evidence.desired_state_sha256,
        registry_scope_id=evidence.registry_scope_id,
        source_project=evidence.source_project,
        source_ref=evidence.source_ref,
        release_id=evidence.release_id,
        deployment_id=evidence.deployment_id,
        occurrence_id=evidence.occurrence_id,
        source_git_sha=evidence.source_git_sha,
        airflow_index_sha256=evidence.airflow_index_sha256,
        runtime_image_digest=evidence.runtime_image_digest,
        expected_dag_ids=evidence.expected_dag_ids,
        activation_id=evidence.activation_id,
    )
    (status_dir / "desired-state-checkpoint.json").write_bytes(checkpoint.to_json_bytes())
    return evidence, path


def test_required_missing_reconcile_status_is_a_blocker(tmp_path: Path) -> None:
    _, blockers, warnings_ = read_reconcile_status(
        tmp_path,
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        activation_id=ACTIVATION_ID,
        required=True,
    )

    assert {item["code"] for item in blockers} == {"airflow_pack_reconcile_status_missing"}
    assert warnings_ == ()


def test_partial_reconcile_status_is_a_blocker(tmp_path: Path) -> None:
    path = tmp_path / "status" / "last-reconcile-status.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}", encoding="utf-8")

    payload, blockers, warnings_ = read_reconcile_status(
        tmp_path,
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        activation_id=ACTIVATION_ID,
        required=True,
    )

    assert payload == {}
    assert {item["code"] for item in blockers} == {"airflow_pack_reconcile_status_invalid"}
    assert warnings_ == ()


def test_incomplete_success_reconcile_status_is_a_blocker(tmp_path: Path) -> None:
    path = tmp_path / "status" / "last-reconcile-status.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-desired-state-reconcile.v1",
                "passed": True,
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "activation_id": ACTIVATION_ID,
            }
        ),
        encoding="utf-8",
    )

    _, blockers, _ = read_reconcile_status(
        tmp_path,
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        activation_id=ACTIVATION_ID,
        required=True,
    )

    assert {item["code"] for item in blockers} == {"airflow_pack_reconcile_status_invalid"}


def test_lightweight_reconcile_reader_accepts_canonical_runtime_evidence(tmp_path: Path) -> None:
    evidence, _ = _write_canonical_reconcile_evidence(tmp_path)

    payload, blockers, warnings_ = read_reconcile_status(
        tmp_path,
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        activation_id=ACTIVATION_ID,
        required=True,
    )

    assert payload == evidence.to_dict()
    assert blockers == ()
    assert warnings_ == ()


def test_lightweight_reconcile_reader_blocks_stale_evidence(tmp_path: Path) -> None:
    _, path = _write_canonical_reconcile_evidence(tmp_path)
    os.utime(path, (1, 1))

    _, blockers, _ = read_reconcile_status(
        tmp_path,
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        activation_id=ACTIVATION_ID,
        required=True,
        max_age_seconds=60,
    )

    assert {item["code"] for item in blockers} == {"airflow_pack_reconcile_status_stale"}


def test_lightweight_reconcile_reader_blocks_active_index_digest_mismatch(tmp_path: Path) -> None:
    _write_canonical_reconcile_evidence(tmp_path)

    _, blockers, _ = read_reconcile_status(
        tmp_path,
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        activation_id=ACTIVATION_ID,
        required=True,
        airflow_index_sha256="sha256:" + "9" * 64,
    )

    assert {item["code"] for item in blockers} == {"airflow_pack_reconcile_identity_mismatch"}


def test_lightweight_reconcile_reader_blocks_checkpoint_revision_mismatch(tmp_path: Path) -> None:
    _write_canonical_reconcile_evidence(tmp_path)
    checkpoint_path = tmp_path / "status" / "desired-state-checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    checkpoint["observed_revision"] = '"new-etag"'
    checkpoint_path.write_text(json.dumps(checkpoint, sort_keys=True), encoding="utf-8")

    _, blockers, _ = read_reconcile_status(
        tmp_path,
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        activation_id=ACTIVATION_ID,
        required=True,
    )

    assert {item["code"] for item in blockers} == {"airflow_pack_reconcile_checkpoint_mismatch"}


@pytest.mark.parametrize("ack_location", ["root", "child", "alias"])
def test_external_ack_root_must_be_separate_from_cache(tmp_path: Path, ack_location: str) -> None:
    cache = _cache_with_index(tmp_path)
    if ack_location == "root":
        ack_root = cache
    elif ack_location == "child":
        ack_root = cache / "ack"
        ack_root.mkdir()
    else:
        ack_root = tmp_path / "cache-alias"
        ack_root.symlink_to(cache, target_is_directory=True)
    target = ack_root / "current-pointer.json"
    original = (cache / "current-pointer.json").read_bytes()

    with pytest.raises(LoaderAcknowledgementError, match="separate"):
        write_dpone_loader_ack(
            _load_report(cache),
            index_path=cache / "current" / "airflow-index.json",
            ack_path=target,
            ack_root=ack_root,
        )

    assert (cache / "current-pointer.json").read_bytes() == original


def test_external_ack_mode_is_group_readable_under_restrictive_umask(tmp_path: Path) -> None:
    cache = _cache_with_index(tmp_path)
    ack_root = tmp_path / "ack"
    ack_root.mkdir()
    previous_umask = os.umask(0o077)
    try:
        write_dpone_loader_ack(
            _load_report(cache),
            index_path=cache / "current" / "airflow-index.json",
            ack_path=ack_root / "loader-ack.json",
            ack_root=ack_root,
        )
    finally:
        os.umask(previous_umask)

    assert (ack_root / "loader-ack.json").stat().st_mode & 0o777 == 0o640


def test_cache_status_attaches_only_matching_external_ack(tmp_path: Path) -> None:
    cache = _cache_with_index(tmp_path)
    ack_root = tmp_path / "ack"
    ack_root.mkdir()
    ack_path = ack_root / "loader-ack.json"
    write_dpone_loader_ack(
        _load_report(cache),
        index_path=cache / "current" / "airflow-index.json",
        ack_path=ack_path,
        ack_root=ack_root,
    )
    status = read_airflow_pack_cache_status(cache)

    combined = attach_loader_ack_status(
        status,
        cache_root=cache,
        ack_path=ack_path,
        ack_root=ack_root,
    )

    assert combined["loader_ack"]["activation_id"] == ACTIVATION_ID
    assert not {item["code"] for item in combined["blockers"]} & {"airflow_loader_ack_invalid"}


def test_cache_status_rejects_ack_that_omits_expected_dag(tmp_path: Path) -> None:
    cache = _cache_with_index(tmp_path)
    ack_root = tmp_path / "ack"
    ack_root.mkdir()
    ack_path = ack_root / "loader-ack.json"
    write_dpone_loader_ack(
        _load_report(cache),
        index_path=cache / "current" / "airflow-index.json",
        ack_path=ack_path,
        ack_root=ack_root,
    )
    status = read_airflow_pack_cache_status(cache)
    status["last_reconcile_status"] = {"expected_dag_ids": ["DAG__platform__smoke__run", "DAG__sales__orders__refresh"]}

    combined = attach_loader_ack_status(
        status,
        cache_root=cache,
        ack_path=ack_path,
        ack_root=ack_root,
    )

    assert {item["code"] for item in combined["blockers"]} >= {"airflow_loader_ack_invalid"}


@pytest.mark.parametrize(
    ("loaded", "skipped", "errors"),
    [
        ([], ["DAG__platform__smoke__run"], ["DPONE_AIRFLOW_DAG_SPEC_INVALID"]),
        ([], ["DAG__platform__smoke__run"], []),
    ],
)
def test_cache_status_rejects_ack_that_did_not_load_every_expected_dag(
    tmp_path: Path,
    loaded: list[str],
    skipped: list[str],
    errors: list[str],
) -> None:
    cache = _cache_with_index(tmp_path)
    ack_root = tmp_path / "ack"
    ack_root.mkdir()
    ack_path = ack_root / "loader-ack.json"
    report = _load_report(cache)
    ack_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow_loader_ack.v2",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "airflow_index_sha256": report.airflow_index_sha256,
                "activation_id": ACTIVATION_ID,
                "loaded_dag_ids": loaded,
                "skipped_dag_ids": skipped,
                "error_codes": errors,
                "fatal": False,
                "acknowledged_at": "2026-08-03T00:00:00+00:00",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    status = read_airflow_pack_cache_status(cache)
    status["last_reconcile_status"] = {"expected_dag_ids": ["DAG__platform__smoke__run"]}

    combined = attach_loader_ack_status(
        status,
        cache_root=cache,
        ack_path=ack_path,
        ack_root=ack_root,
    )

    assert combined["status"] == "blocked"
    assert {item["code"] for item in combined["blockers"]} >= {"airflow_loader_ack_invalid"}


def test_cache_status_rejects_incomplete_external_ack(tmp_path: Path) -> None:
    cache = _cache_with_index(tmp_path)
    ack_root = tmp_path / "ack"
    ack_root.mkdir()
    ack_path = ack_root / "loader-ack.json"
    ack_path.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow_loader_ack.v2",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "airflow_index_sha256": _load_report(cache).airflow_index_sha256,
                "activation_id": ACTIVATION_ID,
                "fatal": False,
            }
        ),
        encoding="utf-8",
    )

    combined = attach_loader_ack_status(
        read_airflow_pack_cache_status(cache),
        cache_root=cache,
        ack_path=ack_path,
        ack_root=ack_root,
    )

    assert {item["code"] for item in combined["blockers"]} >= {"airflow_loader_ack_invalid"}


def test_exact_status_rejects_symlinked_diagnostic_and_pack(tmp_path: Path) -> None:
    cache = _cache_with_index(tmp_path, include_pack=True)
    outside = tmp_path / "outside.json"
    outside.write_text('{"outside":true}', encoding="utf-8")
    status_path = cache / "status" / "last-sync-status.json"
    status_path.parent.mkdir()
    status_path.symlink_to(outside)
    pack_path = cache / "releases" / "release" / "orders.json"
    pack_path.unlink()
    pack_path.symlink_to(outside)

    status = read_airflow_pack_cache_status(cache, workload_ids=("orders",))

    assert status["last_sync_status"] == {}
    assert status["workloads"]["orders"].get("sha256") is None
    assert status["workloads"]["orders"]["blockers"]


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO requires POSIX")
def test_exact_status_rejects_fifo_without_blocking(tmp_path: Path) -> None:
    cache = _cache_with_index(tmp_path, include_pack=True)
    status_path = cache / "status" / "last-sync-status.json"
    status_path.parent.mkdir()
    os.mkfifo(status_path)
    pack_path = cache / "releases" / "release" / "orders.json"
    pack_path.unlink()
    os.mkfifo(pack_path)

    status = read_airflow_pack_cache_status(cache, workload_ids=("orders",))

    assert status["last_sync_status"] == {}
    assert status["workloads"]["orders"]["blockers"]


def test_release_warnings_cannot_mask_primary_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class _BrokenLock:
        LOCK_EX = 2
        LOCK_UN = 1

        @staticmethod
        def flock(_descriptor: object, operation: object) -> None:
            if operation == _BrokenLock.LOCK_UN:
                raise OSError("release failed")

    monkeypatch.setattr(cache_activation_contract, "import_module", lambda _name: _BrokenLock)
    monkeypatch.setattr(cache_writer_coordination, "import_module", lambda _name: _BrokenLock)
    warnings.simplefilter("error", RuntimeWarning)
    try:
        with pytest.raises(ValueError, match="primary"):
            with cache_activation_contract.cache_write_lease(tmp_path / "cache"):
                raise ValueError("primary")
        with pytest.raises(ValueError, match="primary"):
            with cache_writer_coordination.cache_evidence_lease(tmp_path / "evidence"):
                raise ValueError("primary")
    finally:
        warnings.resetwarnings()


def test_external_only_warning_without_sink_is_emitted(tmp_path: Path) -> None:
    options = AirflowPackSyncOptions(
        index_uri="s3://example.invalid/private/latest/pack-index.json",
        cache_dir=tmp_path / "missing-cache",
    )

    with pytest.warns(RuntimeWarning, match="DPONE_AIRFLOW_PACK_EXTERNAL_WARNING_UNPUBLISHED"):
        evidence = write_sync_warning(options, reason="watch_sync_failed", message="redacted")

    assert evidence["diagnostic_authority"] == "external_only"


def test_no_argument_cache_root_preserves_compatibility_window(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DPONE_AIRFLOW_PACK_CACHE_DIR", raising=False)

    assert airflow_pack_cache_dir() == Path("/opt/airflow/dags/.dpone-cache/airflow")

    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", "/opt/airflow/.dpone-cache")
    assert airflow_pack_cache_dir() == Path("/opt/airflow/.dpone-cache")


def _cache_with_index(tmp_path: Path, *, include_pack: bool = False) -> Path:
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / ".promotion.lock").touch()
    activation = cache / "activations" / "dev" / ("sha256-" + "b" * 64)
    activation.mkdir(parents=True)
    workload_packs = []
    if include_pack:
        pack = cache / "releases" / "release" / "orders.json"
        pack.parent.mkdir(parents=True)
        pack.write_text('{"orders":true}', encoding="utf-8")
        workload_packs.append(
            {
                "id": "orders",
                "artifact_ref": "cache://releases/release/orders.json",
                "sha256": "sha256:" + hashlib.sha256(pack.read_bytes()).hexdigest(),
            }
        )
    (activation / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v2",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "dag_specs": [],
                "workload_packs": workload_packs,
                "runtime_artifact_delivery": {"mode": "init_fetch"},
            }
        ),
        encoding="utf-8",
    )
    (cache / "current").symlink_to(activation.relative_to(cache), target_is_directory=True)
    (cache / "current-pointer.json").write_text(
        json.dumps(
            {
                "schema": "dpone.current-pointer.v1",
                "environment": "dev",
                "release_id": RELEASE_ID,
                "deployment_id": DEPLOYMENT_ID,
                "activation_id": ACTIVATION_ID,
                "promoted_by": "test",
                "promoted_at": "2026-08-03T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    return cache


def _load_report(cache: Path) -> LoadReport:
    index_path = cache / "current" / "airflow-index.json"
    return LoadReport(
        loaded=("DAG__platform__smoke__run",),
        skipped=(),
        errors=(),
        fatal=False,
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        airflow_index_sha256="sha256:" + hashlib.sha256(index_path.read_bytes()).hexdigest(),
        activation_id=ACTIVATION_ID,
        cache_root=cache.as_posix(),
    )

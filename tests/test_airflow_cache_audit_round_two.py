from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from dpone_airflow_pack import cache_authority, cache_generation_retention
from dpone_airflow_pack.cache_activation_contract import cache_write_lease
from dpone_airflow_pack.cache_artifact_contract import infer_cache_root, read_confined_cache_file
from dpone_airflow_pack.cache_generation_retention import enforce_cache_retention
from dpone_airflow_pack.cache_generation_store import (
    CURRENT_COMMIT_PATH,
    recover_interrupted_commit,
)
from dpone_airflow_pack.cache_layout import LEGACY_PACK_INDEX_LAYOUT, ensure_cache_layout
from dpone_airflow_pack.cache_status import read_airflow_pack_cache_status
from dpone_airflow_pack.cache_sync import AirflowPackSyncOptions, sync_airflow_pack_cache
from dpone_airflow_pack.dag_spec_cache_paths import _indexed_dag_specs
from dpone_airflow_pack.dag_spec_loader import compute_dag_spec_fingerprint, load_dag_spec_file
from dpone_airflow_pack.deployment_index import load_airflow_deployment_index


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _write_remote_index(
    root: Path,
    generation: str,
    *,
    artifact_path: str = "orders/airflow-pack.json",
) -> Path:
    payload = b'{"kind":"gitops.airflow_pack"}'
    artifact = root / generation / "remote" / "airflow-pack.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(payload)
    index = {
        "git_sha": generation,
        "artifacts": {
            "orders": {
                "path": artifact_path,
                "uri": artifact.as_uri(),
                "sha256": _sha256(payload),
                "bytes": len(payload),
            }
        },
    }
    latest = root / "latest" / "pack-index.json"
    latest.parent.mkdir(parents=True)
    latest.write_text(json.dumps(index), encoding="utf-8")
    return latest


def _sync(root: Path, cache: Path, generation: str) -> None:
    result = sync_airflow_pack_cache(
        AirflowPackSyncOptions(
            index_uri=_write_remote_index(root, generation).as_uri(),
            cache_dir=cache,
        )
    )
    assert result["status"] == "success"


def test_sync_rejects_artifact_collision_with_authoritative_pack_index(tmp_path: Path) -> None:
    remote = tmp_path / "remote"
    cache = tmp_path / "cache"
    index = _write_remote_index(remote, "a" * 40, artifact_path="pack-index.json")

    with pytest.raises(ValueError, match="reserved|collision"):
        sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri=index.as_uri(), cache_dir=cache))

    assert not (cache / "current").exists()


def test_duplicate_dag_spec_destinations_are_rejected_before_dict_collapse(tmp_path: Path) -> None:
    generation = "a" * 40
    cache = tmp_path / "cache"
    generation_dir = cache / "generations" / generation
    generation_dir.mkdir(parents=True)
    duplicate = "airflow/_dags/shared.dag-spec.json"
    (generation_dir / "pack-index.json").write_text(
        json.dumps(
            {
                "git_sha": generation,
                "dag_specs": {
                    "DAG__one": {"path": duplicate, "sha256": "1" * 64, "bytes": 1},
                    "DAG__two": {"path": duplicate, "sha256": "2" * 64, "bytes": 1},
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(Exception, match="duplicate|differ|collision"):
        _indexed_dag_specs(cache, generation_dir)


def test_dag_spec_payload_id_must_match_authorized_index_key(tmp_path: Path) -> None:
    path = tmp_path / "DAG__expected.dag-spec.json"
    payload = {
        "kind": "gitops.airflow_dag_spec",
        "schema_version": "1",
        "producer": "test",
        "dag_id": "DAG__other",
        "schedule": None,
        "start_date": "2026-08-03",
        "nodes": [
            {
                "node_id": "app",
                "workload_id": "app",
                "pack_path": ".dpone/gitops/airflow/app/airflow-pack.json",
            }
        ],
        "edges": [],
        "topological_order": ["app"],
    }
    payload["spec_fingerprint"] = compute_dag_spec_fingerprint(payload)
    path.write_text(json.dumps(payload), encoding="utf-8")

    payload, issues = load_dag_spec_file(path, expected_dag_id="DAG__expected")

    assert payload is None
    assert issues[0].code == "DPONE_AIRFLOW_DAG_SPEC_IDENTITY_MISMATCH"


def test_invalid_managed_authority_never_drives_status_path_traversal(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "pack-index.json").write_text('{"secret":"must-not-be-read"}', encoding="utf-8")
    ensure_cache_layout(cache, expected_layout=LEGACY_PACK_INDEX_LAYOUT)
    (cache / "current").write_text(outside.as_posix(), encoding="utf-8")

    status = read_airflow_pack_cache_status(cache)

    assert status["status"] == "blocked"
    assert status["index_sha256"] is None
    assert status["current_generation"] is None


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO contract is POSIX-only")
@pytest.mark.parametrize("reader", ["control", "current"])
def test_control_readers_reject_fifo_without_blocking(tmp_path: Path, reader: str) -> None:
    fifo = tmp_path / ("current" if reader == "current" else "control")
    os.mkfifo(fifo)
    expression = "from pathlib import Path; " + (
        "from dpone_airflow_pack.cache_generation_files import read_control_json as read; read(Path(__import__('sys').argv[1]))"
        if reader == "control"
        else "from dpone_airflow_pack.cache_authority import read_current_generation as read; read(Path(__import__('sys').argv[1]).parent)"
    )

    result = subprocess.run(
        [sys.executable, "-c", expression, str(fifo)],
        capture_output=True,
        text=True,
        timeout=2,
        check=False,
    )

    assert result.returncode != 0


def test_dangling_managed_current_receipt_is_never_adopted_as_legacy(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    ensure_cache_layout(cache, expected_layout=LEGACY_PACK_INDEX_LAYOUT)
    receipt = cache / CURRENT_COMMIT_PATH
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.symlink_to("missing.json")

    with cache_write_lease(cache):
        with pytest.raises((OSError, ValueError), match="receipt|control|invalid|unreadable"):
            recover_interrupted_commit(cache)

    assert receipt.is_symlink()


def test_retention_rechecks_active_integrity_before_detaching_lkg(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    first = "a" * 40
    second = "b" * 40
    _sync(tmp_path / "remote-a", cache, first)
    _sync(tmp_path / "remote-b", cache, second)
    plan = cache_generation_retention._build_plan(
        cache,
        keep_generations=1,
        max_total_bytes=512 * 1024 * 1024,
        high_watermark_pct=80,
        low_watermark_pct=60,
        partial_download_ttl_minutes=30,
    )
    active = cache / "generations" / second / "orders" / "airflow-pack.json"
    active.chmod(0o640)
    active.write_bytes(active.read_bytes() + b"corrupt")

    detached = cache_generation_retention._detach_plan(cache, plan)

    assert detached.paths == ()
    assert {item["code"] for item in detached.blockers} == {"airflow_pack_cache_authority_mismatch"}
    assert (cache / "generations" / first).exists()


def test_retention_applies_policy_budget_before_hashing_active_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    generation = "a" * 40
    _sync(tmp_path / "remote", cache, generation)
    receipt = cache / "generations" / generation / ".dpone-generation-receipt.json"
    receipt.chmod(0o640)
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["size_bytes"] = 1024
    receipt.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(
        cache_authority,
        "file_sha256",
        lambda _path: pytest.fail("oversized active generation must be rejected before hashing"),
    )

    result = enforce_cache_retention(
        cache,
        keep_generations=1,
        max_total_bytes=128,
        high_watermark_pct=80,
        low_watermark_pct=60,
        partial_download_ttl_minutes=30,
    )

    assert result.blockers


def test_generation_order_is_deterministic_when_mtime_is_equal(tmp_path: Path) -> None:
    root = tmp_path / "cache" / "generations"
    root.mkdir(parents=True)
    paths = [root / "b", root / "a"]
    for path in paths:
        path.mkdir()
        os.utime(path, ns=(1_000_000_000, 1_000_000_000))

    assert [path.name for path in cache_generation_retention._generation_dirs(tmp_path / "cache")] == ["a", "b"]


def test_confined_read_accepts_symlink_spelled_cache_root_without_escaping(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    artifact = real / "artifact.json"
    artifact.write_bytes(b"{}")
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)

    assert read_confined_cache_file(alias / "artifact.json", cache_root=alias, max_bytes=16) == b"{}"


def test_inferred_cache_root_preserves_symlink_spelling_for_confinement(tmp_path: Path) -> None:
    real = tmp_path / "real"
    deployment = real / "deployments" / "candidate"
    deployment.mkdir(parents=True)
    index = deployment / "airflow-index.json"
    index.write_bytes(b"{}")
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    alias_index = alias / "deployments" / deployment.name / index.name

    inferred_root = infer_cache_root(alias_index)

    assert inferred_root == alias
    assert read_confined_cache_file(alias_index, cache_root=inferred_root, max_bytes=16) == b"{}"


def test_deployment_index_preserves_symlink_spelling_for_release_identity(tmp_path: Path) -> None:
    release_id = "sha256:" + "a" * 64
    deployment_id = "sha256:" + "b" * 64
    real = tmp_path / "real"
    artifact = real / "releases" / release_id.replace(":", "-") / "dags" / "orders.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"{}")
    index = real / "deployments" / "test" / deployment_id.replace(":", "-") / "airflow-index.json"
    index.parent.mkdir(parents=True)
    index.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "environment": "test",
                "release_id": release_id,
                "deployment_id": deployment_id,
                "dag_specs": [
                    {
                        "id": "orders",
                        "artifact_ref": f"cache://releases/{release_id.replace(':', '-')}/dags/orders.json",
                        "sha256": "sha256:" + _sha256(b"{}"),
                        "bytes": 2,
                    }
                ],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)

    loaded = load_airflow_deployment_index(alias / index.relative_to(real))

    assert loaded.dag_specs[0].path == alias / artifact.relative_to(real)


def test_exact_cache_status_surfaces_last_reconcile_failure_with_lkg_present(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / ".promotion.lock").touch()
    release_id = "sha256:" + "a" * 64
    deployment_id = "sha256:" + "b" * 64
    activation_id = "123e4567-e89b-42d3-a456-426614174001"
    activation = cache / "activations" / "dev" / ("sha256-" + "b" * 64)
    activation.mkdir(parents=True)
    (activation / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": release_id,
                "deployment_id": deployment_id,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "shared_pvc"},
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
                "deployment_id": deployment_id,
                "release_id": release_id,
                "activation_id": activation_id,
                "promoted_by": "dpone-ci",
                "promoted_at": "2026-08-03T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    status_root = cache / "status"
    status_root.mkdir()
    (status_root / "last-reconcile-status.json").write_text(
        json.dumps(
            {
                "schema": "dpone.error.v1",
                "passed": False,
                "errors": [{"code": "DPONE_AIRFLOW_DESIRED_STATE_FETCH_FAILED", "message": "redacted"}],
                "state_may_have_changed": False,
            }
        ),
        encoding="utf-8",
    )

    status = read_airflow_pack_cache_status(cache)

    assert status["status"] == "blocked"
    assert status["last_reconcile_status"]["passed"] is False
    assert "airflow_pack_reconcile_failed" in {item["code"] for item in status["blockers"]}

    (status_root / "last-reconcile-status.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-desired-state-reconcile.v1",
                "passed": True,
                "status": "activated",
                "environment": "dev",
                "observed_revision": '"etag"',
                "desired_state_sha256": "sha256:" + "d" * 64,
                "registry_scope_id": "sha256:" + "e" * 64,
                "source_project": "group/repository",
                "source_ref": "master",
                "release_id": release_id,
                "deployment_id": "sha256:" + "c" * 64,
                "occurrence_id": "123e4567-e89b-42d3-a456-426614174000",
                "source_git_sha": "1" * 40,
                "airflow_index_sha256": "sha256:" + "f" * 64,
                "runtime_image_digest": "sha256:" + "1" * 64,
                "expected_dag_ids": ["DAG__platform__smoke__run"],
                "activation_id": activation_id,
                "previous_deployment_id": None,
                "predecessor_status": "bootstrap",
                "materialized": True,
                "activated": True,
            }
        ),
        encoding="utf-8",
    )

    mismatched = read_airflow_pack_cache_status(cache)

    assert mismatched["status"] == "blocked"
    assert "airflow_pack_reconcile_identity_mismatch" in {item["code"] for item in mismatched["blockers"]}

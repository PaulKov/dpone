from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from threading import Barrier, Event
from typing import Any

import pytest

from dpone.app.airflow_cache_retention_composition import build_deployment_cache_retention_applier
from dpone.cli import main as cli_main
from dpone.contracts.airflow_deployment import deployment_id as compute_deployment_id
from dpone.contracts.airflow_deployment import release_id as compute_release_id


@dataclass(frozen=True)
class _CacheFixture:
    deployment: Path
    deployment_id: str
    release_dir: Path
    release_id: str
    artifacts: dict[str, Path]


def _run_cli(args: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err


def _process_promote(
    cache_root: str,
    deployment_dir: str,
    expected_current_deployment_id: str,
    start: Any,
    results: Any,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    start.wait()
    try:
        promoted = DeploymentCacheMaterializer(cache_root).promote(
            deployment_dir,
            environment="dev",
            expected_current_deployment_id=expected_current_deployment_id,
        )
    except DeploymentCacheError as exc:
        results.put(("error", exc.code))
        return
    results.put(("success", promoted.deployment_id))


def _write_fixture(
    cache_root: Path,
    *,
    deployment_id: str = "sha256:" + "b" * 64,
    label: str = "candidate",
    artifact_sections: tuple[str, ...] = ("dag_specs",),
    release_extension: dict[str, object] | None = None,
) -> _CacheFixture:
    artifact_payloads = {
        "dag_specs": ("orders_daily", "dags/orders_daily.dag-spec.json", {"dag_id": "orders_daily", "label": label}),
        "workload_packs": (
            "load_orders",
            "packs/load_orders.airflow-pack.json",
            {"schema_version": 3, "workload_id": "load_orders", "label": label},
        ),
    }
    release_artifacts: dict[str, list[dict[str, object]]] = {
        "dag_specs": [],
        "workload_packs": [],
        "canonical_schemas": [],
    }
    artifact_bytes: dict[str, bytes] = {}
    for section in artifact_sections:
        logical_id, relative_path, payload = artifact_payloads[section]
        content = json.dumps(payload, sort_keys=True).encode("utf-8")
        artifact_bytes[section] = content
        release_artifacts[section].append(
            {
                "id": logical_id,
                "path": relative_path,
                "sha256": "sha256:" + hashlib.sha256(content).hexdigest(),
            }
        )
    release = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": release_artifacts,
        **(release_extension or {}),
    }
    release_id = compute_release_id(release)
    release["release_id"] = release_id
    release_dir = cache_root / "releases" / release_id.replace(":", "-")
    release_dir.mkdir(parents=True, exist_ok=True)
    artifact_paths: dict[str, Path] = {}
    for section in artifact_sections:
        item = release_artifacts[section][0]
        path = release_dir / str(item["path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(artifact_bytes[section])
        artifact_paths[section] = path
    (release_dir / "release-set.json").write_text(json.dumps(release), encoding="utf-8")

    runtime_delivery = {"mode": "local_preview"}
    deployment_payload = {
        "schema": "dpone.deployment-set.v1",
        "deployment_id": "",
        "deployment_type": "preview",
        "runnable": False,
        "environment": "dev",
        "release_ref": release_id,
        "binding_set_ref": None,
        "connection_registry_ref": None,
        "credential_runtime_ref": None,
        "runtime_image_digest": None,
        "airflow_bundle_ref": None,
        "runtime_artifact_delivery": runtime_delivery,
        "fixture_identity": deployment_id,
    }
    computed_deployment_id = compute_deployment_id(deployment_payload)
    deployment_payload["deployment_id"] = computed_deployment_id
    deployment = cache_root / "deployments" / "dev" / computed_deployment_id.replace(":", "-")
    deployment.mkdir(parents=True)
    (deployment / "deployment.json").write_text(
        json.dumps(deployment_payload),
        encoding="utf-8",
    )
    index_artifacts: dict[str, list[dict[str, object]]] = {"dag_specs": [], "workload_packs": []}
    for section in artifact_sections:
        item = release_artifacts[section][0]
        index_artifacts[section].append(
            {
                "id": item["id"],
                "artifact_ref": f"cache://releases/{release_id.replace(':', '-')}/{item['path']}",
                "sha256": item["sha256"],
                "bytes": len(artifact_bytes[section]),
            }
        )
    (deployment / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": release_id,
                "deployment_id": computed_deployment_id,
                "binding_set_ref": None,
                "connection_registry_ref": None,
                "credential_runtime_ref": None,
                "runtime_image_digest": None,
                "airflow_bundle_ref": None,
                "runtime_artifact_delivery": runtime_delivery,
                **index_artifacts,
            }
        ),
        encoding="utf-8",
    )
    (deployment / "_SUCCESS").write_text("ok\n", encoding="utf-8")
    return _CacheFixture(deployment, computed_deployment_id, release_dir, release_id, artifact_paths)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_corrupt_release_artifact_does_not_change_current_pointer_or_audit(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    current = _write_fixture(cache_root, deployment_id="sha256:" + "b" * 64, label="current")
    materializer = DeploymentCacheMaterializer(cache_root)
    materializer.promote(current.deployment, environment="dev")
    original_pointer = (cache_root / "current-pointer.json").read_text(encoding="utf-8")
    original_audit = (cache_root / "current-pointer-audit.jsonl").read_text(encoding="utf-8")
    candidate = _write_fixture(cache_root, deployment_id="sha256:" + "c" * 64, label="candidate")
    artifact = candidate.artifacts["dag_specs"]
    artifact.write_bytes(b"x" * artifact.stat().st_size)

    with pytest.raises(DeploymentCacheError) as exc:
        materializer.promote(candidate.deployment, environment="dev")

    assert exc.value.code == "DPONE_CACHE_CHECKSUM_MISMATCH"
    assert exc.value.path == artifact.as_posix()
    assert (cache_root / "current-pointer.json").read_text(encoding="utf-8") == original_pointer
    assert (cache_root / "current-pointer-audit.jsonl").read_text(encoding="utf-8") == original_audit
    index = _load_json(cache_root / "current" / "airflow-index.json")
    assert index["deployment_id"] == current.deployment_id


def test_promotion_activates_sealed_snapshot_not_late_mutated_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    original_commit = DeploymentCacheMaterializer._commit_promotion

    def mutate_candidate_before_commit(self: DeploymentCacheMaterializer, **kwargs: Any):
        (fixture.deployment / "airflow-index.json").write_text("{}", encoding="utf-8")
        return original_commit(self, **kwargs)

    monkeypatch.setattr(DeploymentCacheMaterializer, "_commit_promotion", mutate_candidate_before_commit)

    result = DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert result.deployment_id == fixture.deployment_id
    assert _load_json(cache_root / "current" / "airflow-index.json")["deployment_id"] == fixture.deployment_id
    assert _load_json(fixture.deployment / "airflow-index.json") == {}
    assert "activations/dev" in (cache_root / "current").resolve().as_posix()


def test_promotion_revalidates_snapshot_after_candidate_changes_before_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime import deployment_cache_activation
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    original_copy = deployment_cache_activation._copy_regular_tree

    def mutate_then_copy(source: Path, target: Path, **kwargs: Any) -> None:
        (fixture.deployment / "airflow-index.json").write_text("{}", encoding="utf-8")
        original_copy(source, target, **kwargs)

    monkeypatch.setattr(deployment_cache_activation, "_copy_regular_tree", mutate_then_copy)

    with pytest.raises(DeploymentCacheError):
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    _assert_no_promotion_side_effects(cache_root)
    assert not (cache_root / "activations" / "dev" / fixture.deployment_id.replace(":", "-")).exists()


def test_snapshot_cleanup_failure_preserves_structured_mutation_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime import deployment_cache_activation
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    original_copy = deployment_cache_activation._copy_regular_tree

    def mutate_then_copy(source: Path, target: Path, **kwargs: Any) -> None:
        (fixture.deployment / "airflow-index.json").write_text("{}", encoding="utf-8")
        original_copy(source, target, **kwargs)

    def fail_cleanup(path: Path) -> None:
        del path
        raise OSError("synthetic cleanup failure")

    monkeypatch.setattr(deployment_cache_activation, "_copy_regular_tree", mutate_then_copy)
    monkeypatch.setattr(deployment_cache_activation, "remove_sealed_activation", fail_cleanup)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_AIRFLOW_INDEX_SCHEMA_INVALID"
    assert exc.value.details["state_may_have_changed"] is True
    assert exc.value.details["recovery_required"] is True
    assert exc.value.details["cleanup_failed_paths"] == ["staging"]


def test_promotion_revalidates_release_artifact_changed_during_snapshot_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime import deployment_cache_activation
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    original_copy = deployment_cache_activation._copy_regular_tree

    def mutate_release_then_copy(source: Path, target: Path, **kwargs: Any) -> None:
        artifact = fixture.artifacts["dag_specs"]
        artifact.write_bytes(b"x" * artifact.stat().st_size)
        original_copy(source, target, **kwargs)

    monkeypatch.setattr(deployment_cache_activation, "_copy_regular_tree", mutate_release_then_copy)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_CACHE_CHECKSUM_MISMATCH"
    _assert_no_promotion_side_effects(cache_root)
    assert not (cache_root / "activations" / "dev" / fixture.deployment_id.replace(":", "-")).exists()


def test_existing_activation_must_match_new_candidate_tree(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    materializer = DeploymentCacheMaterializer(cache_root)
    materializer.promote(fixture.deployment, environment="dev")
    pointer_before = (cache_root / "current-pointer.json").read_bytes()
    audit_before = (cache_root / "current-pointer-audit.jsonl").read_bytes()
    index_path = fixture.deployment / "airflow-index.json"
    index = _load_json(index_path)
    index["extension"] = "different candidate bytes"
    _write_json(index_path, index)

    with pytest.raises(DeploymentCacheError) as exc:
        materializer.promote(
            fixture.deployment,
            environment="dev",
            expected_current_deployment_id=fixture.deployment_id,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_ACTIVATION_CONFLICT"
    assert (cache_root / "current-pointer.json").read_bytes() == pointer_before
    assert (cache_root / "current-pointer-audit.jsonl").read_bytes() == audit_before
    assert "extension" not in _load_json(cache_root / "current" / "airflow-index.json")


def test_activation_snapshot_rejects_unreadable_source_subtree(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    hidden = fixture.deployment / "hidden"
    hidden.mkdir()
    (hidden / "must-not-be-omitted.txt").write_text("required", encoding="utf-8")
    hidden.chmod(0o000)
    try:
        with pytest.raises(DeploymentCacheError) as exc:
            DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")
    finally:
        hidden.chmod(0o700)

    assert exc.value.code == "DPONE_DEPLOYMENT_ACTIVATION_FAILED"
    _assert_no_promotion_side_effects(cache_root)
    assert not (cache_root / "activations" / "dev" / fixture.deployment_id.replace(":", "-")).exists()


def test_activation_snapshot_rejects_fifo_without_blocking(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    fifo = fixture.deployment / "blocking.fifo"
    os.mkfifo(fifo)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_DEPLOYMENT_ACTIVATION_FAILED"
    _assert_no_promotion_side_effects(cache_root)


def test_uppercase_deployment_identity_is_rejected_before_promotion(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    deployment = _load_json(fixture.deployment / "deployment.json")
    index = _load_json(fixture.deployment / "airflow-index.json")
    uppercase_id = "sha256:" + fixture.deployment_id.split(":", 1)[1].upper()
    deployment["deployment_id"] = uppercase_id
    index["deployment_id"] = uppercase_id
    uppercase_path = fixture.deployment.parent / uppercase_id.replace(":", "-")
    fixture.deployment.rename(uppercase_path)
    _write_json(uppercase_path / "deployment.json", deployment)
    _write_json(uppercase_path / "airflow-index.json", index)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(uppercase_path, environment="dev")

    assert exc.value.code == "DPONE_DEPLOYMENT_ID_INVALID"
    _assert_no_promotion_side_effects(cache_root)


def test_uppercase_release_set_identity_is_rejected_before_promotion(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    release_path = fixture.release_dir / "release-set.json"
    release = _load_json(release_path)
    release["release_id"] = "sha256:" + fixture.release_id.split(":", 1)[1].upper()
    _write_json(release_path, release)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_RELEASE_ID_INVALID"
    _assert_no_promotion_side_effects(cache_root)


def test_promotion_recomputes_release_fingerprint_before_accepting_reblessed_artifact(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    artifact = fixture.artifacts["dag_specs"]
    changed = b'{"dag_id":"orders_daily","changed":true}'
    artifact.write_bytes(changed)
    changed_digest = "sha256:" + hashlib.sha256(changed).hexdigest()
    release_path = fixture.release_dir / "release-set.json"
    release = _load_json(release_path)
    release["artifacts"]["dag_specs"][0]["sha256"] = changed_digest
    _write_json(release_path, release)
    index_path = fixture.deployment / "airflow-index.json"
    index = _load_json(index_path)
    index["dag_specs"][0]["sha256"] = changed_digest
    index["dag_specs"][0]["bytes"] = len(changed)
    _write_json(index_path, index)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_RELEASE_FINGERPRINT_MISMATCH"
    _assert_no_promotion_side_effects(cache_root)


def test_promotion_rejects_non_digest_release_identity(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    invalid_id = "release-v1"
    invalid_dir = cache_root / "releases" / invalid_id
    fixture.release_dir.rename(invalid_dir)
    release = _load_json(invalid_dir / "release-set.json")
    release["release_id"] = invalid_id
    _write_json(invalid_dir / "release-set.json", release)
    deployment = _load_json(fixture.deployment / "deployment.json")
    deployment["release_ref"] = invalid_id
    _write_json(fixture.deployment / "deployment.json", deployment)
    index = _load_json(fixture.deployment / "airflow-index.json")
    index["release_id"] = invalid_id
    _write_json(fixture.deployment / "airflow-index.json", index)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_RELEASE_ID_INVALID"
    _assert_no_promotion_side_effects(cache_root)


def test_promotion_requires_the_pinned_release_set(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    (fixture.release_dir / "release-set.json").unlink()

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_RELEASE_NOT_FOUND"
    _assert_no_promotion_side_effects(cache_root)


def test_promotion_rejects_release_set_symlink_escape(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    release_path = fixture.release_dir / "release-set.json"
    outside = tmp_path / "outside-release-set.json"
    release_path.replace(outside)
    release_path.symlink_to(outside)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_CACHE_PATH_ESCAPE"
    _assert_no_promotion_side_effects(cache_root)


def test_promotion_rejects_release_set_symlink_inside_release(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    release_path = fixture.release_dir / "release-set.json"
    alternate = fixture.release_dir / "same-release-set.json"
    alternate.write_bytes(release_path.read_bytes())
    release_path.unlink()
    release_path.symlink_to(alternate.name)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_CACHE_PATH_ESCAPE"
    _assert_no_promotion_side_effects(cache_root)


@pytest.mark.parametrize(
    ("filename", "expected_code"),
    [
        ("deployment.json", "DPONE_DEPLOYMENT_INVALID"),
        ("airflow-index.json", "DPONE_AIRFLOW_INDEX_INVALID"),
        ("_SUCCESS", "DPONE_DEPLOYMENT_INCOMPLETE"),
    ],
)
def test_promotion_never_follows_symlinked_projection_control_files(
    tmp_path: Path,
    filename: str,
    expected_code: str,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    control_file = fixture.deployment / filename
    outside = tmp_path / f"outside-{filename}"
    control_file.replace(outside)
    control_file.symlink_to(outside)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == expected_code
    assert exc.value.path == control_file.as_posix()
    _assert_no_promotion_side_effects(cache_root)


@pytest.mark.parametrize(
    ("mutation", "max_artifact_bytes", "expected_code"),
    [
        ("unpinned", 64 * 1024 * 1024, "DPONE_CACHE_UNPINNED_REFERENCE"),
        ("path_escape", 64 * 1024 * 1024, "DPONE_CACHE_PATH_ESCAPE"),
        ("symlink_escape", 64 * 1024 * 1024, "DPONE_CACHE_PATH_ESCAPE"),
        ("missing", 64 * 1024 * 1024, "DPONE_CACHE_ARTIFACT_NOT_FOUND"),
        ("oversized", 1, "DPONE_CACHE_ARTIFACT_TOO_LARGE"),
        ("index_mismatch", 64 * 1024 * 1024, "DPONE_RELEASE_INDEX_ARTIFACT_MISMATCH"),
        ("declared_size_mismatch", 64 * 1024 * 1024, "DPONE_CACHE_ARTIFACT_SIZE_MISMATCH"),
    ],
)
def test_promotion_rejects_unsafe_or_unverifiable_release_artifacts(
    tmp_path: Path,
    mutation: str,
    max_artifact_bytes: int,
    expected_code: str,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    artifact = fixture.artifacts["dag_specs"]
    index_path = fixture.deployment / "airflow-index.json"
    index = _load_json(index_path)
    if mutation == "unpinned":
        index["dag_specs"][0]["artifact_ref"] = "cache://current/dags/orders_daily.dag-spec.json"
        _write_json(index_path, index)
    elif mutation == "path_escape":
        index["dag_specs"][0]["artifact_ref"] = (
            f"cache://releases/{fixture.release_id.replace(':', '-')}/dags/../../outside.json"
        )
        _write_json(index_path, index)
    elif mutation == "missing":
        artifact.unlink()
    elif mutation == "symlink_escape":
        outside = tmp_path / "outside.dag-spec.json"
        artifact.replace(outside)
        artifact.symlink_to(outside)
    elif mutation == "index_mismatch":
        index["dag_specs"] = []
        _write_json(index_path, index)
    elif mutation == "declared_size_mismatch":
        index["dag_specs"][0]["bytes"] += 1
        _write_json(index_path, index)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root, max_artifact_bytes=max_artifact_bytes).promote(
            fixture.deployment,
            environment="dev",
        )

    assert exc.value.code == expected_code
    _assert_no_promotion_side_effects(cache_root)


def test_promotion_accepts_artifact_at_exact_size_limit(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    size = fixture.artifacts["dag_specs"].stat().st_size

    result = DeploymentCacheMaterializer(cache_root, max_artifact_bytes=size).promote(
        fixture.deployment,
        environment="dev",
    )

    assert result.release_id == fixture.release_id


def test_promotion_verifies_workload_pack_checksum(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root, artifact_sections=("workload_packs",))
    pack = fixture.artifacts["workload_packs"]
    pack.write_bytes(b"x" * pack.stat().st_size)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_CACHE_CHECKSUM_MISMATCH"
    _assert_no_promotion_side_effects(cache_root)


def test_late_artifact_read_failure_is_structured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.runtime import deployment_cache_artifact_verifier
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    artifact = fixture.artifacts["dag_specs"]
    original_open = deployment_cache_artifact_verifier.open_regular_file

    def fail_artifact_open(path: Path, **kwargs: Any):
        if Path(path) == artifact:
            raise DeploymentCacheError(
                "DPONE_CACHE_ARTIFACT_READ_FAILED",
                "indexed cache artifact could not be opened",
                path=path.as_posix(),
            )
        return original_open(path, **kwargs)

    monkeypatch.setattr(deployment_cache_artifact_verifier, "open_regular_file", fail_artifact_open)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_CACHE_ARTIFACT_READ_FAILED"
    _assert_no_promotion_side_effects(cache_root)


def test_pointer_write_failure_is_structured_and_requires_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime import deployment_cache_commit
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)

    def fail_pointer_write(path: Path, payload: dict[str, Any]) -> None:
        del path, payload
        raise PermissionError("read-only pointer")

    monkeypatch.setattr(deployment_cache_commit, "atomic_write_json", fail_pointer_write)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_CACHE_PROMOTION_WRITE_FAILED"
    assert exc.value.path == (cache_root / "current-pointer.json").as_posix()
    assert not (cache_root / "current").exists()
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current-pointer-audit.jsonl").exists()


def test_pointer_directory_fsync_failure_reports_possible_visible_pointer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime import deployment_cache_common
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)

    def fail_directory_fsync(path: Path) -> None:
        if path == cache_root:
            raise OSError("directory fsync unavailable")

    monkeypatch.setattr(deployment_cache_common, "_fsync_directory", fail_directory_fsync)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_CACHE_PROMOTION_WRITE_FAILED"
    assert exc.value.details == {
        "failed_step": "prepare_pointer",
        "state_may_have_changed": True,
        "recovery_required": True,
    }
    assert (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current").exists()


def test_artifact_symlink_inside_release_is_rejected_even_when_bytes_match(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    artifact = fixture.artifacts["dag_specs"]
    alternate = artifact.with_name("same-bytes.dag-spec.json")
    alternate.write_bytes(artifact.read_bytes())
    artifact.unlink()
    artifact.symlink_to(alternate.name)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_CACHE_PATH_ESCAPE"
    _assert_no_promotion_side_effects(cache_root)


def test_artifact_parent_symlink_swap_between_resolution_and_open_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime import deployment_cache_artifact_verifier
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    artifact = fixture.artifacts["dag_specs"]
    original_parent = artifact.parent.with_name("dags-original")
    outside_parent = tmp_path / "outside-dags"
    outside_parent.mkdir()
    (outside_parent / artifact.name).write_bytes(artifact.read_bytes())
    original_require_inside = deployment_cache_artifact_verifier._require_inside_root
    inside_checks = 0

    def swap_after_confinement(*args: Any, **kwargs: Any) -> None:
        nonlocal inside_checks
        original_require_inside(*args, **kwargs)
        inside_checks += 1
        if inside_checks == 2:
            artifact.parent.rename(original_parent)
            artifact.parent.symlink_to(outside_parent, target_is_directory=True)

    monkeypatch.setattr(deployment_cache_artifact_verifier, "_require_inside_root", swap_after_confinement)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_CACHE_ARTIFACT_READ_FAILED"
    _assert_no_promotion_side_effects(cache_root)


def test_promotion_fsyncs_audit_directory_before_current_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime import deployment_cache_audit
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    calls: list[Path] = []
    original = deployment_cache_audit.fsync_directory

    def record_directory_fsync(path: Path) -> None:
        calls.append(path)
        original(path)

    monkeypatch.setattr(deployment_cache_audit, "fsync_directory", record_directory_fsync)

    DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert calls == [cache_root]


def test_audit_write_failure_is_detected_and_repaired(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime import deployment_cache_commit
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_recovery import (
        DeploymentCacheRecoveryApplier,
        DeploymentCacheRecoveryPlanner,
    )

    cache_root = tmp_path / ".dpone-cache"
    initial = _write_fixture(cache_root, deployment_id="sha256:" + "b" * 64, label="initial")
    candidate = _write_fixture(cache_root, deployment_id="sha256:" + "c" * 64, label="candidate")
    materializer = DeploymentCacheMaterializer(cache_root)
    materializer.promote(initial.deployment, environment="dev")
    append_audit = deployment_cache_commit.append_promotion_audit

    def fail_audit_write(path: Path, payload: dict[str, Any]) -> None:
        del path, payload
        raise PermissionError("audit storage unavailable")

    monkeypatch.setattr(deployment_cache_commit, "append_promotion_audit", fail_audit_write)

    with pytest.raises(DeploymentCacheError) as exc:
        materializer.promote(candidate.deployment, environment="dev")

    assert exc.value.code == "DPONE_CACHE_PROMOTION_WRITE_FAILED"
    assert exc.value.path == (cache_root / "current-pointer-audit.jsonl").as_posix()
    plan = DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev").to_dict()
    assert plan["status"] == "repairable"
    assert plan["preferred_repair_deployment_id"] == initial.deployment_id
    assert {issue["code"] for issue in plan["issues"]} == {
        "DPONE_CURRENT_DEPLOYMENT_ID_MISMATCH",
        "DPONE_CURRENT_RELEASE_ID_MISMATCH",
        "DPONE_CURRENT_POINTER_AUDIT_MISMATCH",
    }

    monkeypatch.setattr(deployment_cache_commit, "append_promotion_audit", append_audit)
    DeploymentCacheRecoveryApplier(cache_root).apply(
        environment="dev",
        deployment_id=initial.deployment_id,
        confirm_repair=True,
        promoted_by="ci://recovery",
        expected_current_deployment_id=initial.deployment_id,
    )

    repaired = DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev").to_dict()
    assert repaired["status"] == "ok"
    assert repaired["issues"] == []


def test_current_entry_preparation_failure_is_structured(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)

    def fail_symlink(path: Path, *args: Any, **kwargs: Any) -> None:
        del path, args, kwargs
        raise OSError("symlinks unavailable")

    monkeypatch.setattr(Path, "symlink_to", fail_symlink)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_CACHE_PROMOTION_WRITE_FAILED"
    assert exc.value.path == (cache_root / "current").as_posix()
    assert exc.value.details == {
        "failed_step": "prepare_current",
        "state_may_have_changed": True,
        "recovery_required": False,
    }
    _assert_no_promotion_side_effects(cache_root)


def test_current_replace_failure_reports_split_state_and_cleans_temporary_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_commit import DeploymentCacheCommitter
    from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryPlanner

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)

    def fail_activation(self: DeploymentCacheCommitter, temporary_current: Path, current: Path) -> None:
        del self, temporary_current, current
        raise OSError("current replace unavailable")

    monkeypatch.setattr(DeploymentCacheCommitter, "_activate_current", fail_activation)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_CACHE_PROMOTION_WRITE_FAILED"
    assert exc.value.details == {
        "failed_step": "activate_current",
        "state_may_have_changed": True,
        "recovery_required": True,
    }
    assert not (cache_root / "current").exists()
    assert (cache_root / "current-pointer.json").exists()
    assert (cache_root / "current-pointer-audit.jsonl").exists()
    assert list(cache_root.glob(".current.*.tmp")) == []
    assert DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev").status == "repairable"


def test_current_directory_fsync_failure_reports_visible_consistent_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime import deployment_cache_commit
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryPlanner

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)

    def fail_current_fsync(path: Path) -> None:
        assert path == cache_root
        raise OSError("current directory fsync unavailable")

    monkeypatch.setattr(deployment_cache_commit, "fsync_directory", fail_current_fsync)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_CACHE_PROMOTION_WRITE_FAILED"
    assert exc.value.details == {
        "failed_step": "activate_current",
        "state_may_have_changed": True,
        "recovery_required": True,
    }
    assert (cache_root / "current").resolve().name == fixture.deployment_id.replace(":", "-")
    assert (cache_root / "current-pointer.json").exists()
    assert (cache_root / "current-pointer-audit.jsonl").exists()
    assert list(cache_root.glob(".current.*.tmp")) == []
    assert DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev").status == "ok"


def test_release_set_schema_keeps_relative_legacy_artifact_ref_compatible() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = _load_json(Path("docs/schemas/gitops/release-set.schema.json"))
    payload = {
        "schema": "dpone.release-set.v1",
        "release_id": "sha256:" + "a" * 64,
        "artifacts": {
            "dag_specs": [
                {
                    "id": "orders_daily",
                    "artifact_ref": "dags/orders_daily.dag-spec.json",
                    "sha256": "sha256:" + "b" * 64,
                }
            ],
            "workload_packs": [],
            "canonical_schemas": [],
        },
    }

    jsonschema.validate(payload, schema)


def test_release_set_schema_accepts_canonical_compact_pack_promotion() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = _load_json(Path("docs/schemas/gitops/release-set.schema.json"))
    payload = {
        "schema": "dpone.release-set.v1",
        "release_id": "sha256:" + "a" * 64,
        "artifacts": {
            "dag_specs": [],
            "workload_packs": [],
            "canonical_schemas": [],
        },
        "promotion": {
            "schema": "dpone.compact-pack-release-promotion.v1",
            "profile": "compact_v2_runtime_connection_context",
        },
    }

    jsonschema.validate(payload, schema)
    legacy_schema = deepcopy(schema)
    legacy_schema["properties"].pop("promotion")
    legacy_schema.pop("allOf")
    jsonschema.validate(payload, legacy_schema)


def test_release_set_schema_preserves_unrelated_legacy_promotion_extension() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = _load_json(Path("docs/schemas/gitops/release-set.schema.json"))
    payload = {
        "schema": "dpone.release-set.v1",
        "release_id": "sha256:" + "a" * 64,
        "artifacts": {
            "dag_specs": [],
            "workload_packs": [],
            "canonical_schemas": [],
        },
        "promotion": {
            "schema": "acme.release-promotion.v1",
            "profile": "blue_green",
            "vendor_extension": True,
        },
    }

    jsonschema.validate(payload, schema)


@pytest.mark.parametrize(
    "promotion",
    (
        {
            "schema": "dpone.compact-pack-release-promotion.v1",
            "profile": "unsafe",
        },
        {
            "schema": "dpone.compact-pack-release-promotion.v999",
            "profile": "unsafe",
        },
    ),
)
def test_cache_sync_rejects_invalid_compact_promotion_before_activation(
    tmp_path: Path,
    promotion: dict[str, object],
) -> None:
    from dpone.readiness.airflow_self_service_cache_sync import cache_sync_result

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(
        cache_root,
        release_extension={
            "promotion": promotion,
        },
    )

    result = cache_sync_result(
        cache_root=cache_root,
        deployment_dir=fixture.deployment,
        environment="dev",
        promoted_by="ci://tests",
        allowed_promoters=("ci://tests",),
        confirm_promote=True,
        expect_current_absent=True,
    )

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_RELEASE_SET_INVALID"
    _assert_no_promotion_side_effects(cache_root)


@pytest.mark.parametrize(
    "artifact",
    [
        {"id": "orders_daily", "sha256": "sha256:" + "b" * 64},
        {
            "id": "orders_daily",
            "path": "dags/orders_daily.dag-spec.json",
            "artifact_ref": "dags/orders_daily.dag-spec.json",
            "sha256": "sha256:" + "b" * 64,
        },
        {
            "id": "orders_daily",
            "artifact_ref": "cache://releases/sha256-aaa/dags/orders_daily.dag-spec.json",
            "sha256": "sha256:" + "b" * 64,
        },
        {
            "id": "orders_daily",
            "path": "/etc/passwd",
            "sha256": "sha256:" + "b" * 64,
        },
        {
            "id": "orders_daily",
            "path": "dags/orders daily.dag-spec.json",
            "sha256": "sha256:" + "b" * 64,
        },
        {
            "id": "orders_daily",
            "path": "dags//orders.json",
            "sha256": "sha256:" + "b" * 64,
        },
        {
            "id": "orders_daily",
            "path": "./dags/orders.json",
            "sha256": "sha256:" + "b" * 64,
        },
        {
            "id": "orders_daily",
            "path": "dags/\x00orders.json",
            "sha256": "sha256:" + "b" * 64,
        },
    ],
)
def test_release_set_schema_rejects_ambiguous_or_unsafe_artifact_locators(artifact: dict[str, str]) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = _load_json(Path("docs/schemas/gitops/release-set.schema.json"))
    payload = {
        "schema": "dpone.release-set.v1",
        "release_id": "sha256:" + "a" * 64,
        "artifacts": {
            "dag_specs": [artifact],
            "workload_packs": [],
            "canonical_schemas": [],
        },
    }

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, schema)


def test_promotion_accepts_relative_legacy_artifact_ref(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    release_path = fixture.release_dir / "release-set.json"
    release = _load_json(release_path)
    item = release["artifacts"]["dag_specs"][0]
    item["artifact_ref"] = item.pop("path")
    legacy_release_id = compute_release_id(release)
    release["release_id"] = legacy_release_id
    legacy_release_dir = cache_root / "releases" / legacy_release_id.replace(":", "-")
    fixture.release_dir.rename(legacy_release_dir)
    _write_json(legacy_release_dir / "release-set.json", release)
    deployment = _load_json(fixture.deployment / "deployment.json")
    deployment["release_ref"] = legacy_release_id
    _write_json(fixture.deployment / "deployment.json", deployment)
    index = _load_json(fixture.deployment / "airflow-index.json")
    index["release_id"] = legacy_release_id
    index["dag_specs"][0]["artifact_ref"] = (
        f"cache://releases/{legacy_release_id.replace(':', '-')}/dags/orders_daily.dag-spec.json"
    )
    _write_json(fixture.deployment / "airflow-index.json", index)

    result = DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert result.release_id == legacy_release_id


def test_cache_sync_checksum_failure_has_json_detail_and_topology_free_text(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    artifact = fixture.artifacts["dag_specs"]
    artifact.write_bytes(b"x" * artifact.stat().st_size)
    command = [
        "airflow",
        "cache-sync",
        "--cache-root",
        str(cache_root),
        "--environment",
        "dev",
        "--deployment-dir",
        str(fixture.deployment),
        "--promoted-by",
        "ci://github-actions/dpone-airflow",
        "--allowed-promoter",
        "ci://github-actions/dpone-airflow",
        "--expect-current-absent",
        "--confirm-promote",
    ]

    code, stdout, stderr = _run_cli([*command, "--format", "json"], capsys)

    assert code == 4
    assert stderr == ""
    payload = json.loads(stdout)
    assert payload["errors"][0] == {
        "schema": "dpone.error.v1",
        "code": "DPONE_CACHE_CHECKSUM_MISMATCH",
        "stage": "cache_sync",
        "severity": "error",
        "message": "indexed cache artifact checksum does not match the release-set",
        "fixes": [{"id": "restore_pinned_release_artifacts", "safety": "manual"}],
        "path": "$ABSOLUTE_PATH",
        "docs_url": "https://paulkov.github.io/dpone/errors/DPONE_CACHE_CHECKSUM_MISMATCH/",
    }
    _assert_no_promotion_side_effects(cache_root)

    code, stdout, stderr = _run_cli(command, capsys)

    assert code == 4
    assert stderr == ""
    assert "dpone airflow cache sync: FAILED" in stdout
    assert "- environment: dev" in stdout
    assert "- issue: DPONE_CACHE_CHECKSUM_MISMATCH:" in stdout
    assert "- help: https://paulkov.github.io/dpone/errors/DPONE_CACHE_CHECKSUM_MISMATCH/" in stdout
    assert "- current pointer: not changed" in stdout
    assert "- action: restore the pinned release artifacts, then rerun dpone airflow cache-sync" in stdout
    assert "- details: rerun with --format json for the structured error and failing path" in stdout
    assert str(cache_root) not in stdout
    assert artifact.as_posix() not in stdout


@pytest.mark.parametrize(
    ("code", "expected_action"),
    [
        (
            "DPONE_RELEASE_FINGERPRINT_MISMATCH",
            "rebuild the immutable release-set and deployment projection, then rerun dpone airflow cache-sync",
        ),
        (
            "DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
            "rebuild the deployment projection from the pinned release, then rerun dpone airflow cache-sync",
        ),
        (
            "DPONE_CACHE_ARTIFACT_TOO_LARGE",
            "review the cache artifact size policy or build a smaller release before retrying",
        ),
        (
            "DPONE_RELEASE_NOT_FOUND",
            "materialize the pinned release or rebuild a projection with a valid release_ref",
        ),
        (
            "DPONE_DEPLOYMENT_PATH_OUTSIDE_CACHE_ROOT",
            "select a deployment projection inside the configured cache root",
        ),
        (
            "DPONE_DEPLOYMENT_PATH_INVALID",
            "use the canonical deployments/<environment>/<deployment_id> cache layout",
        ),
        (
            "DPONE_AIRFLOW_INDEX_NOT_FOUND",
            "rebuild the deployment projection from the pinned release, then rerun dpone airflow cache-sync",
        ),
        (
            "DPONE_DEPLOYMENT_NOT_FOUND",
            "rebuild the deployment projection from the pinned release, then rerun dpone airflow cache-sync",
        ),
        (
            "DPONE_DEPLOYMENT_CACHE_SYNC_CONFIRMATION_REQUIRED",
            "add --confirm-promote after reviewing the candidate deployment",
        ),
        (
            "DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED",
            "use an allowed CI/service identity or update the platform allowlist",
        ),
        (
            "DPONE_CURRENT_POINTER_CAS_MISMATCH",
            "refresh current state and retry with its deployment id as the CAS guard",
        ),
    ],
)
def test_cache_sync_text_gives_code_specific_recovery_action(code: str, expected_action: str) -> None:
    from dpone.commands.airflow_cache_sync_rendering import self_service_cache_sync_text

    output = self_service_cache_sync_text(
        {
            "passed": False,
            "environment": "prod",
            "errors": [{"code": code, "message": "blocked", "path": "/private/cache/path"}],
        }
    )

    assert f"- action: {expected_action}" in output
    assert "/private/cache/path" not in output


def test_cache_sync_text_marks_partial_write_failure_for_recovery() -> None:
    from dpone.commands.airflow_cache_sync_rendering import self_service_cache_sync_text

    output = self_service_cache_sync_text(
        {
            "passed": False,
            "environment": "prod",
            "errors": [
                {
                    "code": "DPONE_CACHE_PROMOTION_WRITE_FAILED",
                    "message": "pointer write failed",
                    "path": "/private/cache/current-pointer.json",
                }
            ],
        }
    )

    assert "- current pointer: recovery required" in output
    assert "- action: run dpone airflow cache-recovery-plan before retrying promotion" in output
    assert "current pointer: not changed" not in output
    assert "structured error and failing path" in output
    assert "artifact path" not in output
    assert "/private/cache" not in output


def test_cache_sync_path_errors_include_structured_manual_fixes(tmp_path: Path) -> None:
    from dpone.readiness.airflow_self_service_cache_sync import cache_sync_result

    cache_root = tmp_path / ".dpone-cache"
    invalid_layout = cache_root / "candidate"
    invalid_layout.mkdir(parents=True)
    outside = cache_sync_result(
        cache_root=cache_root,
        deployment_dir=tmp_path / "outside",
        environment="prod",
        promoted_by="ci://test",
        allowed_promoters=("ci://test",),
        confirm_promote=True,
    ).to_dict()
    invalid = cache_sync_result(
        cache_root=cache_root,
        deployment_dir=invalid_layout,
        environment="prod",
        promoted_by="ci://test",
        allowed_promoters=("ci://test",),
        confirm_promote=True,
    ).to_dict()

    assert outside["errors"][0]["fixes"] == [{"id": "select_deployment_inside_cache_root", "safety": "manual"}]
    assert invalid["errors"][0]["fixes"] == [{"id": "use_canonical_deployment_cache_layout", "safety": "manual"}]


def test_release_artifact_locator_rejects_schema_invalid_whitespace() -> None:
    from dpone.contracts.airflow_release_artifacts import ReleaseArtifactLocatorError, release_artifact_path

    with pytest.raises(ReleaseArtifactLocatorError):
        release_artifact_path({"path": "dags/orders daily.dag-spec.json"})


@pytest.mark.parametrize(
    "locator",
    (
        "./dags/orders.json",
        "dags//orders.json",
        "dags/\x00orders.json",
        "dags/\x1forders.json",
        "dags/orders.json/..",
    ),
)
def test_release_artifact_locator_rejects_noncanonical_raw_paths(locator: str) -> None:
    from dpone.contracts.airflow_release_artifacts import ReleaseArtifactLocatorError, release_artifact_path

    with pytest.raises(ReleaseArtifactLocatorError):
        release_artifact_path({"path": locator})


def test_release_artifact_legacy_alias_has_same_semantic_release_id() -> None:
    canonical = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {
            "dag_specs": [
                {
                    "id": "orders_daily",
                    "path": "dags/orders_daily.dag-spec.json",
                    "sha256": "sha256:" + "a" * 64,
                }
            ],
            "workload_packs": [],
            "canonical_schemas": [],
        },
    }
    legacy = json.loads(json.dumps(canonical))
    item = legacy["artifacts"]["dag_specs"][0]
    item["artifact_ref"] = item.pop("path")

    assert compute_release_id(canonical) == compute_release_id(legacy)


def test_uppercase_sha256_artifact_metadata_is_verified_case_insensitively(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    release_path = fixture.release_dir / "release-set.json"
    release = _load_json(release_path)
    release_digest = release["artifacts"]["dag_specs"][0]["sha256"]
    release["artifacts"]["dag_specs"][0]["sha256"] = "sha256:" + release_digest.split(":", 1)[1].upper()
    upper_release_id = compute_release_id(release)
    release["release_id"] = upper_release_id
    upper_release_dir = cache_root / "releases" / upper_release_id.replace(":", "-")
    fixture.release_dir.rename(upper_release_dir)
    _write_json(upper_release_dir / "release-set.json", release)
    deployment = _load_json(fixture.deployment / "deployment.json")
    deployment["release_ref"] = upper_release_id
    index = _load_json(fixture.deployment / "airflow-index.json")
    index["release_id"] = upper_release_id
    index_digest = index["dag_specs"][0]["sha256"]
    index["dag_specs"][0]["sha256"] = "sha256:" + index_digest.split(":", 1)[1].upper()
    index["dag_specs"][0]["artifact_ref"] = (
        f"cache://releases/{upper_release_id.replace(':', '-')}/dags/orders_daily.dag-spec.json"
    )
    deployment["deployment_id"] = ""
    updated_deployment_id = compute_deployment_id(deployment)
    deployment["deployment_id"] = updated_deployment_id
    index["deployment_id"] = updated_deployment_id
    updated_deployment_dir = fixture.deployment.parent / updated_deployment_id.replace(":", "-")
    fixture.deployment.rename(updated_deployment_dir)
    _write_json(updated_deployment_dir / "deployment.json", deployment)
    _write_json(updated_deployment_dir / "airflow-index.json", index)

    result = DeploymentCacheMaterializer(cache_root).promote(updated_deployment_dir, environment="dev")

    assert result.release_id == upper_release_id


def test_invalid_utf8_projection_is_reported_as_structured_error(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    (fixture.deployment / "deployment.json").write_bytes(b"\xff\xfe")

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_DEPLOYMENT_INVALID"
    _assert_no_promotion_side_effects(cache_root)


@pytest.mark.parametrize("control_name", ("current-pointer.json", "current-pointer-audit.jsonl", ".promotion.lock"))
def test_promotion_never_follows_symlinked_control_files(tmp_path: Path, control_name: str) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    outside = tmp_path / f"outside-{control_name.replace('/', '-')}"
    outside.write_text("keep\n", encoding="utf-8")
    (cache_root / control_name).symlink_to(outside)

    with pytest.raises(DeploymentCacheError):
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert outside.read_text(encoding="utf-8") == "keep\n"
    assert not (cache_root / "current").exists()


def test_promotion_rejects_symlinked_deployment_directory(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    alias = fixture.deployment.parent / ("sha256-" + "d" * 64)
    alias.symlink_to(fixture.deployment.name, target_is_directory=True)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(alias, environment="dev")

    assert exc.value.code == "DPONE_CACHE_PATH_ESCAPE"
    _assert_no_promotion_side_effects(cache_root)


def test_concurrent_guarded_promotions_have_one_cas_winner(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    initial = _write_fixture(cache_root, deployment_id="sha256:" + "a" * 64, label="initial")
    DeploymentCacheMaterializer(cache_root).promote(initial.deployment, environment="dev")
    candidates = (
        _write_fixture(cache_root, deployment_id="sha256:" + "b" * 64, label="first"),
        _write_fixture(cache_root, deployment_id="sha256:" + "c" * 64, label="second"),
    )
    barrier = Barrier(2)

    def promote(candidate: _CacheFixture) -> tuple[str, str]:
        barrier.wait()
        try:
            result = DeploymentCacheMaterializer(cache_root).promote(
                candidate.deployment,
                environment="dev",
                expected_current_deployment_id=initial.deployment_id,
            )
        except DeploymentCacheError as exc:
            return "error", exc.code
        return "success", result.deployment_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(promote, candidates))

    assert [kind for kind, _ in results].count("success") == 1
    assert ("error", "DPONE_CURRENT_POINTER_CAS_MISMATCH") in results
    pointer = _load_json(cache_root / "current-pointer.json")
    current = _load_json(cache_root / "current" / "deployment.json")
    latest_audit = json.loads((cache_root / "current-pointer-audit.jsonl").read_text(encoding="utf-8").splitlines()[-1])
    assert pointer["deployment_id"] == current["deployment_id"] == latest_audit["deployment_id"]


def test_multiprocess_guarded_promotions_have_one_cas_winner(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    initial = _write_fixture(cache_root, deployment_id="sha256:" + "a" * 64, label="initial")
    DeploymentCacheMaterializer(cache_root).promote(initial.deployment, environment="dev")
    candidates = (
        _write_fixture(cache_root, deployment_id="sha256:" + "b" * 64, label="first"),
        _write_fixture(cache_root, deployment_id="sha256:" + "c" * 64, label="second"),
    )
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    results = context.Queue()
    processes = [
        context.Process(
            target=_process_promote,
            args=(
                cache_root.as_posix(),
                candidate.deployment.as_posix(),
                initial.deployment_id,
                start,
                results,
            ),
        )
        for candidate in candidates
    ]
    for process in processes:
        process.start()
    start.set()
    for process in processes:
        process.join(timeout=15)

    assert [process.exitcode for process in processes] == [0, 0]
    outcomes = [results.get(timeout=2) for _ in processes]
    assert [kind for kind, _ in outcomes].count("success") == 1
    assert ("error", "DPONE_CURRENT_POINTER_CAS_MISMATCH") in outcomes


def test_concurrent_first_promotions_support_expected_absent_cas(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    candidates = (
        _write_fixture(cache_root, deployment_id="sha256:" + "b" * 64, label="first"),
        _write_fixture(cache_root, deployment_id="sha256:" + "c" * 64, label="second"),
    )
    barrier = Barrier(2)

    def promote(candidate: _CacheFixture) -> tuple[str, str]:
        barrier.wait()
        try:
            result = DeploymentCacheMaterializer(cache_root).promote(
                candidate.deployment,
                environment="dev",
                expect_current_absent=True,
            )
        except DeploymentCacheError as exc:
            return "error", exc.code
        return "success", result.deployment_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(promote, candidates))

    assert [kind for kind, _ in results].count("success") == 1
    assert ("error", "DPONE_CURRENT_POINTER_CAS_MISMATCH") in results


def test_mutated_deployment_payload_cannot_reuse_content_identity(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    deployment_path = fixture.deployment / "deployment.json"
    index_path = fixture.deployment / "airflow-index.json"
    deployment = _load_json(deployment_path)
    index = _load_json(index_path)
    changed_digest = "sha256:" + "9" * 64
    deployment["runtime_image_digest"] = changed_digest
    index["runtime_image_digest"] = changed_digest
    _write_json(deployment_path, deployment)
    _write_json(index_path, index)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_DEPLOYMENT_FINGERPRINT_MISMATCH"
    _assert_no_promotion_side_effects(cache_root)


def test_deployment_index_mirrors_environment_specific_runtime_fields(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    index_path = fixture.deployment / "airflow-index.json"
    index = _load_json(index_path)
    index["runtime_image_digest"] = "sha256:" + "9" * 64
    _write_json(index_path, index)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    assert exc.value.code == "DPONE_DEPLOYMENT_INDEX_MIRROR_MISMATCH"
    assert exc.value.path == index_path.as_posix()


def test_deployment_projection_requires_runtime_delivery_contract(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    deployment_path = fixture.deployment / "deployment.json"
    index_path = fixture.deployment / "airflow-index.json"
    deployment = _load_json(deployment_path)
    index = _load_json(index_path)
    deployment.pop("runtime_artifact_delivery")
    index.pop("runtime_artifact_delivery")
    deployment["deployment_id"] = ""
    changed_id = compute_deployment_id(deployment)
    deployment["deployment_id"] = changed_id
    index["deployment_id"] = changed_id
    changed_dir = fixture.deployment.parent / changed_id.replace(":", "-")
    fixture.deployment.rename(changed_dir)
    _write_json(changed_dir / "deployment.json", deployment)
    _write_json(changed_dir / "airflow-index.json", index)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(changed_dir, environment="dev")

    assert exc.value.code == "DPONE_DEPLOYMENT_SCHEMA_INVALID"


def test_cache_sync_accepts_matching_v2_deployment_index_wire(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    deployment_path = fixture.deployment / "deployment.json"
    index_path = fixture.deployment / "airflow-index.json"
    deployment = _load_json(deployment_path)
    index = _load_json(index_path)
    deployment["schema"] = "dpone.deployment-set.v2"
    index["schema"] = "dpone.airflow-deployment-index.v2"
    deployment["deployment_id"] = ""
    changed_id = compute_deployment_id(deployment)
    deployment["deployment_id"] = changed_id
    index["deployment_id"] = changed_id
    changed_dir = fixture.deployment.parent / changed_id.replace(":", "-")
    fixture.deployment.rename(changed_dir)
    _write_json(changed_dir / "deployment.json", deployment)
    _write_json(changed_dir / "airflow-index.json", index)
    (changed_dir / "_SUCCESS").write_text("", encoding="utf-8")

    promoted = DeploymentCacheMaterializer(cache_root).promote(changed_dir, environment="dev")

    assert promoted.deployment_id == changed_id
    assert (cache_root / "current" / "airflow-index.json").is_file()
    assert _load_json(cache_root / "current" / "airflow-index.json")["schema"] == ("dpone.airflow-deployment-index.v2")


def test_cache_sync_rejects_mismatched_v1_v2_schema_wire(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    deployment_path = fixture.deployment / "deployment.json"
    index_path = fixture.deployment / "airflow-index.json"
    deployment = _load_json(deployment_path)
    index = _load_json(index_path)
    deployment["schema"] = "dpone.deployment-set.v2"
    # Keep index on v1 to prove mixed wires are rejected.
    deployment["deployment_id"] = ""
    changed_id = compute_deployment_id(deployment)
    deployment["deployment_id"] = changed_id
    index["deployment_id"] = changed_id
    changed_dir = fixture.deployment.parent / changed_id.replace(":", "-")
    fixture.deployment.rename(changed_dir)
    _write_json(changed_dir / "deployment.json", deployment)
    _write_json(changed_dir / "airflow-index.json", index)
    (changed_dir / "_SUCCESS").write_text("", encoding="utf-8")

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(changed_dir, environment="dev")

    assert exc.value.code == "DPONE_DEPLOYMENT_INDEX_MIRROR_MISMATCH"


def test_recovery_plan_never_offers_projection_without_verified_index(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryPlanner

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    (fixture.deployment / "airflow-index.json").unlink()

    plan = DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev").to_dict()

    assert plan["status"] == "blocked"
    assert plan["repair_candidates"] == []
    assert "DPONE_AIRFLOW_INDEX_NOT_FOUND" in {issue["code"] for issue in plan["issues"]}


def test_recovery_apply_is_cas_bound_to_reviewed_active_state(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_recovery import (
        DeploymentCacheRecoveryApplier,
        DeploymentCacheRecoveryApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    initial = _write_fixture(cache_root, deployment_id="sha256:" + "b" * 64, label="initial")
    candidate = _write_fixture(cache_root, deployment_id="sha256:" + "c" * 64, label="candidate")
    materializer = DeploymentCacheMaterializer(cache_root)
    materializer.promote(initial.deployment, environment="dev")
    (cache_root / "current-pointer.json").unlink()
    materializer.recover(
        candidate.deployment,
        environment="dev",
        promoted_by="ci://other-recovery",
        expected_current_deployment_id=initial.deployment_id,
    )

    with pytest.raises(DeploymentCacheRecoveryApplyError) as exc:
        DeploymentCacheRecoveryApplier(cache_root).apply(
            environment="dev",
            deployment_id=initial.deployment_id,
            confirm_repair=True,
            promoted_by="ci://stale-recovery",
            expected_current_deployment_id=initial.deployment_id,
        )

    assert exc.value.code == "DPONE_CURRENT_POINTER_CAS_MISMATCH"
    assert _load_json(cache_root / "current" / "deployment.json")["deployment_id"] == candidate.deployment_id


def test_recovery_rejects_stale_plan_before_publishing_activation(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    initial = _write_fixture(cache_root, deployment_id="sha256:" + "b" * 64, label="initial")
    candidate = _write_fixture(cache_root, deployment_id="sha256:" + "c" * 64, label="candidate")
    materializer = DeploymentCacheMaterializer(cache_root)
    materializer.promote(initial.deployment, environment="dev")
    activation_root = cache_root / "activations" / "dev"
    activations_before = sorted(path.name for path in activation_root.iterdir())

    with pytest.raises(DeploymentCacheError) as exc:
        materializer.recover(
            candidate.deployment,
            environment="dev",
            promoted_by="ci://stale-recovery",
            expected_current_deployment_id="sha256:" + "9" * 64,
        )

    assert exc.value.code == "DPONE_CURRENT_POINTER_CAS_MISMATCH"
    assert exc.value.details == {}
    assert sorted(path.name for path in activation_root.iterdir()) == activations_before


def test_recovery_apply_refuses_healthy_cache(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_recovery import (
        DeploymentCacheRecoveryApplier,
        DeploymentCacheRecoveryApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")

    with pytest.raises(DeploymentCacheRecoveryApplyError) as exc:
        DeploymentCacheRecoveryApplier(cache_root).apply(
            environment="dev",
            deployment_id=fixture.deployment_id,
            confirm_repair=True,
            promoted_by="ci://recovery",
            expected_current_deployment_id=fixture.deployment_id,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_RECOVERY_NOT_REQUIRED"


def test_audit_only_recovery_preserves_pointer_provenance_and_enforces_actor(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_recovery import (
        DeploymentCacheRecoveryApplier,
        DeploymentCacheRecoveryApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    DeploymentCacheMaterializer(cache_root).promote(
        fixture.deployment,
        environment="dev",
        promoted_by="ci://publisher",
        source_commit="7ac31f2",
        attestation_ref="attestation://release/1",
    )
    audit_path = cache_root / "current-pointer-audit.jsonl"
    audit_path.unlink()
    pointer_path = cache_root / "current-pointer.json"
    original_pointer = pointer_path.read_bytes()

    with pytest.raises(DeploymentCacheRecoveryApplyError) as exc:
        DeploymentCacheRecoveryApplier(cache_root).apply(
            environment="dev",
            deployment_id=fixture.deployment_id,
            confirm_repair=True,
            promoted_by="ci://untrusted",
            expected_current_deployment_id=fixture.deployment_id,
            allowed_promoters=("ci://trusted-recovery",),
        )
    assert exc.value.code == "DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED"

    DeploymentCacheRecoveryApplier(cache_root).apply(
        environment="dev",
        deployment_id=fixture.deployment_id,
        confirm_repair=True,
        promoted_by="ci://trusted-recovery",
        expected_current_deployment_id=fixture.deployment_id,
        allowed_promoters=("ci://trusted-recovery",),
    )

    assert pointer_path.read_bytes() == original_pointer
    audit_event = json.loads(audit_path.read_text(encoding="utf-8").splitlines()[-1])
    assert audit_event["promoted_by"] == "ci://publisher"
    assert audit_event["source_commit"] == "7ac31f2"
    assert audit_event["recovery"]["actor"] == "ci://trusted-recovery"


def test_recovery_rebuilds_pointer_when_release_identity_differs_from_current(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_current_state import DeploymentCacheCurrentState
    from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryApplier, DeploymentCacheRecoveryPlanner

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")
    pointer_path = cache_root / "current-pointer.json"
    pointer = _load_json(pointer_path)
    pointer["release_id"] = "sha256:" + "f" * 64
    _write_json(pointer_path, pointer)
    (cache_root / "current-pointer-audit.jsonl").unlink()

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheCurrentState(cache_root).active_deployment_id(expected_environment="dev")
    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED"

    plan = DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev")
    assert plan.status == "repairable"
    assert "DPONE_CURRENT_RELEASE_ID_MISMATCH" in {issue.code for issue in plan.issues}

    DeploymentCacheRecoveryApplier(cache_root).apply(
        environment="dev",
        deployment_id=fixture.deployment_id,
        confirm_repair=True,
        promoted_by="ci://recovery",
        expected_current_deployment_id=fixture.deployment_id,
        allowed_promoters=("ci://recovery",),
    )

    assert _load_json(pointer_path)["release_id"] == fixture.release_id
    assert DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev").status == "ok"


def test_current_pointer_requires_authorization_fields_for_all_consumers(tmp_path: Path) -> None:
    from dpone.readiness.airflow_verified_current_deployment import load_verified_airflow_deployment_context
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_current_state import DeploymentCacheCurrentState
    from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryPlanner

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")
    pointer_path = cache_root / "current-pointer.json"
    pointer = _load_json(pointer_path)
    pointer.pop("promoted_by")
    pointer.pop("promoted_at")
    _write_json(pointer_path, pointer)
    audit_path = cache_root / "current-pointer-audit.jsonl"
    event = json.loads(audit_path.read_text(encoding="utf-8").splitlines()[-1])
    event.pop("promoted_by")
    event.pop("promoted_at")
    audit_path.write_text(json.dumps(event) + "\n", encoding="utf-8")

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheCurrentState(cache_root).active_deployment_id(expected_environment="dev")
    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED"
    plan = DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev")
    assert plan.status == "repairable"
    assert "DPONE_CURRENT_POINTER_INVALID" in {issue.code for issue in plan.issues}
    assert load_verified_airflow_deployment_context(cache_root, expected_environment="dev") is None


def test_audit_only_recovery_revalidates_candidate_inside_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_recovery import (
        DeploymentCacheRecoveryApplier,
        DeploymentCacheRecoveryApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")
    audit_path = cache_root / "current-pointer-audit.jsonl"
    audit_path.unlink()
    original = DeploymentCacheMaterializer.repair_audit

    def corrupt_before_repair(self: DeploymentCacheMaterializer, **kwargs: Any):
        artifact = fixture.artifacts["dag_specs"]
        artifact.chmod(0o600)
        artifact.write_bytes(b"x" * artifact.stat().st_size)
        return original(self, **kwargs)

    monkeypatch.setattr(DeploymentCacheMaterializer, "repair_audit", corrupt_before_repair)

    with pytest.raises(DeploymentCacheRecoveryApplyError) as exc:
        DeploymentCacheRecoveryApplier(cache_root).apply(
            environment="dev",
            deployment_id=fixture.deployment_id,
            confirm_repair=True,
            promoted_by="ci://recovery",
            expected_current_deployment_id=fixture.deployment_id,
        )

    assert exc.value.code == "DPONE_CACHE_CHECKSUM_MISMATCH"
    assert not audit_path.exists()


def test_concurrent_audit_recovery_has_one_transaction_winner(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_recovery import (
        DeploymentCacheRecoveryApplier,
        DeploymentCacheRecoveryApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")
    (cache_root / "current-pointer-audit.jsonl").unlink()
    barrier = Barrier(2)

    def recover() -> tuple[str, str]:
        barrier.wait()
        try:
            report = DeploymentCacheRecoveryApplier(cache_root).apply(
                environment="dev",
                deployment_id=fixture.deployment_id,
                confirm_repair=True,
                promoted_by="ci://recovery",
                expected_current_deployment_id=fixture.deployment_id,
            )
        except DeploymentCacheRecoveryApplyError as exc:
            return "error", exc.code
        return "success", report.recovered_deployment_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: recover(), range(2)))

    assert [kind for kind, _ in results].count("success") == 1
    assert ("error", "DPONE_DEPLOYMENT_CACHE_RECOVERY_NOT_REQUIRED") in results


def test_retention_waits_for_promotion_and_never_deletes_new_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    initial = _write_fixture(cache_root, deployment_id="sha256:" + "a" * 64, label="initial")
    candidate = _write_fixture(cache_root, deployment_id="sha256:" + "b" * 64, label="candidate")
    DeploymentCacheMaterializer(cache_root).promote(initial.deployment, environment="dev")
    entered_commit = Event()
    allow_commit = Event()
    retention_started = Event()
    original = DeploymentCacheMaterializer._commit_promotion

    def blocking_commit(self: DeploymentCacheMaterializer, **kwargs: Any):
        entered_commit.set()
        assert allow_commit.wait(timeout=5)
        return original(self, **kwargs)

    def apply_retention():
        retention_started.set()
        return build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",)).apply(
            environment="dev", confirm_delete=True, promoted_by="ci://retention"
        )

    monkeypatch.setattr(DeploymentCacheMaterializer, "_commit_promotion", blocking_commit)
    with ThreadPoolExecutor(max_workers=2) as executor:
        promotion = executor.submit(
            DeploymentCacheMaterializer(cache_root).promote,
            candidate.deployment,
            environment="dev",
            expected_current_deployment_id=initial.deployment_id,
        )
        assert entered_commit.wait(timeout=5)
        retention = executor.submit(apply_retention)
        assert retention_started.wait(timeout=5)
        assert not retention.done()
        allow_commit.set()
        promoted = promotion.result(timeout=5)
        retained = retention.result(timeout=5)

    assert promoted.deployment_id == candidate.deployment_id
    assert candidate.deployment.exists()
    assert retained.current_deployment_id == candidate.deployment_id
    assert candidate.deployment_id in retained.skipped_deployment_ids


def test_recovery_plan_blocks_unreadable_active_projection(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryPlanner

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")
    active_deployment = cache_root / "current" / "deployment.json"
    active_deployment.chmod(0o600)
    active_deployment.write_bytes(b"\xff\xfe")

    plan = DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev").to_dict()

    assert plan["status"] == "repairable"
    assert plan["current_path_deployment_id"] == fixture.deployment_id
    assert [item["deployment_id"] for item in plan["repair_candidates"]] == [fixture.deployment_id]
    assert "DPONE_DEPLOYMENT_INVALID" in {issue["code"] for issue in plan["issues"]}


def test_recovery_uses_physical_current_id_when_active_artifact_is_corrupt(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryApplier, DeploymentCacheRecoveryPlanner

    cache_root = tmp_path / ".dpone-cache"
    current = _write_fixture(cache_root, label="current")
    replacement = _write_fixture(cache_root, label="replacement")
    DeploymentCacheMaterializer(cache_root).promote(current.deployment, environment="dev")
    current.artifacts["dag_specs"].chmod(0o600)
    current.artifacts["dag_specs"].write_text("corrupt", encoding="utf-8")

    plan = DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev")

    assert plan.current_path_deployment_id == current.deployment_id
    assert plan.preferred_repair_deployment_id == replacement.deployment_id
    report = DeploymentCacheRecoveryApplier(cache_root).apply(
        environment="dev",
        deployment_id=replacement.deployment_id,
        confirm_repair=True,
        promoted_by="ci://recovery",
        expected_current_deployment_id=current.deployment_id,
        allowed_promoters=("ci://recovery",),
    )
    assert report.recovered_deployment_id == replacement.deployment_id


def test_recovery_rejects_stale_guard_when_corrupt_current_target_changes(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_recovery import (
        DeploymentCacheRecoveryApplier,
        DeploymentCacheRecoveryApplyError,
        DeploymentCacheRecoveryPlanner,
    )

    cache_root = tmp_path / ".dpone-cache"
    first = _write_fixture(cache_root, label="first")
    second = _write_fixture(cache_root, label="second")
    replacement = _write_fixture(cache_root, label="replacement")
    materializer = DeploymentCacheMaterializer(cache_root)
    materializer.promote(first.deployment, environment="dev")
    first_pointer = (cache_root / "current-pointer.json").read_bytes()
    materializer.promote(
        second.deployment,
        environment="dev",
        expected_current_deployment_id=first.deployment_id,
    )
    first_activation = cache_root / "activations" / "dev" / first.deployment_id.replace(":", "-")
    second_activation = cache_root / "activations" / "dev" / second.deployment_id.replace(":", "-")
    (cache_root / "current").unlink()
    (cache_root / "current").symlink_to(first_activation.relative_to(cache_root), target_is_directory=True)
    (cache_root / "current-pointer.json").write_bytes(first_pointer)
    for activation in (first_activation, second_activation):
        deployment_json = activation / "deployment.json"
        deployment_json.chmod(0o600)
        deployment_json.write_bytes(b"not-json")

    plan = DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev")

    assert plan.current_path_deployment_id == first.deployment_id
    (cache_root / "current").unlink()
    (cache_root / "current").symlink_to(second_activation.relative_to(cache_root), target_is_directory=True)
    with pytest.raises(DeploymentCacheRecoveryApplyError) as exc:
        DeploymentCacheRecoveryApplier(cache_root).apply(
            environment="dev",
            deployment_id=replacement.deployment_id,
            confirm_repair=True,
            promoted_by="ci://recovery",
            expected_current_deployment_id=plan.current_path_deployment_id,
            allowed_promoters=("ci://recovery",),
        )

    assert exc.value.code == "DPONE_CURRENT_POINTER_CAS_MISMATCH"
    assert (cache_root / "current").resolve() == second_activation.resolve()


def test_recovery_blocks_current_path_without_canonical_cas_identity(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_recovery import (
        DeploymentCacheRecoveryApplier,
        DeploymentCacheRecoveryApplyError,
        DeploymentCacheRecoveryPlanner,
    )

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")
    current = cache_root / "current"
    canonical_target = current.resolve()
    current.unlink()
    current.symlink_to(canonical_target, target_is_directory=True)

    plan = DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev")

    assert plan.status == "blocked"
    assert plan.current_path_deployment_id is None
    assert "DPONE_CURRENT_PATH_ID_UNAVAILABLE" in {issue.code for issue in plan.issues}
    with pytest.raises(DeploymentCacheRecoveryApplyError) as exc:
        DeploymentCacheRecoveryApplier(cache_root).apply(
            environment="dev",
            deployment_id=fixture.deployment_id,
            confirm_repair=True,
            promoted_by="ci://recovery",
            expected_current_deployment_id=None,
            allowed_promoters=("ci://recovery",),
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_RECOVERY_BLOCKED"
    assert current.is_symlink()
    assert Path(os.readlink(current)).is_absolute()


def test_current_state_rejects_absolute_symlink_even_when_target_is_inside_cache(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_current_state import DeploymentCacheCurrentState

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")
    (cache_root / "current").unlink()
    (cache_root / "current").symlink_to(fixture.deployment.absolute(), target_is_directory=True)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheCurrentState(cache_root).active_deployment_id(expected_environment="dev")

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED"


@pytest.mark.parametrize("unsafe_form", ["dot", "parent", "duplicate_separator"])
def test_current_state_rejects_noncanonical_relative_symlink_targets(
    tmp_path: Path,
    unsafe_form: str,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_current_state import DeploymentCacheCurrentState

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")
    current = cache_root / "current"
    original_target = os.readlink(current)
    if unsafe_form == "dot":
        unsafe_target = f"./{original_target}"
    elif unsafe_form == "parent":
        layout, environment, digest = original_target.split("/")
        unsafe_target = f"{layout}/{environment}/../{environment}/{digest}"
    else:
        unsafe_target = original_target.replace("/", "//", 1)
    current.unlink()
    os.symlink(unsafe_target, current)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheCurrentState(cache_root).active_deployment_id(expected_environment="dev")

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED"


def test_current_state_rejects_symlinked_intermediate_cache_component(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_current_state import DeploymentCacheCurrentState

    cache_root = tmp_path / ".dpone-cache"
    fixture = _write_fixture(cache_root)
    DeploymentCacheMaterializer(cache_root).promote(fixture.deployment, environment="dev")
    current = cache_root / "current"
    original_target = Path(os.readlink(current))
    alias = cache_root / "activation-alias"
    alias.symlink_to("activations", target_is_directory=True)
    current.unlink()
    current.symlink_to(Path(alias.name, *original_target.parts[1:]), target_is_directory=True)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheCurrentState(cache_root).active_deployment_id(expected_environment="dev")

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED"


def test_symlinked_active_deployment_control_blocks_safe_sample_and_retention(tmp_path: Path) -> None:
    from dpone.readiness.airflow_verified_current_deployment import load_verified_airflow_deployment_context
    from dpone.runtime.deployment_cache import (
        DeploymentCacheMaterializer,
        DeploymentCacheRetentionApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    current_fixture = _write_fixture(cache_root, label="current")
    stale_fixture = _write_fixture(cache_root, label="stale")
    DeploymentCacheMaterializer(cache_root).promote(current_fixture.deployment, environment="dev")
    activation = (cache_root / "current").resolve()
    deployment_json = activation / "deployment.json"
    original = deployment_json.read_bytes()
    activation.chmod(0o755)
    deployment_json.unlink()
    alternate = activation / "deployment.real.json"
    alternate.write_bytes(original)
    deployment_json.symlink_to(alternate.name)

    assert load_verified_airflow_deployment_context(cache_root, expected_environment="dev") is None
    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",)).apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED"
    assert stale_fixture.deployment.exists()


def _assert_no_promotion_side_effects(cache_root: Path) -> None:
    assert not (cache_root / "current").exists()
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current-pointer-audit.jsonl").exists()

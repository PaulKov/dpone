from __future__ import annotations

import hashlib
import json
import os
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import dpone_airflow_pack.cache_status as cache_status_module
import dpone_airflow_pack.pack_tasks as pack_tasks_module
import dpone_airflow_pack.pack_wiring as pack_wiring_module
import pytest
from dpone_airflow_pack import load_dpone_airflow_pack_with_provenance
from dpone_airflow_pack.cache_operational_status import read_cache_operational_status
from dpone_airflow_pack.cache_status import read_airflow_pack_cache_status
from dpone_airflow_pack.cli_cache_status import main as cache_status_main
from dpone_airflow_pack.cli_sync import main as sync_main
from dpone_airflow_pack.pack_identity import (
    PACK_IDENTITY_SCHEMA,
    compute_pack_fingerprint,
)
from dpone_airflow_pack.runtime_adapter import DponeAirflowContractError
from dpone_airflow_pack.workload_catalog import (
    load_gitops_workload_groups,
    workload_ids_from_gitops_domain,
)


def _pack(workload_id: str) -> dict[str, object]:
    payload: dict[str, object] = {
        "kind": "gitops.airflow_pack",
        "schema_version": "3",
        "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
        "workload": {"workload_id": workload_id},
        "kpo_kwargs": {"task_id": f"{workload_id}__dpone_runtime"},
    }
    payload["pack_fingerprint"] = compute_pack_fingerprint(payload)
    return payload


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _activation_dir(cache: Path, deployment_id: str) -> Path:
    _initialize_cache_lock(cache)
    return cache / "activations" / "dev" / ("sha256-" + deployment_id.removeprefix("sha256:"))


def _initialize_cache_lock(cache: Path) -> None:
    cache.mkdir(parents=True, exist_ok=True)
    (cache / ".promotion.lock").touch(mode=0o644, exist_ok=True)


def _write_current_pointer(
    cache: Path,
    *,
    deployment_id: str,
    activation_id: str,
    release_id: str = "sha256:" + "a" * 64,
) -> None:
    (cache / "current-pointer.json").write_text(
        json.dumps(
            {
                "schema": "dpone.current-pointer.v1",
                "environment": "dev",
                "deployment_id": deployment_id,
                "release_id": release_id,
                "activation_id": activation_id,
                "promoted_by": "dpone-ci",
                "promoted_at": "2026-08-01T00:00:00+00:00",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _write_cached_pack(cache_dir: Path, *, workload_id: str = "orders", generation: str = "abc123") -> Path:
    _initialize_cache_lock(cache_dir)
    pack_path = cache_dir / "generations" / generation / "airflow" / workload_id / "airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text(json.dumps(_pack(workload_id), sort_keys=True), encoding="utf-8")
    index_path = cache_dir / "generations" / generation / "pack-index.json"
    index_path.write_text(
        json.dumps(
            {
                "kind": "dpone.airflow_pack_index",
                "schema_version": "1",
                "git_sha": generation,
                "packs": {
                    workload_id: {
                        "path": f"airflow/{workload_id}/airflow-pack.json",
                        "sha256": _sha256(pack_path),
                        "bytes": pack_path.stat().st_size,
                    }
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (cache_dir / "current").write_text(generation, encoding="utf-8")
    status_dir = cache_dir / "status"
    status_dir.mkdir(parents=True)
    (status_dir / "last-sync-status.json").write_text(
        json.dumps({"status": "success", "current_generation": generation}),
        encoding="utf-8",
    )
    return pack_path


def test_read_cache_status_reads_exact_deployment_index_layout(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    pack_path = cache / "releases" / "sha256-release" / "airflow" / "orders" / "airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text(json.dumps(_pack("orders"), sort_keys=True), encoding="utf-8")
    release_id = "sha256:" + "a" * 64
    deployment_id = "sha256:" + "b" * 64
    activation = _activation_dir(cache, deployment_id)
    activation.mkdir(parents=True)
    activation_id = "123e4567-e89b-42d3-a456-426614174001"
    expected = "sha256:" + _sha256(pack_path)
    (activation / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": release_id,
                "deployment_id": deployment_id,
                "dag_specs": [],
                "workload_packs": [
                    {
                        "id": "orders",
                        "artifact_ref": "cache://releases/sha256-release/airflow/orders/airflow-pack.json",
                        "sha256": expected,
                    }
                ],
                "runtime_artifact_delivery": {"mode": "shared_pvc"},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (cache / "current").symlink_to(activation.relative_to(cache), target_is_directory=True)
    _write_current_pointer(
        cache,
        deployment_id=deployment_id,
        activation_id=activation_id,
    )

    status = read_airflow_pack_cache_status(cache, workload_ids=("orders",))

    assert status["status"] == "success"
    assert status["layout"] == "exact_deployment_index"
    assert status["release_id"] == release_id
    assert status["deployment_id"] == deployment_id
    assert status["activation_id"] == activation_id
    assert "airflow_pack_json_missing" not in {item["code"] for item in status["blockers"]}
    assert status["workloads"]["orders"]["exists"] is True
    assert status["workloads"]["orders"]["sha256"] == expected


def test_read_cache_status_does_not_false_missing_when_exact_index_present(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    activation = _activation_dir(cache, "sha256:" + "b" * 64)
    activation.mkdir(parents=True)
    (activation / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": "sha256:" + "b" * 64,
                "activation_id": "4f60628e-ef48-48b0-84c3-a9e27a82a7f2",
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "shared_pvc"},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (cache / "current").symlink_to(activation.relative_to(cache), target_is_directory=True)

    status = read_airflow_pack_cache_status(cache)

    assert status["layout"] == "exact_deployment_index"
    assert status["status"] == "success"
    assert status["activation_id"] is None
    assert not any(item["code"] == "airflow_pack_json_missing" for item in status["blockers"])


def test_read_cache_status_blocks_v2_without_current_pointer_activation(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    activation = _activation_dir(cache, deployment_id)
    activation.mkdir(parents=True)
    (activation / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v2",
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": deployment_id,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "init_fetch"},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (cache / "current").symlink_to(activation.relative_to(cache), target_is_directory=True)

    status = read_airflow_pack_cache_status(cache)

    assert status["status"] == "blocked"
    assert {item["code"] for item in status["blockers"]} == {
        "airflow_pack_activation_id_missing",
        "airflow_pack_reconcile_status_missing",
    }


def test_read_cache_status_blocks_invalid_exact_activation_id(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    activation = _activation_dir(cache, deployment_id)
    activation.mkdir(parents=True)
    (activation / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v2",
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": deployment_id,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "init_fetch"},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (cache / "current").symlink_to(activation.relative_to(cache), target_is_directory=True)
    _write_current_pointer(
        cache,
        deployment_id=deployment_id,
        activation_id="not-a-uuid",
    )

    status = read_airflow_pack_cache_status(cache)

    assert status["status"] == "blocked"
    assert {item["code"] for item in status["blockers"]} == {"airflow_pack_current_pointer_invalid"}


def test_missing_cache_status_does_not_read_after_frozen_absence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import cache_status

    cache_root = tmp_path / "not-created"

    def fail_unleased_read(*args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        raise AssertionError("an absent cache snapshot must not re-read mutable paths")

    monkeypatch.setattr(cache_status, "_read_airflow_pack_cache_status_unleased", fail_unleased_read)

    status = read_airflow_pack_cache_status(cache_root, workload_ids=("orders",))

    assert status["status"] == "blocked"
    assert status["current_generation"] is None
    assert status["workloads"]["orders"]["exists"] is False
    assert status["blockers"][0]["code"] == "airflow_pack_cache_missing"


@pytest.mark.parametrize("missing", ["release_id", "promoted_by", "promoted_at"])
def test_read_cache_status_blocks_incomplete_current_pointer(
    tmp_path: Path,
    missing: str,
) -> None:
    cache = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    activation = _activation_dir(cache, deployment_id)
    activation.mkdir(parents=True)
    (activation / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v2",
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": deployment_id,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "init_fetch"},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (cache / "current").symlink_to(activation.relative_to(cache), target_is_directory=True)
    _write_current_pointer(
        cache,
        deployment_id=deployment_id,
        activation_id="123e4567-e89b-42d3-a456-426614174001",
    )
    pointer_path = cache / "current-pointer.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer.pop(missing)
    pointer_path.write_text(json.dumps(pointer, sort_keys=True), encoding="utf-8")

    status = read_airflow_pack_cache_status(cache)

    assert status["status"] == "blocked"
    assert {item["code"] for item in status["blockers"]} == {"airflow_pack_current_pointer_invalid"}


def test_read_cache_status_blocks_current_pointer_release_mismatch(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    activation = _activation_dir(cache, deployment_id)
    activation.mkdir(parents=True)
    (activation / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v2",
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": deployment_id,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "init_fetch"},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (cache / "current").symlink_to(activation.relative_to(cache), target_is_directory=True)
    _write_current_pointer(
        cache,
        deployment_id=deployment_id,
        activation_id="123e4567-e89b-42d3-a456-426614174001",
        release_id="sha256:" + "c" * 64,
    )

    status = read_airflow_pack_cache_status(cache)

    assert status["status"] == "blocked"
    assert {item["code"] for item in status["blockers"]} == {
        "airflow_pack_current_pointer_mismatch",
        "airflow_pack_reconcile_status_missing",
    }


def test_read_cache_status_blocks_when_v2_has_no_reconcile_evidence(tmp_path: Path) -> None:
    """Strict v2 bytes remain inspectable, but convergence is fail-closed."""

    cache = tmp_path / ".dpone-cache"
    pack_path = cache / "releases" / "sha256-release" / "airflow" / "orders" / "airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text(json.dumps(_pack("orders"), sort_keys=True), encoding="utf-8")
    release_id = "sha256:" + "a" * 64
    deployment_id = "sha256:" + "b" * 64
    activation = _activation_dir(cache, deployment_id)
    activation.mkdir(parents=True)
    activation_id = "123e4567-e89b-42d3-a456-426614174001"
    expected = "sha256:" + _sha256(pack_path)
    (activation / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v2",
                "release_id": release_id,
                "deployment_id": deployment_id,
                "dag_specs": [],
                "workload_packs": [
                    {
                        "id": "orders",
                        "artifact_ref": "cache://releases/sha256-release/airflow/orders/airflow-pack.json",
                        "sha256": expected,
                    }
                ],
                "runtime_artifact_delivery": {"mode": "init_fetch"},
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (cache / "current").symlink_to(activation.relative_to(cache), target_is_directory=True)
    _write_current_pointer(
        cache,
        deployment_id=deployment_id,
        activation_id=activation_id,
    )

    status = read_airflow_pack_cache_status(cache, workload_ids=("orders",))

    assert status["status"] == "blocked"
    assert status["layout"] == "exact_deployment_index"
    assert status["release_id"] == release_id
    assert status["deployment_id"] == deployment_id
    assert status["activation_id"] == activation_id
    assert not any(item["code"] == "airflow_pack_index_schema_mismatch" for item in status["blockers"])
    assert {item["code"] for item in status["blockers"]} == {"airflow_pack_reconcile_status_missing"}
    assert status["warnings"] == []
    assert status["workloads"]["orders"]["exists"] is True
    assert status["workloads"]["orders"]["sha256"] == expected


def test_read_cache_status_blocks_unknown_exact_index_schema(tmp_path: Path) -> None:
    cache = tmp_path / ".dpone-cache"
    activation = _activation_dir(cache, "sha256:" + "b" * 64)
    activation.mkdir(parents=True)
    (activation / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v99",
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": "sha256:" + "b" * 64,
                "dag_specs": [],
                "workload_packs": [],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (cache / "current").symlink_to(activation.relative_to(cache), target_is_directory=True)

    status = read_airflow_pack_cache_status(cache)

    assert status["status"] == "blocked"
    assert status["release_id"] is None
    assert any(item["code"] == "airflow_pack_index_schema_mismatch" for item in status["blockers"])


def test_read_cache_status_reports_current_generation_and_pack_hash(tmp_path: Path) -> None:
    pack_path = _write_cached_pack(tmp_path)

    status = read_airflow_pack_cache_status(tmp_path, workload_ids=("orders",))

    assert status["status"] == "success"
    assert status["current_generation"] == "abc123"
    assert status["workloads"]["orders"]["exists"] is True
    assert status["workloads"]["orders"]["sha256"] == _sha256(pack_path)
    assert status["last_sync_status"]["status"] == "success"


def test_cached_pack_loader_returns_provenance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pack_path = _write_cached_pack(tmp_path)
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(tmp_path))

    pack, provenance = load_dpone_airflow_pack_with_provenance("cached://orders")

    assert pack["workload"]["workload_id"] == "orders"
    assert provenance["source"] == "cached"
    assert provenance["generation"] == "abc123"
    assert provenance["pack_sha256"] == _sha256(pack_path)
    assert provenance["verified_pack_fingerprint"] == pack["pack_fingerprint"]
    assert provenance["cache_status"]["status"] == "success"


def test_cached_pack_loader_holds_lease_through_pack_checksum(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fcntl = pytest.importorskip("fcntl")
    import dpone_airflow_pack.pack_provenance as provenance_module

    _write_cached_pack(tmp_path)
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(tmp_path))
    original = provenance_module._load_pack_bytes

    def assert_lease(*args: object, **kwargs: object):
        descriptor = os.open(tmp_path / ".promotion.lock", os.O_RDWR)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)
        return original(*args, **kwargs)

    monkeypatch.setattr(provenance_module, "_load_pack_bytes", assert_lease)

    pack, provenance = load_dpone_airflow_pack_with_provenance("cached://orders")

    assert pack["workload"]["workload_id"] == "orders"
    assert provenance["cache_status"]["status"] == "success"


def test_cached_pack_loader_accepts_workloads_prefixed_ref(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Compact dag-specs emit cached://workloads/<id>; provenance must resolve the id."""

    pack_path = _write_cached_pack(tmp_path)
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(tmp_path))

    pack, provenance = load_dpone_airflow_pack_with_provenance("cached://workloads/orders")

    assert pack["workload"]["workload_id"] == "orders"
    assert provenance["workload_id"] == "orders"
    assert provenance["path"] == str(pack_path)


def test_cached_pack_loader_blocks_hash_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pack_path = _write_cached_pack(tmp_path)
    pack_path.write_text(json.dumps(_pack("orders") | {"extra": "drift"}), encoding="utf-8")
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(tmp_path))

    with pytest.raises(DponeAirflowContractError) as exc_info:
        load_dpone_airflow_pack_with_provenance("cached://orders")

    assert exc_info.value.blockers[0]["code"] == "airflow_pack_hash_mismatch"


def test_local_pack_loader_checks_expected_hash_on_the_loaded_bytes(tmp_path: Path) -> None:
    pack_path = tmp_path / "airflow-pack.json"
    pack_path.write_text(json.dumps(_pack("orders"), sort_keys=True), encoding="utf-8")

    with pytest.raises(DponeAirflowContractError) as exc_info:
        load_dpone_airflow_pack_with_provenance(
            pack_path,
            expected_sha256="sha256:" + ("0" * 64),
        )

    assert exc_info.value.blockers[0]["code"] == "airflow_pack_hash_mismatch"
    assert exc_info.value.blockers[0]["path"] == str(pack_path)


def test_local_pack_loader_rejects_stale_whole_pack_identity(tmp_path: Path) -> None:
    payload = _pack("orders")
    payload["steps"] = [{"name": "mutated-after-signing"}]
    pack_path = tmp_path / "airflow-pack.json"
    pack_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")

    with pytest.raises(DponeAirflowContractError) as exc_info:
        load_dpone_airflow_pack_with_provenance(pack_path)

    assert exc_info.value.blockers[0]["code"] == "airflow_pack_identity_invalid"


def test_local_pack_loader_rejects_oversized_pack_before_json_parse(tmp_path: Path) -> None:
    pack_path = tmp_path / "airflow-pack.json"
    pack_path.write_text(json.dumps(_pack("orders"), sort_keys=True), encoding="utf-8")

    with pytest.raises(DponeAirflowContractError) as exc_info:
        load_dpone_airflow_pack_with_provenance(pack_path, max_bytes=8)

    assert exc_info.value.blockers[0]["code"] == "airflow_pack_too_large"


def test_local_pack_loader_rejects_cache_parent_swapped_to_symlink(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    packs = cache / "releases" / "sha256-release" / "packs"
    packs.mkdir(parents=True)
    pack_path = packs / "airflow-pack.json"
    pack_path.write_text(json.dumps(_pack("orders"), sort_keys=True), encoding="utf-8")
    expected_sha256 = "sha256:" + _sha256(pack_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / pack_path.name).write_bytes(pack_path.read_bytes())
    releases = cache / "releases"
    releases.rename(cache / "original-releases")
    releases.symlink_to(outside, target_is_directory=True)

    with pytest.raises(DponeAirflowContractError) as exc_info:
        load_dpone_airflow_pack_with_provenance(
            pack_path,
            expected_sha256=expected_sha256,
            confined_root=cache,
        )

    assert exc_info.value.blockers[0]["code"] == "DPONE_CACHE_ARTIFACT_REF_UNSAFE"


def test_pack_wiring_consumes_one_verified_pack_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pack_path = tmp_path / "airflow-pack.json"
    pack_path.write_text(json.dumps(_pack("orders"), sort_keys=True), encoding="utf-8")
    expected_sha256 = "sha256:" + _sha256(pack_path)
    original_loader = pack_wiring_module.load_dpone_airflow_pack_with_provenance
    loaded_paths: list[str] = []

    def recording_loader(
        pack_ref: str | Path,
        **kwargs: object,
    ) -> tuple[dict[str, object], dict[str, object]]:
        loaded_paths.append(str(pack_ref))
        return original_loader(pack_ref, **kwargs)

    runtime = SimpleNamespace(kwargs={})
    monkeypatch.setattr(
        pack_tasks_module,
        "_build_tasks_from_loaded_pack",
        lambda *args, **kwargs: {"dpone_runtime": runtime},
    )
    monkeypatch.setattr(
        pack_wiring_module,
        "load_dpone_airflow_pack_with_provenance",
        recording_loader,
    )
    monkeypatch.setattr(
        pack_tasks_module,
        "load_dpone_airflow_pack_with_provenance",
        recording_loader,
    )

    pack_wiring_module.wire_pack_workload(
        "orders",
        dag=SimpleNamespace(),
        repo_root=tmp_path,
        pack_ref=pack_path,
        expected_sha256=expected_sha256,
    )

    assert loaded_paths == [str(pack_path)]


def test_pack_task_builder_consumes_one_confined_verified_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root = tmp_path / "cache"
    pack_path = cache_root / "releases" / "sha256-release" / "packs" / "airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text(json.dumps(_pack("orders"), sort_keys=True), encoding="utf-8")
    expected_sha256 = "sha256:" + _sha256(pack_path)
    loaded_pack = _pack("orders")
    provenance = {"source": "local_path", "pack_sha256": expected_sha256.removeprefix("sha256:")}
    load_calls: list[tuple[Path, Path | None]] = []
    consumed: list[dict[str, object]] = []

    def load_once(
        pack_ref: str | Path,
        **kwargs: object,
    ) -> tuple[dict[str, object], dict[str, object]]:
        load_calls.append((Path(pack_ref), kwargs.get("confined_root")))  # type: ignore[arg-type]
        return loaded_pack, provenance

    def build_loaded(
        pack: dict[str, object],
        **kwargs: object,
    ) -> dict[str, object]:
        del kwargs
        consumed.append(pack)
        return {"dpone_runtime": object()}

    monkeypatch.setattr(pack_tasks_module, "load_dpone_airflow_pack_with_provenance", load_once)
    monkeypatch.setattr(pack_tasks_module, "_build_dpone_gitops_task_group_from_loaded_pack", build_loaded)

    pack_tasks_module.build_dpone_gitops_task_group_from_pack(
        pack_path,
        expected_sha256=expected_sha256,
        confined_root=cache_root,
    )

    assert load_calls == [(pack_path, cache_root)]
    assert consumed == [loaded_pack]
    assert consumed[0] is loaded_pack


def test_cached_pack_loader_rejects_oversized_pack_before_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pack_path = _write_cached_pack(tmp_path)
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(tmp_path))
    hashed_paths: list[Path] = []
    original = cache_status_module._sha256_file

    def recording_hash(path: Path, *, max_bytes: int | None = None) -> str:
        hashed_paths.append(path)
        return original(path, max_bytes=max_bytes)

    monkeypatch.setattr(cache_status_module, "_sha256_file", recording_hash)

    with pytest.raises(DponeAirflowContractError) as exc_info:
        load_dpone_airflow_pack_with_provenance("cached://orders", max_bytes=8)

    assert exc_info.value.blockers[0]["code"] == "airflow_pack_too_large"
    assert pack_path not in hashed_paths


def test_cached_pack_loader_rejects_oversized_index_before_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_cached_pack(tmp_path)
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(tmp_path))
    index_path = tmp_path / "generations" / "abc123" / "pack-index.json"
    hashed_paths: list[Path] = []
    original = cache_status_module._sha256_file

    def recording_hash(path: Path, *, max_bytes: int | None = None) -> str:
        hashed_paths.append(path)
        return original(path, max_bytes=max_bytes)

    monkeypatch.setattr(cache_status_module, "_sha256_file", recording_hash)

    with pytest.raises(DponeAirflowContractError) as exc_info:
        load_dpone_airflow_pack_with_provenance("cached://orders", max_index_bytes=8)

    assert exc_info.value.blockers[0]["code"] == "airflow_pack_index_too_large"
    assert index_path not in hashed_paths


def test_cache_status_cli_outputs_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _write_cached_pack(tmp_path)

    exit_code = cache_status_main(["--cache-dir", str(tmp_path), "--workload-id", "orders", "--json"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["current_generation"] == "abc123"
    assert payload["workloads"]["orders"]["exists"] is True


def test_sync_cli_downloads_latest_index_and_packs_from_file_store(tmp_path: Path) -> None:
    remote = tmp_path / "remote"
    pack_path = remote / "abc123" / "airflow" / "orders" / "airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text(json.dumps(_pack("orders"), sort_keys=True), encoding="utf-8")
    latest = remote / "latest" / "pack-index.json"
    latest.parent.mkdir(parents=True)
    latest.write_text(
        json.dumps(
            {
                "kind": "dpone.airflow_pack_index",
                "schema_version": "1",
                "git_sha": "abc123",
                "packs": {
                    "orders": {
                        "path": "airflow/orders/airflow-pack.json",
                        "sha256": _sha256(pack_path),
                    }
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    cache_dir = tmp_path / "cache"

    exit_code = sync_main(
        [
            "--once",
            "--index-uri",
            str(latest),
            "--cache-dir",
            str(cache_dir),
            "--keep-generations",
            "2",
        ]
    )

    assert exit_code == 0
    pack, provenance = load_dpone_airflow_pack_with_provenance("cached://orders", cache_dir=cache_dir)
    assert pack["workload"]["workload_id"] == "orders"
    assert provenance["source"] == "cached"
    status = json.loads((cache_dir / "status" / "last-sync-status.json").read_text())
    assert status["status"] == "success"
    assert status["component"] == "unspecified"
    assert status["last_success_at"] == status["finished_at"]


def test_sync_cli_publishes_airflow_variable_status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    remote = tmp_path / "remote"
    pack_path = remote / "abc123" / "airflow" / "orders" / "airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text(json.dumps(_pack("orders"), sort_keys=True), encoding="utf-8")
    latest = remote / "latest" / "pack-index.json"
    latest.parent.mkdir(parents=True)
    latest.write_text(
        json.dumps(
            {
                "git_sha": "abc123",
                "packs": {
                    "orders": {
                        "path": "airflow/orders/airflow-pack.json",
                        "sha256": _sha256(pack_path),
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    published: dict[str, str] = {}

    class FakeVariable:
        @staticmethod
        def set(key: str, value: str) -> None:
            published[key] = value

    variable_module = types.ModuleType("airflow.models.variable")
    variable_module.Variable = FakeVariable
    sdk_module = types.ModuleType("airflow.sdk")
    sdk_module.Variable = FakeVariable
    monkeypatch.setitem(sys.modules, "airflow.models.variable", variable_module)
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk_module)

    exit_code = sync_main(
        [
            "--once",
            "--index-uri",
            str(latest),
            "--cache-dir",
            str(tmp_path / "cache"),
            "--airflow-variable-key",
            "dpone_airflow_pack_cache_status",
        ]
    )

    assert exit_code == 0
    payload = json.loads(published["dpone_airflow_pack_cache_status"])
    assert payload["current_generation"] == "abc123"
    assert payload["airflow_variable_published"] is True
    assert payload["airflow_variable_published_at"].endswith("+00:00")


def test_sync_cli_prefers_metadata_variable_over_task_sdk_variable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    remote = tmp_path / "remote"
    pack_path = remote / "abc123" / "airflow" / "orders" / "airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text(json.dumps(_pack("orders"), sort_keys=True), encoding="utf-8")
    latest = remote / "latest" / "pack-index.json"
    latest.parent.mkdir(parents=True)
    latest.write_text(
        json.dumps(
            {
                "git_sha": "abc123",
                "packs": {
                    "orders": {
                        "path": "airflow/orders/airflow-pack.json",
                        "sha256": _sha256(pack_path),
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    published: dict[str, str] = {}

    class TaskSdkVariable:
        @staticmethod
        def set(key: str, value: str) -> None:
            raise ImportError("cannot import name 'SUPERVISOR_COMMS'")

    class MetadataVariable:
        @staticmethod
        def set(key: str, value: str) -> None:
            published[key] = value

    airflow_module = types.ModuleType("airflow")
    sdk_module = types.ModuleType("airflow.sdk")
    sdk_module.Variable = TaskSdkVariable
    models_module = types.ModuleType("airflow.models")
    variable_module = types.ModuleType("airflow.models.variable")
    variable_module.Variable = MetadataVariable
    monkeypatch.setitem(sys.modules, "airflow", airflow_module)
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk_module)
    monkeypatch.setitem(sys.modules, "airflow.models", models_module)
    monkeypatch.setitem(sys.modules, "airflow.models.variable", variable_module)

    exit_code = sync_main(
        [
            "--once",
            "--index-uri",
            str(latest),
            "--cache-dir",
            str(tmp_path / "cache"),
            "--airflow-variable-key",
            "dpone_airflow_pack_cache_status",
        ]
    )

    assert exit_code == 0
    payload = json.loads(published["dpone_airflow_pack_cache_status"])
    assert payload["current_generation"] == "abc123"
    assert payload["airflow_variable_published"] is True


def test_sync_cli_prunes_old_generations_when_cache_exceeds_budget(tmp_path: Path) -> None:
    remote = tmp_path / "remote"
    pack_path = remote / "new" / "airflow" / "orders" / "airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text(json.dumps(_pack("orders"), sort_keys=True), encoding="utf-8")
    latest = remote / "latest" / "pack-index.json"
    latest.parent.mkdir(parents=True)
    latest.write_text(
        json.dumps(
            {
                "git_sha": "new",
                "packs": {"orders": {"path": "airflow/orders/airflow-pack.json", "sha256": _sha256(pack_path)}},
            }
        ),
        encoding="utf-8",
    )
    cache_dir = tmp_path / "cache"
    old_dir = cache_dir / "generations" / "old"
    old_dir.mkdir(parents=True)
    (old_dir / "payload.bin").write_bytes(b"x" * 4096)
    (cache_dir / "current").write_text("old", encoding="utf-8")

    exit_code = sync_main(
        [
            "--once",
            "--index-uri",
            str(latest),
            "--cache-dir",
            str(cache_dir),
            "--max-total-bytes",
            "1KiB",
            "--high-watermark-pct",
            "50",
            "--low-watermark-pct",
            "40",
            "--keep-generations",
            "10",
        ]
    )

    assert exit_code == 1
    assert old_dir.exists()
    assert not (cache_dir / "generations" / "new").exists()


def test_workload_catalog_resolves_group_order_and_optional_filter(tmp_path: Path) -> None:
    domain_dir = tmp_path / "dpone_workloads" / "gitops" / "domains"
    domain_dir.mkdir(parents=True)
    (domain_dir / "sales.yaml").write_text(
        """
domain: sales
workflow_groups:
  daily:
    workload_ids:
      - orders
      - sales
      - refunds
workloads:
  orders:
    manifest: orders.yaml
  sales:
    manifest: sales.yaml
  refunds:
    manifest: refunds.yaml
""",
        encoding="utf-8",
    )

    assert load_gitops_workload_groups(tmp_path, "sales") == {"daily": ("orders", "sales", "refunds")}
    assert workload_ids_from_gitops_domain(tmp_path, "sales", group="daily") == ("orders", "sales", "refunds")
    assert workload_ids_from_gitops_domain(
        tmp_path,
        "sales",
        group="daily",
        workload_ids=("sales",),
    ) == ("sales",)


def test_workload_catalog_blocks_unknown_group_references(tmp_path: Path) -> None:
    domain_dir = tmp_path / "dpone_workloads" / "gitops" / "domains"
    domain_dir.mkdir(parents=True)
    (domain_dir / "sales.yaml").write_text(
        """
domain: sales
workflow_groups:
  daily:
    workload_ids:
      - orders
      - missing
workloads:
  orders:
    manifest: orders.yaml
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="references unknown workload ids: missing"):
        workload_ids_from_gitops_domain(tmp_path, "sales", group="daily")


def test_cache_operational_status_projects_retention_markers_without_kubernetes_access(tmp_path: Path) -> None:
    status_root = tmp_path / "status"
    status_root.mkdir()
    marker = {
        "schema": "dpone.airflow-cache-status-publication-failure.v1",
        "status": "commit_unknown",
        "source": ".retention-apply.tmp",
        "target": "last-retention-apply.json",
        "expected_schema": "dpone.deployment-cache-retention-apply.v3",
        "attempted_at": "2026-08-03T12:00:00Z",
        "error_code": "DPONE_AIRFLOW_CACHE_STATUS_COMMIT_UNKNOWN",
    }
    (status_root / "last-retention-apply-publication-failure.json").write_text(
        json.dumps(marker),
        encoding="utf-8",
    )

    operational, warnings = read_cache_operational_status(tmp_path)

    assert operational["retention_apply_publication_failure"] == marker
    assert [warning["code"] for warning in warnings] == ["airflow_cache_status_publication_attention"]


def test_cache_operational_status_rejects_symlinked_marker(tmp_path: Path) -> None:
    status_root = tmp_path / "status"
    status_root.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    (status_root / "last-retention-plan.json").symlink_to(outside)

    operational, warnings = read_cache_operational_status(tmp_path)

    assert operational == {}
    assert warnings[0]["code"] == "airflow_cache_operational_status_invalid"

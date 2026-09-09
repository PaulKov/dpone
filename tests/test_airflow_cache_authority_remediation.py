from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path

import pytest
from dpone_airflow_pack import cache_sync as cache_sync_module
from dpone_airflow_pack.cache_activation_contract import cache_write_lease
from dpone_airflow_pack.cache_authority import generation_receipt_path
from dpone_airflow_pack.cache_generation_files import write_json_durable
from dpone_airflow_pack.cache_generation_store import (
    CURRENT_COMMIT_PATH,
    GENERATION_RECEIPT_NAME,
    PENDING_COMMIT_PATH,
    create_generation_stage,
    finalize_generation_stage,
    read_current_commit,
    recover_interrupted_commit,
)
from dpone_airflow_pack.cache_layout import LAYOUT_MARKER_NAME
from dpone_airflow_pack.cache_permissions import SHARED_CONTROL_MODE
from dpone_airflow_pack.cache_status import read_airflow_pack_cache_status
from dpone_airflow_pack.cache_sync import AirflowPackSyncOptions, sync_airflow_pack_cache
from dpone_airflow_pack.cache_sync_evidence import publish_sync_evidence
from dpone_airflow_pack.dag_spec_cache_paths import pinned_dag_spec_paths
from dpone_airflow_pack.pack_storage_consumer import STORAGE_MODE_ENV, DagSpecCacheMissingError


class _LocalReader:
    def read_bytes(self, uri: str, *, max_bytes: int | None = None) -> bytes:
        path = Path(uri.removeprefix("file://"))
        if max_bytes is None:
            return path.read_bytes()
        with path.open("rb") as handle:
            payload = handle.read(max_bytes + 1)
        if len(payload) > max_bytes:
            raise ValueError("artifact exceeds configured byte limit")
        return payload


class _UnavailableReader:
    def __init__(self) -> None:
        self.attempted = False

    def read_bytes(self, uri: str, *, max_bytes: int | None = None) -> bytes:
        del uri, max_bytes
        self.attempted = True
        raise OSError("remote unavailable")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _index_payload(generation: str, *, pack: bytes, spec: bytes) -> dict[str, object]:
    return {
        "git_sha": generation,
        "artifacts": {
            "orders": {
                "path": "orders/airflow-pack.json",
                "sha256": _sha256(pack),
                "bytes": len(pack),
            }
        },
        "dag_specs": {
            "DAG__orders": {
                "path": "airflow/_dags/DAG__orders.dag-spec.json",
                "sha256": _sha256(spec),
                "bytes": len(spec),
            }
        },
    }


def _write_remote(root: Path, generation: str) -> Path:
    pack = b'{"kind":"gitops.airflow_pack","workload":{"workload_id":"orders"}}'
    spec = b'{"kind":"gitops.airflow_dag_spec","dag_id":"DAG__orders"}'
    generation_root = root / generation
    pack_path = generation_root / "orders" / "airflow-pack.json"
    spec_path = generation_root / "airflow" / "_dags" / "DAG__orders.dag-spec.json"
    pack_path.parent.mkdir(parents=True)
    spec_path.parent.mkdir(parents=True)
    pack_path.write_bytes(pack)
    spec_path.write_bytes(spec)
    index = _index_payload(generation, pack=pack, spec=spec)
    index["artifacts"]["orders"]["uri"] = pack_path.as_uri()  # type: ignore[index]
    index["dag_specs"]["DAG__orders"]["uri"] = spec_path.as_uri()  # type: ignore[index]
    latest = root / "latest" / "pack-index.json"
    latest.parent.mkdir(parents=True)
    latest.write_text(json.dumps(index, sort_keys=True), encoding="utf-8")
    return latest


def _sync(index: Path, cache: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cache_sync_module, "ArtifactReader", lambda **_: _LocalReader())
    result = sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri=index.as_uri(), cache_dir=cache))
    assert result["status"] == "success"


def _write_unmanaged_cache(cache: Path, generation: str) -> tuple[Path, Path]:
    pack = b'{"kind":"gitops.airflow_pack","workload":{"workload_id":"orders"}}'
    spec = b'{"kind":"gitops.airflow_dag_spec","dag_id":"DAG__orders"}'
    generation_root = cache / "generations" / generation
    pack_path = generation_root / "orders" / "airflow-pack.json"
    spec_path = generation_root / "airflow" / "_dags" / "DAG__orders.dag-spec.json"
    pack_path.parent.mkdir(parents=True)
    spec_path.parent.mkdir(parents=True)
    pack_path.write_bytes(pack)
    spec_path.write_bytes(spec)
    index_path = generation_root / "pack-index.json"
    index_path.write_text(
        json.dumps(_index_payload(generation, pack=pack, spec=spec), sort_keys=True),
        encoding="utf-8",
    )
    (cache / "current").write_text(generation, encoding="utf-8")
    return pack_path, spec_path


def _prepare_pending_generation(cache: Path, generation: str) -> tuple[Path, str]:
    pack = b'{"kind":"gitops.airflow_pack","version":2}'
    spec = b'{"kind":"gitops.airflow_dag_spec","dag_id":"DAG__pending"}'
    stage = create_generation_stage(cache, generation=generation)
    pack_path = stage / "orders" / "airflow-pack.json"
    spec_path = stage / "airflow" / "_dags" / "DAG__pending.dag-spec.json"
    pack_path.parent.mkdir(parents=True)
    spec_path.parent.mkdir(parents=True)
    pack_path.write_bytes(pack)
    spec_path.write_bytes(spec)
    (stage / "pack-index.json").write_text(
        json.dumps(_index_payload(generation, pack=pack, spec=spec), sort_keys=True),
        encoding="utf-8",
    )
    candidate = finalize_generation_stage(stage, generation=generation)
    generation_root = cache / "generations" / generation
    candidate.path.rename(generation_root)
    generation_root.chmod(0o555)
    write_json_durable(
        cache / PENDING_COMMIT_PATH,
        {
            "schema": "dpone.airflow-pack-cache-commit.v1",
            "commit_id": "pending-recovery",
            "sequence": 2,
            "generation": generation,
            "index_sha256": candidate.index_sha256,
            "committed_at": "2026-08-02T00:00:00Z",
        },
        mode=SHARED_CONTROL_MODE,
    )
    return generation_root, candidate.index_sha256


@pytest.mark.parametrize(
    "tampered_relative_path",
    ["pack-index.json", "orders/airflow-pack.json"],
)
def test_pending_recovery_rehashes_actual_generation_before_pointer_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tampered_relative_path: str,
) -> None:
    cache = tmp_path / "cache"
    active = "a" * 40
    pending = "b" * 40
    _sync(_write_remote(tmp_path / "remote", active), cache, monkeypatch)
    generation_root, _ = _prepare_pending_generation(cache, pending)
    tampered = generation_root / tampered_relative_path
    tampered.chmod(0o644)
    tampered.write_bytes(tampered.read_bytes() + b"\ncorrupt")
    tampered.chmod(0o444)
    current_before = (cache / "current").read_bytes()
    symlink_before = os.readlink(cache / "current_generation")
    receipt_before = (cache / CURRENT_COMMIT_PATH).read_bytes()

    with cache_write_lease(cache):
        with pytest.raises(ValueError, match="airflow_pack_generation_receipt_invalid"):
            recover_interrupted_commit(cache)

    assert (cache / "current").read_bytes() == current_before
    assert os.readlink(cache / "current_generation") == symlink_before
    assert (cache / CURRENT_COMMIT_PATH).read_bytes() == receipt_before
    assert (cache / PENDING_COMMIT_PATH).exists()


@pytest.mark.parametrize("divergent_authority", ["receipt", "current", "current_generation"])
def test_status_and_loader_reject_authority_split_brain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    divergent_authority: str,
) -> None:
    cache = tmp_path / "cache"
    first = "a" * 40
    second = "b" * 40
    _sync(_write_remote(tmp_path / "remote-a", first), cache, monkeypatch)
    _sync(_write_remote(tmp_path / "remote-b", second), cache, monkeypatch)
    if divergent_authority == "receipt":
        receipt_path = cache / CURRENT_COMMIT_PATH
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["generation"] = first
        receipt["index_sha256"] = "sha256:" + _sha256((cache / "generations" / first / "pack-index.json").read_bytes())
        receipt_path.write_text(json.dumps(receipt, sort_keys=True), encoding="utf-8")
    elif divergent_authority == "current":
        (cache / "current").write_text(first, encoding="utf-8")
    else:
        compatibility_link = cache / "current_generation"
        compatibility_link.unlink()
        compatibility_link.symlink_to(Path("generations") / first, target_is_directory=True)
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(cache))
    monkeypatch.setenv(STORAGE_MODE_ENV, "remote")

    status = read_airflow_pack_cache_status(cache)

    assert status["status"] == "blocked"
    assert "airflow_pack_commit_receipt_mismatch" in {item["code"] for item in status["blockers"]}
    with pytest.raises(DagSpecCacheMissingError, match="commit receipt"):
        with pinned_dag_spec_paths(tmp_path):
            pass


def test_valid_unmanaged_cache_is_adopted_before_remote_outage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    generation = "a" * 40
    pack_path, spec_path = _write_unmanaged_cache(cache, generation)
    pack_before = pack_path.read_bytes()
    unavailable = _UnavailableReader()
    monkeypatch.setattr(cache_sync_module, "ArtifactReader", lambda **_: unavailable)

    with pytest.raises(OSError, match="remote unavailable"):
        sync_airflow_pack_cache(
            AirflowPackSyncOptions(index_uri="s3://unavailable/latest/pack-index.json", cache_dir=cache)
        )

    receipt = read_current_commit(cache)
    assert unavailable.attempted is True
    assert receipt is not None and receipt.durable is True
    assert receipt.generation == generation
    assert pack_path.read_bytes() == pack_before
    assert (cache / "current").read_text(encoding="utf-8") == generation
    assert os.readlink(cache / "current_generation") == f"generations/{generation}"
    generation_root = cache / "generations" / generation
    assert generation_receipt_path(generation_root, generation=generation).exists()
    assert read_airflow_pack_cache_status(cache)["status"] == "success"
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(cache))
    monkeypatch.setenv(STORAGE_MODE_ENV, "remote")
    with pinned_dag_spec_paths(tmp_path) as paths:
        assert paths == (spec_path,)


def test_unmanaged_generation_adoption_does_not_mutate_foreign_generation_inode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    generation = "a" * 40
    _write_unmanaged_cache(cache, generation)
    generation_root = cache / "generations" / generation
    real_chmod = Path.chmod

    def reject_generation_chmod(path: Path, *args: object, **kwargs: object) -> None:
        if path == generation_root:
            raise PermissionError("simulated foreign generation owner")
        real_chmod(path, *args, **kwargs)

    monkeypatch.setattr(Path, "chmod", reject_generation_chmod)
    monkeypatch.setattr(cache_sync_module, "ArtifactReader", lambda **_: _UnavailableReader())

    with pytest.raises(OSError, match="remote unavailable"):
        sync_airflow_pack_cache(
            AirflowPackSyncOptions(index_uri="s3://unavailable/latest/pack-index.json", cache_dir=cache)
        )

    receipt_path = generation_receipt_path(generation_root, generation=generation)
    assert receipt_path.parent == cache / "status" / "generation-receipts"
    assert receipt_path.exists()
    assert stat.S_IMODE(receipt_path.stat().st_mode) == 0o664


def test_corrupt_unmanaged_cache_is_not_adopted_or_replaced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    generation = "a" * 40
    pack_path, _ = _write_unmanaged_cache(cache, generation)
    pack_path.write_bytes(b"corrupt")
    monkeypatch.setattr(
        cache_sync_module,
        "ArtifactReader",
        lambda **_: pytest.fail("invalid unmanaged bytes must fail before remote access"),
    )

    with pytest.raises(ValueError, match="airflow_pack_generation_invalid"):
        sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri="s3://unused/latest/pack-index.json", cache_dir=cache))

    assert (cache / "current").read_text(encoding="utf-8") == generation
    assert not os.path.lexists(cache / "current_generation")
    assert not (cache / CURRENT_COMMIT_PATH).exists()
    assert not (cache / "generations" / generation / GENERATION_RECEIPT_NAME).exists()
    assert not generation_receipt_path(cache / "generations" / generation, generation=generation).exists()


def test_unmanaged_cache_publishes_layout_marker_only_after_adoption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    _write_unmanaged_cache(cache, "a" * 40)

    def fail_adoption(_cache_root: Path, **_kwargs: object) -> None:
        raise OSError("adoption interrupted")

    monkeypatch.setattr(cache_sync_module, "recover_interrupted_commit", fail_adoption)

    with pytest.raises(OSError, match="adoption interrupted"):
        sync_airflow_pack_cache(
            AirflowPackSyncOptions(index_uri="s3://unavailable/latest/pack-index.json", cache_dir=cache)
        )

    assert not (cache / LAYOUT_MARKER_NAME).exists()


def test_sync_evidence_blocks_on_authority_split_brain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    generation = "a" * 40
    latest = _write_remote(tmp_path / "remote", generation)
    options = AirflowPackSyncOptions(index_uri=latest.as_uri(), cache_dir=cache)
    _sync(latest, cache, monkeypatch)
    (cache / "current").write_text("b" * 40, encoding="utf-8")

    evidence = publish_sync_evidence(
        options,
        started_at="2026-08-03T00:00:00Z",
        attempt_generation=generation,
        downloaded=1,
        downloaded_specs=1,
        committed=True,
        warnings=[],
        blockers=[],
        cache_bytes=1,
    )

    assert evidence["status"] == "blocked"
    assert "airflow_pack_cache_authority_mismatch" in {item["code"] for item in evidence["blockers"]}


def test_current_generation_read_is_descriptor_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    generation = "a" * 40
    current = cache / "current"
    current.write_text(generation, encoding="utf-8")
    original_read_text = Path.read_text

    def reject_path_read(path: Path, *args: object, **kwargs: object) -> str:
        if path == current:
            raise AssertionError("current pointer must be read through one bounded descriptor")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", reject_path_read)

    receipt = read_current_commit(cache)
    assert receipt is not None and receipt.generation == generation

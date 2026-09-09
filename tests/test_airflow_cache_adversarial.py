from __future__ import annotations

import json
import os
import stat
import threading
import time
from pathlib import Path

import pytest
from dpone_airflow_pack import cache_sync as cache_sync_module
from dpone_airflow_pack.cache_generation_lease import generation_stage_lease
from dpone_airflow_pack.cache_generation_retention import enforce_cache_retention
from dpone_airflow_pack.cache_generation_store import (
    STAGE_MARKER_NAME,
    create_generation_stage,
)
from dpone_airflow_pack.cache_status import read_airflow_pack_cache_status
from dpone_airflow_pack.cache_sync import AirflowPackSyncOptions, sync_airflow_pack_cache
from dpone_airflow_pack.dag_spec_cache_paths import pinned_dag_spec_paths
from dpone_airflow_pack.pack_storage_consumer import STORAGE_MODE_ENV, DagSpecCacheMissingError


class _LocalReader:
    def read_bytes(self, uri: str, *, max_bytes: int | None = None) -> bytes:
        payload = Path(uri.removeprefix("file://")).read_bytes()
        if max_bytes is not None and len(payload) > max_bytes:
            raise ValueError("artifact exceeds test reader bound")
        return payload


def _remote_index(root: Path, generation: str, *, with_spec: bool = False) -> Path:
    payload = b'{"kind":"gitops.airflow_pack"}'
    pack = root / generation / "orders" / "airflow-pack.json"
    pack.parent.mkdir(parents=True)
    pack.write_bytes(payload)
    index: dict[str, object] = {
        "git_sha": generation,
        "artifacts": {
            "orders": {
                "path": "orders/airflow-pack.json",
                "uri": pack.as_uri(),
                "sha256": __import__("hashlib").sha256(payload).hexdigest(),
                "size_bytes": len(payload),
            }
        },
    }
    if with_spec:
        spec_payload = b'{"kind":"gitops.airflow_dag_spec","dag_id":"DAG__demo"}'
        spec = root / generation / "airflow" / "_dags" / "DAG__demo.dag-spec.json"
        spec.parent.mkdir(parents=True)
        spec.write_bytes(spec_payload)
        index["dag_specs"] = {
            "DAG__demo": {
                "path": "airflow/_dags/DAG__demo.dag-spec.json",
                "uri": spec.as_uri(),
                "sha256": __import__("hashlib").sha256(spec_payload).hexdigest(),
                "size_bytes": len(spec_payload),
            }
        }
    latest = root / "latest" / "pack-index.json"
    latest.parent.mkdir(parents=True)
    latest.write_text(json.dumps(index), encoding="utf-8")
    return latest


def _sync(index: Path, cache: Path, monkeypatch: pytest.MonkeyPatch, **options: object) -> dict[str, object]:
    monkeypatch.setattr(cache_sync_module, "ArtifactReader", lambda **_: _LocalReader())
    return sync_airflow_pack_cache(
        AirflowPackSyncOptions(index_uri=index.as_uri(), cache_dir=cache, **options)  # type: ignore[arg-type]
    )


def test_receipt_index_digest_mismatch_blocks_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    _sync(_remote_index(tmp_path / "remote", "a" * 40), cache, monkeypatch)
    receipt_path = cache / "status" / "current-commit.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["index_sha256"] = "sha256:" + "0" * 64
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    status = read_airflow_pack_cache_status(cache)

    assert status["status"] == "blocked"
    assert "airflow_pack_commit_index_mismatch" in {item["code"] for item in status["blockers"]}


def test_retention_never_selects_pointer_or_receipt_generation_during_split(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    first = "a" * 40
    second = "b" * 40
    _sync(_remote_index(tmp_path / "remote-a", first), cache, monkeypatch)
    _sync(_remote_index(tmp_path / "remote-b", second), cache, monkeypatch)
    receipt_path = cache / "status" / "current-commit.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["generation"] = first
    receipt["index_sha256"] = (
        "sha256:"
        + __import__("hashlib").sha256((cache / "generations" / first / "pack-index.json").read_bytes()).hexdigest()
    )
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    result = enforce_cache_retention(
        cache,
        keep_generations=1,
        max_total_bytes=512 * 1024 * 1024,
        high_watermark_pct=80,
        low_watermark_pct=60,
        partial_download_ttl_minutes=30,
    )

    assert (cache / "generations" / first).exists()
    assert (cache / "generations" / second).exists()
    assert "airflow_pack_cache_authority_mismatch" in {item["code"] for item in result.blockers}


def test_retention_blocks_legacy_pointer_symlink_disagreement_without_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    generations = ("a" * 40, "b" * 40, "c" * 40)
    for generation in generations:
        _sync(
            _remote_index(tmp_path / f"remote-{generation[0]}", generation),
            cache,
            monkeypatch,
            keep_generations=3,
        )
    (cache / "status" / "current-commit.json").unlink()
    (cache / "current").write_text(generations[0], encoding="utf-8")

    result = enforce_cache_retention(
        cache,
        keep_generations=1,
        max_total_bytes=512 * 1024 * 1024,
        high_watermark_pct=80,
        low_watermark_pct=60,
        partial_download_ttl_minutes=30,
    )

    assert {path.name for path in (cache / "generations").iterdir()} == set(generations)
    assert "airflow_pack_cache_authority_mismatch" in {item["code"] for item in result.blockers}


def test_retention_rehashes_active_generation_under_detach_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import cache_generation_retention as retention

    cache = tmp_path / "cache"
    first = "a" * 40
    second = "b" * 40
    _sync(_remote_index(tmp_path / "remote-a", first), cache, monkeypatch, keep_generations=2)
    _sync(_remote_index(tmp_path / "remote-b", second), cache, monkeypatch, keep_generations=2)
    original = retention._read_cache_authority
    calls = 0

    def corrupt_before_locked_validation(*args: object, **kwargs: object):
        nonlocal calls
        calls += 1
        if calls == 2:
            index = cache / "generations" / second / "pack-index.json"
            index.chmod(0o640)
            index.write_bytes(index.read_bytes() + b"corrupt")
            index.chmod(0o440)
        return original(*args, **kwargs)

    monkeypatch.setattr(retention, "_read_cache_authority", corrupt_before_locked_validation)

    result = enforce_cache_retention(
        cache,
        keep_generations=1,
        max_total_bytes=512 * 1024 * 1024,
        high_watermark_pct=80,
        low_watermark_pct=60,
        partial_download_ttl_minutes=30,
    )

    assert {path.name for path in (cache / "generations").iterdir()} == {first, second}
    assert "airflow_pack_cache_authority_mismatch" in {item["code"] for item in result.blockers}


def test_published_generation_is_pruned_on_local_filesystem(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    _sync(_remote_index(tmp_path / "remote-a", "a" * 40), cache, monkeypatch, keep_generations=2)
    _sync(_remote_index(tmp_path / "remote-b", "b" * 40), cache, monkeypatch, keep_generations=2)

    result = enforce_cache_retention(
        cache,
        keep_generations=1,
        max_total_bytes=512 * 1024 * 1024,
        high_watermark_pct=80,
        low_watermark_pct=60,
        partial_download_ttl_minutes=30,
    )

    assert result.blockers == ()
    assert [path.name for path in (cache / "generations").iterdir()] == ["b" * 40]


def test_retention_enforcement_failure_is_a_blocker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cache_sync_module, "ArtifactReader", lambda **_: _LocalReader())
    monkeypatch.setattr(
        cache_sync_module,
        "enforce_cache_retention",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("retention unavailable")),
    )

    result = sync_airflow_pack_cache(
        AirflowPackSyncOptions(
            index_uri=_remote_index(tmp_path / "remote", "a" * 40).as_uri(),
            cache_dir=tmp_path / "cache",
        )
    )

    assert result["status"] == "blocked"
    assert "airflow_pack_cache_retention_unavailable" in {item["code"] for item in result["blockers"]}


def test_versioned_loader_refuses_pointer_receipt_split(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    first = "a" * 40
    second = "b" * 40
    _sync(_remote_index(tmp_path / "remote-a", first, with_spec=True), cache, monkeypatch)
    _sync(_remote_index(tmp_path / "remote-b", second, with_spec=True), cache, monkeypatch)
    receipt_path = cache / "status" / "current-commit.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["generation"] = first
    receipt["index_sha256"] = (
        "sha256:"
        + __import__("hashlib").sha256((cache / "generations" / first / "pack-index.json").read_bytes()).hexdigest()
    )
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(cache))
    monkeypatch.setenv(STORAGE_MODE_ENV, "remote")

    with pytest.raises(DagSpecCacheMissingError, match="commit receipt"):
        with pinned_dag_spec_paths(tmp_path):
            pass


def test_stage_lease_protects_stale_foreign_owner(
    tmp_path: Path,
) -> None:
    cache = tmp_path / "cache"
    stage = create_generation_stage(cache, generation="orphan", created_at=time.time() - 7200)
    marker_path = stage / STAGE_MARKER_NAME
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    marker["hostname"] = "other-pod"
    marker["pid"] = 2**30
    marker["heartbeat_at_epoch"] = time.time() - 7200
    marker_path.write_text(json.dumps(marker), encoding="utf-8")

    with generation_stage_lease(stage, marker_name=STAGE_MARKER_NAME):
        result = enforce_cache_retention(
            cache,
            keep_generations=1,
            max_total_bytes=None,
            high_watermark_pct=80,
            low_watermark_pct=60,
            partial_download_ttl_minutes=30,
        )

    assert stage.exists()
    assert "cache_active_stage_preserved" in result.warnings


def test_shared_control_paths_ignore_restrictive_umask(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = os.umask(0o077)
    try:
        cache = tmp_path / "cache"
        _sync(_remote_index(tmp_path / "remote", "a" * 40), cache, monkeypatch)
    finally:
        os.umask(original)

    assert stat.S_IMODE(cache.stat().st_mode) == 0o2775
    assert stat.S_IMODE((cache / ".promotion.lock").stat().st_mode) == 0o664
    assert stat.S_IMODE((cache / ".dpone-cache-layout.json").stat().st_mode) == 0o664
    assert stat.S_IMODE((cache / "status").stat().st_mode) == 0o2775
    assert stat.S_IMODE((cache / "status" / "current-commit.json").stat().st_mode) == 0o664


def test_receipt_directory_fsync_failure_leaves_fail_visible_pending_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import cache_generation_files

    cache = tmp_path / "cache"
    _sync(_remote_index(tmp_path / "remote-a", "a" * 40), cache, monkeypatch)
    second_index = _remote_index(tmp_path / "remote-b", "b" * 40)
    original = cache_generation_files.fsync_directory
    status_fsync_calls = 0

    def fail_second_status_fsync(path: Path) -> None:
        nonlocal status_fsync_calls
        if path == cache / "status":
            status_fsync_calls += 1
            if status_fsync_calls == 2:
                raise OSError("injected receipt directory fsync failure")
        original(path)

    monkeypatch.setattr(cache_generation_files, "fsync_directory", fail_second_status_fsync)

    with pytest.raises(OSError, match="receipt directory fsync failure"):
        _sync(second_index, cache, monkeypatch)

    status = read_airflow_pack_cache_status(cache)
    assert status["status"] == "blocked"
    assert "airflow_pack_commit_receipt_invalid" in {item["code"] for item in status["blockers"]}
    assert (cache / "status" / "pending-commit.json").exists()

    monkeypatch.setattr(cache_generation_files, "fsync_directory", original)
    result = _sync(second_index, cache, monkeypatch)
    assert result["status"] == "success"
    assert result["current_generation"] == "b" * 40
    assert not (cache / "status" / "pending-commit.json").exists()


def test_loader_read_lease_prevents_concurrent_generation_detach(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    _sync(_remote_index(tmp_path / "remote-a", "a" * 40, with_spec=True), cache, monkeypatch)
    _sync(
        _remote_index(tmp_path / "remote-b", "b" * 40, with_spec=True),
        cache,
        monkeypatch,
        keep_generations=2,
    )
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(cache))
    monkeypatch.setenv(STORAGE_MODE_ENV, "remote")
    completed = threading.Event()

    def retain() -> None:
        enforce_cache_retention(
            cache,
            keep_generations=1,
            max_total_bytes=None,
            high_watermark_pct=80,
            low_watermark_pct=60,
            partial_download_ttl_minutes=30,
        )
        completed.set()

    with pinned_dag_spec_paths(tmp_path) as paths:
        worker = threading.Thread(target=retain)
        worker.start()
        assert paths
        assert not completed.wait(timeout=0.1)
        assert all(path.exists() for path in paths)
    worker.join(timeout=5)

    assert completed.is_set()
    assert (cache / "generations" / ("b" * 40)).exists()


def test_delayed_variable_publish_cannot_overwrite_newer_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import cache_sync_evidence

    first = _remote_index(tmp_path / "remote-a", "a" * 40)
    second = _remote_index(tmp_path / "remote-b", "b" * 40)
    cache = tmp_path / "cache"
    monkeypatch.setattr(cache_sync_module, "ArtifactReader", lambda **_: _LocalReader())
    published: list[dict[str, object]] = []
    first_publish_started = threading.Event()
    release_first_publish = threading.Event()

    class _Publisher:
        def __init__(self, key: str | None) -> None:
            self.key = key

        def publish(self, evidence: dict[str, object]) -> dict[str, object]:
            if evidence.get("current_generation") == "a" * 40:
                first_publish_started.set()
                assert release_first_publish.wait(timeout=5)
            published.append(dict(evidence))
            return dict(evidence)

    monkeypatch.setattr(cache_sync_evidence, "AirflowVariableStatusPublisher", _Publisher)
    errors: list[BaseException] = []

    def run(index: Path) -> None:
        try:
            sync_airflow_pack_cache(
                AirflowPackSyncOptions(index_uri=index.as_uri(), cache_dir=cache, airflow_variable_key="status")
            )
        except BaseException as exc:  # pragma: no cover - assertion reports worker failure.
            errors.append(exc)

    stale = threading.Thread(target=run, args=(first,))
    fresh = threading.Thread(target=run, args=(second,))
    stale.start()
    assert first_publish_started.wait(timeout=5)
    fresh.start()
    release_first_publish.set()
    stale.join(timeout=5)
    fresh.join(timeout=5)

    assert errors == []
    assert published[-1]["current_generation"] == "b" * 40
    status = json.loads((cache / "status" / "last-sync-status.json").read_text(encoding="utf-8"))
    assert status["current_generation"] == "b" * 40


def test_concurrent_downloads_reserve_capacity_before_payload_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_generation = "a" * 40
    second_generation = "b" * 40
    first_index = _remote_index(tmp_path / "remote-a", first_generation)
    second_index = _remote_index(tmp_path / "remote-b", second_generation)
    blocked_pack = tmp_path / "remote-a" / first_generation / "orders" / "airflow-pack.json"
    first_download_started = threading.Event()
    release_first_download = threading.Event()

    class _BlockingReader(_LocalReader):
        def read_bytes(self, uri: str, *, max_bytes: int | None = None) -> bytes:
            path = Path(uri.removeprefix("file://"))
            if path == blocked_pack:
                first_download_started.set()
                assert release_first_download.wait(timeout=5)
            return super().read_bytes(uri, max_bytes=max_bytes)

    reader = _BlockingReader()
    monkeypatch.setattr(cache_sync_module, "ArtifactReader", lambda **_: reader)
    cache = tmp_path / "cache"
    results: list[dict[str, object]] = []
    errors: list[BaseException] = []

    def run(index: Path) -> None:
        try:
            results.append(
                sync_airflow_pack_cache(
                    AirflowPackSyncOptions(
                        index_uri=index.as_uri(),
                        cache_dir=cache,
                        max_total_bytes=140 * 1024,
                    )
                )
            )
        except BaseException as exc:  # pragma: no cover - assertion reports worker failure.
            errors.append(exc)

    first = threading.Thread(target=run, args=(first_index,), name="first-capacity-reservation")
    second = threading.Thread(target=run, args=(second_index,), name="second-capacity-reservation")
    first.start()
    assert first_download_started.wait(timeout=5)
    second.start()
    second.join(timeout=5)
    assert not second.is_alive()
    release_first_download.set()
    first.join(timeout=5)

    assert len(results) == 1
    assert len(errors) == 1
    assert "airflow_pack_cache_capacity_unavailable" in str(errors[0])
    assert (cache / "current").read_text(encoding="utf-8") == first_generation
    assert not list((cache / ".reservations").glob("*.json"))


def test_invalid_layout_marker_is_reported_as_cache_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / ".promotion.lock").touch()
    (cache / ".dpone-cache-layout.json").write_text("not-json", encoding="utf-8")
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_CACHE_DIR", str(cache))
    monkeypatch.setenv(STORAGE_MODE_ENV, "remote")

    with pytest.raises(DagSpecCacheMissingError, match="durable commit receipt is invalid"):
        with pinned_dag_spec_paths(tmp_path):
            pass

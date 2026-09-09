from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import threading
import time
import warnings
from dataclasses import replace
from pathlib import Path

import pytest
from dpone_airflow_pack import cache_sync as cache_sync_module
from dpone_airflow_pack.cache_activation_contract import cache_read_lease
from dpone_airflow_pack.cache_generation_retention import enforce_cache_retention
from dpone_airflow_pack.cache_generation_store import (
    STAGE_MARKER_NAME,
    create_generation_stage,
)
from dpone_airflow_pack.cache_status import read_airflow_pack_cache_status
from dpone_airflow_pack.cache_sync import AirflowPackSyncOptions, sync_airflow_pack_cache


class _BlockingLocalReader:
    def __init__(self, blocked_path: Path | None = None) -> None:
        self.blocked_path = blocked_path
        self.started = threading.Event()
        self.release = threading.Event()

    def read_bytes(self, uri: str, *, max_bytes: int | None = None) -> bytes:
        path = Path(uri.removeprefix("file://"))
        if self.blocked_path == path:
            self.started.set()
            assert self.release.wait(timeout=5)
        payload = path.read_bytes()
        if max_bytes is not None and len(payload) > max_bytes:
            raise ValueError("artifact exceeds test reader bound")
        return payload


def _remote_index(root: Path, generation: str, payload: bytes) -> tuple[Path, Path]:
    pack = root / generation / "airflow" / "orders" / "airflow-pack.json"
    pack.parent.mkdir(parents=True)
    pack.write_bytes(payload)
    digest = __import__("hashlib").sha256(payload).hexdigest()
    index = root / "latest" / "pack-index.json"
    index.parent.mkdir(parents=True, exist_ok=True)
    index.write_text(
        json.dumps(
            {
                "git_sha": generation,
                "artifacts": {
                    "orders": {
                        "path": "orders/airflow-pack.json",
                        "uri": pack.as_uri(),
                        "sha256": digest,
                    }
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return index, pack


def test_concurrent_stale_sync_cannot_revert_current_or_publish_false_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import cache_sync as cache_sync_module

    index_a, pack_a = _remote_index(tmp_path / "remote-a", "a" * 40, b'{"version":1}')
    index_b, _ = _remote_index(tmp_path / "remote-b", "b" * 40, b'{"version":2}')
    reader = _BlockingLocalReader(pack_a)
    monkeypatch.setattr(cache_sync_module, "ArtifactReader", lambda **_: reader)
    cache = tmp_path / "cache"
    results: dict[str, dict[str, object]] = {}
    errors: list[BaseException] = []

    def run(name: str, index: Path) -> None:
        try:
            results[name] = sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri=index.as_uri(), cache_dir=cache))
        except BaseException as exc:  # pragma: no cover - assertion reports worker failure.
            errors.append(exc)

    stale = threading.Thread(target=run, args=("stale", index_a), name="stale-sync")
    fresh = threading.Thread(target=run, args=("fresh", index_b), name="fresh-sync")
    stale.start()
    assert reader.started.wait(timeout=5)
    fresh.start()
    fresh.join(timeout=5)
    assert not fresh.is_alive()
    reader.release.set()
    stale.join(timeout=5)

    assert errors == []
    assert (cache / "current").read_text(encoding="utf-8") == "b" * 40
    assert results["fresh"]["current_generation"] == "b" * 40
    assert results["stale"]["current_generation"] == "b" * 40
    assert results["stale"]["reason"] == "sync_superseded"
    status = json.loads((cache / "status" / "last-sync-status.json").read_text(encoding="utf-8"))
    assert status["current_generation"] == "b" * 40
    assert status["commit_sequence"] == 1


def test_generation_is_immutable_and_readable_by_a_separate_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import cache_sync as cache_sync_module

    index, _ = _remote_index(tmp_path / "remote", "abc123", b'{"version":1}')
    monkeypatch.setattr(cache_sync_module, "ArtifactReader", lambda **_: _BlockingLocalReader())
    cache = tmp_path / "cache"
    sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri=index.as_uri(), cache_dir=cache))
    generation = cache / "generations" / "abc123"

    for root, directories, files in os.walk(generation):
        assert stat.S_IMODE(Path(root).stat().st_mode) == 0o2770
        for name in directories:
            assert stat.S_IMODE((Path(root) / name).stat().st_mode) == 0o2770
        for name in files:
            assert stat.S_IMODE((Path(root) / name).stat().st_mode) == 0o440
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "from pathlib import Path; print(Path(__import__('sys').argv[1]).read_text())",
            str(generation / "pack-index.json"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0
    assert '"git_sha": "abc123"' in completed.stdout


def test_stale_owned_stage_is_removed_and_budget_postcondition_is_enforced(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    from dpone_airflow_pack.cache_activation_contract import cache_write_lease
    from dpone_airflow_pack.cache_layout import LEGACY_PACK_INDEX_LAYOUT, ensure_cache_layout

    with cache_write_lease(cache):
        ensure_cache_layout(cache, expected_layout=LEGACY_PACK_INDEX_LAYOUT)
    stage = create_generation_stage(cache, generation="orphan", created_at=time.time() - 7200)
    (stage / "payload.bin").write_bytes(b"x" * 4096)
    marker_path = stage / STAGE_MARKER_NAME
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    marker["pid"] = 2**30
    marker["heartbeat_at_epoch"] = time.time() - 7200
    marker_path.write_text(json.dumps(marker), encoding="utf-8")

    result = enforce_cache_retention(
        cache,
        keep_generations=3,
        max_total_bytes=1024,
        high_watermark_pct=80,
        low_watermark_pct=60,
        partial_download_ttl_minutes=30,
    )

    assert not stage.exists()
    assert result.total_bytes <= 1024
    assert result.blockers == ()


def test_current_generation_over_budget_is_a_blocker_not_false_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import cache_sync as cache_sync_module

    index, _ = _remote_index(tmp_path / "remote", "abc123", b"x" * 700)
    monkeypatch.setattr(cache_sync_module, "ArtifactReader", lambda **_: _BlockingLocalReader())
    cache = tmp_path / "cache"
    sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri=index.as_uri(), cache_dir=cache))

    result = sync_airflow_pack_cache(
        AirflowPackSyncOptions(index_uri=index.as_uri(), cache_dir=cache, max_total_bytes=1400)
    )

    assert result["status"] == "blocked"
    assert result["blockers"][0]["code"] == "airflow_pack_cache_budget_exceeded"


def test_pointer_directory_fsync_failure_never_publishes_false_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import cache_generation_files

    first_index, _ = _remote_index(tmp_path / "remote-a", "a" * 40, b'{"version":1}')
    second_index, _ = _remote_index(tmp_path / "remote-b", "b" * 40, b'{"version":2}')
    monkeypatch.setattr(cache_sync_module, "ArtifactReader", lambda **_: _BlockingLocalReader())
    cache = tmp_path / "cache"
    sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri=first_index.as_uri(), cache_dir=cache))
    real_fsync_directory = cache_generation_files.fsync_directory

    def fail_current_parent_fsync(path: Path) -> None:
        if path == cache:
            raise OSError("injected pointer directory fsync failure")
        real_fsync_directory(path)

    monkeypatch.setattr(cache_generation_files, "fsync_directory", fail_current_parent_fsync)

    with pytest.raises(OSError, match="pointer directory fsync failure"):
        sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri=second_index.as_uri(), cache_dir=cache))

    status = read_airflow_pack_cache_status(cache)
    assert status["status"] == "blocked"
    assert "airflow_pack_commit_receipt_invalid" in {item["code"] for item in status["blockers"]}
    assert not list(cache.glob(".current.*.tmp"))


def test_versioned_legacy_status_requires_matching_durable_commit_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import cache_sync as cache_sync_module

    index, _ = _remote_index(tmp_path / "remote", "abc123", b'{"version":1}')
    monkeypatch.setattr(cache_sync_module, "ArtifactReader", lambda **_: _BlockingLocalReader())
    cache = tmp_path / "cache"
    sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri=index.as_uri(), cache_dir=cache))
    (cache / "status" / "current-commit.json").unlink()

    status = read_airflow_pack_cache_status(cache)

    assert status["status"] == "blocked"
    assert "airflow_pack_commit_receipt_missing" in {item["code"] for item in status["blockers"]}


def test_exact_layout_without_active_index_is_not_misclassified_as_legacy(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_common import promotion_lock

    cache = tmp_path / "cache"
    with promotion_lock(cache):
        pass

    status = read_airflow_pack_cache_status(cache, workload_ids=("orders",))

    assert status["layout"] == "exact_deployment_index"
    assert status["status"] == "blocked"
    assert "airflow_pack_commit_receipt_missing" not in {item["code"] for item in status["blockers"]}


@pytest.mark.parametrize(
    "changes",
    [
        {"max_total_bytes": 0},
        {"max_pack_bytes": 0},
        {"max_index_bytes": -1},
        {"keep_generations": 0},
        {"low_watermark_pct": 80, "high_watermark_pct": 80},
        {"low_watermark_pct": 0},
        {"high_watermark_pct": 101},
        {"partial_download_ttl_minutes": 0},
    ],
)
def test_invalid_cache_policy_fails_before_cache_or_remote_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    changes: dict[str, int],
) -> None:
    cache = tmp_path / "cache"
    monkeypatch.setattr(
        cache_sync_module,
        "ArtifactReader",
        lambda **_: pytest.fail("remote reader must not be created for an invalid cache policy"),
    )
    options = replace(
        AirflowPackSyncOptions(index_uri="s3://bucket/index.json", cache_dir=cache),
        **changes,
    )

    with pytest.raises(ValueError, match="airflow_pack_cache_policy_invalid"):
        sync_airflow_pack_cache(options)

    assert not cache.exists()


def test_watcher_survives_failure_to_publish_diagnostic_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sync_attempts = 0
    warning_attempts = 0

    def fail_sync(_options: AirflowPackSyncOptions) -> dict[str, object]:
        nonlocal sync_attempts
        sync_attempts += 1
        raise OSError("primary sync failure")

    def fail_warning(*_args: object, **_kwargs: object) -> dict[str, object]:
        nonlocal warning_attempts
        warning_attempts += 1
        raise PermissionError("diagnostic path is unavailable")

    def stop_after_first_cycle(_seconds: float) -> None:
        raise StopIteration("watcher reached the next cycle")

    monkeypatch.setattr(cache_sync_module, "sync_airflow_pack_cache", fail_sync)
    monkeypatch.setattr(cache_sync_module, "write_sync_warning", fail_warning)
    monkeypatch.setattr(cache_sync_module.time, "sleep", stop_after_first_cycle)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(StopIteration, match="next cycle"):
            cache_sync_module.watch_airflow_pack_cache(
                AirflowPackSyncOptions(
                    index_uri="s3://bucket/latest/pack-index.json",
                    cache_dir=tmp_path / "cache",
                ),
                interval_seconds=60,
                jitter_seconds=0,
            )

    assert sync_attempts == 1
    assert warning_attempts == 1


def test_existing_generation_preverification_does_not_hold_writer_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import cache_sync as cache_sync_module

    index, _ = _remote_index(tmp_path / "remote", "abc123", b'{"version":1}')
    monkeypatch.setattr(cache_sync_module, "ArtifactReader", lambda **_: _BlockingLocalReader())
    cache = tmp_path / "cache"
    sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri=index.as_uri(), cache_dir=cache))
    original = cache_sync_module.inspect_existing_generation
    started = threading.Event()
    release = threading.Event()

    def blocking_inspection(path: Path, **kwargs: object):
        started.set()
        assert release.wait(timeout=5)
        return original(path, **kwargs)

    monkeypatch.setattr(cache_sync_module, "inspect_existing_generation", blocking_inspection)
    errors: list[BaseException] = []
    worker = threading.Thread(
        target=lambda: _capture_sync_error(index, cache, errors),
        name="existing-generation-sync",
    )
    worker.start()
    assert started.wait(timeout=5)
    with cache_read_lease(cache) as lease:
        assert lease.root_available is True
    release.set()
    worker.join(timeout=5)
    assert errors == []


def test_retention_revalidates_stale_stage_after_heartbeat(tmp_path: Path) -> None:
    from dpone_airflow_pack.cache_activation_contract import cache_write_lease
    from dpone_airflow_pack.cache_generation_retention import _build_plan, _detach_plan
    from dpone_airflow_pack.cache_generation_store import heartbeat_generation_stage
    from dpone_airflow_pack.cache_layout import LEGACY_PACK_INDEX_LAYOUT, ensure_cache_layout

    cache = tmp_path / "cache"
    with cache_write_lease(cache):
        ensure_cache_layout(cache, expected_layout=LEGACY_PACK_INDEX_LAYOUT)
    stage = create_generation_stage(cache, generation="orphan", created_at=time.time() - 7200)
    marker_path = stage / STAGE_MARKER_NAME
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    marker["pid"] = 2**30
    marker["heartbeat_at_epoch"] = time.time() - 7200
    marker_path.write_text(json.dumps(marker), encoding="utf-8")
    plan = _build_plan(
        cache,
        keep_generations=3,
        max_total_bytes=None,
        high_watermark_pct=80,
        low_watermark_pct=60,
        partial_download_ttl_minutes=30,
    )
    assert stage in plan.paths

    heartbeat_generation_stage(stage)
    detached = _detach_plan(cache, plan)

    assert detached.paths == ()
    assert detached.blockers == ()
    assert stage.exists()


def test_retention_detach_fsyncs_source_and_target_parents(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import cache_generation_retention as retention

    source = tmp_path / "generations" / "a"
    target = tmp_path / ".trash" / "a.1"
    source.mkdir(parents=True)
    target.parent.mkdir()
    synced: list[Path] = []
    monkeypatch.setattr(retention, "fsync_directory", synced.append)

    retention._detach_source(source, target)

    assert synced == [source.parent, target.parent]
    assert target.is_dir()


def _capture_sync_error(index: Path, cache: Path, errors: list[BaseException]) -> None:
    try:
        sync_airflow_pack_cache(AirflowPackSyncOptions(index_uri=index.as_uri(), cache_dir=cache))
    except BaseException as exc:  # pragma: no cover - assertion reports worker failure.
        errors.append(exc)

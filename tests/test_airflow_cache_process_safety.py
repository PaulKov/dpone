from __future__ import annotations

import json
import multiprocessing
import os
import stat
import warnings
from multiprocessing.connection import Connection
from pathlib import Path

import pytest


def test_cache_read_lease_opens_existing_lock_read_only(tmp_path: Path) -> None:
    from dpone_airflow_pack.cache_activation_contract import cache_read_lease

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    lock_path = cache_root / ".promotion.lock"
    lock_path.touch(mode=0o444)

    with cache_read_lease(cache_root) as lease:
        assert lease.root_available is True


def test_cache_read_lease_requires_writer_initialized_lock(tmp_path: Path) -> None:
    from dpone_airflow_pack.cache_activation_contract import cache_read_lease
    from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError

    cache_root = tmp_path / "cache"
    cache_root.mkdir()

    with pytest.raises(AirflowDeploymentIndexError) as exc:
        with cache_read_lease(cache_root):
            pass

    assert exc.value.code == "DPONE_CACHE_READ_LEASE_FAILED"
    assert not (cache_root / ".promotion.lock").exists()


def test_cache_leases_do_not_reclassify_body_failures(tmp_path: Path) -> None:
    from dpone_airflow_pack.cache_activation_contract import cache_read_lease, cache_write_lease

    cache_root = tmp_path / "cache"
    with cache_write_lease(cache_root):
        pass

    with pytest.raises(OSError, match="reader body failed"):
        with cache_read_lease(cache_root):
            raise OSError("reader body failed")
    with pytest.raises(OSError, match="writer body failed"):
        with cache_write_lease(cache_root):
            raise OSError("writer body failed")


def test_cache_lease_release_failure_never_masks_body_or_reports_false_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import fcntl

    from dpone_airflow_pack.cache_activation_contract import cache_write_lease

    original = fcntl.flock

    def fail_unlock(descriptor: int, operation: int) -> None:
        if operation == fcntl.LOCK_UN:
            raise OSError("unlock failed")
        original(descriptor, operation)

    monkeypatch.setattr(fcntl, "flock", fail_unlock)
    with pytest.warns(RuntimeWarning, match="DPONE_CACHE_LEASE_RELEASE_WARNING"):
        with pytest.raises(ValueError, match="body failed"):
            with cache_write_lease(tmp_path / "cache"):
                raise ValueError("body failed")
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        with cache_write_lease(tmp_path / "cache"):
            pass
    assert any("DPONE_CACHE_LEASE_RELEASE_WARNING" in str(item.message) for item in captured)


def test_promotion_lock_is_readable_by_separate_cache_consumers(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_common import promotion_lock

    cache_root = tmp_path / "cache"

    with promotion_lock(cache_root):
        pass

    assert stat.S_IMODE((cache_root / ".promotion.lock").stat().st_mode) == 0o664


def test_reconcile_lock_does_not_block_parse_read_lease(tmp_path: Path) -> None:
    from dpone_airflow_pack.cache_activation_contract import cache_read_lease

    from dpone.runtime.deployment_cache_common import promotion_lock, reconcile_lock

    cache_root = tmp_path / "cache"
    with promotion_lock(cache_root):
        pass

    with reconcile_lock(cache_root):
        with cache_read_lease(cache_root) as lease:
            assert lease.root_available is True

    assert stat.S_IMODE((cache_root / ".reconcile.lock").stat().st_mode) == 0o664


def test_existing_shared_promotion_lock_does_not_require_owner_chmod(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.cache_activation_contract import cache_write_lease

    cache_root = tmp_path / "cache"
    cache_root.mkdir(mode=0o2775)
    cache_root.chmod(0o2775)
    lock_path = cache_root / ".promotion.lock"
    lock_path.touch(mode=0o664)
    lock_path.chmod(0o664)
    lock_inode = lock_path.stat().st_ino
    real_fchmod = os.fchmod

    def deny_foreign_owner_chmod(descriptor: int, mode: int) -> None:
        if os.fstat(descriptor).st_ino == lock_inode:
            raise PermissionError("simulated foreign owner")
        real_fchmod(descriptor, mode)

    monkeypatch.setattr(os, "fchmod", deny_foreign_owner_chmod)

    with cache_write_lease(cache_root):
        pass


def test_exact_promotion_lock_does_not_require_owner_chmod(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_common import promotion_lock

    cache_root = tmp_path / "cache"
    cache_root.mkdir(mode=0o2775)
    cache_root.chmod(0o2775)
    lock_path = cache_root / ".promotion.lock"
    lock_path.touch(mode=0o664)
    lock_path.chmod(0o664)
    lock_inode = lock_path.stat().st_ino
    real_fchmod = os.fchmod

    def deny_foreign_owner_chmod(descriptor: int, mode: int) -> None:
        if os.fstat(descriptor).st_ino == lock_inode:
            raise PermissionError("simulated foreign owner")
        real_fchmod(descriptor, mode)

    monkeypatch.setattr(os, "fchmod", deny_foreign_owner_chmod)

    with promotion_lock(cache_root):
        pass


def _hold_parse_lease(
    cache_root: str,
    acquired: Connection,
    release: multiprocessing.synchronize.Event,
) -> None:
    from dpone_airflow_pack.cache_activation_contract import cache_read_lease

    with cache_read_lease(cache_root):
        acquired.send("reader_acquired")
        if not release.wait(timeout=10):
            raise RuntimeError("reader release signal timed out")


def _acquire_promotion_lock(cache_root: str, acquired: Connection) -> None:
    from dpone.runtime.deployment_cache_common import promotion_lock

    with promotion_lock(Path(cache_root)):
        acquired.send("writer_acquired")


def _acquire_cache_write_lease(cache_root: str, acquired: Connection) -> None:
    from dpone_airflow_pack.cache_activation_contract import cache_write_lease

    with cache_write_lease(cache_root):
        acquired.send("pack_writer_acquired")


def _read_activation_status(cache_root: str, completed: Connection) -> None:
    from dpone_airflow_pack.cache_status import read_airflow_pack_cache_status

    completed.send(read_airflow_pack_cache_status(cache_root)["activation_id"])


def _hold_stage_lease(
    stage: str,
    acquired: Connection,
    release: multiprocessing.synchronize.Event,
) -> None:
    from dpone_airflow_pack.cache_generation_lease import generation_stage_lease
    from dpone_airflow_pack.cache_generation_store import STAGE_MARKER_NAME

    with generation_stage_lease(Path(stage), marker_name=STAGE_MARKER_NAME):
        acquired.send("lease_acquired")
        if not release.wait(timeout=10):
            raise RuntimeError("stage lease release signal timed out")


@pytest.mark.skipif(
    not hasattr(multiprocessing, "get_context"),
    reason="multiprocessing context is unavailable",
)
def test_parse_lease_blocks_cross_process_promotion_until_ack_scope_ends(
    tmp_path: Path,
) -> None:
    pytest.importorskip("fcntl")
    from dpone.runtime.deployment_cache_common import promotion_lock

    cache_root = tmp_path / "cache"
    with promotion_lock(cache_root):
        pass
    context = multiprocessing.get_context("spawn")
    release = context.Event()
    reader_parent, reader_child = context.Pipe(duplex=False)
    writer_parent, writer_child = context.Pipe(duplex=False)
    reader = context.Process(
        target=_hold_parse_lease,
        args=(cache_root.as_posix(), reader_child, release),
    )
    writer = context.Process(
        target=_acquire_promotion_lock,
        args=(cache_root.as_posix(), writer_child),
    )
    try:
        reader.start()
        assert reader_parent.poll(5), "reader did not acquire the shared lease"
        assert reader_parent.recv() == "reader_acquired"

        writer.start()
        assert not writer_parent.poll(0.5), "promotion bypassed an active parse lease"

        release.set()
        assert writer_parent.poll(5), "promotion did not resume after parse ACK scope"
        assert writer_parent.recv() == "writer_acquired"
    finally:
        release.set()
        for process in (reader, writer):
            if process.pid is None:
                continue
            process.join(timeout=5)
            if process.is_alive():
                process.terminate()
                process.join(timeout=2)
    assert reader.exitcode == 0
    assert writer.exitcode == 0


@pytest.mark.skipif(
    not hasattr(multiprocessing, "get_context"),
    reason="multiprocessing context is unavailable",
)
def test_lock_file_replacement_cannot_split_pack_and_core_writer_authority(tmp_path: Path) -> None:
    pytest.importorskip("fcntl")
    from dpone_airflow_pack.cache_activation_contract import cache_write_lease

    cache_root = tmp_path / "cache"
    context = multiprocessing.get_context("spawn")
    acquired_parent, acquired_child = context.Pipe(duplex=False)
    contender = context.Process(
        target=_acquire_promotion_lock,
        args=(cache_root.as_posix(), acquired_child),
    )
    try:
        with cache_write_lease(cache_root):
            lock_path = cache_root / ".promotion.lock"
            lock_path.unlink()
            lock_path.touch(mode=0o664)
            contender.start()
            assert not acquired_parent.poll(0.5), "replacement lock inode created split-brain writers"

        assert acquired_parent.poll(5), "contender did not resume after root lease release"
        assert acquired_parent.recv() == "writer_acquired"
    finally:
        if contender.pid is not None:
            contender.join(timeout=5)
            if contender.is_alive():
                contender.terminate()


@pytest.mark.skipif(
    not hasattr(multiprocessing, "get_context"),
    reason="multiprocessing context is unavailable",
)
def test_lock_file_replacement_cannot_split_core_and_pack_writer_authority(tmp_path: Path) -> None:
    pytest.importorskip("fcntl")
    from dpone.runtime.deployment_cache_common import promotion_lock

    cache_root = tmp_path / "cache"
    context = multiprocessing.get_context("spawn")
    acquired_parent, acquired_child = context.Pipe(duplex=False)
    contender = context.Process(
        target=_acquire_cache_write_lease,
        args=(cache_root.as_posix(), acquired_child),
    )
    try:
        with promotion_lock(cache_root):
            lock_path = cache_root / ".promotion.lock"
            lock_path.unlink()
            lock_path.touch(mode=0o664)
            contender.start()
            assert not acquired_parent.poll(0.5), "replacement lock inode created split-brain writers"

        assert acquired_parent.poll(5), "contender did not resume after root lease release"
        assert acquired_parent.recv() == "pack_writer_acquired"
    finally:
        if contender.pid is not None:
            contender.join(timeout=5)
            if contender.is_alive():
                contender.terminate()


@pytest.mark.skipif(
    not hasattr(multiprocessing, "get_context"),
    reason="multiprocessing context is unavailable",
)
def test_stage_retention_waits_for_every_cross_process_holder(tmp_path: Path) -> None:
    pytest.importorskip("fcntl")
    import time

    from dpone_airflow_pack.cache_generation_retention import enforce_cache_retention
    from dpone_airflow_pack.cache_generation_store import STAGE_MARKER_NAME, create_generation_stage

    cache = tmp_path / "cache"
    stage = create_generation_stage(cache, generation="multi-process", created_at=time.time() - 7200)
    marker = json.loads((stage / STAGE_MARKER_NAME).read_text(encoding="utf-8"))
    marker.update(hostname="terminated-writer", pid=2**30, heartbeat_at_epoch=time.time() - 7200)
    (stage / STAGE_MARKER_NAME).write_text(json.dumps(marker), encoding="utf-8")
    context = multiprocessing.get_context("spawn")
    releases = [context.Event(), context.Event()]
    pipes = [context.Pipe(duplex=False), context.Pipe(duplex=False)]
    holders = [
        context.Process(target=_hold_stage_lease, args=(stage.as_posix(), child, releases[index]))
        for index, (_parent, child) in enumerate(pipes)
    ]
    try:
        for holder in holders:
            holder.start()
        assert all(parent.poll(5) and parent.recv() == "lease_acquired" for parent, _child in pipes)
        releases[0].set()
        holders[0].join(timeout=5)

        first = enforce_cache_retention(
            cache,
            keep_generations=1,
            max_total_bytes=None,
            high_watermark_pct=80,
            low_watermark_pct=60,
            partial_download_ttl_minutes=30,
        )
        assert stage.exists()
        assert stage.as_posix() not in first.deleted_paths

        releases[1].set()
        holders[1].join(timeout=5)
        second = enforce_cache_retention(
            cache,
            keep_generations=1,
            max_total_bytes=None,
            high_watermark_pct=80,
            low_watermark_pct=60,
            partial_download_ttl_minutes=30,
        )
        assert not stage.exists()
        assert stage.as_posix() in second.deleted_paths
    finally:
        for release in releases:
            release.set()
        for holder in holders:
            if holder.pid is None:
                continue
            holder.join(timeout=5)
            if holder.is_alive():
                holder.terminate()
                holder.join(timeout=2)
    assert all(holder.exitcode == 0 for holder in holders)


@pytest.mark.skipif(
    not hasattr(multiprocessing, "get_context"),
    reason="multiprocessing context is unavailable",
)
def test_exact_status_waits_for_same_deployment_reactivation_commit(
    tmp_path: Path,
) -> None:
    pytest.importorskip("fcntl")
    from dpone.runtime.deployment_cache_common import promotion_lock

    cache_root = tmp_path / "cache"
    deployment_id = "sha256:" + "b" * 64
    activation = cache_root / "activations" / "dev" / deployment_id.replace(":", "-", 1)
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
    target = activation.relative_to(cache_root)
    (cache_root / "current").symlink_to(target, target_is_directory=True)
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    reader = context.Process(
        target=_read_activation_status,
        args=(cache_root.as_posix(), child),
    )
    new_activation_id = "223e4567-e89b-42d3-a456-426614174000"
    try:
        with promotion_lock(cache_root):
            _write_pointer(cache_root, deployment_id, new_activation_id)
            reader.start()
            assert not parent.poll(0.5), "status exposed an activation before current commit"
            temporary = cache_root / ".current.test.tmp"
            temporary.symlink_to(target, target_is_directory=True)
            os.replace(temporary, cache_root / "current")
        assert parent.poll(5), "status did not resume after promotion commit"
        assert parent.recv() == new_activation_id
    finally:
        reader.join(timeout=5)
        if reader.is_alive():
            reader.terminate()
            reader.join(timeout=2)
    assert reader.exitcode == 0


def _write_pointer(cache_root: Path, deployment_id: str, activation_id: str) -> None:
    (cache_root / "current-pointer.json").write_text(
        json.dumps(
            {
                "schema": "dpone.current-pointer.v1",
                "environment": "dev",
                "deployment_id": deployment_id,
                "release_id": "sha256:" + "a" * 64,
                "activation_id": activation_id,
                "promoted_by": "test",
                "promoted_at": "2026-08-01T00:00:00+00:00",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

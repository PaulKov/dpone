from __future__ import annotations

import json
import os
import socket
import stat
import threading
import time
import warnings
from pathlib import Path

import pytest
from dpone_airflow_pack import cache_generation_budget as budget_module
from dpone_airflow_pack import cache_generation_lease as lease_module
from dpone_airflow_pack import cache_generation_retention as retention_module
from dpone_airflow_pack.cache_generation_budget import cache_capacity_reservation
from dpone_airflow_pack.cache_generation_lease import ensure_stage_lock, generation_stage_lease, stage_has_active_lease
from dpone_airflow_pack.cache_generation_retention import enforce_cache_retention
from dpone_airflow_pack.cache_generation_store import (
    STAGE_MARKER_NAME,
    create_generation_stage,
    finalize_generation_stage,
)
from dpone_airflow_pack.cache_permissions import ensure_private_directory, ensure_shared_directory
from dpone_airflow_pack.cache_writer_coordination import cache_evidence_lease


def test_shared_directory_normalizes_owned_group_and_setgid_mode(tmp_path: Path) -> None:
    shared = tmp_path / "shared"
    shared.mkdir()
    shared.chmod(0o755)

    ensure_shared_directory(shared)

    metadata = shared.stat(follow_symlinks=False)
    assert metadata.st_gid in {os.getegid(), *os.getgroups()}
    assert stat.S_IMODE(metadata.st_mode) == 0o2775


def test_capacity_reservation_records_recoverable_owner_ttl_and_shared_modes(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    original_umask = os.umask(0o077)
    try:
        with cache_capacity_reservation(
            cache,
            requested_payload_bytes=1024,
            max_total_bytes=1024 * 1024,
        ):
            reservation_path = next((cache / ".reservations").glob("*.json"))
            reservation = json.loads(reservation_path.read_text(encoding="utf-8"))

            assert reservation["schema"] == "dpone.airflow-pack-cache-reservation.v1"
            assert reservation["reservation_id"] == reservation_path.stem
            assert reservation["hostname"] == socket.gethostname()
            assert reservation["pid"] == os.getpid()
            assert reservation["expires_at_epoch"] > reservation["created_at_epoch"]
            assert stat.S_IMODE(reservation_path.parent.stat().st_mode) == 0o2775
            assert stat.S_IMODE(reservation_path.stat().st_mode) == 0o664
    finally:
        os.umask(original_umask)

    assert not list((cache / ".reservations").glob("*.json"))


def test_expired_unlocked_reservation_is_reclaimed_before_capacity_check(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    reservations = cache / ".reservations"
    reservations.mkdir(parents=True)
    reservation_id = "a" * 32
    stale = reservations / f"{reservation_id}.json"
    stale.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-pack-cache-reservation.v1",
                "reservation_id": reservation_id,
                "reserved_bytes": 1024 * 1024,
                "hostname": "terminated-writer",
                "pid": 2**30,
                "created_at_epoch": time.time() - 7200,
                "expires_at_epoch": time.time() - 3600,
            }
        ),
        encoding="utf-8",
    )

    with cache_capacity_reservation(
        cache,
        requested_payload_bytes=1,
        max_total_bytes=budget_module._CONTROL_RESERVE_BYTES + 1,
    ):
        assert not stale.exists()


def test_active_expired_reservation_is_not_reclaimed(tmp_path: Path) -> None:
    file_lock = pytest.importorskip("fcntl")
    cache = tmp_path / "cache"
    reservations = cache / ".reservations"
    reservations.mkdir(parents=True)
    reservation_id = "b" * 32
    active = reservations / f"{reservation_id}.json"
    active.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-pack-cache-reservation.v1",
                "reservation_id": reservation_id,
                "reserved_bytes": 1024 * 1024,
                "hostname": "slow-writer",
                "pid": 2**30,
                "created_at_epoch": time.time() - 7200,
                "expires_at_epoch": time.time() - 3600,
            }
        ),
        encoding="utf-8",
    )
    descriptor = os.open(active, os.O_RDWR)
    file_lock.flock(descriptor, file_lock.LOCK_SH)
    try:
        with pytest.raises(ValueError, match="airflow_pack_cache_capacity_unavailable"):
            with cache_capacity_reservation(
                cache,
                requested_payload_bytes=1,
                max_total_bytes=budget_module._CONTROL_RESERVE_BYTES + 1,
            ):
                pass
    finally:
        file_lock.flock(descriptor, file_lock.LOCK_UN)
        os.close(descriptor)

    assert active.exists()


def test_malformed_reservation_blocks_capacity_instead_of_being_discarded(tmp_path: Path) -> None:
    reservations = tmp_path / "cache" / ".reservations"
    reservations.mkdir(parents=True)
    (reservations / "uncertain.json").write_text("not-json", encoding="utf-8")

    with pytest.raises(ValueError, match="airflow_pack_cache_capacity_reservation_invalid"):
        with cache_capacity_reservation(
            tmp_path / "cache",
            requested_payload_bytes=1,
            max_total_bytes=1024 * 1024,
        ):
            pass


def test_expired_legacy_reservation_without_ttl_is_reclaimed(tmp_path: Path) -> None:
    reservations = tmp_path / "cache" / ".reservations"
    reservations.mkdir(parents=True)
    reservation = reservations / "legacy.json"
    reservation.write_text(
        json.dumps({"reservation_id": "legacy", "reserved_bytes": 1024 * 1024}),
        encoding="utf-8",
    )
    expired = time.time() - budget_module._RESERVATION_TTL_SECONDS - 1
    os.utime(reservation, (expired, expired))

    with cache_capacity_reservation(
        tmp_path / "cache",
        requested_payload_bytes=1,
        max_total_bytes=budget_module._CONTROL_RESERVE_BYTES + 1,
    ):
        assert not reservation.exists()


def test_fresh_legacy_reservation_without_ttl_remains_capacity_protected(tmp_path: Path) -> None:
    reservations = tmp_path / "cache" / ".reservations"
    reservations.mkdir(parents=True)
    (reservations / "legacy.json").write_text(
        json.dumps({"reservation_id": "legacy", "reserved_bytes": 1024 * 1024}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="airflow_pack_cache_capacity_unavailable"):
        with cache_capacity_reservation(
            tmp_path / "cache",
            requested_payload_bytes=1,
            max_total_bytes=budget_module._CONTROL_RESERVE_BYTES + 1,
        ):
            pass


def test_orphan_stage_bytes_are_included_in_capacity_projection(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    orphan = cache / ".staging" / "orphan"
    orphan.mkdir(parents=True)
    (orphan / "payload.bin").write_bytes(b"x")

    with pytest.raises(ValueError, match="airflow_pack_cache_capacity_unavailable"):
        with cache_capacity_reservation(
            cache,
            requested_payload_bytes=1,
            max_total_bytes=budget_module._CONTROL_RESERVE_BYTES + 1,
        ):
            pass


def test_reservation_cleanup_warning_cannot_mask_body_under_werror(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_release(descriptor: int) -> None:
        os.close(descriptor)
        raise OSError("release failed")

    monkeypatch.setattr(
        budget_module,
        "_release_reservation_lease",
        fail_release,
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(ValueError, match="primary failure"):
            with cache_capacity_reservation(
                tmp_path / "cache",
                requested_payload_bytes=1,
                max_total_bytes=budget_module._CONTROL_RESERVE_BYTES + 1024,
            ):
                raise ValueError("primary failure")


def test_stage_attempt_lock_uses_shared_fsgroup_modes_and_private_mode_stays_private(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    stage = cache / ".staging" / "attempt"
    stage.mkdir(parents=True)
    (stage / STAGE_MARKER_NAME).write_text(json.dumps({"attempt_id": "abc123"}), encoding="utf-8")
    original_umask = os.umask(0o077)
    try:
        lock_path = ensure_stage_lock(stage, STAGE_MARKER_NAME)
        private = tmp_path / "private"
        ensure_private_directory(private)
    finally:
        os.umask(original_umask)

    assert stat.S_IMODE(lock_path.parent.stat().st_mode) == 0o2775
    assert stat.S_IMODE(lock_path.stat().st_mode) == 0o664
    assert stat.S_IMODE(private.stat().st_mode) == 0o700


def test_finalized_orphan_stage_retains_marker_and_is_reclaimable(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    stage = create_generation_stage(cache, generation="orphan", created_at=time.time() - 7200)
    marker_path = stage / STAGE_MARKER_NAME
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    marker["hostname"] = "terminated-writer"
    marker_path.write_text(json.dumps(marker), encoding="utf-8")
    (stage / "pack-index.json").write_text('{"git_sha":"orphan"}', encoding="utf-8")

    finalize_generation_stage(stage, generation="orphan")

    assert marker_path.exists()
    result = enforce_cache_retention(
        cache,
        keep_generations=1,
        max_total_bytes=1024 * 1024,
        high_watermark_pct=80,
        low_watermark_pct=60,
        partial_download_ttl_minutes=30,
    )
    assert not stage.exists()
    assert not result.blockers
    assert not tuple((cache / ".stage-locks").iterdir())


def test_retention_never_detaches_stage_that_acquires_lease_at_delete_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    stage = create_generation_stage(cache, generation="racing", created_at=time.time() - 7200)
    marker_path = stage / STAGE_MARKER_NAME
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    marker.update(hostname="terminated-writer", pid=2**30, heartbeat_at_epoch=time.time() - 7200)
    marker_path.write_text(json.dumps(marker), encoding="utf-8")
    lease_acquired = threading.Event()
    release_lease = threading.Event()
    original_stale_check = retention_module._stage_is_stale
    producer: threading.Thread | None = None

    def hold_producer_lease() -> None:
        with generation_stage_lease(stage, marker_name=STAGE_MARKER_NAME):
            lease_acquired.set()
            release_lease.wait(timeout=5)

    def acquire_after_stale_check(path: Path, *, cutoff: float) -> bool:
        nonlocal producer
        stale = original_stale_check(path, cutoff=cutoff)
        if path == stage and stale and producer is None:
            producer = threading.Thread(target=hold_producer_lease)
            producer.start()
            assert lease_acquired.wait(timeout=5)
        return stale

    monkeypatch.setattr(retention_module, "_stage_is_stale", acquire_after_stale_check)
    try:
        result = enforce_cache_retention(
            cache,
            keep_generations=1,
            max_total_bytes=None,
            high_watermark_pct=80,
            low_watermark_pct=60,
            partial_download_ttl_minutes=30,
        )
    finally:
        release_lease.set()
        if producer is not None:
            producer.join(timeout=5)

    assert stage.exists()
    assert stage.as_posix() not in result.deleted_paths


def test_stage_lock_inode_remains_stable_until_all_shared_holders_exit(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    stage = create_generation_stage(cache, generation="multi-holder", created_at=time.time() - 7200)
    marker_path = stage / STAGE_MARKER_NAME
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    marker.update(hostname="terminated-writer", pid=2**30, heartbeat_at_epoch=time.time() - 7200)
    marker_path.write_text(json.dumps(marker), encoding="utf-8")
    lock_path = ensure_stage_lock(stage, STAGE_MARKER_NAME)
    entered = [threading.Event(), threading.Event()]
    release = [threading.Event(), threading.Event()]

    def hold(index: int) -> None:
        with generation_stage_lease(stage, marker_name=STAGE_MARKER_NAME):
            entered[index].set()
            release[index].wait(timeout=5)

    holders = [threading.Thread(target=hold, args=(index,)) for index in range(2)]
    for holder in holders:
        holder.start()
    assert all(event.wait(timeout=5) for event in entered)

    release[0].set()
    holders[0].join(timeout=5)
    assert lock_path.exists()

    result = enforce_cache_retention(
        cache,
        keep_generations=1,
        max_total_bytes=None,
        high_watermark_pct=80,
        low_watermark_pct=60,
        partial_download_ttl_minutes=30,
    )

    release[1].set()
    holders[1].join(timeout=5)
    assert stage.exists()
    assert stage.as_posix() not in result.deleted_paths

    second_result = enforce_cache_retention(
        cache,
        keep_generations=1,
        max_total_bytes=None,
        high_watermark_pct=80,
        low_watermark_pct=60,
        partial_download_ttl_minutes=30,
    )

    assert not stage.exists()
    assert stage.as_posix() in second_result.deleted_paths
    assert not lock_path.exists()


def test_stage_and_generation_container_modes_are_shared(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    stage = create_generation_stage(cache, generation="shared")

    assert stat.S_IMODE((cache / ".staging").stat().st_mode) == 0o2775
    assert stat.S_IMODE(stage.stat().st_mode) == 0o2770


def test_existing_shared_evidence_lease_does_not_require_owner_chmod(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = tmp_path / "cache"
    cache.mkdir(mode=0o2775)
    cache.chmod(0o2775)
    evidence_path = cache / ".evidence.lock"
    evidence_path.touch(mode=0o664)
    evidence_path.chmod(0o664)
    evidence_inode = evidence_path.stat().st_ino
    real_fchmod = os.fchmod

    def deny_foreign_owner_chmod(descriptor: int, mode: int) -> None:
        if os.fstat(descriptor).st_ino == evidence_inode:
            raise PermissionError("simulated foreign owner")
        real_fchmod(descriptor, mode)

    monkeypatch.setattr(os, "fchmod", deny_foreign_owner_chmod)

    with cache_evidence_lease(cache):
        pass


def test_evidence_lease_rejects_hardlinked_control_inode(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    outside = tmp_path / "outside.lock"
    outside.touch()
    os.link(outside, cache / ".evidence.lock")

    with pytest.raises(ValueError, match="one unlinked regular inode"):
        with cache_evidence_lease(cache):
            pass


@pytest.mark.parametrize("failure", ["malformed_marker", "non_finite_heartbeat", "unreadable_lock"])
def test_retention_preserves_stage_when_attempt_lease_is_uncertain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    cache = tmp_path / "cache"
    stage = create_generation_stage(cache, generation="orphan", created_at=time.time() - 7200)
    marker_path = stage / STAGE_MARKER_NAME
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    lock_path = cache / ".stage-locks" / f"{marker['attempt_id']}.lock"
    if failure == "malformed_marker":
        marker_path.write_text("not-json", encoding="utf-8")
        os.utime(stage, (time.time() - 7200, time.time() - 7200))
    elif failure == "unreadable_lock":
        marker["hostname"] = "terminated-writer"
        marker["pid"] = 2**30
        marker["heartbeat_at_epoch"] = time.time() - 7200
        marker_path.write_text(json.dumps(marker), encoding="utf-8")
        real_open = lease_module.os.open

        def deny_lease_open(
            path: str | bytes | os.PathLike[str] | os.PathLike[bytes], flags: int, mode: int = 0o777
        ) -> int:
            if Path(path) == lock_path:
                raise PermissionError("simulated unreadable lease")
            return real_open(path, flags, mode)

        monkeypatch.setattr(lease_module.os, "open", deny_lease_open)
    else:
        marker["hostname"] = "terminated-writer"
        marker["pid"] = 2**30
        marker["heartbeat_at_epoch"] = float("nan")
        marker_path.write_text(json.dumps(marker), encoding="utf-8")

    if failure != "non_finite_heartbeat":
        assert stage_has_active_lease(stage, marker_name=STAGE_MARKER_NAME) is True
    result = enforce_cache_retention(
        cache,
        keep_generations=1,
        max_total_bytes=None,
        high_watermark_pct=80,
        low_watermark_pct=60,
        partial_download_ttl_minutes=30,
    )

    assert stage.exists()
    assert "airflow_pack_cache_stage_lease_uncertain" in {item["code"] for item in result.blockers}

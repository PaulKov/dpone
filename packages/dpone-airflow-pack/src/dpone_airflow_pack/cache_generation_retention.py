"""Plan-first bounded retention for legacy Airflow pack cache generations."""

from __future__ import annotations

import math
import os
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from dpone_airflow_pack.cache_activation_contract import cache_write_lease
from dpone_airflow_pack.cache_authority import verify_generation_receipt
from dpone_airflow_pack.cache_generation_files import fsync_directory, read_control_json, remove_tree
from dpone_airflow_pack.cache_generation_lease import (
    StageLeaseStatus,
    exclusive_stage_detach_lease,
    stage_lease_status,
)
from dpone_airflow_pack.cache_generation_store import (
    STAGE_MARKER_NAME,
    read_current_commit,
    read_current_generation,
)
from dpone_airflow_pack.cache_permissions import ensure_shared_directory


@dataclass(frozen=True, slots=True)
class CacheRetentionResult:
    total_bytes: int
    deleted_paths: tuple[str, ...]
    warnings: tuple[str, ...]
    blockers: tuple[dict[str, object], ...]


@dataclass(frozen=True, slots=True)
class _RetentionPlan:
    authority_signature: tuple[object, ...]
    paths: tuple[Path, ...]
    partial_download_ttl_minutes: int
    warnings: tuple[str, ...]
    blockers: tuple[dict[str, object], ...]
    max_total_bytes: int | None


@dataclass(frozen=True, slots=True)
class _CacheAuthority:
    signature: tuple[object, ...]
    protected_generations: frozenset[str]
    safe_to_prune_generations: bool
    blockers: tuple[dict[str, object], ...]


@dataclass(frozen=True, slots=True)
class _DetachResult:
    paths: tuple[tuple[Path, Path], ...] | None
    blockers: tuple[dict[str, object], ...] = ()


def enforce_cache_retention(
    cache_root: Path,
    *,
    keep_generations: int,
    max_total_bytes: int | None,
    high_watermark_pct: int,
    low_watermark_pct: int,
    partial_download_ttl_minutes: int,
) -> CacheRetentionResult:
    """Detach candidates under a short lease, delete outside it, then enforce budget."""

    warnings: list[str] = []
    deleted: list[str] = []
    _remove_detached_trash(cache_root, warnings=warnings, deleted=deleted)
    plan = _build_plan(
        cache_root,
        keep_generations=keep_generations,
        max_total_bytes=max_total_bytes,
        high_watermark_pct=high_watermark_pct,
        low_watermark_pct=low_watermark_pct,
        partial_download_ttl_minutes=partial_download_ttl_minutes,
    )
    warnings.extend(plan.warnings)
    detach_result = _detach_plan(cache_root, plan)
    if detach_result.paths is None:
        warnings.append("cache_retention_skipped_current_changed")
    else:
        for original, trash in detach_result.paths:
            try:
                remove_tree(trash)
                deleted.append(original.as_posix())
            except OSError:
                warnings.append("cache_generation_prune_failed")
    total = _dir_size(cache_root)
    blockers: list[dict[str, object]] = [*plan.blockers, *detach_result.blockers]
    if max_total_bytes is not None and max_total_bytes > 0 and total > max_total_bytes:
        blockers.append(
            {
                "code": "airflow_pack_cache_budget_exceeded",
                "message": "cache remains above max_total_bytes after safe retention",
                "current_bytes": total,
                "max_total_bytes": max_total_bytes,
            }
        )
    return CacheRetentionResult(
        total_bytes=total,
        deleted_paths=tuple(deleted),
        warnings=tuple(dict.fromkeys(warnings)),
        blockers=tuple(blockers),
    )


def _build_plan(
    cache_root: Path,
    *,
    keep_generations: int,
    max_total_bytes: int | None,
    high_watermark_pct: int,
    low_watermark_pct: int,
    partial_download_ttl_minutes: int,
) -> _RetentionPlan:
    authority = _read_cache_authority(cache_root, max_total_bytes=max_total_bytes)
    candidates, stage_warnings, stage_blockers = _stale_stages(
        cache_root,
        ttl_minutes=partial_download_ttl_minutes,
    )
    generations = _generation_dirs(cache_root)
    stale_generations = [path for path in generations if path.name not in authority.protected_generations]
    if not authority.safe_to_prune_generations:
        stale_generations = []
    excess_count = max(0, len(generations) - max(1, keep_generations))
    selected = list(candidates) + stale_generations[:excess_count]
    if max_total_bytes is not None and max_total_bytes > 0:
        total = _dir_size(cache_root)
        high = max_total_bytes * high_watermark_pct // 100
        low = max_total_bytes * low_watermark_pct // 100
        if total > high:
            selected_set = set(selected)
            estimated = total - sum(_dir_size(path) for path in selected_set)
            for path in stale_generations:
                if estimated <= low:
                    break
                if path not in selected_set:
                    selected.append(path)
                    selected_set.add(path)
                    estimated -= _dir_size(path)
    return _RetentionPlan(
        authority_signature=authority.signature,
        paths=tuple(dict.fromkeys(selected)),
        partial_download_ttl_minutes=partial_download_ttl_minutes,
        warnings=stage_warnings,
        blockers=(*authority.blockers, *stage_blockers),
        max_total_bytes=max_total_bytes,
    )


def _detach_plan(cache_root: Path, plan: _RetentionPlan) -> _DetachResult:
    if not plan.paths:
        return _DetachResult(paths=())
    detached: list[tuple[Path, Path]] = []
    with cache_write_lease(cache_root):
        authority = _read_cache_authority(
            cache_root,
            max_total_bytes=plan.max_total_bytes,
        )
        if not authority.safe_to_prune_generations:
            return _DetachResult(paths=(), blockers=authority.blockers)
        if authority.signature != plan.authority_signature:
            return _DetachResult(paths=None)
        trash_root = cache_root / ".trash"
        ensure_shared_directory(trash_root)
        for source in plan.paths:
            if not os.path.lexists(source):
                continue
            target = trash_root / f"{source.name}.{uuid4().hex}"
            if source.parent == cache_root / ".staging":
                cutoff = time.time() - max(1, plan.partial_download_ttl_minutes) * 60
                if not _stage_is_stale(source, cutoff=cutoff):
                    continue
                with exclusive_stage_detach_lease(source, marker_name=STAGE_MARKER_NAME) as lease:
                    if not lease.acquired or not _stage_is_stale_under_exclusive_lease(source, cutoff=cutoff):
                        continue
                    _detach_source(source, target)
                    lease.mark_detached()
            else:
                _detach_source(source, target)
            detached.append((source, target))
    return _DetachResult(paths=tuple(detached))


def _remove_detached_trash(cache_root: Path, *, warnings: list[str], deleted: list[str]) -> None:
    trash_root = cache_root / ".trash"
    if not trash_root.exists() or trash_root.is_symlink():
        return
    for path in tuple(trash_root.iterdir()):
        try:
            remove_tree(path)
            deleted.append(path.as_posix())
        except OSError:
            warnings.append("cache_generation_prune_failed")


def _stale_stages(
    cache_root: Path,
    *,
    ttl_minutes: int,
) -> tuple[list[Path], tuple[str, ...], tuple[dict[str, object], ...]]:
    root = cache_root / ".staging"
    if not root.exists() or root.is_symlink():
        return [], (), ()
    cutoff = time.time() - max(1, ttl_minutes) * 60
    stale: list[Path] = []
    active_preserved = False
    blockers: list[dict[str, object]] = []
    for path in root.iterdir():
        is_stale, is_active, blocker = _inspect_stage(path, cutoff=cutoff)
        if is_stale:
            stale.append(path)
        if is_active:
            active_preserved = True
        if blocker is not None:
            blockers.append(blocker)
    warnings = ("cache_active_stage_preserved",) if active_preserved else ()
    return stale, warnings, tuple(blockers)


def _stage_is_stale(path: Path, *, cutoff: float) -> bool:
    stale, _, _ = _inspect_stage(path, cutoff=cutoff)
    return stale


def _stage_is_stale_under_exclusive_lease(path: Path, *, cutoff: float) -> bool:
    """Revalidate a stage without recursively probing the lease held by this process."""

    try:
        if path.is_symlink() or not path.is_dir():
            return path.lstat().st_mtime < cutoff
        marker = read_control_json(path / STAGE_MARKER_NAME)
        heartbeat_value = marker.get("heartbeat_at_epoch")
        if (
            not isinstance(heartbeat_value, (int, float))
            or isinstance(heartbeat_value, bool)
            or not math.isfinite(float(heartbeat_value))
            or float(heartbeat_value) >= cutoff
        ):
            return False
        owner_host = marker.get("hostname")
        owner_pid = marker.get("pid")
        if (
            not isinstance(owner_host, str)
            or not owner_host
            or not isinstance(owner_pid, int)
            or isinstance(owner_pid, bool)
            or owner_pid <= 0
        ):
            return False
        return not (owner_host == socket.gethostname() and _process_exists(owner_pid))
    except (OSError, TypeError, ValueError):
        return False


def _inspect_stage(path: Path, *, cutoff: float) -> tuple[bool, bool, dict[str, object] | None]:
    try:
        if path.is_symlink() or not path.is_dir():
            return path.lstat().st_mtime < cutoff, False, None
        marker = read_control_json(path / STAGE_MARKER_NAME)
        heartbeat_value = marker.get("heartbeat_at_epoch")
        if (
            not isinstance(heartbeat_value, (int, float))
            or isinstance(heartbeat_value, bool)
            or not math.isfinite(float(heartbeat_value))
        ):
            raise ValueError("stage heartbeat must be numeric")
        heartbeat = float(heartbeat_value)
    except (OSError, TypeError, ValueError):
        return False, False, _stage_lease_blocker(path)
    if heartbeat >= cutoff:
        return False, False, None
    lease_status = stage_lease_status(path, marker_name=STAGE_MARKER_NAME)
    if lease_status is StageLeaseStatus.UNCERTAIN:
        return False, False, _stage_lease_blocker(path)
    if lease_status is StageLeaseStatus.ACTIVE:
        return False, True, None
    owner_host = marker.get("hostname")
    owner_pid = marker.get("pid")
    if (
        not isinstance(owner_host, str)
        or not owner_host
        or not isinstance(owner_pid, int)
        or isinstance(owner_pid, bool)
        or owner_pid <= 0
    ):
        return False, False, _stage_lease_blocker(path)
    if owner_host == socket.gethostname() and isinstance(owner_pid, int) and _process_exists(owner_pid):
        return False, False, None
    return True, False, None


def _stage_lease_blocker(path: Path) -> dict[str, object]:
    return {
        "code": "airflow_pack_cache_stage_lease_uncertain",
        "message": "stage ownership or attempt lease could not be validated; stage was preserved",
        "path": path.as_posix(),
    }


def _read_cache_authority(
    cache_root: Path,
    *,
    max_total_bytes: int | None = None,
) -> _CacheAuthority:
    blockers: list[dict[str, object]] = []
    receipt = None
    try:
        receipt = read_current_commit(cache_root)
    except (OSError, ValueError) as exc:
        blockers.append(_authority_blocker(str(exc)))
    try:
        pointer = read_current_generation(cache_root)
    except (OSError, ValueError) as exc:
        pointer = None
        blockers.append(_authority_blocker(str(exc)))
    symlink = _current_symlink_generation(cache_root)
    protected = frozenset(value for value in (getattr(receipt, "generation", None), pointer, symlink) if value)
    if receipt is not None and receipt.durable:
        try:
            actual = verify_generation_receipt(
                cache_root / "generations" / receipt.generation,
                generation=receipt.generation,
                max_total_bytes=max_total_bytes,
            )
            if actual.index_sha256 != receipt.index_sha256:
                raise ValueError("durable receipt differs from active generation index")
        except (OSError, ValueError) as exc:
            blockers.append(_authority_blocker(str(exc)))
    if receipt is not None and receipt.durable:
        if not (receipt.generation == pointer == symlink):
            blockers.append(_authority_blocker("current pointers differ from the durable commit receipt"))
    elif pointer != symlink or (receipt is not None and receipt.generation not in {pointer, symlink}):
        blockers.append(_authority_blocker("legacy current authorities disagree without a durable commit receipt"))
    safe = not blockers
    return _CacheAuthority(
        signature=(getattr(receipt, "commit_id", None), pointer, symlink, tuple(sorted(protected))),
        protected_generations=protected,
        safe_to_prune_generations=safe,
        blockers=tuple(blockers),
    )


def _current_symlink_generation(cache_root: Path) -> str | None:
    path = cache_root / "current_generation"
    if not os.path.lexists(path):
        return None
    if not path.is_symlink():
        return "<invalid>"
    target = os.readlink(path)
    parts = Path(target).parts
    return parts[1] if len(parts) == 2 and parts[0] == "generations" else "<invalid>"


def _authority_blocker(message: str) -> dict[str, object]:
    return {
        "code": "airflow_pack_cache_authority_mismatch",
        "message": message,
    }


def _detach_source(source: Path, target: Path) -> None:
    source.rename(target)
    fsync_directory(source.parent)
    fsync_directory(target.parent)


def _process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _generation_dirs(cache_root: Path) -> list[Path]:
    root = cache_root / "generations"
    if not root.exists() or root.is_symlink():
        return []
    return sorted(
        (path for path in root.iterdir() if path.is_dir() and not path.is_symlink()),
        key=lambda item: (item.stat().st_mtime_ns, item.name),
    )


def _dir_size(root: Path) -> int:
    if not root.exists() or root.is_symlink():
        return 0
    total = 0
    for directory, directory_names, file_names in os.walk(root, followlinks=False, onerror=_raise_walk_error):
        parent = Path(directory)
        directory_names[:] = [name for name in directory_names if not (parent / name).is_symlink()]
        for name in file_names:
            path = parent / name
            if not path.is_symlink():
                total += path.stat().st_size
    return total


def _raise_walk_error(error: OSError) -> None:
    raise error


__all__ = ["CacheRetentionResult", "enforce_cache_retention"]

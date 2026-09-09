"""Crash-durable immutable generations for the legacy Airflow pack cache."""

from __future__ import annotations

import os
import socket
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from uuid import uuid4

from dpone_airflow_pack.cache_activation_contract import cache_write_lease
from dpone_airflow_pack.cache_authority import (
    CURRENT_COMMIT_PATH,
    GENERATION_RECEIPT_NAME,
    PENDING_COMMIT_PATH,
    LegacyCacheCommitReceipt,
    VerifiedGenerationInventory,
    commit_receipt_from_payload,
    generation_receipt_path,
    generation_receipt_payload,
    inspect_generation_inventory,
    read_current_commit,
    read_current_generation,
    read_legacy_cache_authority,
    verify_generation_receipt,
    verify_unmanaged_generation,
)
from dpone_airflow_pack.cache_generation_files import (
    fsync_directory,
    read_control_json,
    remove_tree,
    seal_tree,
    tree_metadata_digest,
    unlink_durable,
    write_json_durable,
    write_text_durable,
)
from dpone_airflow_pack.cache_generation_lease import ensure_stage_lock
from dpone_airflow_pack.cache_permissions import (
    SHARED_CONTROL_MODE,
    SHARED_WORK_CONTROL_MODE,
    ensure_shared_directory,
    ensure_shared_work_directory,
)

STAGE_MARKER_NAME: Final = ".dpone-stage-owner.json"
_STAGE_MARKER_SCHEMA = "dpone.airflow-pack-cache-stage.v1"


@dataclass(frozen=True, slots=True)
class GenerationCandidate:
    generation: str
    path: Path
    inventory_sha256: str
    index_sha256: str
    size_bytes: int
    file_count: int
    payload_size_bytes: int
    payload_file_count: int
    directory_identity: tuple[int, int, int, int]
    metadata_sha256: str


@dataclass(frozen=True, slots=True)
class ExistingGenerationSnapshot:
    path: Path
    directory_identity: tuple[int, int, int, int]
    inventory_sha256: str
    has_receipt: bool
    metadata_sha256: str
    inventory: VerifiedGenerationInventory


def create_generation_stage(cache_root: Path, *, generation: str, created_at: float | None = None) -> Path:
    """Create one private, owner-marked staging tree under the bounded cache root."""

    with cache_write_lease(cache_root):
        staging_root = cache_root / ".staging"
        ensure_shared_directory(staging_root)
        attempt_id = uuid4().hex
        stage = staging_root / attempt_id
        ensure_shared_work_directory(stage)
        now = time.time() if created_at is None else created_at
        write_json_durable(
            stage / STAGE_MARKER_NAME,
            {
                "schema": _STAGE_MARKER_SCHEMA,
                "attempt_id": attempt_id,
                "generation": generation,
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "created_at_epoch": now,
                "heartbeat_at_epoch": now,
            },
            mode=SHARED_WORK_CONTROL_MODE,
        )
        ensure_stage_lock(stage, STAGE_MARKER_NAME)
    return stage


def heartbeat_generation_stage(stage: Path) -> None:
    """Refresh ownership evidence so retention never removes an active download."""

    with cache_write_lease(stage.parent.parent):
        marker_path = stage / STAGE_MARKER_NAME
        marker = read_control_json(marker_path)
        marker["heartbeat_at_epoch"] = time.time()
        write_json_durable(marker_path, marker, mode=SHARED_WORK_CONTROL_MODE)


def finalize_generation_stage(stage: Path, *, generation: str) -> GenerationCandidate:
    """Write deterministic inventory evidence and seal a generation for cross-UID reads."""

    inventory = inspect_generation_inventory(stage, generation=generation)
    write_json_durable(
        stage / GENERATION_RECEIPT_NAME,
        generation_receipt_payload(inventory),
        mode=SHARED_WORK_CONTROL_MODE,
    )
    seal_tree(stage, seal_root=False, group_managed=True)
    metadata = stage.lstat()
    return GenerationCandidate(
        generation=generation,
        path=stage,
        inventory_sha256=inventory.inventory_sha256,
        index_sha256=inventory.index_sha256,
        size_bytes=inventory.size_bytes + (stage / GENERATION_RECEIPT_NAME).stat().st_size,
        file_count=inventory.file_count + 1,
        payload_size_bytes=inventory.size_bytes,
        payload_file_count=inventory.file_count,
        directory_identity=_directory_identity(metadata),
        metadata_sha256=tree_metadata_digest(stage),
    )


def inspect_existing_generation(
    path: Path,
    *,
    max_generation_bytes: int | None = None,
) -> ExistingGenerationSnapshot | None:
    """Hash a historical generation outside the scheduler-blocking writer lease."""

    if not os.path.lexists(path):
        return None
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode):
        raise ValueError("airflow_pack_generation_conflict: generation must be a regular directory")
    receipt_path = generation_receipt_path(path, generation=path.name)
    if os.path.lexists(receipt_path):
        try:
            inventory = verify_generation_receipt(
                path,
                generation=path.name,
                max_total_bytes=max_generation_bytes,
            )
        except ValueError as exc:
            raise ValueError("airflow_pack_generation_conflict: existing generation receipt is invalid") from exc
        has_receipt = True
    else:
        inventory = inspect_generation_inventory(
            path,
            generation=path.name,
            max_total_bytes=max_generation_bytes,
        )
        has_receipt = False
    return ExistingGenerationSnapshot(
        path=path,
        directory_identity=_directory_identity(metadata),
        inventory_sha256=inventory.inventory_sha256,
        has_receipt=has_receipt,
        metadata_sha256=tree_metadata_digest(path),
        inventory=inventory,
    )


def recover_interrupted_commit(
    cache_root: Path,
    *,
    max_generation_bytes: int | None = None,
) -> LegacyCacheCommitReceipt | None:
    """Complete a previously prepared commit before accepting new remote bytes."""

    pending_path = cache_root / PENDING_COMMIT_PATH
    if not os.path.lexists(pending_path):
        return _adopt_or_read_current(cache_root, max_generation_bytes=max_generation_bytes)
    receipt = commit_receipt_from_payload(read_control_json(pending_path))
    generation_dir = cache_root / "generations" / receipt.generation
    actual = verify_generation_receipt(
        generation_dir,
        generation=receipt.generation,
        max_total_bytes=max_generation_bytes,
    )
    if actual.index_sha256 != receipt.index_sha256:
        raise ValueError("airflow_pack_cache_commit_invalid: pending receipt differs from generation index")
    write_text_durable(cache_root / "current", receipt.generation)
    _replace_generation_symlink(cache_root, receipt.generation)
    write_json_durable(cache_root / CURRENT_COMMIT_PATH, receipt.to_dict(), mode=SHARED_CONTROL_MODE)
    _clear_pending_commit(pending_path, receipt)
    return receipt


def commit_generation(
    cache_root: Path,
    *,
    candidate: GenerationCandidate,
    expected_commit_id: str | None,
    existing: ExistingGenerationSnapshot | None,
    committed_at: str,
) -> tuple[LegacyCacheCommitReceipt, bool]:
    """Commit one candidate while the caller holds the exclusive cache lease."""

    _verify_candidate(candidate)
    current = read_current_commit(cache_root)
    current_id = current.commit_id if current is not None else None
    if current_id != expected_commit_id and (current is None or current.generation != candidate.generation):
        return _require_durable_current(current), False
    generation_dir = cache_root / "generations" / candidate.generation
    ensure_shared_directory(generation_dir.parent)
    if os.path.lexists(generation_dir):
        _adopt_or_compare_existing(generation_dir, candidate=candidate, snapshot=existing)
        require_directory_identity = False
    else:
        candidate.path.rename(generation_dir)
        fsync_directory(generation_dir.parent)
        require_directory_identity = True
    _verify_generation_matches_candidate(
        generation_dir,
        candidate=candidate,
        require_directory_identity=require_directory_identity,
    )
    current = read_current_commit(cache_root)
    if current is not None and current.generation == candidate.generation and current.durable:
        if current.index_sha256 != candidate.index_sha256:
            raise ValueError("airflow_pack_cache_commit_invalid: current receipt differs from generation index")
        write_text_durable(cache_root / "current", candidate.generation)
        _replace_generation_symlink(cache_root, candidate.generation)
        _verify_generation_matches_candidate(
            generation_dir,
            candidate=candidate,
            require_directory_identity=require_directory_identity,
        )
        return current, False
    sequence = (current.sequence if current is not None else 0) + 1
    receipt = LegacyCacheCommitReceipt(
        commit_id=str(uuid4()),
        sequence=sequence,
        generation=candidate.generation,
        index_sha256=candidate.index_sha256,
        committed_at=committed_at,
    )
    write_json_durable(cache_root / PENDING_COMMIT_PATH, receipt.to_dict(), mode=SHARED_CONTROL_MODE)
    write_text_durable(cache_root / "current", candidate.generation)
    _replace_generation_symlink(cache_root, candidate.generation)
    write_json_durable(cache_root / CURRENT_COMMIT_PATH, receipt.to_dict(), mode=SHARED_CONTROL_MODE)
    _clear_pending_commit(cache_root / PENDING_COMMIT_PATH, receipt)
    _verify_generation_matches_candidate(
        generation_dir,
        candidate=candidate,
        require_directory_identity=require_directory_identity,
    )
    return receipt, True


def _verify_candidate(candidate: GenerationCandidate) -> None:
    """Rehash the staged generation before it can become cache authority."""

    if (
        _directory_identity(candidate.path.lstat()) != candidate.directory_identity
        or tree_metadata_digest(candidate.path) != candidate.metadata_sha256
    ):
        raise ValueError("airflow_pack_generation_conflict: staged generation changed before commit")
    actual = verify_generation_receipt(
        candidate.path,
        generation=candidate.generation,
        max_total_bytes=candidate.payload_size_bytes,
    )
    if actual != _candidate_inventory(candidate):
        raise ValueError("airflow_pack_generation_conflict: staged generation bytes changed before commit")


def _verify_generation_matches_candidate(
    path: Path,
    *,
    candidate: GenerationCandidate,
    require_directory_identity: bool,
) -> None:
    """Revalidate the published path before and after authority publication."""

    if require_directory_identity and _inode_identity(path.lstat()) != candidate.directory_identity[:2]:
        raise ValueError("airflow_pack_generation_conflict: published generation path was replaced")
    actual = verify_generation_receipt(
        path,
        generation=candidate.generation,
        max_total_bytes=candidate.payload_size_bytes,
    )
    if actual != _candidate_inventory(candidate):
        raise ValueError("airflow_pack_generation_conflict: published generation bytes changed before commit")


def _adopt_or_compare_existing(
    path: Path,
    *,
    candidate: GenerationCandidate,
    snapshot: ExistingGenerationSnapshot | None,
) -> None:
    receipt_path = generation_receipt_path(path, generation=candidate.generation)
    if os.path.lexists(receipt_path):
        if snapshot is None or not snapshot.has_receipt or snapshot.path != path:
            raise ValueError("airflow_pack_generation_conflict: existing generation bytes differ")
        if _directory_identity(path.lstat()) != snapshot.directory_identity:
            raise ValueError("airflow_pack_generation_conflict: existing generation bytes differ")
        if tree_metadata_digest(path) != snapshot.metadata_sha256:
            raise ValueError("airflow_pack_generation_conflict: existing generation bytes differ")
        actual = verify_generation_receipt(
            path,
            generation=candidate.generation,
            max_total_bytes=candidate.payload_size_bytes,
        )
        if snapshot.inventory_sha256 != candidate.inventory_sha256 or actual != _candidate_inventory(candidate):
            raise ValueError("airflow_pack_generation_conflict: existing generation bytes differ")
        return
    if snapshot is None or snapshot.has_receipt or snapshot.path != path:
        raise ValueError("airflow_pack_generation_conflict: historical generation was not preverified")
    if _directory_identity(path.lstat()) != snapshot.directory_identity:
        raise ValueError("airflow_pack_generation_conflict: historical generation changed before commit")
    if tree_metadata_digest(path) != snapshot.metadata_sha256:
        raise ValueError("airflow_pack_generation_conflict: existing generation bytes differ")
    actual = verify_unmanaged_generation(
        path,
        generation=candidate.generation,
        max_total_bytes=candidate.payload_size_bytes,
    )
    if snapshot.inventory.inventory_sha256 != candidate.inventory_sha256 or actual != _candidate_inventory(candidate):
        raise ValueError("airflow_pack_generation_conflict: existing generation bytes differ")
    _write_adopted_generation_receipt(path, inventory=actual)


def _adopt_or_read_current(
    cache_root: Path,
    *,
    max_generation_bytes: int | None,
) -> LegacyCacheCommitReceipt | None:
    authority = read_legacy_cache_authority(cache_root)
    if authority.receipt is not None and authority.receipt.durable:
        return authority.require_consistent_durable()
    pointers = {value for value in (authority.current_generation, authority.symlink_generation) if value}
    if len(pointers) > 1:
        raise ValueError("airflow_pack_cache_authority_mismatch: unmanaged cache pointers disagree")
    if not pointers:
        return None
    generation = pointers.pop()
    generation_dir = cache_root / "generations" / generation
    inventory = verify_unmanaged_generation(
        generation_dir,
        generation=generation,
        max_total_bytes=max_generation_bytes,
    )
    _write_adopted_generation_receipt(generation_dir, inventory=inventory)
    receipt = LegacyCacheCommitReceipt(
        commit_id=str(uuid4()),
        sequence=1,
        generation=generation,
        index_sha256=inventory.index_sha256,
        committed_at=None,
    )
    write_json_durable(cache_root / PENDING_COMMIT_PATH, receipt.to_dict(), mode=SHARED_CONTROL_MODE)
    write_text_durable(cache_root / "current", generation)
    _replace_generation_symlink(cache_root, generation)
    write_json_durable(cache_root / CURRENT_COMMIT_PATH, receipt.to_dict(), mode=SHARED_CONTROL_MODE)
    _clear_pending_commit(cache_root / PENDING_COMMIT_PATH, receipt)
    return receipt


def _write_adopted_generation_receipt(path: Path, *, inventory: VerifiedGenerationInventory) -> None:
    receipt_path = generation_receipt_path(path, generation=inventory.generation)
    ensure_shared_directory(receipt_path.parent.parent)
    write_json_durable(
        receipt_path,
        generation_receipt_payload(inventory),
        mode=SHARED_CONTROL_MODE,
    )


def _candidate_inventory(candidate: GenerationCandidate) -> VerifiedGenerationInventory:
    return VerifiedGenerationInventory(
        generation=candidate.generation,
        inventory_sha256=candidate.inventory_sha256,
        index_sha256=candidate.index_sha256,
        size_bytes=candidate.payload_size_bytes,
        file_count=candidate.payload_file_count,
    )


def _replace_generation_symlink(cache_root: Path, generation: str) -> None:
    target = cache_root / "current_generation"
    temporary = cache_root / f".current_generation.{uuid4().hex}.tmp"
    temporary.symlink_to(Path("generations") / generation, target_is_directory=True)
    try:
        os.replace(temporary, target)
        fsync_directory(cache_root)
    finally:
        if os.path.lexists(temporary):
            temporary.unlink()


def _directory_identity(metadata: os.stat_result) -> tuple[int, int, int, int]:
    return metadata.st_dev, metadata.st_ino, metadata.st_mtime_ns, metadata.st_ctime_ns


def _inode_identity(metadata: os.stat_result) -> tuple[int, int]:
    return metadata.st_dev, metadata.st_ino


def _clear_pending_commit(path: Path, receipt: LegacyCacheCommitReceipt) -> None:
    try:
        unlink_durable(path)
    except OSError:
        if not path.exists():
            try:
                write_json_durable(path, receipt.to_dict(), mode=SHARED_CONTROL_MODE)
            except OSError:
                pass
        raise


def _require_durable_current(receipt: LegacyCacheCommitReceipt | None) -> LegacyCacheCommitReceipt:
    if receipt is None or not receipt.durable:
        raise ValueError("airflow_pack_cache_commit_missing: concurrent cache state has no durable receipt")
    return receipt


__all__ = [
    "CURRENT_COMMIT_PATH",
    "PENDING_COMMIT_PATH",
    "GENERATION_RECEIPT_NAME",
    "STAGE_MARKER_NAME",
    "ExistingGenerationSnapshot",
    "GenerationCandidate",
    "LegacyCacheCommitReceipt",
    "commit_generation",
    "create_generation_stage",
    "finalize_generation_stage",
    "heartbeat_generation_stage",
    "inspect_existing_generation",
    "read_current_commit",
    "read_current_generation",
    "recover_interrupted_commit",
    "remove_tree",
]

"""Authoritative legacy-cache receipts, pointers, and byte verification."""

from __future__ import annotations

import json
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from dpone_airflow_pack.cache_generation_files import (
    file_sha256,
    inventory_digest,
    read_control_json,
    tree_inventory,
)
from dpone_airflow_pack.pack_index import (
    dag_spec_index_entries,
    index_entries,
    index_generation,
    safe_pack_relative_path,
)
from dpone_airflow_pack.pack_index_security import (
    normalize_index_checksum,
    require_entry_sha256,
    validate_index_generation,
)

CURRENT_COMMIT_PATH: Final = "status/current-commit.json"
PENDING_COMMIT_PATH: Final = "status/pending-commit.json"
GENERATION_RECEIPT_NAME: Final = ".dpone-generation-receipt.json"
GENERATION_RECEIPTS_PATH: Final = "status/generation-receipts"
_STAGE_MARKER_NAME = ".dpone-stage-owner.json"
_CURRENT_COMMIT_SCHEMA = "dpone.airflow-pack-cache-commit.v1"
_GENERATION_RECEIPT_SCHEMA = "dpone.airflow-pack-generation-receipt.v1"
_IGNORED_INVENTORY_NAMES = frozenset({GENERATION_RECEIPT_NAME, _STAGE_MARKER_NAME})
_MAX_CURRENT_POINTER_BYTES = 4096
_MAX_GENERATION_INDEX_BYTES = 25 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class LegacyCacheCommitReceipt:
    commit_id: str
    sequence: int
    generation: str
    index_sha256: str | None
    committed_at: str | None
    durable: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": _CURRENT_COMMIT_SCHEMA,
            "commit_id": self.commit_id,
            "sequence": self.sequence,
            "generation": self.generation,
            "index_sha256": self.index_sha256,
            "committed_at": self.committed_at,
        }


@dataclass(frozen=True, slots=True)
class LegacyCacheAuthority:
    """One receipt plus both compatibility projections read under one lease."""

    receipt: LegacyCacheCommitReceipt | None
    current_generation: str | None
    symlink_generation: str | None

    @property
    def is_consistent_durable(self) -> bool:
        return (
            self.receipt is not None
            and self.receipt.durable
            and self.receipt.generation == self.current_generation == self.symlink_generation
        )

    def require_consistent_durable(self) -> LegacyCacheCommitReceipt:
        if self.receipt is None or not self.receipt.durable:
            raise ValueError("airflow_pack_cache_commit_missing: durable current commit receipt is missing")
        if not self.is_consistent_durable:
            raise ValueError("airflow_pack_cache_authority_mismatch: receipt, current, and current_generation disagree")
        return self.receipt


@dataclass(frozen=True, slots=True)
class VerifiedGenerationInventory:
    generation: str
    inventory_sha256: str
    index_sha256: str
    size_bytes: int
    file_count: int


def read_legacy_cache_authority(cache_root: Path) -> LegacyCacheAuthority:
    """Read receipt, text pointer, and compatibility symlink as one authority."""

    return LegacyCacheAuthority(
        receipt=read_current_commit(cache_root),
        current_generation=read_current_generation(cache_root),
        symlink_generation=read_current_symlink_generation(cache_root),
    )


def read_current_commit(cache_root: Path) -> LegacyCacheCommitReceipt | None:
    """Read the durable commit point, including one historical synthetic receipt."""

    if os.path.lexists(cache_root / PENDING_COMMIT_PATH):
        raise ValueError("airflow_pack_cache_commit_uncertain: pending commit requires recovery")
    return read_current_commit_unchecked(cache_root)


def read_current_commit_unchecked(cache_root: Path) -> LegacyCacheCommitReceipt | None:
    path = cache_root / CURRENT_COMMIT_PATH
    if os.path.lexists(path):
        try:
            payload = read_control_json(path)
        except (OSError, TypeError, ValueError) as exc:
            raise ValueError("airflow_pack_cache_commit_invalid: current receipt is unreadable") from exc
        return commit_receipt_from_payload(payload)
    generation = read_current_generation(cache_root)
    if generation is None:
        return None
    return LegacyCacheCommitReceipt(
        commit_id=f"legacy:{generation}",
        sequence=0,
        generation=generation,
        index_sha256=None,
        committed_at=None,
        durable=False,
    )


def read_current_generation(cache_root: Path) -> str | None:
    path = cache_root / "current"
    if not os.path.lexists(path):
        return None
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ValueError("airflow_pack_cache_current_invalid: current could not be opened safely") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > _MAX_CURRENT_POINTER_BYTES:
            raise ValueError("airflow_pack_cache_current_invalid: current must be a small regular file")
        with os.fdopen(os.dup(descriptor), "rb") as handle:
            raw = handle.read(_MAX_CURRENT_POINTER_BYTES + 1)
    finally:
        os.close(descriptor)
    if len(raw) > _MAX_CURRENT_POINTER_BYTES:
        raise ValueError("airflow_pack_cache_current_invalid: current must be a small regular file")
    try:
        value = raw.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise ValueError("airflow_pack_cache_current_invalid: current must be UTF-8") from exc
    if value:
        validate_index_generation(value)
    return value or None


def read_current_symlink_generation(cache_root: Path) -> str | None:
    path = cache_root / "current_generation"
    if not os.path.lexists(path):
        return None
    if not path.is_symlink():
        raise ValueError("airflow_pack_cache_authority_mismatch: current_generation must be a relative symlink")
    target_text = os.readlink(path)
    target = Path(target_text)
    if target.is_absolute() or len(target.parts) != 2 or target.parts[0] != "generations":
        raise ValueError(
            "airflow_pack_cache_authority_mismatch: current_generation must target generations/<generation>"
        )
    generation = target.parts[1]
    validate_index_generation(generation)
    if target_text != (Path("generations") / generation).as_posix():
        raise ValueError("airflow_pack_cache_authority_mismatch: current_generation target is not canonical")
    return generation


def inspect_generation_inventory(
    path: Path,
    *,
    generation: str,
    max_total_bytes: int | None = None,
) -> VerifiedGenerationInventory:
    """Hash the actual index and complete generation inventory."""

    validate_index_generation(generation)
    try:
        entries, size_bytes = tree_inventory(
            path,
            ignored_names=_IGNORED_INVENTORY_NAMES,
            max_total_bytes=max_total_bytes,
        )
        index_sha256 = file_sha256(path / "pack-index.json")
    except (OSError, ValueError) as exc:
        raise ValueError("airflow_pack_generation_invalid: actual generation inventory is unreadable") from exc
    return VerifiedGenerationInventory(
        generation=generation,
        inventory_sha256=inventory_digest(entries),
        index_sha256=index_sha256,
        size_bytes=size_bytes,
        file_count=len(entries),
    )


def verify_generation_receipt(
    path: Path,
    *,
    generation: str,
    max_total_bytes: int | None = None,
) -> VerifiedGenerationInventory:
    """Rehash actual bytes and compare every recorded generation invariant."""

    try:
        receipt = read_control_json(generation_receipt_path(path, generation=generation))
    except (OSError, ValueError) as exc:
        raise ValueError("airflow_pack_generation_receipt_invalid: receipt is unreadable") from exc
    receipt_size = receipt.get("size_bytes")
    if not isinstance(receipt_size, int) or isinstance(receipt_size, bool) or receipt_size < 0:
        raise ValueError("airflow_pack_generation_receipt_invalid: size_bytes")
    if max_total_bytes is not None and receipt_size > max_total_bytes:
        raise ValueError("airflow_pack_generation_too_large: receipt exceeds configured cache budget")
    try:
        actual = inspect_generation_inventory(path, generation=generation, max_total_bytes=receipt_size)
    except ValueError as exc:
        raise ValueError("airflow_pack_generation_receipt_invalid: actual generation exceeds receipt") from exc
    expected = generation_receipt_payload(actual)
    if any(receipt.get(key) != value for key, value in expected.items()):
        raise ValueError("airflow_pack_generation_receipt_invalid: receipt differs from actual generation bytes")
    return actual


def generation_receipt_path(path: Path, *, generation: str) -> Path:
    """Resolve inline receipts first and the shared adoption sidecar second."""

    validate_index_generation(generation)
    inline = path / GENERATION_RECEIPT_NAME
    if os.path.lexists(inline):
        return inline
    if path.name != generation or path.parent.name != "generations":
        raise ValueError("airflow_pack_generation_receipt_invalid: generation path is not canonical")
    return path.parent.parent / GENERATION_RECEIPTS_PATH / f"{generation}.json"


def verify_unmanaged_generation(
    path: Path,
    *,
    generation: str,
    max_total_bytes: int | None = None,
) -> VerifiedGenerationInventory:
    """Validate every indexed unmanaged artifact before creating authority evidence."""

    actual = inspect_generation_inventory(path, generation=generation, max_total_bytes=max_total_bytes)
    try:
        index_raw = _read_bounded_text(path / "pack-index.json", max_bytes=_MAX_GENERATION_INDEX_BYTES)
        index = json.loads(index_raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("airflow_pack_generation_invalid: pack index is unreadable") from exc
    if not isinstance(index, Mapping) or index_generation(index) != generation:
        raise ValueError("airflow_pack_generation_invalid: pack index generation differs from cache generation")
    for entry in (*index_entries(index), *dag_spec_index_entries(index)):
        relative_path = safe_pack_relative_path(entry)
        if relative_path is None:
            raise ValueError("airflow_pack_generation_invalid: pack index contains an unsafe path")
        artifact = path / relative_path
        try:
            metadata = artifact.lstat()
        except OSError as exc:
            raise ValueError("airflow_pack_generation_invalid: indexed artifact is missing") from exc
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("airflow_pack_generation_invalid: indexed artifact is not a regular file")
        if entry.bytes is not None and metadata.st_size != entry.bytes:
            raise ValueError("airflow_pack_generation_invalid: indexed artifact size differs")
        expected_sha256 = normalize_index_checksum(require_entry_sha256(entry, artifact_kind="artifact"))
        if file_sha256(artifact).removeprefix("sha256:") != expected_sha256:
            raise ValueError("airflow_pack_generation_invalid: indexed artifact checksum differs")
    return actual


def generation_receipt_payload(inventory: VerifiedGenerationInventory) -> dict[str, object]:
    return {
        "schema": _GENERATION_RECEIPT_SCHEMA,
        "generation": inventory.generation,
        "inventory_sha256": inventory.inventory_sha256,
        "index_sha256": inventory.index_sha256,
        "size_bytes": inventory.size_bytes,
        "file_count": inventory.file_count,
    }


def commit_receipt_from_payload(payload: dict[str, Any]) -> LegacyCacheCommitReceipt:
    if payload.get("schema") != _CURRENT_COMMIT_SCHEMA:
        raise ValueError("airflow_pack_cache_commit_invalid: unsupported schema")
    return LegacyCacheCommitReceipt(
        commit_id=_required_text(payload, "commit_id"),
        sequence=_required_non_negative_int(payload, "sequence"),
        generation=_required_generation(payload, "generation"),
        index_sha256=_optional_text(payload.get("index_sha256")),
        committed_at=_optional_text(payload.get("committed_at")),
    )


def _required_text(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"airflow_pack_cache_commit_invalid: {key}")
    return value


def _required_generation(payload: dict[str, Any], key: str) -> str:
    value = _required_text(payload, key)
    validate_index_generation(value)
    return value


def _optional_text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _required_non_negative_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"airflow_pack_cache_commit_invalid: {key}")
    return value


def _read_bounded_text(path: Path, *, max_bytes: int) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > max_bytes:
            raise ValueError("file is not a bounded regular file")
        with os.fdopen(os.dup(descriptor), "rb") as handle:
            raw = handle.read(max_bytes + 1)
    finally:
        os.close(descriptor)
    if len(raw) > max_bytes:
        raise ValueError("file exceeds configured bound")
    return raw.decode("utf-8")


__all__ = [
    "CURRENT_COMMIT_PATH",
    "GENERATION_RECEIPT_NAME",
    "GENERATION_RECEIPTS_PATH",
    "PENDING_COMMIT_PATH",
    "LegacyCacheAuthority",
    "LegacyCacheCommitReceipt",
    "VerifiedGenerationInventory",
    "commit_receipt_from_payload",
    "generation_receipt_payload",
    "generation_receipt_path",
    "inspect_generation_inventory",
    "read_current_commit",
    "read_current_commit_unchecked",
    "read_current_generation",
    "read_current_symlink_generation",
    "read_legacy_cache_authority",
    "verify_generation_receipt",
    "verify_unmanaged_generation",
]

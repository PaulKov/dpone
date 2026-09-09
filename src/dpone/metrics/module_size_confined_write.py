"""Descriptor-confined compare-and-swap writes for module-size baselines."""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from dpone.manifest.confined_files import ConfinedFileError, read_confined_leaf
from dpone.manifest.confined_mutations import ConfinedMutationError, replace_file_if_digest


class ModuleSizeConfinedWriteError(ValueError):
    """Fail-closed descriptor writer error mapped by the public baseline codec."""


class ModuleSizeRootIdentity(Protocol):
    """Minimal immutable root identity required by the confined writer."""

    @property
    def path(self) -> Path: ...

    def matches(self, metadata: os.stat_result) -> bool: ...


_MAX_BASELINE_BYTES = 1 << 20
_DIRECTORY_FLAGS = (
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)
_WRITE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
_DIR_FD_SUPPORTED = (
    hasattr(os, "O_DIRECTORY")
    and hasattr(os, "O_NOFOLLOW")
    and all(function in os.supports_dir_fd for function in (os.open, os.stat, os.rename, os.unlink))
)


def write_confined_baseline_bytes(
    *,
    anchor: Path,
    parts: tuple[str, ...],
    content: bytes,
    expected_bytes: bytes | None,
    root_identity: ModuleSizeRootIdentity | None = None,
) -> None:
    """Persist bytes through a stable parent descriptor and exact-digest CAS."""

    normalized = _validated_parts(parts)
    _require_descriptor_relative_support()
    if len(content) > _MAX_BASELINE_BYTES or (expected_bytes is not None and len(expected_bytes) > _MAX_BASELINE_BYTES):
        raise ModuleSizeConfinedWriteError("Module-size baseline exceeds its transaction byte limit")
    anchor_path = root_identity.path if root_identity is not None else Path(anchor).resolve(strict=True)
    root_fd = _open_directory_path(anchor_path, root_identity=root_identity)
    parent_fd = root_fd
    preserve_temporary = False
    temporary: str | None = None
    try:
        open_parent = _open_child_directory if expected_bytes is None else _open_existing_child_directory
        for part in normalized[:-1]:
            next_fd = open_parent(parent_fd, part)
            if parent_fd != root_fd:
                os.close(parent_fd)
            parent_fd = next_fd
        leaf = normalized[-1]
        _require_current_parent(anchor_path, normalized[:-1], parent_fd, root_identity=root_identity)
        temporary = _stage(parent_fd, leaf, content)
        if expected_bytes is None:
            _replace_unconditionally(parent_fd, leaf, temporary)
        else:
            try:
                outcome = replace_file_if_digest(
                    parent_fd,
                    leaf,
                    temporary,
                    expected_sha256=_sha256(expected_bytes),
                    max_bytes=_MAX_BASELINE_BYTES,
                )
            except ConfinedMutationError as exc:
                preserve_temporary = exc.cleanup_required
                raise ModuleSizeConfinedWriteError(_mutation_message(exc)) from exc
            preserve_temporary = outcome.cleanup_required
            if outcome.cleanup_required:
                raise ModuleSizeConfinedWriteError(
                    "Module-size baseline transaction committed but requires recovery before certification"
                )
        try:
            _require_current_parent(anchor_path, normalized[:-1], parent_fd, root_identity=root_identity)
            _require_canonical_candidate(
                anchor_path,
                normalized[:-1],
                leaf,
                expected_content=content,
                root_identity=root_identity,
            )
        except ModuleSizeConfinedWriteError:
            if expected_bytes is not None:
                _restore_expected(parent_fd, leaf, expected_bytes=expected_bytes, candidate_bytes=content)
            raise
    except ModuleSizeConfinedWriteError:
        raise
    except (ConfinedFileError, OSError) as exc:
        raise ModuleSizeConfinedWriteError("Cannot atomically persist the module-size baseline") from exc
    finally:
        if temporary is not None and not preserve_temporary:
            try:
                os.unlink(temporary, dir_fd=parent_fd)
            except FileNotFoundError:
                pass
        if parent_fd != root_fd:
            os.close(parent_fd)
        os.close(root_fd)


def _validated_parts(parts: tuple[str, ...]) -> tuple[str, ...]:
    if not parts or any(
        not part or part in {".", ".."} or "/" in part or "\\" in part or "\x00" in part for part in parts
    ):
        raise ModuleSizeConfinedWriteError("Module-size baseline path must be a normalized confined path")
    return parts


def _require_descriptor_relative_support() -> None:
    if not _DIR_FD_SUPPORTED:
        raise ModuleSizeConfinedWriteError(
            "Safe descriptor-relative baseline replacement is unavailable on this platform; no file was changed"
        )


def _open_directory_path(path: Path, *, root_identity: ModuleSizeRootIdentity | None) -> int:
    descriptor = os.open(path, _DIRECTORY_FLAGS)
    opened = os.fstat(descriptor)
    if root_identity is not None:
        if not root_identity.matches(opened):
            os.close(descriptor)
            raise ModuleSizeConfinedWriteError("Module-size repository root changed during evaluation")
        return descriptor
    current = os.stat(path, follow_symlinks=False)
    if not stat.S_ISDIR(current.st_mode) or _identity(opened) != _identity(current):
        os.close(descriptor)
        raise ModuleSizeConfinedWriteError("Module-size baseline anchor changed while it was opened")
    return descriptor


def _open_child_directory(parent_fd: int, name: str) -> int:
    try:
        return os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except FileNotFoundError:
        try:
            os.mkdir(name, 0o755, dir_fd=parent_fd)
            os.fsync(parent_fd)
        except FileExistsError:
            pass
        return os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)


def _open_existing_child_directory(parent_fd: int, name: str) -> int:
    """Open one existing no-follow parent without mutating the namespace."""

    return os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)


def _require_current_parent(
    anchor: Path,
    parent_parts: tuple[str, ...],
    parent_fd: int,
    *,
    root_identity: ModuleSizeRootIdentity | None,
) -> None:
    if root_identity is not None:
        current_root = os.stat(root_identity.path, follow_symlinks=False)
        if not root_identity.matches(current_root):
            raise ModuleSizeConfinedWriteError("Module-size repository root changed during evaluation")
    current_path = anchor.joinpath(*parent_parts)
    try:
        current = os.stat(current_path, follow_symlinks=False)
    except OSError as exc:
        raise ModuleSizeConfinedWriteError("Module-size baseline parent changed during replacement") from exc
    opened = os.fstat(parent_fd)
    if not stat.S_ISDIR(current.st_mode) or _identity(current) != _identity(opened):
        raise ModuleSizeConfinedWriteError("Module-size baseline parent changed during replacement")


def _require_canonical_candidate(
    anchor: Path,
    parent_parts: tuple[str, ...],
    leaf: str,
    *,
    expected_content: bytes,
    root_identity: ModuleSizeRootIdentity | None,
) -> None:
    """Re-anchor the final candidate through the current canonical namespace."""

    root_fd = _open_directory_path(anchor, root_identity=root_identity)
    parent_fd = root_fd
    try:
        for part in parent_parts:
            next_fd = _open_existing_child_directory(parent_fd, part)
            if parent_fd != root_fd:
                os.close(parent_fd)
            parent_fd = next_fd
        snapshot = read_confined_leaf(parent_fd, leaf, max_bytes=_MAX_BASELINE_BYTES)
        _require_current_parent(anchor, parent_parts, parent_fd, root_identity=root_identity)
        if snapshot.content != expected_content:
            raise ModuleSizeConfinedWriteError(
                "Module-size canonical baseline changed after replacement; the final write outcome is uncertain"
            )
    except ModuleSizeConfinedWriteError:
        raise
    except (ConfinedFileError, OSError) as exc:
        raise ModuleSizeConfinedWriteError(
            "Module-size canonical baseline is unavailable after replacement; the final write outcome is uncertain"
        ) from exc
    finally:
        if parent_fd != root_fd:
            os.close(parent_fd)
        os.close(root_fd)


def _stage(parent_fd: int, leaf: str, content: bytes) -> str:
    temporary = f".{leaf}.{uuid4().hex}.tmp"
    descriptor = os.open(temporary, _WRITE_FLAGS, 0o644, dir_fd=parent_fd)
    try:
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("module-size staging write made no progress")
            view = view[written:]
        os.fsync(descriptor)
        os.fsync(parent_fd)
    except BaseException:
        try:
            _cleanup_staged(parent_fd, temporary)
        finally:
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise
    try:
        os.close(descriptor)
    except BaseException:
        _cleanup_staged(parent_fd, temporary)
        raise
    return temporary


def _cleanup_staged(parent_fd: int, temporary: str) -> None:
    try:
        os.unlink(temporary, dir_fd=parent_fd)
    except FileNotFoundError:
        return
    os.fsync(parent_fd)


def _replace_unconditionally(parent_fd: int, leaf: str, temporary: str) -> None:
    os.rename(temporary, leaf, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
    os.fsync(parent_fd)


def _restore_expected(parent_fd: int, leaf: str, *, expected_bytes: bytes, candidate_bytes: bytes) -> None:
    temporary = _stage(parent_fd, leaf, expected_bytes)
    preserve = False
    try:
        outcome = replace_file_if_digest(
            parent_fd,
            leaf,
            temporary,
            expected_sha256=_sha256(candidate_bytes),
            max_bytes=_MAX_BASELINE_BYTES,
        )
        preserve = outcome.cleanup_required
        if outcome.cleanup_required:
            raise ModuleSizeConfinedWriteError(
                "Module-size parent changed after replacement and rollback requires recovery"
            )
    except ConfinedMutationError as exc:
        preserve = exc.cleanup_required
        raise ModuleSizeConfinedWriteError(
            "Module-size parent changed after replacement; concurrent bytes were preserved or recovery is required"
        ) from exc
    finally:
        if not preserve:
            try:
                os.unlink(temporary, dir_fd=parent_fd)
            except FileNotFoundError:
                pass


def _mutation_message(error: ConfinedMutationError) -> str:
    if error.code in {"source_changed", "replacement_changed"}:
        return "Module-size inputs changed during replacement; concurrent bytes were preserved"
    if error.cleanup_required or error.committed:
        return "Module-size baseline write outcome is uncertain; transaction recovery is required"
    return "Module-size baseline transaction could not be applied safely"


def _sha256(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def _identity(metadata: os.stat_result) -> tuple[int, int]:
    return metadata.st_dev, metadata.st_ino


__all__ = ["ModuleSizeConfinedWriteError", "ModuleSizeRootIdentity", "write_confined_baseline_bytes"]

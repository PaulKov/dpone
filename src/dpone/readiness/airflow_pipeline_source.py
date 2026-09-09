"""Utilities for locating and editing beginner Airflow pipeline sources."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.confined_mutations import ConfinedRollbackOutcome


import difflib
import os
import secrets
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from dpone.manifest.confined_mutations import OwnedFile, remove_file_if_owned
from dpone.manifest.project_root import (
    ProjectRootError,
    ProjectRootIdentity,
    ensure_project_root,
    verify_project_root,
)
from dpone.readiness.airflow_authoring_directories import (
    ConfinedAuthoringPathError,
    ConfinedDirectoryCreation,
    confined_path_error,
    confined_path_parts,
    fsync_directory_descriptor,
    open_confined_parent,
    remove_owned_empty_directories,
)
from dpone.readiness.airflow_pipeline_source_reader import (
    load_pipeline_mapping as _load_confined_pipeline_mapping,
)
from dpone.readiness.airflow_pipeline_source_reader import (
    resolve_pipeline_path as _resolve_confined_pipeline_path,
)
from dpone.security_redaction import redact_text

AIRFLOW_AUTHORING_DIFF_MAX_BYTES = 16 * 1024
_DIFF_TRUNCATION_MARKER = "\n... [diff truncated by dpone]\n"
_DIFF_INPUT_MAX_CHARS = AIRFLOW_AUTHORING_DIFF_MAX_BYTES * 4
_CONFINED_PREVIEW_MAX_BYTES = 64 * 1024
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)


class ConcurrentAuthoringCreate(OSError):
    def __init__(self, existing: ConfinedFileContent | None) -> None:
        super().__init__("Authoring target was created concurrently with different content.")
        self.existing = existing


@dataclass(frozen=True, slots=True)
class ConfinedFileContent:
    content: bytes
    complete: bool


@dataclass(frozen=True, slots=True)
class ConfinedFileCreation:
    path: Path
    content: bytes
    device: int
    inode: int
    created_directories: tuple[ConfinedDirectoryCreation, ...] = ()


@dataclass(frozen=True, slots=True)
class ConfinedFileRollbackOutcome:
    """Project-relative receipt for a no-clobber file rollback."""

    path: Path
    removed: bool
    preserved: bool
    recovery_path: Path | None = None
    directory_recovery_paths: tuple[Path, ...] = ()


class ConfinedAuthoringFileSystem:
    """Create and roll back project-relative authoring files through stable descriptors."""

    def __init__(
        self,
        root: str | Path,
        *,
        root_identity: ProjectRootIdentity | None = None,
    ) -> None:
        try:
            if root_identity is None:
                self._root_identity = ensure_project_root(root)
            else:
                verify_project_root(root_identity)
                self._root_identity = root_identity
        except ProjectRootError as exc:
            raise ConfinedAuthoringPathError(str(exc)) from exc
        self.root = self._root_identity.path

    def read(self, path: Path) -> ConfinedFileContent | None:
        parts = confined_path_parts(path)
        with open_confined_parent(
            self.root,
            parts,
            create=False,
            root_identity=self._root_identity,
        ) as opened:
            if opened.descriptor is None:
                return None
            return _read_confined_leaf(opened.descriptor, parts[-1])

    def create(self, path: Path, content: bytes) -> ConfinedFileCreation | None:
        parts = confined_path_parts(path)
        with open_confined_parent(
            self.root,
            parts,
            create=True,
            root_identity=self._root_identity,
        ) as opened:
            parent_fd = opened.descriptor
            assert parent_fd is not None
            temporary, temporary_stat = _write_confined_temporary(
                parent_fd,
                parts[-1],
                content,
            )
            try:
                try:
                    os.link(
                        temporary,
                        parts[-1],
                        src_dir_fd=parent_fd,
                        dst_dir_fd=parent_fd,
                        follow_symlinks=False,
                    )
                except FileExistsError as exc:
                    existing = _read_confined_leaf(parent_fd, parts[-1])
                    if existing is not None and existing.complete and existing.content == content:
                        return None
                    raise ConcurrentAuthoringCreate(existing) from exc
                current = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
                identity = (temporary_stat.st_dev, temporary_stat.st_ino)
                if (
                    not stat.S_ISREG(current.st_mode)
                    or (
                        current.st_dev,
                        current.st_ino,
                    )
                    != identity
                ):
                    raise ConfinedAuthoringPathError("Authoring target changed during create.")
                fsync_directory_descriptor(parent_fd)
                return ConfinedFileCreation(
                    path=path,
                    content=content,
                    device=current.st_dev,
                    inode=current.st_ino,
                    created_directories=opened.created_directories,
                )
            finally:
                try:
                    os.unlink(temporary, dir_fd=parent_fd)
                except FileNotFoundError:
                    pass

    def rollback(self, created: ConfinedFileCreation) -> ConfinedFileRollbackOutcome:
        parts = confined_path_parts(created.path)
        with open_confined_parent(
            self.root,
            parts,
            create=False,
            root_identity=self._root_identity,
        ) as opened:
            if opened.descriptor is None:
                return ConfinedFileRollbackOutcome(
                    path=created.path,
                    removed=False,
                    preserved=False,
                )
            outcome = _unlink_if_owned(opened.descriptor, parts[-1], created)
        directory_outcome = (
            remove_owned_empty_directories(
                self.root,
                created.created_directories,
                root_identity=self._root_identity,
            )
            if outcome.removed
            else None
        )
        recovery_path = created.path.with_name(outcome.recovery_name) if outcome.recovery_name is not None else None
        return ConfinedFileRollbackOutcome(
            path=created.path,
            removed=outcome.removed,
            preserved=outcome.preserved,
            recovery_path=recovery_path,
            directory_recovery_paths=directory_outcome.unresolved if directory_outcome is not None else (),
        )


def resolve_pipeline_path(root: str | Path, target: str | Path) -> Path:
    """Compatibility facade for the project-confined pipeline resolver."""

    return _resolve_confined_pipeline_path(root, target)


def load_pipeline_mapping(
    path: Path,
    *,
    root: str | Path = ".",
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Compatibility facade for bounded, project-confined pipeline reads."""

    return _load_confined_pipeline_mapping(path, root=root)


def write_pipeline_mapping_atomic(path: Path, payload: dict[str, Any]) -> None:
    """Write a pipeline YAML mapping with a replace-at-end update."""

    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    tmp_path.replace(path)


def _read_confined_leaf(parent_fd: int, name: str) -> ConfinedFileContent | None:
    try:
        descriptor = os.open(name, _READ_FLAGS, dir_fd=parent_fd)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise confined_path_error(exc) from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ConfinedAuthoringPathError("Authoring target must be a regular file.")
        content = _read_at_most(descriptor, _CONFINED_PREVIEW_MAX_BYTES + 1)
        after = os.fstat(descriptor)
        if _read_identity(before) != _read_identity(after):
            raise ConfinedAuthoringPathError("Authoring target changed while it was read.")
        complete = len(content) <= _CONFINED_PREVIEW_MAX_BYTES and len(content) == after.st_size
        return ConfinedFileContent(
            content=content[:_CONFINED_PREVIEW_MAX_BYTES],
            complete=complete,
        )
    finally:
        os.close(descriptor)


def _write_confined_temporary(
    parent_fd: int,
    leaf: str,
    content: bytes,
) -> tuple[str, os.stat_result]:
    temporary = f".{leaf}.dpone-authoring-{secrets.token_hex(8)}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(temporary, flags, 0o666, dir_fd=parent_fd)
    try:
        _write_all(descriptor, content)
        os.fsync(descriptor)
        return temporary, os.fstat(descriptor)
    except Exception:
        try:
            os.unlink(temporary, dir_fd=parent_fd)
        except FileNotFoundError:
            pass
        raise
    finally:
        os.close(descriptor)


def _write_all(descriptor: int, content: bytes) -> None:
    remaining = memoryview(content)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise OSError("Authoring write made no progress.")
        remaining = remaining[written:]


def _unlink_if_owned(
    parent_fd: int,
    leaf: str,
    created: ConfinedFileCreation,
) -> ConfinedRollbackOutcome:
    return remove_file_if_owned(
        parent_fd,
        leaf,
        owned=OwnedFile(
            device=created.device,
            inode=created.inode,
            content=created.content,
        ),
        max_bytes=max(_CONFINED_PREVIEW_MAX_BYTES, len(created.content)),
    )


def _read_at_most(descriptor: int, limit: int) -> bytes:
    chunks: list[bytes] = []
    remaining = limit
    while remaining and (chunk := os.read(descriptor, min(64 * 1024, remaining))):
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_identity(metadata: os.stat_result) -> tuple[int, int, int, int]:
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns


def bounded_redacted_diff(diff: str) -> str:
    """Redact and byte-bound authoring text before it crosses an output boundary."""

    redacted = redact_text(diff)
    encoded = redacted.encode("utf-8")
    if len(encoded) <= AIRFLOW_AUTHORING_DIFF_MAX_BYTES:
        return redacted
    marker = _DIFF_TRUNCATION_MARKER.encode()
    prefix = encoded[: AIRFLOW_AUTHORING_DIFF_MAX_BYTES - len(marker)].decode(
        "utf-8",
        errors="ignore",
    )
    return prefix + _DIFF_TRUNCATION_MARKER


def bounded_redacted_unified_diff(
    existing: str,
    desired: str,
    *,
    fromfile: str,
    tofile: str,
) -> str:
    """Build a bounded redacted unified diff from bounded input prefixes."""

    diff = "".join(
        difflib.unified_diff(
            existing[:_DIFF_INPUT_MAX_CHARS].splitlines(keepends=True),
            desired[:_DIFF_INPUT_MAX_CHARS].splitlines(keepends=True),
            fromfile=fromfile,
            tofile=tofile,
        )
    )
    return bounded_redacted_diff(diff)


__all__ = [
    "load_pipeline_mapping",
    "resolve_pipeline_path",
    "write_pipeline_mapping_atomic",
]

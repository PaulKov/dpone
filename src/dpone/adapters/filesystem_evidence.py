"""Descriptor-pinned durable create-only filesystem evidence writer."""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path


class EvidenceWriteError(RuntimeError):
    """Evidence target or durability boundary cannot be safely established."""


@dataclass(frozen=True)
class DescriptorPinnedCreateOnlyEvidenceWriter:
    """Write one direct evidence file with atomic no-replace and fsync barriers.

    The root is pinned at construction.  Evidence names are deliberately a flat,
    confined vocabulary: callers may not smuggle directory traversal or a nested
    path into the writer's durable boundary.
    """

    root: Path

    def __post_init__(self) -> None:
        resolved = self.root.resolve(strict=True)
        if not resolved.is_dir() or resolved.is_symlink():
            raise EvidenceWriteError("evidence root must be an existing regular directory")
        object.__setattr__(self, "root", resolved)

    def write(self, relative_name: str, payload: bytes) -> None:
        """Durably create payload or revalidate an existing identical target."""

        _require_safe_name(relative_name)
        if not payload:
            raise EvidenceWriteError("evidence payload must not be empty")
        directory_fd = _open_directory(self.root)
        stage_name = f".{relative_name}.stage"
        try:
            self._remove_authenticated_stage(directory_fd, stage_name)
            self._write_stage(directory_fd, stage_name, payload)
            try:
                os.link(stage_name, relative_name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
            except FileExistsError:
                self._verify_existing(directory_fd, relative_name, payload)
            finally:
                _unlink_if_regular(directory_fd, stage_name)
            _fsync(directory_fd, "evidence parent")
        finally:
            os.close(directory_fd)

    def _remove_authenticated_stage(self, directory_fd: int, stage_name: str) -> None:
        try:
            metadata = os.stat(stage_name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        if not stat.S_ISREG(metadata.st_mode):
            raise EvidenceWriteError("evidence staging target is unsafe")
        os.unlink(stage_name, dir_fd=directory_fd)
        _fsync(directory_fd, "evidence parent after stage removal")

    def _write_stage(self, directory_fd: int, stage_name: str, payload: bytes) -> None:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            file_fd = os.open(stage_name, flags, 0o600, dir_fd=directory_fd)
        except OSError as exc:
            raise EvidenceWriteError("cannot create evidence staging target") from exc
        try:
            _write_all(file_fd, payload)
            _fsync(file_fd, "evidence file")
        finally:
            os.close(file_fd)

    def _verify_existing(self, directory_fd: int, name: str, payload: bytes) -> None:
        metadata = _regular_stat(directory_fd, name)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            file_fd = os.open(name, flags, dir_fd=directory_fd)
        except OSError as exc:
            raise EvidenceWriteError("cannot reopen existing evidence target") from exc
        try:
            opened = os.fstat(file_fd)
            if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
                raise EvidenceWriteError("existing evidence target changed during revalidation")
            existing = _read_exact(file_fd, len(payload))
            if existing != payload:
                raise EvidenceWriteError("existing evidence target has different bytes")
            _fsync(file_fd, "existing evidence file")
        finally:
            os.close(file_fd)


def _require_safe_name(name: str) -> None:
    path = Path(name)
    if not name or path.name != name or name in {".", ".."} or "\x00" in name:
        raise EvidenceWriteError("evidence name must be one confined filename")


def _open_directory(root: Path) -> int:
    try:
        return os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise EvidenceWriteError("cannot pin evidence root directory") from exc


def _regular_stat(directory_fd: int, name: str) -> os.stat_result:
    try:
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError as exc:
        raise EvidenceWriteError("cannot inspect existing evidence target") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise EvidenceWriteError("existing evidence target is not regular")
    return metadata


def _unlink_if_regular(directory_fd: int, name: str) -> None:
    try:
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if not stat.S_ISREG(metadata.st_mode):
        raise EvidenceWriteError("evidence staging target changed to an unsafe file")
    os.unlink(name, dir_fd=directory_fd)
    _fsync(directory_fd, "evidence parent after stage cleanup")


def _write_all(file_fd: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(file_fd, payload[offset:])
        if written < 1:
            raise EvidenceWriteError("cannot write complete evidence payload")
        offset += written


def _read_exact(file_fd: int, size: int) -> bytes:
    content = os.read(file_fd, size + 1)
    if len(content) != size:
        raise EvidenceWriteError("existing evidence target has different bytes")
    return content


def _fsync(file_fd: int, label: str) -> None:
    try:
        os.fsync(file_fd)
    except OSError as exc:
        raise EvidenceWriteError(f"cannot fsync {label}") from exc


__all__ = ["DescriptorPinnedCreateOnlyEvidenceWriter", "EvidenceWriteError"]

"""Descriptor-confined directory access for build-plane projections."""

from __future__ import annotations

import errno
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | int(getattr(os, "O_CLOEXEC", 0)) | int(getattr(os, "O_NOFOLLOW", 0))
_DIR_FD_SUPPORTED = all(
    operation in os.supports_dir_fd for operation in (os.open, os.mkdir, os.rename, os.rmdir, os.stat, os.unlink)
)


@contextmanager
def open_or_create_confined_directory(root: Path, directory: Path) -> Iterator[int]:
    """Open or create one no-follow directory path anchored below ``root``."""

    if not _DIR_FD_SUPPORTED:
        raise OSError(errno.ENOTSUP, "descriptor-confined directory operations are unavailable")
    parts = _relative_directory_parts(root, directory)
    root_fd = os.open(root.resolve(strict=True), _DIRECTORY_FLAGS)
    current_fd = root_fd
    try:
        for part in parts:
            next_fd = _open_or_create_child(current_fd, part)
            if current_fd != root_fd:
                close_descriptor(current_fd)
            current_fd = next_fd
        yield current_fd
    finally:
        if current_fd != root_fd:
            close_descriptor(current_fd)
        close_descriptor(root_fd)


def open_child_directory(parent_fd: int, name: str) -> int:
    """Open one no-follow child directory relative to a trusted descriptor."""

    validate_child_name(name)
    return os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)


def validate_child_name(value: str) -> None:
    """Reject separators, traversal and non-leaf directory names."""

    path = PurePosixPath(value)
    if not value or "\\" in value or path.is_absolute() or path.parts != (value,) or value in {".", ".."}:
        raise ValueError("projection directory name must be one safe leaf")


def close_descriptor(descriptor: int) -> None:
    """Best-effort close for a directory descriptor used only as confinement."""

    try:
        os.close(descriptor)
    except OSError:
        pass


def _open_or_create_child(parent_fd: int, name: str) -> int:
    validate_child_name(name)
    try:
        return os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except FileNotFoundError:
        try:
            os.mkdir(name, 0o755, dir_fd=parent_fd)
        except FileExistsError:
            pass
        return os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)


def _relative_directory_parts(root: Path, directory: Path) -> tuple[str, ...]:
    root_path = root.resolve(strict=True)
    absolute_directory = Path(os.path.abspath(directory))
    try:
        relative = absolute_directory.relative_to(root_path)
    except ValueError as exc:
        raise ValueError("projection directory must stay inside the project root") from exc
    path = PurePosixPath(relative.as_posix())
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("projection directory must be a safe project-relative path")
    return path.parts


__all__ = [
    "close_descriptor",
    "open_child_directory",
    "open_or_create_confined_directory",
    "validate_child_name",
]

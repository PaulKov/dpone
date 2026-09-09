"""Confined, create-exclusive writes for one cache generation stage."""

from __future__ import annotations

import os
import stat
from pathlib import Path, PurePosixPath

_DIRECTORY_FLAGS = (
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)
_FILE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
_DIRECTORY_MODE = 0o2770
_FILE_MODE = 0o660


def write_generation_file_exclusive(stage: Path, relative_path: str, payload: bytes) -> Path:
    """Create one regular file below ``stage`` without following path links."""

    parts = _normalized_parts(relative_path)
    root_descriptor = os.open(stage, _DIRECTORY_FLAGS)
    parent_descriptor = os.dup(root_descriptor)
    file_descriptor: int | None = None
    try:
        for part in parts[:-1]:
            next_descriptor = _open_or_create_directory(parent_descriptor, part)
            os.close(parent_descriptor)
            parent_descriptor = next_descriptor
        file_descriptor = os.open(parts[-1], _FILE_FLAGS, _FILE_MODE, dir_fd=parent_descriptor)
        if not stat.S_ISREG(os.fstat(file_descriptor).st_mode):
            raise ValueError("airflow_pack_generation_write_invalid: destination must be a regular file")
        os.fchmod(file_descriptor, _FILE_MODE)
        written = 0
        while written < len(payload):
            count = os.write(file_descriptor, payload[written:])
            if count <= 0:
                raise OSError("cache generation write made no progress")
            written += count
        os.fsync(file_descriptor)
        os.fsync(parent_descriptor)
    finally:
        if file_descriptor is not None:
            os.close(file_descriptor)
        os.close(parent_descriptor)
        os.close(root_descriptor)
    return stage.joinpath(*parts)


def _open_or_create_directory(parent_descriptor: int, name: str) -> int:
    try:
        os.mkdir(name, mode=_DIRECTORY_MODE, dir_fd=parent_descriptor)
    except FileExistsError:
        pass
    descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_descriptor)
    if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise ValueError("airflow_pack_generation_write_invalid: parent must be a regular directory")
    os.fchmod(descriptor, _DIRECTORY_MODE)
    return descriptor


def _normalized_parts(relative_path: str) -> tuple[str, ...]:
    path = PurePosixPath(relative_path)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("airflow_pack_generation_path_unsafe")
    if path.as_posix() != relative_path:
        raise ValueError("airflow_pack_generation_path_not_canonical")
    return path.parts


__all__ = ["write_generation_file_exclusive"]

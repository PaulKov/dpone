"""No-follow directory traversal and rollback receipts for authoring files."""

from __future__ import annotations

import errno
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol

_DIRECTORY_FLAGS = (
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)


class ConfinedAuthoringPathError(OSError): ...


class RootDirectoryIdentity(Protocol):
    """Stable directory identity required by confined descriptor traversal."""

    def matches(self, metadata: os.stat_result) -> bool: ...


@dataclass(frozen=True, slots=True)
class ConfinedDirectoryCreation:
    path: Path
    device: int
    inode: int


@dataclass(frozen=True, slots=True)
class OpenConfinedParent:
    descriptor: int | None
    created_directories: tuple[ConfinedDirectoryCreation, ...]


@dataclass(frozen=True, slots=True)
class DirectoryRollbackOutcome:
    """Truthful cleanup result for transaction-created directories."""

    removed: tuple[Path, ...] = ()
    preserved: tuple[Path, ...] = ()
    unresolved: tuple[Path, ...] = ()


def confined_path_parts(path: Path) -> tuple[str, ...]:
    raw = path.as_posix()
    parsed = PurePosixPath(raw)
    if (
        not raw
        or path.is_absolute()
        or parsed.is_absolute()
        or not parsed.parts
        or ".." in parsed.parts
        or "." in parsed.parts
        or "\\" in raw
        or "\x00" in raw
    ):
        raise ConfinedAuthoringPathError("Authoring target must be a normalized project-relative path.")
    return parsed.parts


@contextmanager
def open_confined_parent(
    root: Path,
    parts: tuple[str, ...],
    *,
    create: bool,
    root_identity: RootDirectoryIdentity | None = None,
) -> Iterator[OpenConfinedParent]:
    created_directories: list[ConfinedDirectoryCreation] = []
    try:
        descriptor = os.open(root, _DIRECTORY_FLAGS)
    except OSError as exc:
        raise confined_path_error(exc) from exc
    try:
        if root_identity is not None and not root_identity.matches(os.fstat(descriptor)):
            raise ConfinedAuthoringPathError("Project root changed during the authoring operation.")
        for index, part in enumerate(parts[:-1]):
            try:
                next_descriptor = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    yield OpenConfinedParent(None, ())
                    return
                try:
                    os.mkdir(part, mode=0o777, dir_fd=descriptor)
                    fsync_directory_descriptor(descriptor)
                    metadata = os.stat(part, dir_fd=descriptor, follow_symlinks=False)
                    created_directories.append(
                        ConfinedDirectoryCreation(
                            path=Path(*parts[: index + 1]),
                            device=metadata.st_dev,
                            inode=metadata.st_ino,
                        )
                    )
                except FileExistsError:
                    pass
                try:
                    next_descriptor = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
                except OSError as exc:
                    raise confined_path_error(exc) from exc
            except OSError as exc:
                raise confined_path_error(exc) from exc
            os.close(descriptor)
            descriptor = next_descriptor
        yield OpenConfinedParent(descriptor, tuple(created_directories))
    except BaseException as exc:
        rollback = remove_owned_empty_directories(
            root,
            tuple(created_directories),
            root_identity=root_identity,
        )
        recovery_paths = tuple(dict.fromkeys((*rollback.preserved, *rollback.unresolved)))
        recovery_path_set = set(recovery_paths)
        recovery_entries = tuple(
            {
                "action": "verify_and_remove",
                "kind": "directory",
                "path": created.path.as_posix(),
                "device": created.device,
                "inode": created.inode,
                "require_empty": True,
            }
            for created in created_directories
            if created.path in recovery_path_set
        )
        if recovery_paths:
            try:
                setattr(exc, "scaffold_directory_recovery_paths", recovery_paths)
                setattr(exc, "scaffold_directory_recovery_entries", recovery_entries)
                setattr(exc, "scaffold_directory_rollback_outcome", rollback)
            except (AttributeError, TypeError):
                pass
        raise
    finally:
        os.close(descriptor)


def remove_owned_empty_directories(
    root: Path,
    created_directories: tuple[ConfinedDirectoryCreation, ...],
    *,
    root_identity: RootDirectoryIdentity | None = None,
) -> DirectoryRollbackOutcome:
    removed: list[Path] = []
    preserved: list[Path] = []
    unresolved: list[Path] = []
    reversed_directories = tuple(reversed(created_directories))
    for index, created in enumerate(reversed_directories):
        parts = confined_path_parts(created.path)
        try:
            with open_confined_parent(
                root,
                parts,
                create=False,
                root_identity=root_identity,
            ) as opened:
                if opened.descriptor is None:
                    continue
                metadata = os.stat(parts[-1], dir_fd=opened.descriptor, follow_symlinks=False)
                if (
                    not stat.S_ISDIR(metadata.st_mode)
                    or metadata.st_dev != created.device
                    or metadata.st_ino != created.inode
                ):
                    preserved.extend(item.path for item in reversed_directories[index:])
                    break
                os.rmdir(parts[-1], dir_fd=opened.descriptor)
                removed.append(created.path)
                try:
                    os.fsync(opened.descriptor)
                except OSError:
                    unresolved.append(created.path)
        except FileNotFoundError:
            continue
        except OSError as exc:
            if exc.errno in {errno.ENOTEMPTY, errno.EEXIST}:
                preserved.extend(item.path for item in reversed_directories[index:])
            else:
                unresolved.append(created.path)
                preserved.extend(item.path for item in reversed_directories[index + 1 :])
            break
    return DirectoryRollbackOutcome(
        removed=tuple(removed),
        preserved=tuple(dict.fromkeys(preserved)),
        unresolved=tuple(dict.fromkeys(unresolved)),
    )


def confined_path_error(error: OSError) -> OSError:
    if error.errno in {errno.ELOOP, errno.ENOTDIR}:
        return ConfinedAuthoringPathError("Authoring path must not contain symlink components.")
    return error


def fsync_directory_descriptor(descriptor: int) -> None:
    try:
        os.fsync(descriptor)
    except OSError:
        return


__all__ = [
    "ConfinedAuthoringPathError",
    "ConfinedDirectoryCreation",
    "DirectoryRollbackOutcome",
    "OpenConfinedParent",
    "confined_path_error",
    "confined_path_parts",
    "fsync_directory_descriptor",
    "open_confined_parent",
    "remove_owned_empty_directories",
]

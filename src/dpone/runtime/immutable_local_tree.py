"""Atomic create-or-compare publication for immutable local directory trees."""

from __future__ import annotations

import ctypes
import errno
import os
import stat
import sys
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import BinaryIO
from uuid import uuid4

TreeSource = bytes | Path
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)


class ImmutableLocalTreeError(RuntimeError):
    def __init__(self, message: str, *, path: Path) -> None:
        super().__init__(message)
        self.path = path


class ImmutableLocalTreeDurabilityError(ImmutableLocalTreeError):
    """A tree became visible but durable persistence could not be proven."""


def materialize_immutable_local_tree(
    tree_dir: Path,
    files: Mapping[str, TreeSource],
    *,
    allowed_parent: Path,
    root: Path,
) -> str:
    """Create an entire tree atomically or prove exact equality with an existing tree."""

    tree_dir = tree_dir.absolute()
    allowed_parent = allowed_parent.absolute()
    root = root.absolute()
    if tree_dir.parent != allowed_parent:
        raise ValueError("immutable tree must be a direct child of its allowed parent")
    try:
        parent_parts = allowed_parent.relative_to(root).parts
    except ValueError as exc:
        raise ValueError("immutable tree parent must be inside its configured root") from exc
    root.mkdir(parents=True, exist_ok=True)
    root_descriptor = os.open(root, _DIRECTORY_FLAGS)
    try:
        _require_path_matches_descriptor(root, root_descriptor, error_path=tree_dir)
        return materialize_immutable_local_tree_at(
            root_descriptor,
            PurePosixPath(*parent_parts, tree_dir.name),
            files,
            error_root=root,
        )
    finally:
        try:
            _require_path_matches_descriptor(root, root_descriptor, error_path=tree_dir)
        finally:
            os.close(root_descriptor)


def materialize_immutable_local_tree_at(
    root_descriptor: int,
    tree_relative: PurePosixPath,
    files: Mapping[str, TreeSource],
    *,
    error_root: Path,
) -> str:
    """Create one immutable tree relative to an already pinned root descriptor."""

    normalized = _normalized_files(files)
    if (
        tree_relative.is_absolute()
        or not tree_relative.parts
        or any(part in {"", ".", ".."} for part in tree_relative.parts)
    ):
        raise ValueError("immutable tree path must be a normalized relative path")
    parent_descriptor = _open_parent_at(root_descriptor, tree_relative.parts[:-1], create=True)
    tree_dir = error_root / tree_relative.as_posix()
    try:
        return _materialize_at(
            parent_descriptor,
            tree_name=tree_relative.name,
            tree_dir=tree_dir,
            staging_name=f".{tree_relative.name}.tmp.{uuid4().hex}",
            files=normalized,
        )
    finally:
        os.close(parent_descriptor)


def _materialize_at(
    parent_descriptor: int,
    *,
    tree_name: str,
    tree_dir: Path,
    staging_name: str,
    files: Mapping[str, TreeSource],
) -> str:
    try:
        if _entry_exists(parent_descriptor, tree_name):
            _verify_existing_at(parent_descriptor, tree_name, files, tree_dir=tree_dir)
            _require_parent_durability(parent_descriptor, tree_dir)
            return "no_op"
        os.mkdir(staging_name, mode=0o700, dir_fd=parent_descriptor)
        staging_descriptor = os.open(staging_name, _DIRECTORY_FLAGS, dir_fd=parent_descriptor)
        try:
            for relative, source in files.items():
                _write_new_file_at(staging_descriptor, PurePosixPath(relative), source)
            os.fsync(staging_descriptor)
        finally:
            os.close(staging_descriptor)
        try:
            _rename_no_replace_at(
                parent_descriptor,
                staging_name,
                tree_name,
                parent_path=tree_dir.parent,
            )
        except FileExistsError:
            _remove_tree_at(parent_descriptor, staging_name)
            _verify_existing_at(parent_descriptor, tree_name, files, tree_dir=tree_dir)
            _require_parent_durability(parent_descriptor, tree_dir)
            return "no_op"
        _require_parent_durability(parent_descriptor, tree_dir)
        return "created"
    except BaseException:
        if _entry_exists(parent_descriptor, staging_name):
            _remove_tree_at(parent_descriptor, staging_name)
        raise


def _require_parent_durability(parent_descriptor: int, tree_dir: Path) -> None:
    """An equal retry must also finish any previously uncertain rename durability."""
    try:
        os.fsync(parent_descriptor)
    except OSError as exc:
        raise ImmutableLocalTreeDurabilityError(
            "immutable tree is visible but parent durability could not be proven", path=tree_dir
        ) from exc


def _normalized_files(files: Mapping[str, TreeSource]) -> dict[str, TreeSource]:
    normalized: dict[str, TreeSource] = {}
    for raw_path, source in files.items():
        relative = PurePosixPath(raw_path)
        if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
            raise ValueError(f"immutable tree path is unsafe: {raw_path}")
        normalized[relative.as_posix()] = bytes(source) if isinstance(source, bytes) else Path(source)
    if not normalized:
        raise ValueError("immutable tree requires at least one file")
    return dict(sorted(normalized.items()))


def _write_new_file_at(root_descriptor: int, relative: PurePosixPath, source: TreeSource) -> None:
    parent_descriptor = _open_parent_at(root_descriptor, relative.parts[:-1], create=True)
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(relative.name, flags, 0o600, dir_fd=parent_descriptor)
        with os.fdopen(descriptor, "wb") as target:
            if isinstance(source, bytes):
                target.write(source)
            else:
                _copy_regular_file(source, target)
            target.flush()
            os.fsync(target.fileno())
        os.fsync(parent_descriptor)
    finally:
        os.close(parent_descriptor)


def _verify_existing_at(
    parent_descriptor: int,
    tree_name: str,
    expected: Mapping[str, TreeSource],
    *,
    tree_dir: Path,
) -> None:
    try:
        tree_descriptor = os.open(tree_name, _DIRECTORY_FLAGS, dir_fd=parent_descriptor)
    except OSError as exc:
        raise ImmutableLocalTreeError("immutable tree path is not a regular directory", path=tree_dir) from exc
    try:
        actual_paths = _collect_regular_files(tree_descriptor)
        if actual_paths != set(expected):
            raise ImmutableLocalTreeError("immutable tree file set differs from expected content", path=tree_dir)
        for relative, source in expected.items():
            if not _equal_regular_file_at(tree_descriptor, PurePosixPath(relative), source):
                raise ImmutableLocalTreeError(
                    "immutable tree file differs from expected content",
                    path=tree_dir / relative,
                )
    finally:
        os.close(tree_descriptor)


def _collect_regular_files(directory_descriptor: int, *, prefix: str = "") -> set[str]:
    files: set[str] = set()
    for name in os.listdir(directory_descriptor):
        relative = f"{prefix}/{name}" if prefix else name
        item_stat = os.stat(name, dir_fd=directory_descriptor, follow_symlinks=False)
        if stat.S_ISREG(item_stat.st_mode):
            files.add(relative)
            continue
        if stat.S_ISDIR(item_stat.st_mode):
            child = os.open(name, _DIRECTORY_FLAGS, dir_fd=directory_descriptor)
            try:
                files.update(_collect_regular_files(child, prefix=relative))
            finally:
                os.close(child)
            continue
        files.add(relative)
    return files


def _equal_regular_file_at(root_descriptor: int, relative: PurePosixPath, source: TreeSource) -> bool:
    parent_descriptor = _open_parent_at(root_descriptor, relative.parts[:-1], create=False)
    try:
        descriptor = os.open(
            relative.name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_descriptor,
        )
    finally:
        os.close(parent_descriptor)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            return False
        with os.fdopen(descriptor, "rb") as actual:
            descriptor = -1
            if isinstance(source, bytes):
                return actual.read() == source
            source_descriptor = _open_regular_source(source)
            with os.fdopen(source_descriptor, "rb") as expected:
                while True:
                    actual_chunk = actual.read(1024 * 1024)
                    expected_chunk = expected.read(1024 * 1024)
                    if actual_chunk != expected_chunk:
                        return False
                    if not actual_chunk:
                        return True
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _open_parent_at(root_descriptor: int, parts: tuple[str, ...], *, create: bool) -> int:
    current = os.dup(root_descriptor)
    try:
        for part in parts:
            if create:
                try:
                    os.mkdir(part, mode=0o700, dir_fd=current)
                except FileExistsError:
                    pass
            child = os.open(part, _DIRECTORY_FLAGS, dir_fd=current)
            os.close(current)
            current = child
        return current
    except BaseException:
        os.close(current)
        raise


def _copy_regular_file(source: Path, target: BinaryIO) -> None:
    descriptor = _open_regular_source(source)
    with os.fdopen(descriptor, "rb") as handle:
        while chunk := handle.read(1024 * 1024):
            target.write(chunk)


def _open_regular_source(source: Path) -> int:
    descriptor = os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    if stat.S_ISREG(os.fstat(descriptor).st_mode):
        return descriptor
    os.close(descriptor)
    raise ImmutableLocalTreeError("immutable tree source is not a regular file", path=source)


def _remove_tree_at(parent_descriptor: int, name: str) -> None:
    descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_descriptor)
    try:
        for child_name in os.listdir(descriptor):
            item_stat = os.stat(child_name, dir_fd=descriptor, follow_symlinks=False)
            if stat.S_ISDIR(item_stat.st_mode):
                _remove_tree_at(descriptor, child_name)
            else:
                os.unlink(child_name, dir_fd=descriptor)
    finally:
        os.close(descriptor)
    os.rmdir(name, dir_fd=parent_descriptor)


def _entry_exists(parent_descriptor: int, name: str) -> bool:
    try:
        os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return True


def _require_path_matches_descriptor(path: Path, descriptor: int, *, error_path: Path) -> None:
    try:
        path_stat = os.stat(path, follow_symlinks=False)
    except OSError as exc:
        raise ImmutableLocalTreeError("immutable tree parent changed during publication", path=error_path) from exc
    descriptor_stat = os.fstat(descriptor)
    if not stat.S_ISDIR(path_stat.st_mode) or (path_stat.st_dev, path_stat.st_ino) != (
        descriptor_stat.st_dev,
        descriptor_stat.st_ino,
    ):
        raise ImmutableLocalTreeError("immutable tree parent changed during publication", path=error_path)


def _rename_no_replace_at(parent_descriptor: int, source: str, target: str, *, parent_path: Path) -> None:
    if sys.platform == "darwin":
        _call_rename(
            "renameatx_np",
            parent_descriptor,
            source,
            target,
            flags=0x00000004,
        )
        return
    if sys.platform.startswith("linux"):
        _call_rename("renameat2", parent_descriptor, source, target, flags=1)
        return
    if os.name == "nt":
        os.rename(parent_path / source, parent_path / target)
        return
    raise OSError(errno.ENOTSUP, "atomic no-replace directory rename is unsupported")


def _call_rename(function_name: str, parent_descriptor: int, source: str, target: str, *, flags: int) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        rename = getattr(libc, function_name)
    except AttributeError as exc:
        raise OSError(errno.ENOTSUP, f"{function_name} is unavailable") from exc
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    result = rename(
        parent_descriptor,
        os.fsencode(source),
        parent_descriptor,
        os.fsencode(target),
        flags,
    )
    if result != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number), target)


__all__ = [
    "ImmutableLocalTreeError",
    "materialize_immutable_local_tree",
    "materialize_immutable_local_tree_at",
]

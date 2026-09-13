"""Read administrator-staged authority from the nonroot dispatcher boundary.

Provisioning owns every file as root with the enrolled dispatcher group. Files
are group-readable and directories group-searchable; group/world writes, links
and special files are forbidden. This reader grants no provisioning authority.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path, PurePosixPath

from dpone.adapters.composition_supervisor_filesystem import absolute_supervisor_path, open_protected
from dpone.contracts.composition_identity import CompositionAdmissionError


class DispatcherContextFiles:
    """Bounded no-follow reads beneath one explicitly configured protected root."""

    def __init__(self, root: Path, *, dispatcher_gid: int) -> None:
        if type(dispatcher_gid) is not int or dispatcher_gid <= 0:
            raise CompositionAdmissionError("dispatcher_staging")
        self.root = absolute_supervisor_path(root)
        self._gid = dispatcher_gid

    def _require(self, info: os.stat_result, *, directory: bool) -> None:
        valid_kind = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
        group_access = 0o050 if directory else 0o040
        if (
            not valid_kind
            or info.st_uid != 0
            or info.st_gid != self._gid
            or info.st_mode & 0o027
            or info.st_mode & group_access != group_access
            or (not directory and info.st_nlink != 1)
        ):
            raise CompositionAdmissionError("dispatcher_staging")

    def _open(self, relative: str) -> int:
        path = PurePosixPath(relative)
        if path.is_absolute() or not path.parts or any(part in {".", ".."} for part in path.parts):
            raise CompositionAdmissionError("dispatcher_staging")
        descriptor = open_protected(self.root, traversable=False)
        try:
            self._require(os.fstat(descriptor), directory=True)
            for part in path.parts:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
                self._require(os.fstat(descriptor), directory=True)
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def read(self, relative: str, *, max_bytes: int = 1024 * 1024) -> bytes:
        """Reopen a bounded exact original without following any path component."""
        parent = descriptor = None
        try:
            if type(max_bytes) is not int or not 0 < max_bytes <= 4 * 1024 * 1024:
                raise ValueError
            path = PurePosixPath(relative)
            if path.is_absolute() or ".." in path.parts or len(path.parts) < 2:
                raise ValueError
            parent = self._open(str(path.parent))
            descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            before = os.fstat(descriptor)
            self._require(before, directory=False)
            if not 0 < before.st_size <= max_bytes:
                raise ValueError
            chunks, remaining = [], max_bytes + 1
            while remaining:
                chunk = os.read(descriptor, min(65536, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            result = b"".join(chunks)
            after = os.fstat(descriptor)
            stable = (
                "st_dev",
                "st_ino",
                "st_mode",
                "st_uid",
                "st_gid",
                "st_nlink",
                "st_size",
                "st_mtime_ns",
                "st_ctime_ns",
            )
            if (
                any(getattr(before, name) != getattr(after, name) for name in stable)
                or len(result) != before.st_size
                or len(result) > max_bytes
            ):
                raise ValueError
            return result
        except Exception:
            raise CompositionAdmissionError("dispatcher_staging") from None
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if parent is not None:
                os.close(parent)

    def require_tree(self, relative: str) -> Path:
        """Check a bounded staged cache before existing digest-verifying readers.

        Only root may replace these originals. Content authenticity remains the
        responsibility of the runtime loader and producer-backed plan reader.
        """
        budget = 8192

        def walk(current: int, depth: int) -> None:
            nonlocal budget
            if depth > 32:
                raise ValueError
            with os.scandir(current) as entries:
                for entry in entries:
                    budget -= 1
                    if budget < 0:
                        raise ValueError
                    child = os.open(entry.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=current)
                    try:
                        info = os.fstat(child)
                        directory = stat.S_ISDIR(info.st_mode)
                        self._require(info, directory=directory)
                        if directory:
                            walk(child, depth + 1)
                    finally:
                        os.close(child)

        descriptor = None
        try:
            descriptor = self._open(relative)
            walk(descriptor, 0)
            return self.root / relative
        except Exception:
            raise CompositionAdmissionError("dispatcher_staging") from None
        finally:
            if descriptor is not None:
                os.close(descriptor)

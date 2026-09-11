"""No-follow protected path primitives for the root composition supervisor.

Every protected filesystem authority in this feature shares one traversal
discipline: the configured root must be an absolute, traversal-free path whose
complete ancestry is root-owned and never group/other writable, and every
component is opened with ``O_NOFOLLOW`` so a symlink can never redirect an
allocation. Keeping the discipline in one module prevents the two supervisor
adapters from drifting apart on a security-critical check.
"""

from __future__ import annotations

import os
import stat
import sys
from collections.abc import Callable
from pathlib import Path, PurePosixPath

from dpone.contracts.composition_dbt_outcome import MAX_ARTIFACT_BYTES, DbtCaptureError

PROTECTED_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def absolute_supervisor_path(path: Path) -> Path:
    """Return one absolute, traversal-free supervisor path."""

    path = Path(path)
    if not path.is_absolute() or ".." in path.parts:
        raise DbtCaptureError("capture_allocation_path")
    return path


def require_supervisor() -> None:
    """Require the actual Linux root supervisor process."""

    if sys.platform != "linux" or os.geteuid() != 0 or os.getegid() != 0:
        raise DbtCaptureError("capture_supervisor_boundary")


def open_protected(path: Path, *, traversable: bool = True) -> int:
    """Open one directory component by component, never following a symlink."""

    descriptor = os.open("/", PROTECTED_FLAGS)
    try:
        for part in path.parts[1:]:
            following = os.open(part, PROTECTED_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = following
            info = os.fstat(descriptor)
            if info.st_uid != 0 or (info.st_mode & 0o022 and not info.st_mode & stat.S_ISVTX):
                raise DbtCaptureError("capture_allocation_ancestry")
            if traversable and not info.st_mode & stat.S_IXOTH:
                raise DbtCaptureError("capture_allocation_inaccessible")
        info = os.fstat(descriptor)
        if info.st_mode & 0o022:
            raise DbtCaptureError("capture_allocation_permissions")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def read_protected_original(root: Path, relative: str, *, max_bytes: int = MAX_ARTIFACT_BYTES) -> bytes:
    """Reopen one root-owned original and return its exact current bytes.

    Every component below ``root`` is opened with ``O_NOFOLLOW``, the file must be
    a single-linked regular root-owned file that no other account can write, and
    its inode identity plus size must be unchanged across the read. A protected
    original that was replaced or truncated while being read is rejected rather
    than hashed, so a mutated artifact can never authorize a dispatch.
    """

    root = absolute_supervisor_path(root)
    path = PurePosixPath(relative)
    if path.is_absolute() or not path.parts or any(part in {".", ".."} for part in path.parts):
        raise DbtCaptureError("capture_allocation_path")
    require_supervisor()
    parent = open_protected(root / path.parent, traversable=False)
    descriptor = None
    try:
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != 0
            or before.st_mode & 0o022
            or not 0 < before.st_size <= max_bytes
        ):
            raise DbtCaptureError("capture_preflight_original")
        chunks = []
        while chunk := os.read(descriptor, 65536):
            chunks.append(chunk)
        payload = b"".join(chunks)
        after = os.fstat(descriptor)
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) or len(payload) != before.st_size:
            raise DbtCaptureError("capture_preflight_changed")
        return payload
    except OSError:
        raise DbtCaptureError("capture_preflight_original") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(parent)


def protected_original_reader(root: Path, relative: str) -> Callable[[], bytes]:
    """Bind one protected original to a reader that reopens it on every call."""

    def read() -> bytes:
        return read_protected_original(root, relative)

    return read


__all__ = [
    "PROTECTED_FLAGS",
    "absolute_supervisor_path",
    "open_protected",
    "protected_original_reader",
    "read_protected_original",
    "require_supervisor",
]

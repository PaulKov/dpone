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
from pathlib import Path

from dpone.contracts.composition_dbt_outcome import DbtCaptureError

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


__all__ = [
    "PROTECTED_FLAGS",
    "absolute_supervisor_path",
    "open_protected",
    "require_supervisor",
]

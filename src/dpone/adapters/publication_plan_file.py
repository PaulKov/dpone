"""Bounded private operator-plan files on a local POSIX filesystem.

The final name is linked only after its complete temporary inode is flushed.
An existing name, including a symlink, is never overwritten. These guarantees
protect file publication, not the plan's authority: application services must
still validate its exact schema/digest and re-observe external facts at apply.
"""

from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path

MAX_PLAN_BYTES = 1024 * 1024


def write_private_plan(path: Path, content: bytes) -> None:
    """Publish once, mode 0600; refuse overwrite even for concurrent writers.

    A failure after linking may leave a complete final file. Preserve it for
    readback rather than deleting evidence or automatically retrying. Use a
    trusted local parent directory; no distributed-filesystem durability claim.
    """
    _require_posix()
    if not isinstance(content, bytes) or not 0 < len(content) <= MAX_PLAN_BYTES:
        raise ValueError("publication plan must contain bounded nonempty bytes")
    descriptor, temporary = tempfile.mkstemp(prefix=".dpone-plan-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            os.fchmod(output.fileno(), 0o600)
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, path, follow_symlinks=False)
        os.unlink(temporary)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        # Only the uniquely created temporary path belongs to this invocation.
        # Never remove the final path after a failed durability acknowledgement.
        Path(temporary).unlink(missing_ok=True)


def read_private_plan(path: Path) -> bytes:
    """Read a private regular file, without blocking on a FIFO or a device.

    No-follow and descriptor checks avoid final-path symlink/replace races.
    Reject multiple hard links and changes observed during reading. Content
    authentication and external authority admission remain the caller's job.
    """
    _require_posix()
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source:
        before = os.fstat(source.fileno())
        _require_private_regular(before)
        content = source.read(MAX_PLAN_BYTES + 1)
        after = os.fstat(source.fileno())
        _require_private_regular(after)
    if (
        len(content) != before.st_size
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or before.st_ctime_ns != after.st_ctime_ns
    ):
        raise ValueError("publication plan changed while reading")
    return content


def _require_private_regular(metadata: os.stat_result) -> None:
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_mode & 0o077
        or metadata.st_nlink != 1
        or not 0 < metadata.st_size <= MAX_PLAN_BYTES
    ):
        raise ValueError("publication plan must be an owner-private bounded regular file")


def _require_posix() -> None:
    if os.name != "posix" or not all(hasattr(os, name) for name in ("O_NOFOLLOW", "O_DIRECTORY", "O_NONBLOCK")):
        raise ValueError("publication plan files require a local POSIX filesystem")

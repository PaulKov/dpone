"""POSIX descriptor primitives for anonymous external-process input files."""

from __future__ import annotations

import os
import stat
from collections.abc import Callable


def create_anonymous_consumer_file(
    *,
    directory_descriptor: int,
    entry_name: str,
    path: str,
    create_named_file: Callable[[], tuple[int, str, str]],
    unlink_named_file: Callable[[str], None],
) -> tuple[int, int, str, str, Callable[[], None]]:
    """Create a writer/reader pair whose basename disappears before extraction."""

    tmpfile = getattr(os, "O_TMPFILE", 0)
    if tmpfile:
        flags = os.O_WRONLY | os.O_EXCL | tmpfile | getattr(os, "O_CLOEXEC", 0)
        try:
            writer_descriptor = os.open(
                ".",
                flags,
                0o600,
                dir_fd=directory_descriptor,
            )
        except OSError:
            writer_descriptor = -1
        if writer_descriptor >= 0:
            try:
                require_regular_descriptor(writer_descriptor)
                reader_descriptor = open_read_descriptor_projection(writer_descriptor)
                return writer_descriptor, reader_descriptor, entry_name, path, _no_op
            except OSError:
                os.close(writer_descriptor)

    writer_descriptor, entry_name, path = create_named_file()
    reader_descriptor = -1
    try:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        reader_descriptor = os.open(
            entry_name,
            flags,
            dir_fd=directory_descriptor,
        )
        require_same_regular_file(writer_descriptor, reader_descriptor)
        require_read_only_descriptor(reader_descriptor)
        unlink_named_file(entry_name)
    except BaseException:
        if reader_descriptor >= 0:
            os.close(reader_descriptor)
        os.close(writer_descriptor)
        raise
    return writer_descriptor, reader_descriptor, entry_name, path, _no_op


def require_regular_descriptor(descriptor: int) -> None:
    """Require one live regular-file descriptor."""

    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        raise OSError("created_spool_is_not_regular")


def require_same_regular_file(writer_descriptor: int, reader_descriptor: int) -> None:
    """Require separate writer and reader descriptors for the same inode."""

    writer = os.fstat(writer_descriptor)
    reader = os.fstat(reader_descriptor)
    if not stat.S_ISREG(writer.st_mode) or not stat.S_ISREG(reader.st_mode):
        raise OSError("created_spool_is_not_regular")
    if (int(writer.st_dev), int(writer.st_ino)) != (int(reader.st_dev), int(reader.st_ino)):
        raise OSError("pinned_consumer_file_identity_changed")


def require_read_only_descriptor(descriptor: int) -> None:
    """Reject a consumer descriptor that retains writer access."""

    import fcntl

    flags = int(fcntl.fcntl(descriptor, fcntl.F_GETFL))
    if flags & os.O_ACCMODE != os.O_RDONLY:
        raise OSError("pinned_consumer_read_descriptor_required")


def open_read_descriptor_projection(writer_descriptor: int) -> int:
    """Open and prove a read-only view of one anonymous writer descriptor."""

    for root in ("/proc/self/fd", "/dev/fd"):
        try:
            reader_descriptor = os.open(
                f"{root}/{writer_descriptor}",
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0),
            )
        except OSError:
            continue
        try:
            require_same_regular_file(writer_descriptor, reader_descriptor)
            require_read_only_descriptor(reader_descriptor)
            return reader_descriptor
        except OSError:
            os.close(reader_descriptor)
    raise OSError("pinned_consumer_read_descriptor_unsupported")


def descriptor_consumer_path(descriptor: int, expected: os.stat_result) -> str:
    """Resolve a verified process-visible path for one held reader."""

    for root in ("/proc/self/fd", "/dev/fd"):
        candidate = f"{root}/{descriptor}"
        try:
            probe = os.open(candidate, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        except OSError:
            continue
        try:
            observed = os.fstat(probe)
            if (int(observed.st_dev), int(observed.st_ino)) == (
                int(expected.st_dev),
                int(expected.st_ino),
            ):
                return candidate
        finally:
            os.close(probe)
    raise OSError("pinned_file_descriptor_path_unsupported")


def rewind_descriptor(descriptor: int) -> None:
    """Reset a held reader before the external process starts."""

    os.lseek(descriptor, 0, os.SEEK_SET)


def _no_op() -> None:
    return None


__all__ = [
    "create_anonymous_consumer_file",
    "descriptor_consumer_path",
    "open_read_descriptor_projection",
    "require_read_only_descriptor",
    "require_regular_descriptor",
    "require_same_regular_file",
    "rewind_descriptor",
]

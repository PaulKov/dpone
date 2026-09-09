"""Private receipt workspace lifecycle for Python import probes."""

from __future__ import annotations

import os
import stat
import tempfile
from dataclasses import dataclass

from dpone.readiness.python_import_probe_child import RECEIPT_FILENAME


@dataclass(frozen=True, slots=True)
class ProbeWorkspace:
    """Private startup workspace with one precreated receipt inode."""

    directory: str
    receipt_path: str


def create_probe_workspace() -> ProbeWorkspace | None:
    """Create a private empty directory and one regular receipt file."""

    directory: str | None = None
    receipt_path: str | None = None
    try:
        directory = tempfile.mkdtemp(prefix="dpone-python-import-")
        os.chmod(directory, 0o700)
        receipt_path = os.path.join(directory, RECEIPT_FILENAME)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        descriptor = os.open(receipt_path, flags, 0o600)
        try:
            if hasattr(os, "fchmod"):
                os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
        os.chmod(receipt_path, 0o600)
        if os.name == "posix":
            os.chmod(directory, 0o500)
    except OSError:
        _remove_incomplete_workspace(directory, receipt_path)
        return None
    return ProbeWorkspace(directory=directory, receipt_path=receipt_path)


def _remove_incomplete_workspace(directory: str | None, receipt_path: str | None) -> None:
    if receipt_path is not None:
        try:
            os.unlink(receipt_path)
        except OSError:
            pass
    if directory is not None:
        try:
            os.rmdir(directory)
        except OSError:
            pass


def receipt_matches(workspace: ProbeWorkspace, expected: bytes) -> bool:
    """Read at most one byte beyond the receipt and reject inode swaps."""

    flags = os.O_RDONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(workspace.receipt_path, flags)
        try:
            metadata = os.fstat(descriptor)
            owner_matches = not hasattr(os, "geteuid") or metadata.st_uid == os.geteuid()
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or not owner_matches:
                return False
            observed = os.read(descriptor, len(expected) + 1)
        finally:
            os.close(descriptor)
    except OSError:
        return False
    return observed == expected


def cleanup_probe_workspace(workspace: ProbeWorkspace) -> bool:
    """Perform constant-count cleanup without target-controlled walks."""

    clean = True
    try:
        os.chmod(workspace.directory, 0o700)
    except OSError:
        clean = False
    try:
        os.unlink(workspace.receipt_path)
    except FileNotFoundError:
        pass
    except OSError:
        clean = False
    try:
        os.rmdir(workspace.directory)
    except OSError:
        clean = False
    return clean


__all__ = [
    "ProbeWorkspace",
    "cleanup_probe_workspace",
    "create_probe_workspace",
    "receipt_matches",
]

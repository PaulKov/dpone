"""Explicit shared-cache modes independent of the container umask.

Directory mode policy is ownership-aware:

* inodes owned by the effective UID are mutated to the exact target mode so
  process-created cache trees stay deterministic across Airflow components;
* foreign-owned inodes (typical CSI/PVC mount roots owned by uid 0) are never
  chmod/chown'd — the runtime validates an access contract instead.

Shared cache roots use a minimum-bit contract: every required shared bit must
already be present. Extra bits (for example world-write on a 02777 volume root)
are outside process control and do not violate the shared-cache contract.
Private and shared-work directories keep an exact contract so a world-writable
foreign mount cannot silently pass as a private work tree.
"""

from __future__ import annotations

import errno
import os
import stat
from enum import Enum
from pathlib import Path

SHARED_DIRECTORY_MODE = 0o2775
SHARED_WORK_DIRECTORY_MODE = 0o2770
SHARED_CONTROL_MODE = 0o664
SHARED_WORK_CONTROL_MODE = 0o660
PRIVATE_DIRECTORY_MODE = 0o700
PRIVATE_CONTROL_MODE = 0o600


class ForeignDirectoryContract(Enum):
    """How a foreign-owned directory must relate to the target mode."""

    EXACT = "exact"
    MINIMUM = "minimum"


def ensure_shared_directory(path: Path) -> None:
    """Create a non-secret cache directory readable across Airflow UIDs."""

    path.mkdir(parents=True, exist_ok=True)
    _ensure_directory_mode(
        path,
        SHARED_DIRECTORY_MODE,
        foreign_contract=ForeignDirectoryContract.MINIMUM,
    )


def ensure_private_directory(path: Path) -> None:
    """Create one writer-private staging/control directory."""

    path.mkdir(parents=True, exist_ok=True)
    _ensure_directory_mode(
        path,
        PRIVATE_DIRECTORY_MODE,
        foreign_contract=ForeignDirectoryContract.EXACT,
    )


def ensure_shared_work_directory(path: Path) -> None:
    """Create a group-managed cache work directory hidden from other users."""

    path.mkdir(parents=True, exist_ok=True)
    _ensure_directory_mode(
        path,
        SHARED_WORK_DIRECTORY_MODE,
        foreign_contract=ForeignDirectoryContract.EXACT,
    )


def ensure_directory_mode(path: Path, mode: int) -> None:
    """Apply one directory mode through a no-follow descriptor."""

    _ensure_directory_mode(
        path,
        mode,
        foreign_contract=ForeignDirectoryContract.EXACT,
    )


def ensure_control_file_mode(descriptor: int, mode: int) -> None:
    """Set an explicit control-file mode without chmod of an already-correct inode."""

    if stat.S_IMODE(os.fstat(descriptor).st_mode) != mode:
        os.fchmod(descriptor, mode)


def directory_meets_mode_contract(
    current_mode: int,
    required_mode: int,
    *,
    foreign_contract: ForeignDirectoryContract,
) -> bool:
    """Return whether ``current_mode`` satisfies the directory access contract."""

    current = stat.S_IMODE(current_mode)
    required = stat.S_IMODE(required_mode)
    if foreign_contract == ForeignDirectoryContract.EXACT:
        return current == required
    if foreign_contract == ForeignDirectoryContract.MINIMUM:
        return (current & required) == required
    raise ValueError(f"unsupported foreign directory contract: {foreign_contract!r}")


def _ensure_directory_mode(
    path: Path,
    mode: int,
    *,
    foreign_contract: ForeignDirectoryContract,
) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISDIR(metadata.st_mode):
            raise NotADirectoryError(path)
        owned = _inode_owned_by_effective_user(metadata)
        if owned and mode & stat.S_ISGID and metadata.st_gid not in _effective_groups():
            os.fchown(descriptor, -1, os.getegid())
            metadata = os.fstat(descriptor)
        current_mode = stat.S_IMODE(metadata.st_mode)
        if current_mode == mode:
            return
        if owned:
            os.fchmod(descriptor, mode)
            return
        if directory_meets_mode_contract(
            current_mode,
            mode,
            foreign_contract=foreign_contract,
        ):
            return
        raise OSError(
            errno.EPERM,
            (
                f"foreign-owned directory {path.as_posix()} mode {oct(current_mode)} "
                f"does not satisfy {foreign_contract.value} contract {oct(mode)}"
            ),
        )
    finally:
        os.close(descriptor)


def _inode_owned_by_effective_user(metadata: os.stat_result) -> bool:
    return metadata.st_uid == os.geteuid()


def _effective_groups() -> frozenset[int]:
    return frozenset((os.getegid(), *os.getgroups()))


__all__ = [
    "ForeignDirectoryContract",
    "PRIVATE_CONTROL_MODE",
    "PRIVATE_DIRECTORY_MODE",
    "SHARED_CONTROL_MODE",
    "SHARED_DIRECTORY_MODE",
    "SHARED_WORK_CONTROL_MODE",
    "SHARED_WORK_DIRECTORY_MODE",
    "ensure_control_file_mode",
    "ensure_directory_mode",
    "ensure_private_directory",
    "ensure_shared_directory",
    "ensure_shared_work_directory",
]

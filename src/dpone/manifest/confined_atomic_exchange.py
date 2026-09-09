"""Certified native atomic name exchange for confined sibling files."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.confined_files import ConfinedFileSnapshot


import ctypes
import errno
import os
import secrets
import sys
from collections.abc import Callable
from dataclasses import dataclass

from dpone.manifest.confined_files import ConfinedFileError, ConfinedFileIdentity, read_confined_leaf

AtomicExchange = Callable[[int, str, str], None]

_LINUX_RENAME_EXCHANGE = 0x00000002
_MACOS_RENAME_SWAP = 0x00000002


class AtomicExchangeUnsupported(OSError):
    """The current platform has no certified atomic name-exchange adapter."""

    def __init__(self, message: str) -> None:
        super().__init__(errno.ENOTSUP, message)


@dataclass(frozen=True, slots=True)
class ExchangeBackOutcome:
    """Artifacts left after restoring a source whose displaced digest mismatched."""

    recovery_name: str | None = None
    cleanup_required: bool = False


def get_native_atomic_exchange(*, platform: str | None = None) -> AtomicExchange:
    """Resolve one certified adapter without mutating the filesystem namespace."""

    selected_platform = platform or sys.platform
    if selected_platform.startswith("linux"):
        return _libc_exchange("renameat2", flags=_LINUX_RENAME_EXCHANGE)
    if selected_platform == "darwin":
        return _libc_exchange("renameatx_np", flags=_MACOS_RENAME_SWAP)
    raise AtomicExchangeUnsupported("Atomic file exchange is unsupported on this platform.")


def _libc_exchange(function_name: str, *, flags: int) -> AtomicExchange:
    library = ctypes.CDLL(None, use_errno=True)
    try:
        rename = getattr(library, function_name)
    except AttributeError as exc:
        raise AtomicExchangeUnsupported("The platform atomic file-exchange function is unavailable.") from exc
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int

    def exchange(parent_fd: int, left_name: str, right_name: str) -> None:
        left = _leaf_bytes(left_name)
        right = _leaf_bytes(right_name)
        result = rename(parent_fd, left, parent_fd, right, flags)
        if result != 0:
            error_number = ctypes.get_errno()
            raise OSError(error_number, "Atomic file exchange failed.")

    return exchange


def exchange_back(
    parent_fd: int,
    *,
    target_name: str,
    exchange_name: str,
    displaced_source: ConfinedFileSnapshot,
    desired_sha256: str,
    desired_identity: ConfinedFileIdentity | None,
    atomic_exchange: AtomicExchange,
    max_bytes: int,
) -> ExchangeBackOutcome:
    """Restore the displaced source and quarantine any third pathname winner."""

    atomic_exchange(parent_fd, target_name, exchange_name)
    sync_directory(parent_fd)
    restored = read_confined_leaf(parent_fd, target_name, max_bytes=max_bytes)
    if not _same_file(restored, displaced_source):
        raise OSError("Exchanged source was not restored.")
    try:
        candidate = read_confined_leaf(parent_fd, exchange_name, max_bytes=max_bytes)
    except ConfinedFileError:
        return _preserve_unknown(parent_fd, exchange_name, atomic_exchange=atomic_exchange)
    owned = candidate.sha256 == desired_sha256
    if desired_identity is not None:
        owned = owned and same_identity(candidate.identity, desired_identity)
    if owned:
        remove_snapshot(parent_fd, exchange_name, candidate)
        return ExchangeBackOutcome()
    return _preserve_unknown(parent_fd, exchange_name, atomic_exchange=atomic_exchange)


def remove_snapshot(parent_fd: int, name: str, snapshot: ConfinedFileSnapshot) -> None:
    """Unlink only the same regular snapshot that was read immediately before cleanup."""

    try:
        current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if ConfinedFileIdentity.from_stat(current) != snapshot.identity:
        raise OSError("Confined cleanup target changed.")
    os.unlink(name, dir_fd=parent_fd)
    sync_directory(parent_fd)


def leaf_exists(parent_fd: int, name: str) -> bool:
    try:
        os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return True


def leaf_matches_snapshot(
    parent_fd: int,
    name: str,
    *,
    expected: ConfinedFileSnapshot,
    max_bytes: int,
) -> bool:
    """Check that one published leaf still has the prepared identity and digest."""

    try:
        observed = read_confined_leaf(parent_fd, name, max_bytes=max_bytes)
    except ConfinedFileError:
        return False
    return observed.sha256 == expected.sha256 and same_identity(observed.identity, expected.identity)


def restore_quarantined_leaf(parent_fd: int, quarantine_name: str, target_name: str) -> bool:
    """Restore one quarantined inode without overwriting a concurrent winner."""

    try:
        os.link(
            quarantine_name,
            target_name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
            follow_symlinks=False,
        )
    except OSError:
        return False
    try:
        os.unlink(quarantine_name, dir_fd=parent_fd)
        sync_directory(parent_fd)
    except OSError:
        return False
    return True


def same_identity(left: ConfinedFileIdentity, right: ConfinedFileIdentity) -> bool:
    return left.device == right.device and left.inode == right.inode and left.size == right.size


def _same_file(left: ConfinedFileSnapshot, right: ConfinedFileSnapshot) -> bool:
    return left.sha256 == right.sha256 and same_identity(left.identity, right.identity)


def _preserve_unknown(
    parent_fd: int,
    name: str,
    *,
    atomic_exchange: AtomicExchange,
) -> ExchangeBackOutcome:
    recovery_name, marker = _create_marker(parent_fd)
    try:
        atomic_exchange(parent_fd, name, recovery_name)
        sync_directory(parent_fd)
    except Exception:
        _remove_marker_best_effort(parent_fd, recovery_name, marker)
        raise
    try:
        marker_snapshot = read_confined_leaf(parent_fd, name, max_bytes=0)
        if not same_identity(marker_snapshot.identity, marker):
            raise OSError("Recovery marker changed before cleanup.")
        remove_snapshot(parent_fd, name, marker_snapshot)
    except Exception:
        return ExchangeBackOutcome(recovery_name=recovery_name, cleanup_required=True)
    return ExchangeBackOutcome(recovery_name=recovery_name)


def _create_marker(parent_fd: int) -> tuple[str, ConfinedFileIdentity]:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    for _ in range(16):
        name = f".dpone-recovery-{secrets.token_hex(16)}"
        try:
            descriptor = os.open(name, flags, 0o600, dir_fd=parent_fd)
        except FileExistsError:
            continue
        try:
            os.fsync(descriptor)
            identity = ConfinedFileIdentity.from_stat(os.fstat(descriptor))
        finally:
            os.close(descriptor)
        sync_directory(parent_fd)
        return name, identity
    raise OSError("A unique recovery leaf could not be reserved.")


def _remove_marker_best_effort(parent_fd: int, name: str, marker: ConfinedFileIdentity) -> None:
    try:
        current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if same_identity(ConfinedFileIdentity.from_stat(current), marker):
            os.unlink(name, dir_fd=parent_fd)
            sync_directory(parent_fd)
    except OSError:
        return


def sync_directory(parent_fd: int) -> None:
    os.fsync(parent_fd)


def _leaf_bytes(name: str) -> bytes:
    if not isinstance(name, str) or not name or name in {".", ".."} or "/" in name or "\\" in name or "\0" in name:
        raise ValueError("Atomic exchange names must be sibling leaf names.")
    return os.fsencode(name)


__all__ = [
    "AtomicExchange",
    "AtomicExchangeUnsupported",
    "ExchangeBackOutcome",
    "exchange_back",
    "get_native_atomic_exchange",
    "leaf_exists",
    "leaf_matches_snapshot",
    "remove_snapshot",
    "restore_quarantined_leaf",
    "same_identity",
    "sync_directory",
]

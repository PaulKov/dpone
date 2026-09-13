"""Hold an administrator-installed bootstrap original through startup admission.

This read-only primitive authenticates protected filesystem custody, not policy,
enrollment or readiness semantics. The startup owner validates those documents
before its final require_current call and retains one fixed deadline throughout.
"""

from __future__ import annotations

import math
import os
import stat
import time
from collections.abc import Callable
from contextlib import ExitStack
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import Any

from dpone.adapters.composition_supervisor_filesystem import open_protected
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

_MAX_BYTES = 1024 * 1024
_STABLE = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")


def _failure() -> CompositionAdmissionError:
    return CompositionAdmissionError("dispatcher_bootstrap_original")


def _identity(info: os.stat_result) -> tuple[int, ...]:
    return tuple(getattr(info, name) for name in _STABLE)


def _require_info(info: Any, gid: int, *, directory: bool) -> None:
    kind = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    access = 0o050 if directory else 0o040
    if (
        not kind
        or info.st_uid != 0
        or info.st_gid != gid
        or info.st_mode & 0o027
        or info.st_mode & access != access
        or (not directory and (info.st_nlink != 1 or not 0 < info.st_size <= _MAX_BYTES))
    ):
        raise _failure()


def _parent(path: Path, gid: int, check: Callable[[], None]) -> int:
    parent = None
    try:
        check()
        root = open_protected(path.parent.parent, traversable=False, require_current=check)
        try:
            check()
            _require_info(os.fstat(root), gid, directory=True)
            check()
            parent = os.open(path.parent.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root)
            check()
            _require_info(os.fstat(parent), gid, directory=True)
            check()
        finally:
            os.close(root)
        return parent
    except BaseException:
        if parent is not None:
            os.close(parent)
        raise


def _read(descriptor: int, check: Callable[[], None]) -> bytes:
    chunks: list[bytes] = []
    offset = 0
    while offset <= _MAX_BYTES:
        check()
        chunk = os.pread(descriptor, min(65536, _MAX_BYTES + 1 - offset), offset)
        check()
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
    if offset > _MAX_BYTES:
        raise _failure()
    return b"".join(chunks)


class HeldDispatcherBootstrap:
    """Owned immutable bytes and descriptors; close explicitly on every exit.

    Failed revalidation closes the owner permanently. Closure is attempted even
    after deadline expiry and attempts both descriptors if one close fails.
    """

    def __init__(
        self,
        path: Path,
        gid: int,
        parent: int,
        descriptor: int,
        document: bytes,
        parent_identity: tuple[int, ...],
        identity: tuple[int, ...],
        check: Callable[[], None],
    ) -> None:
        self._path, self._gid = path, gid
        self._parent, self._descriptor = parent, descriptor
        self._document = document
        self._parent_identity, self._identity = parent_identity, identity
        self._check = check
        self._closed = False

    @property
    def document(self) -> bytes:
        return self._document

    @property
    def sha256(self) -> str:
        return "sha256:" + sha256(self._document).hexdigest()

    def require_current(self) -> None:
        """Reopen protected ancestry and compare path, held inode and exact bytes."""
        try:
            if self._closed:
                raise _failure()
            self._check()
            with ExitStack() as stack:
                current = _parent(self._path, self._gid, self._check)
                stack.callback(os.close, current)
                for descriptor in (current, self._parent):
                    self._check()
                    info = os.fstat(descriptor)
                    self._check()
                    _require_info(info, self._gid, directory=True)
                    if _identity(info) != self._parent_identity:
                        raise _failure()
                for parent in (current, self._parent):
                    self._check()
                    info = os.stat(self._path.name, dir_fd=parent, follow_symlinks=False)
                    self._check()
                    _require_info(info, self._gid, directory=False)
                    if _identity(info) != self._identity:
                        raise _failure()
                self._check()
                if _identity(os.fstat(self._descriptor)) != self._identity:
                    raise _failure()
                self._check()
                if _read(self._descriptor, self._check) != self._document:
                    raise _failure()
                self._check()
                if _identity(os.fstat(self._descriptor)) != self._identity:
                    raise _failure()
                self._check()
                final_parent = _parent(self._path, self._gid, self._check)
                stack.callback(os.close, final_parent)
                self._check()
                if _identity(os.fstat(final_parent)) != self._parent_identity:
                    raise _failure()
                self._check()
                if _identity(os.stat(self._path.name, dir_fd=final_parent, follow_symlinks=False)) != self._identity:
                    raise _failure()
                self._check()
            self._check()
        except BaseException as exc:
            try:
                self.close()
            except Exception:
                pass
            if not isinstance(exc, Exception):
                raise
            raise _failure() from None

    def close(self) -> None:
        """Release both owned descriptors once, without consulting the deadline."""
        if self._closed:
            return
        self._closed = True
        failed = False
        for descriptor in (self._descriptor, self._parent):
            try:
                os.close(descriptor)
            except OSError:
                failed = True
        if failed:
            raise _failure()


def open_dispatcher_bootstrap(
    path: Path,
    *,
    dispatcher_gid: int,
    deadline: float,
    clock: Callable[[], float] = time.monotonic,
) -> HeldDispatcherBootstrap | None:
    """Return None only for an absent leaf beneath verified protected parents.

    Present partial, noncanonical or unsafe files are terminal errors. This
    function never waits, creates files, follows links or renews the deadline.
    """
    owner = None
    try:
        if type(dispatcher_gid) is not int or not 0 < dispatcher_gid < 2**31:
            raise _failure()
        if type(deadline) not in (int, float) or not math.isfinite(deadline) or not callable(clock):
            raise _failure()
        text = str(path)
        if (
            not isinstance(path, Path)
            or len(path.parts) < 3
            or not path.is_absolute()
            or text.startswith("//")
            or str(PurePosixPath(text)) != text
            or ".." in path.parts
            or len(text) > 4096
            or any(ord(char) < 32 or ord(char) == 127 for char in text)
        ):
            raise _failure()

        def check() -> None:
            now = clock()
            if type(now) not in (int, float) or not math.isfinite(now) or now >= deadline:
                raise _failure()

        with ExitStack() as stack:
            parent = _parent(path, dispatcher_gid, check)
            stack.callback(os.close, parent)
            check()
            parent_identity = _identity(os.fstat(parent))
            check()
            try:
                descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            except FileNotFoundError:
                check()
                current = _parent(path, dispatcher_gid, check)
                stack.callback(os.close, current)
                check()
                if _identity(os.fstat(current)) != parent_identity or _identity(os.fstat(parent)) != parent_identity:
                    raise _failure()
                check()
            else:
                stack.callback(os.close, descriptor)
                check()
                info = os.fstat(descriptor)
                check()
                _require_info(info, dispatcher_gid, directory=False)
                original = _read(descriptor, check)
                if len(original) != info.st_size or canonical_json_bytes(strict_json_object(original)) != original:
                    raise _failure()
                check()
                owner = HeldDispatcherBootstrap(
                    path, dispatcher_gid, parent, descriptor, original, parent_identity, _identity(info), check
                )
                stack.pop_all()
        check()
        if owner is None:
            return None
        owner.require_current()
        return owner
    except BaseException as exc:
        if owner is not None:
            try:
                owner.close()
            except Exception:
                pass
        if not isinstance(exc, Exception):
            raise
        raise _failure() from None

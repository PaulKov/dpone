"""Single-host POSIX exclusion tied to protected authority and subject identity.

Persistent lock files must never be removed during normal operation. This is
not fencing against the owning OS user or a copied/restored authority volume.
Process exit releases only the local mutex, never durable target ownership.
"""

from __future__ import annotations

import os
import stat
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from dpone.contracts.clickhouse_authority import (
    AuthorityConflict,
    AuthorityError,
    AuthorityStorageIdentity,
    OperationBinding,
)
from dpone.ports.clickhouse_publication_transport import ExecutionSession, PublicationAuthority


def _file_identity(info: os.stat_result) -> tuple[int, int]:
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_nlink != 1:
        raise AuthorityError("Execution lock must be a private owned regular file")
    return info.st_dev, info.st_ino


def _open_lock(path: Path) -> int:
    flags = os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC
    try:
        descriptor = os.open(path, flags | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return os.open(path, flags)
    try:
        os.fsync(descriptor)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


class _LocalSession:
    def __init__(
        self,
        authority: PublicationAuthority,
        identity: AuthorityStorageIdentity,
        binding: OperationBinding,
        path: Path,
        descriptor: int,
    ) -> None:
        self._authority, self._identity, self._binding = authority, identity, binding
        self._path, self._descriptor = path, descriptor
        self._pid, self._thread = os.getpid(), threading.get_ident()
        self._active = True
        self._file = _file_identity(os.fstat(descriptor))

    def assert_current(self) -> None:
        """Reject stale/inherited sessions before using a dispatch capability."""
        if not self._active or self._pid != os.getpid() or self._thread != threading.get_ident():
            raise AuthorityError("Execution session is not current in this invocation")
        try:
            if (
                self._authority.execution_identity() != self._identity
                or self._authority.binding(self._binding.operation_id) != self._binding
                or _file_identity(os.fstat(self._descriptor)) != self._file
                or _file_identity(self._path.lstat()) != self._file
            ):
                raise AuthorityError("Execution authority or lock identity changed")
        except OSError:
            raise AuthorityError("Execution authority or lock unavailable") from None


class LocalPublicationExclusion:
    """Nonblocking exclusion with a fresh open file description for every hold."""

    def __init__(self, authority: PublicationAuthority) -> None:
        self._authority = authority

    @contextmanager
    def hold(self, operation_id: str) -> Iterator[ExecutionSession]:
        """Acquire the original subject's lock; conflict never retries implicitly."""
        if os.name != "posix":
            raise AuthorityError("Publication execution requires local POSIX storage")
        import fcntl

        identity = self._authority.execution_identity()
        binding = self._authority.binding(operation_id)
        if identity.deployment_id != binding.subject.deployment_id:
            raise AuthorityError("Execution authority deployment mismatch")
        path = Path(identity.path + f".execution-{binding.subject.key}.lock")
        descriptor, session = None, None
        acquired = False
        pid = os.getpid()
        try:
            descriptor = _open_lock(path)
            session = _LocalSession(self._authority, identity, binding, path, descriptor)
            session.assert_current()
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise AuthorityConflict("Target publication execution is busy") from None
            acquired = True
            session.assert_current()
            yield session
        except OSError:
            raise AuthorityError("Publication execution lock unavailable") from None
        finally:
            if session is not None:
                session._active = False
            if descriptor is not None:
                try:
                    # A fork shares the open file description: child unlock would
                    # unlock the parent's mutex. Child may only close its copy.
                    if acquired and pid == os.getpid():
                        fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)

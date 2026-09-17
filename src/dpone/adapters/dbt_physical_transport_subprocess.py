"""POSIX one-use anonymous read-only delivery, without another process runner.

The application must supply an authenticated producer and held profile custody.
This resource validates representation and OS ownership, not policy authority.
"""

import os
import stat
import subprocess
from collections.abc import Callable
from pathlib import Path
from threading import Event, Lock
from time import monotonic_ns
from uuid import uuid4

from dpone.adapters.dbt_process_supervisor import ManagedProcess
from dpone.contracts.dbt_physical_transport_delivery import PhysicalTransportDelivery
from dpone.ports.dbt_physical_transport import PhysicalTransportLaunchContext

_WAIT_POLL_SECONDS = 0.05


def open_physical_transport_directory(path: Path) -> int:
    """Walk an absolute directory without following any symlink component."""
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("physical launch requires an absolute held profile directory")
    fd = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in path.parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        status = os.fstat(fd)
        if status.st_uid != os.getuid() or status.st_mode & 0o077:
            raise ValueError("physical profile directory must be private and owned by the current user")
        return fd
    except BaseException:
        os.close(fd)
        raise


def physical_transport_spawn_settings(
    environment: dict[str, str],
    context: PhysicalTransportLaunchContext | None,
) -> tuple[dict[str, str], dict[str, object]]:
    """Add only the reserved inherited descriptor to an isolated environment."""

    if context is None:
        return environment, {}
    if set(environment).intersection(context.environment):
        raise ValueError("physical transport environment collides with the isolated dbt context")
    return {**environment, **context.environment}, {"pass_fds": context.pass_fds}


def wait_for_physical_transport_process(
    process: ManagedProcess,
    *,
    context: PhysicalTransportLaunchContext,
    timeout_seconds: int,
    executable: str,
    monotonic_ns_clock: Callable[[], int] = monotonic_ns,
) -> int:
    """Wait within both command and authenticated absolute bounds.

    Cancellation and expiry are surfaced as the runner's existing timeout path,
    which owns process-tree cleanup.  This helper never terminates, retries or
    replaces the process itself.
    """

    deadline = min(
        monotonic_ns_clock() + timeout_seconds * 1_000_000_000,
        context.deadline_monotonic_ns,
    )
    while True:
        remaining_ns = deadline - monotonic_ns_clock()
        if context.cancellation.is_set() or remaining_ns <= 0:
            raise subprocess.TimeoutExpired((executable,), timeout_seconds)
        try:
            return process.wait(timeout=min(_WAIT_POLL_SECONDS, remaining_ns / 1_000_000_000))
        except subprocess.TimeoutExpired:
            continue


class PhysicalTransportLaunchLease:
    """Consume one packet once, closing the parent's copy immediately after spawn.

    Use ``with lease as launch: Popen(..., close_fds=True,
    pass_fds=launch.pass_fds, env=isolated_env | launch.environment)``. The existing
    runner owns waits, absolute deadlines, cancellation and supervisor cleanup.
    Enter failure is terminal; no file or descriptor is retained for retry.
    """

    def __init__(
        self,
        *,
        delivery: PhysicalTransportDelivery,
        profile_directory: Path,
        cancellation: Event,
        clock: Callable[[], int] = monotonic_ns,
    ) -> None:
        if type(delivery) is not PhysicalTransportDelivery:
            raise ValueError("physical launch requires exact validated delivery bytes")
        delivery.__post_init__()
        self.delivery = delivery
        self._path, self._cancel, self._clock = profile_directory, cancellation, clock
        self._lock, self._consumed = Lock(), False
        self._fd: int | None = None

    def _check(self) -> None:
        if self._cancel.is_set() or self._clock() >= self.delivery.deadline_monotonic_ns:
            raise ValueError("physical delivery cancelled or expired before spawn")

    def __enter__(self) -> PhysicalTransportLaunchContext:
        with self._lock:
            if self._consumed:
                raise ValueError("physical delivery launch is already consumed")
            self._consumed = True
            self._check()
            if os.name != "posix":
                raise ValueError("physical descriptor delivery requires POSIX")
            directory = open_physical_transport_directory(self._path)
            try:
                self._fd = self._create(directory)
                self._check()
                return PhysicalTransportLaunchContext((self._fd,), self.delivery.deadline_monotonic_ns, self._cancel)
            except BaseException:
                self._close()
                raise
            finally:
                os.close(directory)

    def _create(self, directory: int) -> int:
        name = ".dpone-transport-" + uuid4().hex
        writer = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        created = os.fstat(writer)
        reader = None
        linked = True
        try:
            view = memoryview(self.delivery.payload)
            while view:
                written = os.write(writer, view)
                if written <= 0:
                    raise ValueError("physical delivery write incomplete")
                view = view[written:]
            os.fsync(writer)
            reader = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
            left, right = os.fstat(writer), os.fstat(reader)
            if (left.st_dev, left.st_ino, left.st_size) != (right.st_dev, right.st_ino, right.st_size):
                raise ValueError("physical delivery inode changed")
            if not stat.S_ISREG(right.st_mode) or right.st_uid != os.getuid() or right.st_nlink != 1:
                raise ValueError("physical delivery file custody changed")
            if os.read(reader, len(self.delivery.payload) + 1) != self.delivery.payload:
                raise ValueError("physical delivery bytes changed")
            os.fchmod(writer, 0o400)
            os.close(writer)
            writer = -1
            current = os.stat(name, dir_fd=directory, follow_symlinks=False)
            if (current.st_dev, current.st_ino) != (right.st_dev, right.st_ino):
                raise ValueError("physical delivery path changed")
            os.unlink(name, dir_fd=directory)
            linked = False
            if os.fstat(reader).st_nlink != 0:
                raise ValueError("physical delivery must be anonymous")
            os.lseek(reader, 0, os.SEEK_SET)
            result, reader = reader, None
            return result
        finally:
            if writer >= 0:
                os.close(writer)
            if reader is not None:
                os.close(reader)
            if linked:
                observed = os.stat(name, dir_fd=directory, follow_symlinks=False)
                if (observed.st_dev, observed.st_ino) != (created.st_dev, created.st_ino):
                    raise ValueError("physical delivery cleanup refused a substituted file")
                os.unlink(name, dir_fd=directory)

    def _close(self) -> None:
        fd, self._fd = self._fd, None
        if fd is not None:
            os.close(fd)

    def close(self) -> None:
        """Close once without suppressing an OS custody/close failure."""
        with self._lock:
            self._consumed = True
            self._close()

    def __exit__(self, *exc: object) -> None:
        self.close()

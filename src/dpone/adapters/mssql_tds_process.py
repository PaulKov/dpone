"""Linux pidfd containment for one TDS worker, without SQL success semantics.

An acquired handle binds a live kernel process to the persisted host/boot/start
identity. Recovery termination is deliberately distinct from direct-child reaping.
The supervisor must separately contain descendants, enforce parent death, validate
worker output and reconcile SQL. No numeric PID is ever used for signalling.
"""

from __future__ import annotations

import hashlib
import math
import os
import select
import signal
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.contracts.mssql_tds_worker_identity import TdsProcessIdentity


@dataclass(frozen=True)
class _AcquisitionFailure:
    """Original unauthenticated descriptor; rollback is never retried."""

    descriptor: int
    close_unknown: bool


class TdsProcessError(RuntimeError):
    """Containment remains unproven; replacement must remain blocked."""

    acquisition: _AcquisitionFailure | None = None


def _parse_stat(raw: str, pid: int) -> int:
    opening, closing = raw.find("("), raw.rfind(")")
    try:
        fields = raw[closing + 1 :].split()
        if opening < 1 or closing <= opening or int(raw[:opening].strip()) != pid:
            raise ValueError
        if len(fields) < 20 or fields[0] not in {"R", "S", "D", "Z", "T", "t", "X", "x", "K", "W", "P", "I"}:
            raise ValueError
        ticks = int(fields[19])
        if ticks < 0:
            raise ValueError
        return ticks
    except (ValueError, IndexError):
        raise TdsProcessError("tds_process_stat_invalid") from None


class _LinuxOps:
    """Private syscall seam; all filesystem and kernel access is invocation-time."""

    def admit(self) -> None:
        if sys.platform != "linux" or not all(
            (
                callable(getattr(os, "pidfd_open", None)),
                callable(getattr(signal, "pidfd_send_signal", None)),
                callable(getattr(os, "waitid", None)),
                hasattr(os, "P_PIDFD"),
            )
        ):
            raise TdsProcessError("mssql_native.tds_platform_unsupported")
        # A usable stable host/procfs identity is also a pre-extraction prerequisite.
        self.identity(os.getpid())
        # Probe kernel support/permission, not merely Python attribute presence.
        fd = self.open(os.getpid())
        try:
            self.signal(fd, 0)
            try:
                self.reap(fd)
            except ChildProcessError:
                pass  # Self is not our child; proves P_PIDFD is recognized.
        finally:
            self.close(fd)

    def identity(self, pid: int) -> TdsProcessIdentity:
        try:
            if int(os.readlink("/proc/self")) != os.getpid():
                raise ValueError
            machine = Path("/etc/machine-id").read_text(encoding="ascii").strip()
            if len(machine) != 32 or machine == "0" * 32 or any(c not in "0123456789abcdef" for c in machine):
                raise ValueError
            namespace = os.readlink("/proc/self/ns/pid")
            if namespace != os.readlink("/proc/1/ns/pid"):
                raise ValueError
            host = hashlib.sha256((machine + "\n" + namespace).encode("ascii")).hexdigest()
            boot = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
            raw = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
            return TdsProcessIdentity(host, boot, pid, _parse_stat(raw, pid))
        except (OSError, ValueError, UnicodeError):
            # ENOENT is not durable proof of death (procfs may be unavailable).
            raise TdsProcessError("tds_process_identity_unavailable") from None

    def open(self, pid: int) -> int:
        return int(getattr(os, "pidfd_open")(pid, 0))

    def signal(self, fd: int, sig: int) -> None:
        getattr(signal, "pidfd_send_signal")(fd, sig)

    def ready_until(self, fd: int, remaining: float) -> bool:
        poller = select.poll()
        poller.register(fd, select.POLLIN)
        events = poller.poll(int(min(remaining, 0.05) * 1000))
        for observed, flags in events:
            if observed != fd or flags & (select.POLLERR | select.POLLNVAL):
                raise TdsProcessError("tds_process_poll_unknown")
            if flags & select.POLLIN:
                return True
            raise TdsProcessError("tds_process_poll_unknown")
        return False

    def reap(self, fd: int) -> Any:
        return getattr(os, "waitid")(getattr(os, "P_PIDFD"), fd, os.WEXITED | os.WNOHANG)

    def close(self, fd: int) -> None:
        os.close(fd)

    def monotonic(self) -> float:
        return time.monotonic()


@dataclass(frozen=True)
class TdsProcessTermination:
    """Kernel exit observation; reaped is true only after successful child waitid."""

    identity: TdsProcessIdentity
    reaped: bool
    exit_code: int | None


class LinuxTdsProcess:
    """Owned pidfd. Call acquire before any signal, then close with a with block.

    deadline is an absolute monotonic timestamp shared across signal/wait/reap;
    callers must not allocate a fresh timeout at each containment phase. A failed
    operation provides no success receipt. The handle remains closable on failure.
    """

    def __init__(self, identity: TdsProcessIdentity, fd: int, ops: Any) -> None:
        self.identity = identity
        self._fd: int | None = fd
        self._original_fd = fd  # Audit token only; never usable after detach.
        self._ops = ops
        self._owner_pid = os.getpid()
        self._owner_thread = threading.current_thread()
        self._unknown = False
        self._close_unknown = False
        self._consumed_reap: TdsProcessTermination | None = None
        self._signalled = False

    @classmethod
    def admit(cls) -> None:
        """Check Linux/pidfd capability before acquiring source data."""
        try:
            _LinuxOps().admit()
        except OSError:
            raise TdsProcessError("mssql_native.tds_platform_unsupported") from None

    @classmethod
    def identify(cls, pid: int) -> TdsProcessIdentity:
        """Capture the current namespace-bound process incarnation."""
        if type(pid) is not int or not 0 < pid < 2**31:
            raise ValueError("tds_process_pid_invalid")
        cls.admit()
        return _LinuxOps().identity(pid)

    @classmethod
    def acquire(cls, identity: TdsProcessIdentity, *, _ops: Any = None) -> LinuxTdsProcess:
        if type(identity) is not TdsProcessIdentity or identity.pid == os.getpid():
            raise TdsProcessError("tds_process_identity_invalid")
        ops = _LinuxOps() if _ops is None else _ops
        fd = None
        try:
            ops.admit()
            if ops.identity(identity.pid) != identity:
                raise TdsProcessError("tds_process_identity_mismatch")
            fd = ops.open(identity.pid)
            if ops.identity(identity.pid) != identity:
                raise TdsProcessError("tds_process_identity_mismatch")
            return cls(identity, fd, ops)
        except BaseException as exc:
            failure = None
            if fd is not None:
                try:
                    ops.close(fd)
                    failure = _AcquisitionFailure(fd, False)
                except BaseException:
                    failure = _AcquisitionFailure(fd, True)
            if failure is not None or isinstance(exc, OSError):
                error = exc if isinstance(exc, TdsProcessError) else TdsProcessError("tds_process_acquisition_unknown")
                error.acquisition = failure
                raise error from None
            raise

    def contain(self, *, deadline: float, direct_child: bool) -> TdsProcessTermination:
        """SIGKILL the bound worker and prove exit; nonchildren are never reaped.

        direct_child=True requires exclusive wait ownership in the supervisor;
        ECHILD (including another thread reaping first) remains an unknown outcome.
        This is containment, not graceful cancellation or a process-tree proof.
        """
        return self._settle(deadline=deadline, direct_child=direct_child, kill=True)

    def wait(self, *, deadline: float, direct_child: bool) -> TdsProcessTermination:
        """Observe natural exit and reap without signalling the live worker."""
        return self._settle(deadline=deadline, direct_child=direct_child, kill=False)

    def _settle(self, *, deadline: float, direct_child: bool, kill: bool) -> TdsProcessTermination:
        return self._settle_effective(deadline=deadline, direct_child=direct_child, kill=kill)

    def _settle_effective(
        self,
        *,
        deadline: float,
        direct_child: bool,
        kill: bool,
        current_deadline: Callable[[], float] | None = None,
    ) -> TdsProcessTermination:
        """One syscall loop, optionally observing a synchronized shortening budget."""
        if type(deadline) not in (int, float) or not math.isfinite(deadline) or type(direct_child) is not bool:
            raise ValueError("tds_process_deadline_invalid")
        if self._fd is None or self._owner_pid != os.getpid() or self._owner_thread is not threading.current_thread():
            raise TdsProcessError("tds_process_handle_unavailable")
        if self._unknown:
            raise TdsProcessError("tds_process_containment_unknown")

        def remaining() -> float:
            nonlocal deadline
            if current_deadline is not None:
                supplied = current_deadline()
                if type(supplied) not in (int, float) or not math.isfinite(supplied):
                    raise TdsProcessError("tds_process_deadline_invalid")
                deadline = min(deadline, supplied)
            value = deadline - self._ops.monotonic()
            if value <= 0:
                raise TdsProcessError("tds_process_deadline_exceeded")
            return value

        try:
            remaining()
            if self._consumed_reap is not None:
                return self._consumed_reap
            if kill and not self._signalled:
                self._signalled = True
                try:
                    self._ops.signal(self._fd, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            while True:
                duration = remaining()
                ready = self._ops.ready_until(self._fd, min(duration, 0.01) if current_deadline else duration)
                remaining()
                if not ready:
                    continue
                if not direct_child:
                    return TdsProcessTermination(self.identity, False, None)
                remaining()
                result = self._ops.reap(self._fd)
                if result is not None:
                    # waitid consumed the kernel receipt even if validation or time
                    # checks subsequently fail. Never reap that child a second time.
                    self._unknown = True
                    if result.si_pid != self.identity.pid or result.si_code not in (1, 2, 3):
                        raise TdsProcessError("tds_process_reap_invalid")
                    code = result.si_status if result.si_code == 1 else -result.si_status
                    self._consumed_reap = TdsProcessTermination(self.identity, True, code)
                    remaining()
                    self._unknown = False
                    return self._consumed_reap
                remaining()
        except BaseException as exc:
            if kill or self._consumed_reap is not None:
                self._unknown = True
            if isinstance(exc, OSError):
                raise TdsProcessError("tds_process_containment_unknown") from None
            raise

    def close(self) -> None:
        """Release this descriptor once; closing does not assert containment."""
        if self._owner_pid != os.getpid() or self._owner_thread is not threading.current_thread():
            raise TdsProcessError("tds_process_handle_unavailable")
        if self._close_unknown:
            raise TdsProcessError("tds_process_close_unknown")
        if self._fd is not None:
            fd, self._fd = self._fd, None
            self._close_unknown = True
            self._ops.close(fd)
            self._close_unknown = False

    def __enter__(self) -> LinuxTdsProcess:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

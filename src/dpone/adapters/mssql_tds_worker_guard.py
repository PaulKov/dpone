"""Install Linux safety guards exclusively inside a newly spawned TDS child.

The fixed bootstrap calls this before reading credentials or importing optional
packages. Failure requires child exit, not retry in the partially guarded process.
PDEATHSIG follows the creating parent thread. A matching parent PID does NOT prove
that thread is alive: credentials must remain gated by supervisor registration and
its startup deadline, including death before PDEATHSIG was installed.
"""

from __future__ import annotations

import ctypes
import os
import signal
import sys
from dataclasses import dataclass
from typing import Any


class TdsWorkerGuardError(RuntimeError):
    """Child startup failed before authority to consume credentials was granted."""


class _LinuxGuardOps:
    """Invocation-time libc/resource access; private deterministic test seam."""

    def __init__(self) -> None:
        self._libc = ctypes.CDLL(None, use_errno=True)
        self._prctl = self._libc.prctl
        self._prctl.restype = ctypes.c_int

    def parent(self) -> int:
        return os.getppid()

    def set_death(self) -> None:
        if (
            self._prctl(
                ctypes.c_int(1),
                ctypes.c_ulong(signal.SIGKILL),
                ctypes.c_ulong(0),
                ctypes.c_ulong(0),
                ctypes.c_ulong(0),
            )
            != 0
        ):
            raise TdsWorkerGuardError("tds_worker_pdeathsig_set_failed")

    def get_death(self) -> int:
        value = ctypes.c_int()
        if (
            self._prctl(
                ctypes.c_int(2),
                ctypes.byref(value),
                ctypes.c_ulong(0),
                ctypes.c_ulong(0),
                ctypes.c_ulong(0),
            )
            != 0
        ):
            raise TdsWorkerGuardError("tds_worker_pdeathsig_get_failed")
        return value.value

    def get_limits(self) -> tuple[int, int]:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        return (
            int(soft) if soft != resource.RLIM_INFINITY else -1,
            int(hard) if hard != resource.RLIM_INFINITY else -1,
        )

    def set_limits(self, limits: tuple[int, int]) -> None:
        import resource

        resource.setrlimit(resource.RLIMIT_AS, limits)


@dataclass(frozen=True)
class TdsWorkerGuard:
    """Verified address-space ceilings, not RSS reservations or SQL evidence."""

    expected_parent_pid: int
    soft_bytes: int
    hard_bytes: int


def _bounded_limits(value: object, ceiling: int) -> tuple[int, int]:
    if type(value) is not tuple or len(value) != 2:
        raise TdsWorkerGuardError("tds_worker_limits_invalid")
    soft, hard = value
    if any(type(item) is not int or not -1 <= item < 2**63 for item in value):
        raise TdsWorkerGuardError("tds_worker_limits_invalid")
    if hard != -1 and (soft == -1 or soft > hard):
        raise TdsWorkerGuardError("tds_worker_limits_invalid")
    return (ceiling if soft == -1 else min(soft, ceiling), ceiling if hard == -1 else min(hard, ceiling))


def install_worker_guard(
    *,
    expected_parent_pid: int,
    max_address_space_bytes: int,
    _ops: Any = None,
) -> TdsWorkerGuard:
    """Install and verify guards before child bootstrap proceeds.

    Arguments receive technical validation only; authored transport policy remains
    parent-owned. Existing finite soft/hard limits are never increased. This
    mutates process-wide limits permanently and is not a parent-side preflight.
    It neither transfers credentials nor proves supervisor-thread liveness.
    """
    if type(expected_parent_pid) is not int or not 0 < expected_parent_pid < 2**31:
        raise ValueError("tds_worker_parent_pid_invalid")
    if type(max_address_space_bytes) is not int or not 0 < max_address_space_bytes < 2**63:
        raise ValueError("tds_worker_address_space_invalid")
    if sys.platform != "linux":
        raise TdsWorkerGuardError("mssql_native.tds_platform_unsupported")
    try:
        ops = _LinuxGuardOps() if _ops is None else _ops
        if ops.parent() != expected_parent_pid:
            raise TdsWorkerGuardError("tds_worker_parent_changed")
        ops.set_death()
        if ops.get_death() != signal.SIGKILL:
            raise TdsWorkerGuardError("tds_worker_pdeathsig_unverified")
        if ops.parent() != expected_parent_pid:
            raise TdsWorkerGuardError("tds_worker_parent_changed")
        requested = _bounded_limits(ops.get_limits(), max_address_space_bytes)
        ops.set_limits(requested)
        observed = ops.get_limits()
        if observed != requested or _bounded_limits(observed, max_address_space_bytes) != requested:
            raise TdsWorkerGuardError("tds_worker_limits_unverified")
        if ops.parent() != expected_parent_pid:
            raise TdsWorkerGuardError("tds_worker_parent_changed")
        return TdsWorkerGuard(expected_parent_pid, requested[0], requested[1])
    except TdsWorkerGuardError:
        raise
    except Exception:
        raise TdsWorkerGuardError("tds_worker_guard_os_failure") from None

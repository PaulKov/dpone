"""Bounded Linux child-group cleanup with an unreaped leader as identity anchor.

Only this lifecycle may reap its Popen child. Competing SIGCHLD handlers/waiters
are unsupported. Dedicated UID exclusivity and complete proc visibility remain
supervisor deployment requirements. Escaped descendants are detected, never
signalled by a scanned PID. Process closure is not database rollback evidence.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Additional cleanup wait/scan budget (OS calls may be uninterruptible): TERM 5s + KILL 5s + reap/UID 2s.
# The authenticated build deadline is unchanged. Every phase shares a final
# deadline; proc scans additionally cap entry count and individual file bytes.
TERM_GRACE = 5.0
KILL_GRACE = 5.0
FINAL_GRACE = 2.0
POLL_INTERVAL = 0.05
MAX_PROC_ENTRIES = 131072
MAX_PROC_BYTES = 65536


class ProcessLifecycleError(RuntimeError):
    """OS lifecycle could not establish safe process closure.

    The capture adapter translates these reasons into its public error contract;
    this module has no dependency on dbt artifacts, receipts, or SQL outcomes.
    """


@dataclass(frozen=True)
class ProcessExit:
    """Observed and reaped child identity with its operating-system exit code."""

    pid: int
    start_ticks: int
    exit_code: int


class ProcessScanDeadline(ProcessLifecycleError):
    """A finite scan exhausted its phase budget; it did not prove absence."""


@dataclass(frozen=True)
class ProcessIdentity:
    """Consistent proc identity; state is observational, not the PID anchor."""

    pid: int
    uid: int
    pgid: int
    start_ticks: int
    state: str


def require_waitid() -> None:
    """Reject unsupported hosts before launching the isolated child."""
    if (
        sys.platform != "linux"
        or not all(hasattr(os, name) for name in ("waitid", "P_PID", "WEXITED", "WNOHANG", "WNOWAIT"))
        or signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL
    ):
        raise ProcessLifecycleError("capture_process_wait_boundary")


def _read_bounded(path: Path) -> str:
    with path.open("rb") as stream:
        content = stream.read(MAX_PROC_BYTES + 1)
    if len(content) > MAX_PROC_BYTES:
        raise ProcessLifecycleError("capture_process_visibility")
    return content.decode("utf-8")


def read_process(pid: int) -> ProcessIdentity | None:
    """Bracket UID reads with stat identity checks; ambiguity blocks capture."""
    try:
        root = Path("/proc") / str(pid)
        before = _read_bounded(root / "stat").rsplit(")", 1)[1].split()
        status = _read_bounded(root / "status")
        after = _read_bounded(root / "stat").rsplit(")", 1)[1].split()
        uids = next(line for line in status.splitlines() if line.startswith("Uid:")).split()[1:]
        if len(uids) != 4 or len(set(uids)) != 1:
            raise ProcessLifecycleError("capture_process_uid")
        if (before[2], before[19]) != (after[2], after[19]):
            raise ProcessLifecycleError("capture_process_identity")
        return ProcessIdentity(pid, int(uids[0]), int(after[2]), int(after[19]), after[0])
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError, ValueError, IndexError, StopIteration):
        raise ProcessLifecycleError("capture_process_visibility") from None


class LinuxProcessOperations:
    """Narrow injectable OS boundary, keeping all waits and scans finite."""

    monotonic = staticmethod(time.monotonic)
    sleep = staticmethod(time.sleep)
    read = staticmethod(read_process)

    def inventory(self, deadline: float) -> list[ProcessIdentity]:
        observed = []
        try:
            with os.scandir("/proc") as entries:
                for count, entry in enumerate(entries):
                    if self.monotonic() >= deadline:
                        raise ProcessScanDeadline("capture_process_scan_deadline")
                    if count >= MAX_PROC_ENTRIES:
                        raise ProcessLifecycleError("capture_process_visibility")
                    if entry.name.isdecimal():
                        item = self.read(int(entry.name))
                        if item is not None:
                            observed.append(item)
            if self.monotonic() >= deadline:
                raise ProcessScanDeadline("capture_process_scan_deadline")
        except OSError:
            raise ProcessLifecycleError("capture_process_visibility") from None
        return observed

    @staticmethod
    def observe(process: subprocess.Popen[Any]) -> bool:
        if process.returncode is not None:
            raise ProcessLifecycleError("capture_process_ownership_lost")
        # Resolved only after require_waitid; macOS typing omits this Linux API.
        result = getattr(os, "waitid")(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
        if result is not None and result.si_pid != process.pid:
            raise ProcessLifecycleError("capture_process_identity")
        return result is not None

    def signal(self, process: subprocess.Popen[Any], identity: ProcessIdentity, signum: int) -> None:
        # waitid WNOWAIT keeps our child PID allocated even after leader exit.
        # No poll/wait/reap may occur before the last possible group signal.
        self.observe(process)
        current = self.read(process.pid)
        if (
            process.pid <= 1
            or identity.pgid != process.pid
            or identity.pgid == os.getpgrp()
            or current is None
            or (current.pid, current.uid, current.pgid, current.start_ticks)
            != (identity.pid, identity.uid, identity.pgid, identity.start_ticks)
        ):
            raise ProcessLifecycleError("capture_process_ownership_lost")
        try:
            os.killpg(identity.pgid, signum)
        except ProcessLookupError:
            # ESRCH is only a group observation, never UID quiescence proof.
            self.observe(process)

    @staticmethod
    def reap(process: subprocess.Popen[Any], timeout: float) -> int:
        return process.wait(timeout=timeout)


class LinuxDbtProcessLifecycle:
    """Own observation, last group signal, finite reap, and independent UID scan.

    Successful leader exit with residual UID processes also fails the invocation.
    Cleanup failure chains the original cancellation/timeout; no child exit is
    returned on either failure path. Zombie group members do not force KILL but
    any residual UID process, including zombies, blocks final quiescence.
    """

    def __init__(self, operations: LinuxProcessOperations | None = None):
        self._ops = operations if operations is not None else LinuxProcessOperations()

    def run(self, process: subprocess.Popen[Any], uid: int, timeout: float) -> ProcessExit:
        identity = None
        reaped = False
        try:
            identity = self._ops.read(process.pid)
            if identity is None or identity.uid != uid or identity.pgid != process.pid:
                raise ProcessLifecycleError("capture_process_identity")
            deadline = self._ops.monotonic() + timeout
            while not self._ops.observe(process):
                if self._ops.monotonic() >= deadline:
                    raise subprocess.TimeoutExpired(process.args, timeout)
                self._pause(deadline)
            # The leader remains unreaped while deciding whether cleanup is needed.
            final = self._ops.monotonic() + FINAL_GRACE
            if self._uid_present(uid, final, exclude=process.pid):
                raise ProcessLifecycleError("capture_child_not_quiescent")
            # From this point on no signal is legal, even if reap/observation fails.
            reaped = True
            code = self._ops.reap(process, self._remaining(final))
            self._wait_quiescent(uid, final)
            return ProcessExit(process.pid, identity.start_ticks, code)
        except BaseException as original:
            if reaped:
                raise
            try:
                if identity is None or identity.uid != uid or identity.pgid != process.pid:
                    raise ProcessLifecycleError("capture_process_ownership_lost")
                self._cleanup(process, identity, uid)
            except BaseException:
                raise ProcessLifecycleError("capture_cleanup_unresolved") from original
            raise

    def _cleanup(self, process: subprocess.Popen[Any], identity: ProcessIdentity, uid: int) -> None:
        final = self._ops.monotonic() + TERM_GRACE + KILL_GRACE + FINAL_GRACE
        self._ops.signal(process, identity, signal.SIGTERM)
        if not self._wait_group(process, identity.pgid, final - KILL_GRACE - FINAL_GRACE):
            self._ops.signal(process, identity, signal.SIGKILL)
            self._wait_group(process, identity.pgid, final - FINAL_GRACE)
        # Never signal after entering reap, including timeout/error paths.
        self._ops.reap(process, self._remaining(final))
        self._wait_quiescent(uid, final)

    def _wait_group(self, process: subprocess.Popen[Any], pgid: int, deadline: float) -> bool:
        while self._ops.monotonic() < deadline:
            self._ops.observe(process)
            try:
                members = self._ops.inventory(deadline)
            except ProcessScanDeadline:
                return False
            if not any(item.pgid == pgid and item.state not in {"Z", "X"} for item in members):
                return True
            self._pause(deadline)
        return False

    def _uid_present(self, uid: int, deadline: float, *, exclude: int | None = None) -> bool:
        return any(item.uid == uid and item.pid != exclude for item in self._ops.inventory(deadline))

    def _wait_quiescent(self, uid: int, deadline: float) -> None:
        while self._ops.monotonic() < deadline:
            if not self._uid_present(uid, deadline):
                return
            self._pause(deadline)
        raise ProcessLifecycleError("capture_child_not_quiescent")

    def _remaining(self, deadline: float) -> float:
        remaining = deadline - self._ops.monotonic()
        if remaining <= 0:
            raise ProcessLifecycleError("capture_cleanup_deadline")
        return remaining

    def _pause(self, deadline: float) -> None:
        # Phase expiry is a loop decision, not an observation failure. A clock
        # tick after inventory must still allow TERM -> KILL escalation.
        remaining = deadline - self._ops.monotonic()
        if remaining > 0:
            self._ops.sleep(min(POLL_INTERVAL, remaining))

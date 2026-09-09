#!/usr/bin/env python3
"""Bound Linux CI process trees with pidfds; exit 124 requires quiescence and 125 means proof failure."""

from __future__ import annotations

import argparse
import ctypes
import errno
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

EXIT_TIMEOUT = 124
EXIT_CONTAINMENT_FAILURE = 125
EXIT_NOT_EXECUTABLE = 126
EXIT_NOT_FOUND = 127

_PR_SET_CHILD_SUBREAPER = 36
_TERMINATION_SIGNALS = (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)
_NON_EXECUTABLE_STATES = frozenset({"X", "Z"})
_received_signal: int | None = None


class ContainmentError(RuntimeError): ...


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
    pid: int
    starttime: int


@dataclass(frozen=True, slots=True)
class ProcessStat:
    identity: ProcessIdentity
    state: str
    ppid: int
    process_group: int
    session: int


@dataclass(frozen=True, slots=True)
class SupervisorConfig:
    timeout_seconds: float
    term_grace_seconds: float = 30.0
    kill_grace_seconds: float = 5.0
    poll_interval_seconds: float = 0.05

    def validate(self) -> None:
        if not (
            self.timeout_seconds > 0
            and self.term_grace_seconds >= 0
            and self.kill_grace_seconds > 0
            and 0 < self.poll_interval_seconds <= 1
        ):
            raise ValueError("invalid bounded process-tree timing policy")


class Procfs:
    def __init__(self, root: Path = Path("/proc")) -> None:
        self._root = root

    def validate(self) -> ProcessStat:
        if sys.platform != "linux":
            raise ContainmentError("linux-required")
        if not callable(getattr(os, "pidfd_open", None)) or not callable(getattr(signal, "pidfd_send_signal", None)):
            raise ContainmentError("pidfd-required")
        own = self.read(os.getpid())
        if own is None or own.identity.pid != os.getpid():
            raise ContainmentError("procfs-unavailable")
        return own

    def read(self, pid: int) -> ProcessStat | None:
        path = self._root / str(pid) / "stat"
        try:
            raw = path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            if exc.errno in {errno.ENOENT, errno.ESRCH}:
                return None
            raise ContainmentError("procfs-read-failed") from exc
        try:
            return parse_process_stat(raw)
        except ValueError as exc:
            raise ContainmentError("procfs-stat-invalid") from exc

    def snapshot(self) -> dict[int, ProcessStat]:
        try:
            entries = tuple(self._root.iterdir())
        except OSError as exc:
            raise ContainmentError("procfs-scan-failed") from exc
        processes: dict[int, ProcessStat] = {}
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            process = self.read(int(entry.name))
            if process is not None:
                processes[process.identity.pid] = process
        if os.getpid() not in processes:
            raise ContainmentError("supervisor-missing-from-procfs")
        return processes


class ProcessTracker:
    def __init__(
        self,
        supervisor: ProcessIdentity,
        baseline_children: frozenset[ProcessIdentity],
    ) -> None:
        self._supervisor = supervisor
        self._baseline_children = baseline_children
        self._tracked: set[ProcessIdentity] = set()

    def discover(self, snapshot: dict[int, ProcessStat]) -> None:
        for process in snapshot.values():
            if (
                process.ppid == self._supervisor.pid
                and process.identity not in self._baseline_children
                and process.identity != self._supervisor
            ):
                self._tracked.add(process.identity)

        changed = True
        while changed:
            changed = False
            current_parent_pids = {
                identity.pid
                for identity in self._tracked
                if (observed := snapshot.get(identity.pid)) is not None and observed.identity == identity
            }
            for process in snapshot.values():
                if process.ppid in current_parent_pids and process.identity not in self._tracked:
                    self._tracked.add(process.identity)
                    changed = True

    def current(self, snapshot: dict[int, ProcessStat]) -> tuple[ProcessStat, ...]:
        current = [
            process
            for identity in self._tracked
            if (process := snapshot.get(identity.pid)) is not None and process.identity == identity
        ]
        return tuple(sorted(current, key=lambda item: item.identity.pid))


class LinuxProcessTreeSupervisor:
    def __init__(self, config: SupervisorConfig, *, procfs: Procfs | None = None) -> None:
        config.validate()
        self._config = config
        self._procfs = procfs or Procfs()
        self._own_stat = self._procfs.validate()
        self._tracker: ProcessTracker | None = None
        self._process: subprocess.Popen[bytes] | None = None

    def run(self, command: tuple[str, ...]) -> int:
        global _received_signal
        _received_signal = None
        _enable_child_subreaper()
        before = self._procfs.snapshot()
        baseline = frozenset(
            process.identity for process in before.values() if process.ppid == self._own_stat.identity.pid
        )
        tracker = ProcessTracker(self._own_stat.identity, baseline)
        self._tracker = tracker
        previous_handlers = _install_signal_handlers()
        try:
            if _received_signal is not None:
                return 128 + _received_signal
            try:
                process = subprocess.Popen(command, start_new_session=True)
            except (FileNotFoundError, PermissionError) as exc:
                return EXIT_NOT_FOUND if isinstance(exc, FileNotFoundError) else EXIT_NOT_EXECUTABLE
            except OSError:
                return EXIT_CONTAINMENT_FAILURE
            self._process = process
            return self._observe(process, tracker)
        except ContainmentError:
            self._best_effort_kill()
            return EXIT_CONTAINMENT_FAILURE
        finally:
            _restore_signal_handlers(previous_handlers)

    def _observe(self, process: subprocess.Popen[bytes], tracker: ProcessTracker) -> int:
        deadline = time.monotonic() + self._config.timeout_seconds
        while time.monotonic() < deadline and _received_signal is None:
            returncode, snapshot = self._refresh(process, tracker)
            if returncode is not None:
                if not self._prove_quiescence(process, tracker, snapshot) and not self._terminate_and_reap(
                    process, tracker
                ):
                    return EXIT_CONTAINMENT_FAILURE
                return returncode if returncode >= 0 else 128 + abs(returncode)
            time.sleep(min(self._config.poll_interval_seconds, max(0, deadline - time.monotonic())))

        interrupted_by = _received_signal
        if not self._terminate_and_reap(process, tracker):
            return EXIT_CONTAINMENT_FAILURE
        if interrupted_by is not None:
            return 128 + interrupted_by
        return EXIT_TIMEOUT

    def _terminate_and_reap(self, process: subprocess.Popen[bytes], tracker: ProcessTracker) -> bool:
        term_deadline = time.monotonic() + self._config.term_grace_seconds
        if self._signal_until(process, tracker, signal.SIGTERM, term_deadline):
            return True
        kill_deadline = time.monotonic() + self._config.kill_grace_seconds
        return self._signal_until(process, tracker, signal.SIGKILL, kill_deadline)

    def _signal_until(
        self,
        process: subprocess.Popen[bytes],
        tracker: ProcessTracker,
        signum: signal.Signals,
        deadline: float,
    ) -> bool:
        while True:
            returncode, snapshot = self._refresh(process, tracker)
            if returncode is not None and self._prove_quiescence(process, tracker, snapshot):
                return True
            for candidate in tracker.current(snapshot):
                if candidate.state not in _NON_EXECUTABLE_STATES:
                    self._signal_authenticated(candidate.identity, signum)
            if time.monotonic() >= deadline:
                _, final_snapshot = self._refresh(process, tracker)
                return process.poll() is not None and self._prove_quiescence(process, tracker, final_snapshot)
            time.sleep(min(self._config.poll_interval_seconds, max(0, deadline - time.monotonic())))

    def _refresh(
        self,
        process: subprocess.Popen[bytes],
        tracker: ProcessTracker,
    ) -> tuple[int | None, dict[int, ProcessStat]]:
        returncode = process.poll()
        snapshot = self._procfs.snapshot()
        tracker.discover(snapshot)
        if self._reap_adopted_zombies(process, tracker, snapshot):
            snapshot = self._procfs.snapshot()
            tracker.discover(snapshot)
        returncode = process.poll() if returncode is None else returncode
        return returncode, snapshot

    def _prove_quiescence(
        self,
        process: subprocess.Popen[bytes],
        tracker: ProcessTracker,
        snapshot: dict[int, ProcessStat],
    ) -> bool:
        if process.poll() is None or tracker.current(snapshot):
            return False
        confirmation = self._procfs.snapshot()
        tracker.discover(confirmation)
        self._reap_adopted_zombies(process, tracker, confirmation)
        final = self._procfs.snapshot()
        tracker.discover(final)
        return process.poll() is not None and not tracker.current(final)

    def _reap_adopted_zombies(
        self,
        process: subprocess.Popen[bytes],
        tracker: ProcessTracker,
        snapshot: dict[int, ProcessStat],
    ) -> bool:
        reaped = False
        for candidate in tracker.current(snapshot):
            if (
                candidate.identity.pid == process.pid
                or candidate.ppid != self._own_stat.identity.pid
                or candidate.state not in _NON_EXECUTABLE_STATES
            ):
                continue
            try:
                waited, _ = os.waitpid(candidate.identity.pid, os.WNOHANG)
                reaped = reaped or waited == candidate.identity.pid
            except ChildProcessError:
                continue
        return reaped

    def _signal_authenticated(self, identity: ProcessIdentity, signum: signal.Signals) -> None:
        current = self._procfs.read(identity.pid)
        if current is None or current.identity != identity or current.state in _NON_EXECUTABLE_STATES:
            return
        if (
            current.identity.pid == self._own_stat.identity.pid
            or current.process_group == self._own_stat.process_group
            or current.session == self._own_stat.session
        ):
            raise ContainmentError("signal-boundary-invalid")

        pidfd_open = getattr(os, "pidfd_open")
        pidfd_send_signal = getattr(signal, "pidfd_send_signal")
        try:
            descriptor = pidfd_open(identity.pid, 0)
        except OSError as exc:
            if exc.errno == errno.ESRCH:
                return
            raise ContainmentError("pidfd-open-failed") from exc
        try:
            authenticated = self._procfs.read(identity.pid)
            if authenticated is not None and authenticated.identity == identity:
                try:
                    pidfd_send_signal(descriptor, signum)
                except OSError as exc:
                    if exc.errno != errno.ESRCH:
                        raise ContainmentError("pidfd-signal-failed") from exc
        finally:
            os.close(descriptor)

    def _best_effort_kill(self) -> None:
        process = self._process
        tracker = self._tracker
        if process is None:
            return
        if tracker is not None:
            try:
                snapshot = self._procfs.snapshot()
                tracker.discover(snapshot)
                for candidate in tracker.current(snapshot):
                    if candidate.state not in _NON_EXECUTABLE_STATES:
                        self._signal_authenticated(candidate.identity, signal.SIGKILL)
            except (ContainmentError, OSError):
                pass
        try:
            process.kill()
            process.wait(timeout=self._config.kill_grace_seconds)
        except (OSError, subprocess.TimeoutExpired):
            pass


def parse_process_stat(raw: str) -> ProcessStat:
    closing = raw.rfind(")")
    if (opening := raw.find("(")) <= 0 or closing <= opening:
        raise ValueError("invalid proc stat envelope")
    pid = int(raw[:opening].strip())
    fields = raw[closing + 1 :].split()
    if len(fields) < 20 or len(fields[0]) != 1:
        raise ValueError("invalid proc stat fields")
    return ProcessStat(ProcessIdentity(pid, int(fields[19])), fields[0], int(fields[1]), int(fields[2]), int(fields[3]))


def _enable_child_subreaper() -> None:
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        result = libc.prctl(_PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)
    except (AttributeError, OSError) as exc:
        raise ContainmentError("subreaper-unavailable") from exc
    if result != 0:
        raise ContainmentError("subreaper-enable-failed")


def _install_signal_handlers() -> dict[signal.Signals, Any]:
    previous: dict[signal.Signals, Any] = {}
    for signum in _TERMINATION_SIGNALS:
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, _record_termination_signal)
    return previous


def _restore_signal_handlers(previous: dict[signal.Signals, Any]) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def _record_termination_signal(signum: int, _frame: object) -> None:
    global _received_signal
    if _received_signal is None:
        _received_signal = signum


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a Linux command with bounded process-tree containment.")
    parser.add_argument("--timeout-seconds", required=True, type=float)
    parser.add_argument("--term-grace-seconds", default=30.0, type=float)
    parser.add_argument("--kill-grace-seconds", default=5.0, type=float)
    parser.add_argument("--poll-interval-seconds", default=0.05, type=float)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = tuple(args.command[1:] if args.command[:1] == ["--"] else args.command)
    if not command:
        parser.error("a command is required after --")
    config = SupervisorConfig(
        args.timeout_seconds, args.term_grace_seconds, args.kill_grace_seconds, args.poll_interval_seconds
    )
    try:
        result = LinuxProcessTreeSupervisor(config).run(command)
    except (ContainmentError, ValueError):
        result = EXIT_CONTAINMENT_FAILURE
    if result == EXIT_TIMEOUT:
        print("bounded process tree: timed out and quiescence was proven", file=sys.stderr)
    elif result == EXIT_CONTAINMENT_FAILURE:
        print("bounded process tree: containment or quiescence proof failed", file=sys.stderr)
    elif result in {EXIT_NOT_EXECUTABLE, EXIT_NOT_FOUND}:
        print("bounded process tree: command could not be started", file=sys.stderr)
    return result


if __name__ == "__main__":
    raise SystemExit(main())

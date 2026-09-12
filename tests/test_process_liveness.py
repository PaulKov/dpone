"""Independent kernel-state evidence for subprocess cleanup assertions."""

from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path

import pytest

from tests.support import process_liveness


@pytest.mark.skipif(sys.platform != "linux", reason="Linux unreaped child state is required")
def test_actual_linux_zombie_is_not_running() -> None:
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(write_fd)
        os.read(read_fd, 1)
        os._exit(0)
    os.close(read_fd)
    try:
        assert process_liveness.pid_is_running(pid)
        os.close(write_fd)
        write_fd = -1
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = Path(f"/proc/{pid}/stat").read_bytes().rsplit(b") ", 1)[1].split()[0]
            if state == b"Z":
                break
            time.sleep(0.01)
        assert state == b"Z"
        os.kill(pid, 0)  # Existence alone cannot prove executable liveness.
        assert not process_liveness.pid_is_running(pid)
    finally:
        if write_fd >= 0:
            os.close(write_fd)
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        os.waitpid(pid, 0)
    assert not process_liveness.pid_is_running(pid)


def _stat(state: bytes, *, pid: int = 123, start: int = 42) -> bytes:
    return str(pid).encode() + b" (worker ) with spaces) " + state + b" 1" * 18 + b" " + str(start).encode() + b" 0\n"


def _linux_reads(monkeypatch: pytest.MonkeyPatch, values: list[bytes]) -> None:
    monkeypatch.setattr(process_liveness.sys, "platform", "linux")
    monkeypatch.setattr(process_liveness.os, "kill", lambda _pid, _signal: None)
    samples = iter(values)
    monkeypatch.setattr(process_liveness.Path, "read_bytes", lambda _path: next(samples))


@pytest.mark.parametrize("state", [b"Z", b"X"])
def test_verified_inert_states_are_stopped(monkeypatch: pytest.MonkeyPatch, state: bytes) -> None:
    _linux_reads(monkeypatch, [_stat(state), _stat(state)])
    assert not process_liveness.pid_is_running(123)


@pytest.mark.parametrize("state", [b"R", b"S", b"D", b"T", b"t", b"I", b"?", b"x"])
def test_live_or_unknown_states_remain_live(monkeypatch: pytest.MonkeyPatch, state: bytes) -> None:
    _linux_reads(monkeypatch, [_stat(state), _stat(state)])
    assert process_liveness.pid_is_running(123)


@pytest.mark.parametrize(
    "samples",
    [
        [_stat(b"Z"), _stat(b"Z", start=43)],
        [_stat(b"Z"), _stat(b"R")],
        [_stat(b"R"), _stat(b"Z")],
        [_stat(b"Z", pid=124), _stat(b"Z", pid=124)],
        [b"123 malformed", b"123 malformed"],
        [b"123 (worker) Z 1", b"123 (worker) Z 1"],
        [_stat(b"Z").replace(b" 42 ", b" invalid ")] * 2,
        [_stat(b"Z").replace(b" Z ", b" ZZ ")] * 2,
    ],
)
def test_identity_reuse_or_malformed_state_remains_live(monkeypatch: pytest.MonkeyPatch, samples: list[bytes]) -> None:
    _linux_reads(monkeypatch, samples)
    assert process_liveness.pid_is_running(123)


@pytest.mark.parametrize("error", [PermissionError(), FileNotFoundError(), OSError()])
def test_unreadable_proc_state_is_ambiguous(monkeypatch: pytest.MonkeyPatch, error: OSError) -> None:
    _linux_reads(monkeypatch, [])

    def unreadable(_path: Path) -> bytes:
        raise error

    monkeypatch.setattr(process_liveness.Path, "read_bytes", unreadable)
    assert process_liveness.pid_is_running(123)


@pytest.mark.parametrize(
    "error,expected", [(ProcessLookupError(), False), (PermissionError(), True), (OSError(), True)]
)
def test_signal_zero_outcomes(monkeypatch: pytest.MonkeyPatch, error: OSError, expected: bool) -> None:
    def unavailable(_pid: int, _signal: int) -> None:
        raise error

    monkeypatch.setattr(process_liveness.os, "kill", unavailable)
    assert process_liveness.pid_is_running(123) is expected


def test_oversized_positive_pid_is_ambiguous(monkeypatch: pytest.MonkeyPatch) -> None:
    def overflow(_pid: int, _signal: int) -> None:
        raise OverflowError("Python int too large to convert to C int")

    monkeypatch.setattr(process_liveness.os, "kill", overflow)
    assert process_liveness.pid_is_running(1 << 100)


def test_non_linux_does_not_read_proc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(process_liveness.sys, "platform", "darwin")
    monkeypatch.setattr(process_liveness.os, "kill", lambda _pid, _signal: None)
    monkeypatch.setattr(process_liveness.Path, "read_bytes", lambda _path: pytest.fail("non-Linux proc read"))
    assert process_liveness.pid_is_running(123)


def test_disappearance_after_kernel_reads_is_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    _linux_reads(monkeypatch, [_stat(b"Z"), _stat(b"Z")])
    calls = iter([None, ProcessLookupError()])

    def signal_zero(_pid: int, _signal: int) -> None:
        error = next(calls)
        if error is not None:
            raise error

    monkeypatch.setattr(process_liveness.os, "kill", signal_zero)
    assert not process_liveness.pid_is_running(123)


def test_pid_reuse_at_second_signal_zero_remains_live(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(process_liveness.sys, "platform", "linux")
    reused = False
    signal_calls = 0

    def signal_zero(_pid: int, _signal: int) -> None:
        nonlocal reused, signal_calls
        signal_calls += 1
        if signal_calls == 2:
            reused = True

    def read_identity(_path: Path) -> bytes:
        return _stat(b"R", start=43) if reused else _stat(b"Z")

    monkeypatch.setattr(process_liveness.os, "kill", signal_zero)
    monkeypatch.setattr(process_liveness.Path, "read_bytes", read_identity)

    assert process_liveness.pid_is_running(123)


@pytest.mark.parametrize("pid", [0, -1, True, "123"])
def test_invalid_pid_is_ambiguous_without_signalling(monkeypatch: pytest.MonkeyPatch, pid: object) -> None:
    monkeypatch.setattr(process_liveness.os, "kill", lambda *_args: pytest.fail("invalid PID signalled"))
    assert process_liveness.pid_is_running(pid)  # type: ignore[arg-type]


def test_changed_process_name_is_ambiguous(monkeypatch: pytest.MonkeyPatch) -> None:
    _linux_reads(monkeypatch, [_stat(b"Z"), _stat(b"Z").replace(b"worker", b"replacement")])
    assert process_liveness.pid_is_running(123)

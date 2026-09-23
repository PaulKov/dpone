"""Actual Linux pidfd proof for executor-owned natural settlement."""

import errno
import os
import select
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from dpone.adapters.mssql_tds_child_process import TdsChildContainmentExecutor
from dpone.adapters.mssql_tds_process import LinuxTdsProcess, _LinuxOps

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="requires actual Linux pidfd custody")


def guarded_child(*, natural: bool, exit_code: int = 0):
    LinuxTdsProcess.admit()
    guard = Path(__file__).parents[1] / "src/dpone/adapters/mssql_tds_worker_guard.py"
    action = f"sys.stdin.buffer.read();sys.exit({exit_code})" if natural else "time.sleep(30)"
    program = (
        "import os,runpy,sys,time\n"
        "guard=runpy.run_path(sys.argv[1])\n"
        "guard['install_worker_guard'](expected_parent_pid=int(sys.argv[2]),max_address_space_bytes=536870912)\n"
        "print('R',flush=True)\n" + action + "\n"
    )
    child = subprocess.Popen(
        [sys.executable, "-I", "-S", "-B", "-c", program, str(guard), str(os.getpid())],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    assert child.stdout is not None and child.stdin is not None
    assert select.select([child.stdout], [], [], 3.0)[0] and child.stdout.readline() == b"R\n"
    return child


def settled_owner(child, *, natural_after: float, cleanup_after: float | None = None, close_input: bool = False):
    owner = TdsChildContainmentExecutor(child, LinuxTdsProcess.identify(child.pid), time.monotonic() + 5.0, 1.0)
    assert owner.ready.wait(1.0)
    if close_input:
        child.stdin.close()
    owner.request_settlement(
        natural_deadline=time.monotonic() + natural_after,
        containment_deadline=None if cleanup_after is None else time.monotonic() + cleanup_after,
    )
    return owner


def assert_reaped_once(child):
    with pytest.raises(ChildProcessError) as caught:
        os.waitpid(child.pid, os.WNOHANG)
    assert caught.value.errno == errno.ECHILD


def test_actual_natural_exit_uses_no_signal_and_reaps_once(monkeypatch):
    signals = []
    original = _LinuxOps.signal
    monkeypatch.setattr(
        _LinuxOps,
        "signal",
        lambda self, fd, sig: (signals.append(sig), original(self, fd, sig))[1],
    )
    child = guarded_child(natural=True)
    owner = settled_owner(child, natural_after=1.0, close_input=True)
    assert owner.done.wait(2.0) and not owner.failed
    assert owner.exit is not None and owner.exit.exit_code == 0 and signal.SIGKILL not in signals
    assert_reaped_once(child)


def test_actual_natural_expiry_forces_same_child_and_reaps_once():
    child = guarded_child(natural=False)
    owner = settled_owner(child, natural_after=0.03)
    assert owner.done.wait(2.0) and not owner.failed
    assert owner.exit is not None and owner.exit.exit_code == -9
    assert_reaped_once(child)


def test_actual_concurrent_force_stays_on_executor_thread(monkeypatch):
    threads = []
    original_signal, original_reap = _LinuxOps.signal, _LinuxOps.reap

    def signal_on_owner(ops, fd, sig):
        if sig == signal.SIGKILL:
            threads.append(threading.current_thread())
        return original_signal(ops, fd, sig)

    def reap_on_owner(ops, fd):
        result = original_reap(ops, fd)
        if result is not None:
            threads.append(threading.current_thread())
        return result

    monkeypatch.setattr(_LinuxOps, "signal", signal_on_owner)
    monkeypatch.setattr(_LinuxOps, "reap", reap_on_owner)
    child = guarded_child(natural=False)
    owner = settled_owner(child, natural_after=2.0)
    owner.request(time.monotonic() + 0.8)
    assert owner.done.wait(2.0) and not owner.failed
    assert threads and set(threads) == {owner._thread}
    assert_reaped_once(child)


def test_actual_force_after_consumed_reap_retains_unknown_handle(monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original = _LinuxOps.reap

    def delayed_reap(ops, fd):
        result = original(ops, fd)
        if result is not None:
            entered.set()
            assert release.wait(2.0)
        return result

    monkeypatch.setattr(_LinuxOps, "reap", delayed_reap)
    child = guarded_child(natural=True)
    owner = settled_owner(child, natural_after=2.0, close_input=True)
    assert entered.wait(1.0)
    owner.request()
    release.set()
    assert owner.done.wait(1.0) and owner.failed and owner.exit is None
    handle = owner._resources.handle
    assert handle is not None and handle._fd is not None and handle._consumed_reap is not None
    with pytest.raises(ChildProcessError):
        os.waitpid(child.pid, os.WNOHANG)
    os.close(handle._fd)

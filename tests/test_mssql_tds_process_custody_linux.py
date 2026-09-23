"""Actual Linux pidfd and guarded-child evidence; no SQL or mocked exit receipt."""

import os
import select
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from dpone.adapters.mssql_tds_child_process import TdsChildContainmentExecutor
from dpone.adapters.mssql_tds_process import LinuxTdsProcess, _LinuxOps

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="UNVERIFIED: requires real Linux pidfd kernel")
GUARD = Path(__file__).parents[1] / "src/dpone/adapters/mssql_tds_worker_guard.py"
PROGRAM = (
    "import os,runpy,sys,time\n"
    "runpy.run_path(sys.argv[1])['install_worker_guard'](expected_parent_pid=int(sys.argv[2]), "
    "max_address_space_bytes=536870912)\n"
    "print('R',flush=True)\n"
    "time.sleep(30)\n"
)


def guarded_process():
    LinuxTdsProcess.admit()
    child = subprocess.Popen(
        [sys.executable, "-I", "-S", "-B", "-c", PROGRAM, str(GUARD), str(os.getpid())],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    assert child.stdout is not None
    assert select.select([child.stdout], [], [], 3)[0]
    assert child.stdout.readline() == b"R\n"
    return child


def test_real_idle_custody_reaps_once_and_closes_stdout():
    child = guarded_process()
    identity = LinuxTdsProcess.identify(child.pid)
    owner = TdsChildContainmentExecutor(child, identity, time.monotonic() + 0.05, 1.0)
    assert owner.done.wait(2)
    assert not owner.failed
    assert owner.exit is not None and owner.exit.exit_code == -9 and owner.exit.reaped
    assert child.returncode == -9 and child.stdout.closed
    captured = owner.cleanup_deadline
    assert owner.request(time.monotonic() + 100) == captured
    with pytest.raises(ChildProcessError):
        os.waitpid(child.pid, os.WNOHANG)


def test_real_kernel_exit_after_shortening_remains_unknown(monkeypatch):
    child = guarded_process()
    entered, release = threading.Event(), threading.Event()
    original = _LinuxOps.ready_until

    def delayed_poll(ops, fd, remaining):
        entered.set()
        assert release.wait(2)
        return original(ops, fd, remaining)

    monkeypatch.setattr(_LinuxOps, "ready_until", delayed_poll)
    owner = TdsChildContainmentExecutor(child, LinuxTdsProcess.identify(child.pid), time.monotonic() + 10, 1.0)
    try:
        assert owner.ready.wait(1)
        owner.request()
        assert entered.wait(1)
        owner.request(time.monotonic() + 0.01)
        time.sleep(0.02)
        release.set()
        assert owner.done.wait(1)
        assert owner.failed and owner.exit is None
        handle = owner._resources.handle
        assert handle is not None and handle._fd is not None
        assert child.returncode is None and not child.stdout.closed
    finally:
        release.set()
        # Test teardown consumes the otherwise retained UNKNOWN capability. It is
        # deliberately separate from production custody and earns no exit proof.
        child.wait(timeout=2)
        child.stdout.close()
        handle = owner._resources.handle
        if handle is not None and handle._fd is not None:
            os.close(handle._fd)


def test_real_parent_death_guard_proves_descendant_exit_without_signalling_it():
    LinuxTdsProcess.admit()
    parent_program = (
        "import os,subprocess,sys,time\n"
        "child=subprocess.Popen([sys.executable,'-I','-S','-B','-c',sys.argv[2],sys.argv[1],str(os.getpid())],"
        "stdout=subprocess.PIPE)\n"
        "assert child.stdout.readline()==b'R\\n'\n"
        "print(child.pid,flush=True)\n"
        "time.sleep(30)\n"
    )
    parent = subprocess.Popen(
        [sys.executable, "-I", "-S", "-B", "-c", parent_program, str(GUARD), PROGRAM],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    assert parent.stdout is not None
    assert select.select([parent.stdout], [], [], 3)[0]
    descendant_pid = int(parent.stdout.readline())
    with LinuxTdsProcess.acquire(LinuxTdsProcess.identify(parent.pid)) as parent_handle:
        with LinuxTdsProcess.acquire(LinuxTdsProcess.identify(descendant_pid)) as descendant:
            result = parent_handle.contain(deadline=time.monotonic() + 1, direct_child=True)
            parent.returncode = result.exit_code
            observation = descendant.wait(deadline=time.monotonic() + 1, direct_child=False)
            assert not observation.reaped and observation.exit_code is None
    assert parent.stdout.read() == b""
    parent.stdout.close()

"""Retained deadline regressions and actual guarded Linux containment cases.

The deterministic deadline test uses the real pidfd wrapper with explicitly
faulted syscall operations; it supplies no successful exit or SQL authority.
Linux-only cases use a real guarded child and remain skipped on other systems.
"""

import os
import select
import subprocess
import sys
from pathlib import Path
from threading import Event
from time import monotonic
from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_tds_child_process import TdsChildContainmentExecutor
from dpone.adapters.mssql_tds_process import LinuxTdsProcess
from tests.test_mssql_tds_coordinator import PROCESS


class FaultedKernelWait:
    """Signal succeeds, but exit observation remains unknown until deadline."""

    def __init__(self):
        self.entered = Event()
        self.release = Event()
        self.signals = []

    def monotonic(self):
        return monotonic() + (60.0 if self.release.is_set() else 0.0)

    def signal(self, fd, signal):
        self.signals.append((fd, signal))

    def ready_until(self, fd, remaining):
        self.entered.set()
        self.release.wait(min(remaining, 0.005))
        return False

    def close(self, fd):
        pass


def test_shorter_cleanup_deadline_reaches_inflight_containment(monkeypatch):
    operations = FaultedKernelWait()
    # Construct the real thread-confined pidfd wrapper inside its actual owner.
    monkeypatch.setattr(LinuxTdsProcess, "acquire", classmethod(lambda cls, identity: cls(identity, 123, operations)))
    owner = TdsChildContainmentExecutor(SimpleNamespace(returncode=None), PROCESS, monotonic() + 10.0, 1.0)
    try:
        assert owner.ready.wait(1.0)
        original = owner.request(monotonic() + 0.8)
        assert operations.entered.wait(1.0)
        shortened = monotonic() + 0.05
        assert owner.request(shortened) == shortened < original
        assert owner.done.wait(0.15), "in-flight containment kept the old deadline after an admitted shortening"
        assert owner.failed and owner.exit is None
    finally:
        operations.release.set()
        assert owner.done.wait(1.0)
        owner._thread.join(1.0)


@pytest.fixture
def guarded_child():
    if sys.platform != "linux":
        pytest.skip("requires actual Linux pidfd/parent-death guard; no containment certification on this platform")
    LinuxTdsProcess.admit()
    guard = Path(__file__).parents[1] / "src/dpone/adapters/mssql_tds_worker_guard.py"
    reader, writer = os.pipe()
    program = (
        "import os, runpy, sys, time\n"
        "guard = runpy.run_path(sys.argv[1])\n"
        "guard['install_worker_guard'](expected_parent_pid=int(sys.argv[2]), max_address_space_bytes=536870912)\n"
        "os.write(int(sys.argv[3]), b'R')\n"
        "os.close(int(sys.argv[3]))\n"
        "time.sleep(30)\n"
    )
    child = subprocess.Popen(
        [sys.executable, "-I", "-S", "-B", "-c", program, str(guard), str(os.getpid()), str(writer)],
        pass_fds=(writer,),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    os.close(writer)
    owner = None
    try:
        assert select.select([reader], [], [], 3.0)[0], "guarded child did not acknowledge startup"
        assert os.read(reader, 1) == b"R"
        identity = LinuxTdsProcess.identify(child.pid)
        owner = TdsChildContainmentExecutor(child, identity, monotonic() + 0.12, 1.0)
        assert owner.ready.wait(1.0)
        yield owner
    finally:
        os.close(reader)
        if owner is not None:
            owner.request(monotonic() + 1.0)
            assert owner.done.wait(2.0)
            owner._thread.join(1.0)
        elif child.returncode is None:
            # Startup failure cleanup still uses authenticated pidfd containment.
            with LinuxTdsProcess.acquire(LinuxTdsProcess.identify(child.pid)) as handle:
                result = handle.contain(deadline=monotonic() + 1.0, direct_child=True)
                child.returncode = result.exit_code


def test_actual_guarded_child_is_contained_during_retained_idle(guarded_child):
    owner = guarded_child
    assert owner.done.wait(2.0)
    assert not owner.failed
    assert owner.exit is not None and owner.exit.reaped and owner.exit.exit_code == -9
    assert owner.child.returncode == -9


def test_actual_child_expiry_survives_stalled_sqlite_parent_actor(guarded_child, tmp_path):
    from contextlib import contextmanager

    from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
    from dpone.adapters.mssql_tds_actor_core import TdsActorPool
    from dpone.app.mssql_tds_attempt_composition import create_tds_attempt
    from dpone.services.mssql_tds_attempt import TdsAttemptUnknown
    from tests.test_mssql_sqlclient_observe import request
    from tests.test_mssql_tds_directory import LIMITS
    from tests.test_mssql_tds_directory_journal import OWNER

    store = SQLiteWindowStore(tmp_path / "blocked.sqlite", clock=lambda: 1.0)
    value = request()
    lease = store.acquire(value.parent.target_key, OWNER.owner, 30)
    pool = TdsActorPool(capacity=1)
    entered, release = Event(), Event()

    @contextmanager
    def factory():
        entered.set()
        release.wait(3.0)
        yield store

    try:
        with pytest.raises(TdsAttemptUnknown) as caught:
            create_tds_attempt(
                pool,
                factory,
                value.parent,
                LIMITS,
                lease,
                supervisor_token=OWNER.supervisor_id,
                backend="mssql_sqlclient",
                deadline=monotonic() + 0.35,
            )
        assert entered.is_set()
        assert guarded_child.done.wait(0.5)
        assert guarded_child.exit is not None and guarded_child.exit.reaped
        assert pool.live_count == 1, "timed-out actor must retain its actual pool reservation"
        assert caught.value._gateways, "UNKNOWN retains the original actor shutdown capability"
    finally:
        release.set()
        pool.close(deadline=monotonic() + 2.0)

"""Deterministic process ownership checks; doubles are not Linux certification."""

import signal
import subprocess

import pytest

from dpone.adapters.composition_dbt_cleanup import LinuxDbtProcessLifecycle, ProcessIdentity, ProcessLifecycleError


class Process:
    pid = 123
    args = ("dbt", "build")
    returncode = None


class Operations:
    def __init__(self):
        self.now = 0.0
        self.events = []
        self.exited = False
        self.residual = False
        self.escaped = False
        self.lost = False
        self.unreapable = False
        self.denied = False
        self.interrupt = None

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        assert 0 < seconds <= 0.05
        self.now += seconds

    def read(self, pid):
        return ProcessIdentity(pid, 1001, 123, 42, "Z" if self.exited else "S")

    def observe(self, process):
        assert process.returncode is None
        if self.interrupt is not None:
            error, self.interrupt = self.interrupt, None
            raise error
        if self.lost:
            raise ChildProcessError
        return self.exited

    def inventory(self, deadline):
        assert self.now < deadline
        leader = [] if "reap" in self.events else [self.read(123)]
        return leader + ([ProcessIdentity(456, 1001, 999 if self.escaped else 123, 43, "S")] if self.residual else [])

    def signal(self, process, identity, signum):
        self.observe(process)
        assert "reap" not in self.events
        self.events.append(signum)
        if self.denied:
            raise PermissionError
        self.exited = True
        if signum == signal.SIGKILL and not self.escaped:
            self.residual = False

    def reap(self, process, timeout):
        assert timeout > 0
        self.events.append("reap")
        if self.unreapable:
            raise subprocess.TimeoutExpired(process.args, timeout)
        process.returncode = 0
        return 0


def test_fast_success_retains_identity_until_reaped():
    ops = Operations()
    ops.exited = True
    child = LinuxDbtProcessLifecycle(ops).run(Process(), 1001, 1)
    assert (child.pid, child.start_ticks, child.exit_code) == (123, 42, 0)
    assert ops.events == ["reap"]


def test_timeout_escalates_group_before_reaping():
    ops = Operations()
    ops.residual = True
    with pytest.raises(subprocess.TimeoutExpired):
        LinuxDbtProcessLifecycle(ops).run(Process(), 1001, 1)
    assert ops.events == [signal.SIGTERM, signal.SIGKILL, "reap"]
    assert 6 <= ops.now < 13


def test_successful_leader_with_residual_child_is_not_success():
    ops = Operations()
    ops.exited = ops.residual = True
    with pytest.raises(ProcessLifecycleError, match="capture_child_not_quiescent"):
        LinuxDbtProcessLifecycle(ops).run(Process(), 1001, 1)
    assert ops.events == [signal.SIGTERM, signal.SIGKILL, "reap"]


def test_escaped_descendant_blocks_independent_uid_quiescence():
    ops = Operations()
    ops.exited = ops.residual = ops.escaped = True
    with pytest.raises(ProcessLifecycleError, match="capture_cleanup_unresolved"):
        LinuxDbtProcessLifecycle(ops).run(Process(), 1001, 1)
    assert ops.now <= 12
    assert ops.events == [signal.SIGTERM, "reap"]


def test_external_reaper_prohibits_numeric_group_signals():
    ops = Operations()
    ops.lost = True
    with pytest.raises(ProcessLifecycleError, match="capture_cleanup_unresolved") as caught:
        LinuxDbtProcessLifecycle(ops).run(Process(), 1001, 1)
    assert isinstance(caught.value.__cause__, ChildProcessError)
    assert ops.events == []


@pytest.mark.parametrize("error", [KeyboardInterrupt, SystemExit])
def test_cancellation_is_preserved_after_bounded_cleanup(error):
    ops = Operations()
    ops.interrupt = error()
    with pytest.raises(error):
        LinuxDbtProcessLifecycle(ops).run(Process(), 1001, 1)
    assert ops.events == [signal.SIGTERM, "reap"]


@pytest.mark.parametrize("fault", ["unreapable", "denied"])
def test_cleanup_failure_chains_original_timeout(fault):
    ops = Operations()
    setattr(ops, fault, True)
    with pytest.raises(ProcessLifecycleError, match="capture_cleanup_unresolved") as caught:
        LinuxDbtProcessLifecycle(ops).run(Process(), 1001, 1)
    assert isinstance(caught.value.__cause__, subprocess.TimeoutExpired)
    assert ops.now < 13


def test_reap_failure_on_normal_exit_never_signals_again():
    ops = Operations()
    ops.exited = ops.unreapable = True
    with pytest.raises(subprocess.TimeoutExpired):
        LinuxDbtProcessLifecycle(ops).run(Process(), 1001, 1)
    assert ops.events == ["reap"]


def test_missing_visibility_is_not_quiescence():
    ops = Operations()
    ops.exited = True

    def unreadable(deadline):
        raise ProcessLifecycleError("capture_process_visibility")

    ops.inventory = unreadable
    with pytest.raises(ProcessLifecycleError, match="capture_cleanup_unresolved") as caught:
        LinuxDbtProcessLifecycle(ops).run(Process(), 1001, 1)
    assert str(caught.value.__cause__) == "capture_process_visibility"
    assert "reap" not in ops.events


def test_signal_validates_retained_identity_and_waitid_without_reaping(monkeypatch):
    import os
    from types import SimpleNamespace

    from dpone.adapters.composition_dbt_cleanup import LinuxProcessOperations

    events = []
    for name, value in [("P_PID", 1), ("WEXITED", 4), ("WNOHANG", 1), ("WNOWAIT", 0x1000000)]:
        monkeypatch.setattr(os, name, value, raising=False)

    def observe(kind, pid, flags):
        assert flags & os.WNOWAIT and flags & os.WNOHANG
        events.append("observe")
        return SimpleNamespace(si_pid=pid)

    monkeypatch.setattr(os, "waitid", observe, raising=False)
    monkeypatch.setattr(os, "getpgrp", lambda: 999)
    monkeypatch.setattr(os, "killpg", lambda pid, signum: events.append((pid, signum)))
    ops = LinuxProcessOperations()
    identity = ProcessIdentity(123, 1001, 123, 42, "Z")
    monkeypatch.setattr(ops, "read", lambda pid: identity)
    process = Process()
    ops.signal(process, identity, signal.SIGTERM)
    assert events == ["observe", (123, signal.SIGTERM)]
    process.returncode = 0
    with pytest.raises(ProcessLifecycleError, match="capture_process_ownership_lost"):
        ops.signal(process, identity, signal.SIGKILL)
    assert len(events) == 2


@pytest.mark.parametrize("fault", ["changed_start", "missing", "own_group"])
def test_signal_refuses_unanchored_identity(monkeypatch, fault):
    import os
    from dataclasses import replace

    from dpone.adapters.composition_dbt_cleanup import LinuxProcessOperations

    ops = LinuxProcessOperations()
    identity = ProcessIdentity(123, 1001, 123, 42, "Z")
    monkeypatch.setattr(ops, "observe", lambda process: True)
    observed = None if fault == "missing" else replace(identity, start_ticks=43 if fault == "changed_start" else 42)
    monkeypatch.setattr(ops, "read", lambda pid: observed)
    monkeypatch.setattr(os, "getpgrp", lambda: 123 if fault == "own_group" else 999)
    monkeypatch.setattr(os, "killpg", lambda *args: pytest.fail("unsafe signal"))
    with pytest.raises(ProcessLifecycleError, match="capture_process_ownership_lost"):
        ops.signal(Process(), identity, signal.SIGTERM)


def test_inventory_expired_deadline_never_reports_empty():
    from dpone.adapters.composition_dbt_cleanup import LinuxProcessOperations

    ops = LinuxProcessOperations()
    with pytest.raises(ProcessLifecycleError, match="capture_process_scan_deadline|capture_process_visibility"):
        ops.inventory(0)


@pytest.mark.skipif(__import__("sys").platform != "linux", reason="Linux waitid WNOWAIT and proc required")
def test_linux_retains_exited_leader_through_last_signal():
    import sys

    from dpone.adapters.composition_dbt_cleanup import LinuxProcessOperations, require_waitid

    require_waitid()
    ops = LinuxProcessOperations()
    process = subprocess.Popen([sys.executable, "-c", "pass"], start_new_session=True)
    try:
        deadline = ops.monotonic() + 5
        while not ops.observe(process):
            assert ops.monotonic() < deadline
            ops.sleep(0.01)
        identity = ops.read(process.pid)
        assert identity is not None and identity.state == "Z" and process.returncode is None
        ops.signal(process, identity, signal.SIGTERM)
        assert ops.read(process.pid) is not None
        assert ops.reap(process, 1) == 0
        assert ops.read(process.pid) is None
        with pytest.raises(ProcessLifecycleError, match="capture_process_ownership_lost"):
            ops.signal(process, identity, signal.SIGKILL)
    finally:
        if process.returncode is None:
            process.kill()
            process.wait(timeout=2)


def test_term_deadline_during_scan_still_escalates():
    from dpone.adapters.composition_dbt_cleanup import ProcessScanDeadline

    ops = Operations()
    ops.residual = True
    original_inventory = ops.inventory

    def crossing(deadline):
        if signal.SIGTERM in ops.events and signal.SIGKILL not in ops.events:
            ops.now = deadline
            raise ProcessScanDeadline("capture_process_scan_deadline")
        return original_inventory(deadline)

    ops.inventory = crossing
    with pytest.raises(subprocess.TimeoutExpired):
        LinuxDbtProcessLifecycle(ops).run(Process(), 1001, 1)
    assert ops.events == [signal.SIGTERM, signal.SIGKILL, "reap"]


def test_proc_identity_change_between_reads_is_rejected(monkeypatch):
    from dpone.adapters import composition_dbt_cleanup as cleanup

    def stat(start):
        fields = ["S", "1", "123"] + ["0"] * 16 + [str(start)]
        return "123 (name with spaces) " + " ".join(fields)

    responses = iter([stat(42), "Uid:\t1001\t1001\t1001\t1001\n", stat(43)])
    monkeypatch.setattr(cleanup, "_read_bounded", lambda path: next(responses))
    with pytest.raises(ProcessLifecycleError, match="capture_process_identity"):
        cleanup.read_process(123)


def test_proc_byte_bound_is_enforced(tmp_path):
    from dpone.adapters.composition_dbt_cleanup import MAX_PROC_BYTES, _read_bounded

    path = tmp_path / "stat"
    path.write_bytes(b"x" * (MAX_PROC_BYTES + 1))
    with pytest.raises(ProcessLifecycleError, match="capture_process_visibility"):
        _read_bounded(path)


def test_proc_entry_bound_is_enforced(monkeypatch):
    from types import SimpleNamespace

    from dpone.adapters import composition_dbt_cleanup as cleanup

    class Entries:
        def __enter__(self):
            return iter([SimpleNamespace(name="not-a-process")] * 3)

        def __exit__(self, *args):
            pass

    monkeypatch.setattr(cleanup, "MAX_PROC_ENTRIES", 2)
    monkeypatch.setattr(cleanup.os, "scandir", lambda path: Entries())
    ops = cleanup.LinuxProcessOperations()
    with pytest.raises(ProcessLifecycleError, match="capture_process_visibility"):
        ops.inventory(ops.monotonic() + 10)


@pytest.mark.skipif(__import__("sys").platform != "linux", reason="Linux proc and waitid required")
def test_linux_timeout_terminates_owned_child_group(tmp_path):
    """Real group/leader mechanics only: filtered inventory is not UID proof."""
    import os
    import sys

    from dpone.adapters.composition_dbt_cleanup import LinuxProcessOperations, require_waitid

    require_waitid()
    ready = tmp_path / "ready"
    program = """
import os, signal, sys, time
child = os.fork()
if child == 0:
    while True:
        time.sleep(1)
def finish(signum, frame):
    os.waitpid(child, 0)
    sys.exit(0)
signal.signal(signal.SIGTERM, finish)
with open(sys.argv[1], 'w') as stream:
    stream.write(str(child))
while True:
    time.sleep(1)
"""
    process = subprocess.Popen([sys.executable, "-c", program, str(ready)], start_new_session=True)

    class GroupOperations(LinuxProcessOperations):
        def inventory(self, deadline):
            return [item for item in super().inventory(deadline) if item.pgid == process.pid]

    ops = GroupOperations()
    try:
        deadline = ops.monotonic() + 5
        while not ready.exists():
            assert ops.monotonic() < deadline
            ops.sleep(0.01)
        child_pid = int(ready.read_text())
        with pytest.raises(subprocess.TimeoutExpired):
            LinuxDbtProcessLifecycle(ops).run(process, os.geteuid(), 0.1)
        assert process.returncode == 0
        assert ops.read(process.pid) is None and ops.read(child_pid) is None
    finally:
        if process.returncode is None:
            # Fixture owns the still-unreaped leader; no scanned PID is targeted.
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=2)


@pytest.mark.skipif(__import__("sys").platform != "linux", reason="Linux dedicated UID boundary required")
def test_linux_dedicated_uid_boundary_when_explicitly_provisioned(tmp_path):
    """No implicit host UID allocation: isolated CI must reserve this identity."""
    import os
    import sys
    from dataclasses import replace

    from dpone.adapters.composition_dbt_capture import LinuxDbtBuildRunner
    from tests.test_composition_dbt_capture import intent

    reserved = os.environ.get("DPONE_TEST_DBT_RESERVED_UID")
    if os.geteuid() != 0 or reserved is None:
        pytest.skip("Requires root and explicitly reserved isolated DPONE_TEST_DBT_RESERVED_UID")
    uid = int(reserved)
    assert uid > 0
    value = replace(
        intent(tmp_path),
        supervisor_uid=0,
        child_uid=uid,
        child_gid=uid,
        working_directory="/",
        argv=(sys.executable, "-c", "pass"),
    )
    result = LinuxDbtBuildRunner(lambda attempt: {})(value)
    assert result.exit_code == 0


def test_term_deadline_after_inventory_before_pause_still_escalates():
    ops = Operations()
    ops.residual = True
    original_inventory = ops.inventory

    def crossing(deadline):
        members = original_inventory(deadline)
        if signal.SIGTERM in ops.events and signal.SIGKILL not in ops.events:
            ops.now = deadline
        return members

    ops.inventory = crossing
    with pytest.raises(subprocess.TimeoutExpired):
        LinuxDbtProcessLifecycle(ops).run(Process(), 1001, 1)
    assert ops.events == [signal.SIGTERM, signal.SIGKILL, "reap"]

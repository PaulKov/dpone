"""Bounded process containment does not infer success from uncertain identity."""

from __future__ import annotations

import errno
import signal
from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_tds_process import LinuxTdsProcess, TdsProcessError, _parse_stat
from dpone.contracts.mssql_tds_worker import TdsProcessIdentity

IDENTITY = TdsProcessIdentity("a" * 64, "11111111-1111-1111-1111-111111111111", 234, 10)


class Ops:
    def __init__(self):
        self.now = 0.0
        self.identities = [IDENTITY] * 4
        self.calls = []
        self.ready = True
        self.wait_result = SimpleNamespace(si_pid=234, si_code=1, si_status=0)
        self.failure = None

    def admit(self):
        self.calls.append("admit")

    def identity(self, pid):
        assert pid == 234
        return self.identities.pop(0)

    def open(self, pid):
        self.calls.append("open")
        if self.failure:
            raise self.failure
        return 8

    def signal(self, fd, sig):
        self.calls.append(("signal", fd, sig))

    def ready_until(self, fd, remaining):
        self.calls.append(("wait", remaining))
        self.now += min(remaining, 0.25)
        return self.ready

    def reap(self, fd):
        self.calls.append("reap")
        return self.wait_result

    def close(self, fd):
        self.calls.append("close")

    def monotonic(self):
        return self.now


def test_contain_child_uses_pidfd_and_reaps():
    ops = Ops()
    process = LinuxTdsProcess.acquire(IDENTITY, _ops=ops)
    with process:
        proof = process.contain(deadline=1, direct_child=True)
    assert proof.reaped and proof.exit_code == 0
    assert ("signal", 8, signal.SIGKILL) in ops.calls
    assert ops.calls[-1] == "close"


def test_wait_observes_natural_exit_without_signal():
    ops = Ops()
    with LinuxTdsProcess.acquire(IDENTITY, _ops=ops) as process:
        proof = process.wait(deadline=1, direct_child=True)
    assert proof.reaped and proof.exit_code == 0
    assert not any(isinstance(call, tuple) and call[0] == "signal" for call in ops.calls)


def test_wait_timeout_does_not_implicitly_kill():
    ops = Ops()
    ops.ready = False
    with LinuxTdsProcess.acquire(IDENTITY, _ops=ops) as process:
        with pytest.raises(TdsProcessError, match="deadline"):
            process.wait(deadline=1, direct_child=True)
    assert not any(isinstance(call, tuple) and call[0] == "signal" for call in ops.calls)


@pytest.mark.parametrize("index", [0, 1])
def test_pid_reuse_never_signals(index):
    ops = Ops()
    ops.identities[index] = TdsProcessIdentity(IDENTITY.host_sha256, IDENTITY.boot_id, 234, 11)
    with pytest.raises(TdsProcessError, match="identity"):
        LinuxTdsProcess.acquire(IDENTITY, _ops=ops)
    assert not any(isinstance(call, tuple) and call[0] == "signal" for call in ops.calls)
    assert ("close" in ops.calls) == bool(index)


@pytest.mark.parametrize("err", [errno.EPERM, errno.EACCES, errno.ESRCH, errno.ENOSYS])
def test_open_failure_is_not_absence(err):
    ops = Ops()
    ops.failure = OSError(err, "private OS error")
    with pytest.raises(TdsProcessError):
        LinuxTdsProcess.acquire(IDENTITY, _ops=ops)


def test_recovery_is_termination_not_child_reaping():
    ops = Ops()
    with LinuxTdsProcess.acquire(IDENTITY, _ops=ops) as process:
        proof = process.contain(deadline=1, direct_child=False)
    assert not proof.reaped and proof.exit_code is None
    assert "reap" not in ops.calls


def test_one_absolute_deadline_bounds_wait_and_reap():
    ops = Ops()
    ops.wait_result = None
    with LinuxTdsProcess.acquire(IDENTITY, _ops=ops) as process:
        with pytest.raises(TdsProcessError, match="deadline"):
            process.contain(deadline=0.5, direct_child=True)
    assert ops.now == 0.5


@pytest.mark.parametrize("raw", ["", "234 (x) S 1", "234 x) " + "0 " * 30])
def test_malformed_stat_rejected(raw):
    with pytest.raises(TdsProcessError):
        _parse_stat(raw, 234)


def test_stat_comm_may_contain_parentheses_and_spaces():
    assert _parse_stat("234 (worker ) x) S " + "0 " * 18 + "10", 234) == 10


@pytest.mark.parametrize("operation", ["signal", "ready_until", "reap"])
@pytest.mark.parametrize("err", [errno.EPERM, errno.ECHILD, errno.EINTR])
def test_syscall_uncertainty_never_returns_receipt(operation, err):
    ops = Ops()

    def fail(*args):
        raise OSError(err, "private failure")

    setattr(ops, operation, fail)
    with LinuxTdsProcess.acquire(IDENTITY, _ops=ops) as process:
        with pytest.raises(TdsProcessError, match="unknown"):
            process.contain(deadline=1, direct_child=True)


def test_unreadable_pidfd_times_out():
    ops = Ops()
    ops.ready = False
    with LinuxTdsProcess.acquire(IDENTITY, _ops=ops) as process:
        with pytest.raises(TdsProcessError, match="deadline"):
            process.contain(deadline=0.5, direct_child=True)
    assert ops.now == 0.5


@pytest.mark.parametrize("deadline", [float("nan"), float("inf"), True, "1"])
def test_invalid_deadline_never_signals(deadline):
    ops = Ops()
    with LinuxTdsProcess.acquire(IDENTITY, _ops=ops) as process:
        with pytest.raises(ValueError):
            process.contain(deadline=deadline, direct_child=True)
    assert not any(isinstance(c, tuple) and c[0] == "signal" for c in ops.calls)


def test_expired_deadline_never_signals():
    ops = Ops()
    with LinuxTdsProcess.acquire(IDENTITY, _ops=ops) as process:
        with pytest.raises(TdsProcessError, match="deadline"):
            process.contain(deadline=0, direct_child=True)
    assert not any(isinstance(c, tuple) and c[0] == "signal" for c in ops.calls)


def test_closed_handle_cannot_signal():
    ops = Ops()
    process = LinuxTdsProcess.acquire(IDENTITY, _ops=ops)
    process.close()
    process.close()
    with pytest.raises(TdsProcessError, match="unavailable"):
        process.contain(deadline=1, direct_child=True)
    assert ops.calls.count("close") == 1


def test_forked_owner_cannot_signal(monkeypatch):
    ops = Ops()
    with LinuxTdsProcess.acquire(IDENTITY, _ops=ops) as process:
        monkeypatch.setattr("dpone.adapters.mssql_tds_process.os.getpid", lambda: 987)
        with pytest.raises(TdsProcessError, match="unavailable"):
            process.contain(deadline=1, direct_child=True)
        monkeypatch.undo()


@pytest.mark.parametrize("code,pid", [(5, 234), (1, 235)])
def test_reap_wrong_identity_or_nonexit_event_rejected(code, pid):
    ops = Ops()
    ops.wait_result = SimpleNamespace(si_pid=pid, si_code=code, si_status=0)
    with LinuxTdsProcess.acquire(IDENTITY, _ops=ops) as process:
        with pytest.raises(TdsProcessError, match="reap_invalid"):
            process.contain(deadline=1, direct_child=True)


def test_esrch_signal_still_requires_exit_observation():
    ops = Ops()

    def exited(*args):
        raise ProcessLookupError()

    ops.signal = exited
    with LinuxTdsProcess.acquire(IDENTITY, _ops=ops) as process:
        assert process.contain(deadline=1, direct_child=True).reaped


def test_platform_admission_is_fail_closed(monkeypatch):
    monkeypatch.setattr("dpone.adapters.mssql_tds_process.sys.platform", "darwin")
    with pytest.raises(TdsProcessError, match="platform_unsupported"):
        LinuxTdsProcess.admit()


@pytest.mark.parametrize("failure", [FileNotFoundError(), PermissionError(), UnicodeError(), ValueError()])
def test_procfs_read_failure_is_unknown_not_absence(monkeypatch, failure):
    from dpone.adapters.mssql_tds_process import _LinuxOps

    def fail(*args, **kwargs):
        raise failure

    monkeypatch.setattr("dpone.adapters.mssql_tds_process.Path.read_text", fail)
    with pytest.raises(TdsProcessError, match="identity_unavailable"):
        _LinuxOps().identity(234)


def test_real_identity_parser_binds_machine_boot_namespace_and_ticks(monkeypatch):
    import hashlib

    from dpone.adapters.mssql_tds_process import _LinuxOps

    values = {
        "/etc/machine-id": "a" * 32 + "\n",
        "/proc/sys/kernel/random/boot_id": IDENTITY.boot_id + "\n",
        "/proc/234/stat": "234 (worker) S " + "0 " * 18 + "10",
    }
    monkeypatch.setattr("dpone.adapters.mssql_tds_process.Path.read_text", lambda p, **kw: values[str(p)])
    monkeypatch.setattr(
        "dpone.adapters.mssql_tds_process.os.readlink",
        lambda p: str(__import__("os").getpid()) if p == "/proc/self" else "pid:[123]",
    )
    actual = _LinuxOps().identity(234)
    assert actual.host_sha256 == hashlib.sha256(("a" * 32 + "\npid:[123]").encode()).hexdigest()
    assert actual.boot_id == IDENTITY.boot_id
    assert (actual.pid, actual.start_ticks) == (234, 10)


@pytest.mark.parametrize("pid", [True, 0, -1, 2**31, "234"])
def test_identify_rejects_bad_pid_before_platform_probe(pid):
    with pytest.raises(ValueError, match="pid_invalid"):
        LinuxTdsProcess.identify(pid)


def test_missing_pidfd_api_is_not_admitted(monkeypatch):
    monkeypatch.setattr("dpone.adapters.mssql_tds_process.sys.platform", "linux")
    monkeypatch.setattr("dpone.adapters.mssql_tds_process.os.pidfd_open", None, raising=False)
    with pytest.raises(TdsProcessError, match="platform_unsupported"):
        LinuxTdsProcess.admit()


def test_kernel_admission_failure_is_not_admitted(monkeypatch):
    from dpone.adapters.mssql_tds_process import _LinuxOps

    def fail(self):
        raise PermissionError("not allowed")

    monkeypatch.setattr(_LinuxOps, "admit", fail)
    with pytest.raises(TdsProcessError, match="platform_unsupported"):
        LinuxTdsProcess.admit()


def test_killed_child_exit_code_is_negative_signal():
    ops = Ops()
    ops.wait_result = SimpleNamespace(si_pid=234, si_code=2, si_status=9)
    with LinuxTdsProcess.acquire(IDENTITY, _ops=ops) as process:
        assert process.contain(deadline=1, direct_child=True).exit_code == -9


def test_late_reap_cannot_extend_deadline():
    ops = Ops()

    def late(fd):
        ops.now = 2
        return ops.wait_result

    ops.reap = late
    with LinuxTdsProcess.acquire(IDENTITY, _ops=ops) as process:
        with pytest.raises(TdsProcessError, match="deadline"):
            process.contain(deadline=1, direct_child=True)


def test_other_supervisor_thread_cannot_signal():
    import threading

    ops = Ops()
    failures = []
    with LinuxTdsProcess.acquire(IDENTITY, _ops=ops) as process:

        def run():
            try:
                process.contain(deadline=1, direct_child=True)
            except TdsProcessError as exc:
                failures.append(str(exc))

        thread = threading.Thread(target=run)
        thread.start()
        thread.join(timeout=2)
        assert not thread.is_alive()
    assert failures == ["tds_process_handle_unavailable"]


@pytest.mark.parametrize("error", [None, ChildProcessError(), OSError(errno.EINVAL, "P_PIDFD rejected")])
def test_admission_probes_waitid_kernel_support(monkeypatch, error):
    from dpone.adapters.mssql_tds_process import _LinuxOps

    monkeypatch.setattr(_LinuxOps, "identity", lambda self, pid: IDENTITY)

    monkeypatch.setattr("dpone.adapters.mssql_tds_process.sys.platform", "linux")
    monkeypatch.setattr("dpone.adapters.mssql_tds_process.os.pidfd_open", lambda *a: 8, raising=False)
    monkeypatch.setattr("dpone.adapters.mssql_tds_process.signal.pidfd_send_signal", lambda *a: None, raising=False)
    monkeypatch.setattr("dpone.adapters.mssql_tds_process.os.waitid", lambda *a: None, raising=False)
    monkeypatch.setattr("dpone.adapters.mssql_tds_process.os.P_PIDFD", 3, raising=False)
    calls = []

    def reap(self, fd):
        calls.append("reap")
        if error:
            raise error

    monkeypatch.setattr(_LinuxOps, "reap", reap)
    monkeypatch.setattr(_LinuxOps, "close", lambda self, fd: calls.append("close"))
    if isinstance(error, OSError) and not isinstance(error, ChildProcessError):
        with pytest.raises(TdsProcessError, match="platform_unsupported"):
            LinuxTdsProcess.admit()
    else:
        LinuxTdsProcess.admit()
    assert calls == ["reap", "close"]


def test_other_thread_cannot_close_or_reuse_live_handle():
    import threading

    ops = Ops()
    errors = []
    with LinuxTdsProcess.acquire(IDENTITY, _ops=ops) as process:

        def close():
            try:
                process.close()
            except TdsProcessError as exc:
                errors.append(str(exc))

        thread = threading.Thread(target=close)
        thread.start()
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert "close" not in ops.calls
        assert process.contain(deadline=1, direct_child=True).reaped
    assert errors == ["tds_process_handle_unavailable"]


@pytest.mark.parametrize("flags,success", [(1, True), (8, False), (32, False), (16, False)])
def test_high_pidfd_uses_poll_and_rejects_invalid_events(monkeypatch, flags, success):
    from dpone.adapters.mssql_tds_process import _LinuxOps

    calls = []

    class Poll:
        def register(self, fd, mask):
            calls.append((fd, mask))

        def poll(self, timeout):
            calls.append(timeout)
            return [(4096, flags)]

    monkeypatch.setattr("dpone.adapters.mssql_tds_process.select.poll", Poll)
    if success:
        assert _LinuxOps().ready_until(4096, 0.02)
    else:
        with pytest.raises(TdsProcessError, match="poll_unknown"):
            _LinuxOps().ready_until(4096, 0.02)
    assert calls == [(4096, 1), 20]


def test_inherited_parent_procfs_namespace_is_rejected(monkeypatch):
    from dpone.adapters.mssql_tds_process import _LinuxOps

    monkeypatch.setattr("dpone.adapters.mssql_tds_process.os.readlink", lambda p: "999")
    monkeypatch.setattr("dpone.adapters.mssql_tds_process.os.getpid", lambda: 123)
    with pytest.raises(TdsProcessError, match="identity_unavailable"):
        _LinuxOps().identity(234)


def test_namespace_mismatch_rejected_even_when_numeric_self_pid_matches(monkeypatch):
    import os

    from dpone.adapters.mssql_tds_process import _LinuxOps

    links = {"/proc/self": str(os.getpid()), "/proc/self/ns/pid": "pid:[2]", "/proc/1/ns/pid": "pid:[1]"}
    monkeypatch.setattr("dpone.adapters.mssql_tds_process.os.readlink", lambda p: links[p])
    monkeypatch.setattr("dpone.adapters.mssql_tds_process.Path.read_text", lambda p, **kw: "a" * 32)
    with pytest.raises(TdsProcessError, match="identity_unavailable"):
        _LinuxOps().identity(234)


@pytest.mark.parametrize("machine", [None, "", "g" * 32, "a" * 31, "0" * 32, "a" * 32])
def test_admission_validates_host_identity(monkeypatch, machine):
    import os

    from dpone.adapters.mssql_tds_process import _LinuxOps

    monkeypatch.setattr("dpone.adapters.mssql_tds_process.sys.platform", "linux")
    monkeypatch.setattr("dpone.adapters.mssql_tds_process.os.pidfd_open", lambda *a: 8, raising=False)
    monkeypatch.setattr("dpone.adapters.mssql_tds_process.signal.pidfd_send_signal", lambda *a: None, raising=False)
    monkeypatch.setattr("dpone.adapters.mssql_tds_process.os.waitid", lambda *a: None, raising=False)
    monkeypatch.setattr("dpone.adapters.mssql_tds_process.os.P_PIDFD", 3, raising=False)
    monkeypatch.setattr(_LinuxOps, "reap", lambda *a: None)
    monkeypatch.setattr(_LinuxOps, "close", lambda *a: None)
    monkeypatch.setattr(
        "dpone.adapters.mssql_tds_process.os.readlink", lambda p: str(os.getpid()) if p == "/proc/self" else "pid:[1]"
    )

    def read(path, **kwargs):
        if str(path) == "/etc/machine-id":
            if machine is None:
                raise FileNotFoundError()
            return machine
        if str(path).endswith("boot_id"):
            return IDENTITY.boot_id
        return str(os.getpid()) + " (worker) S " + "0 " * 18 + "10"

    monkeypatch.setattr("dpone.adapters.mssql_tds_process.Path.read_text", read)
    if machine == "a" * 32:
        LinuxTdsProcess.admit()
    else:
        with pytest.raises(TdsProcessError, match="identity_unavailable"):
            LinuxTdsProcess.admit()

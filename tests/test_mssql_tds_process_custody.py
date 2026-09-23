"""Original resource effects cannot be repeated after lost acknowledgments."""

import threading
from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_tds_child_process import TdsChildProcess
from dpone.adapters.mssql_tds_process import LinuxTdsProcess, TdsProcessError
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity

IDENTITY = TdsProcessIdentity("a" * 64, "11111111-1111-1111-1111-111111111111", 234, 10)


class Ops:
    def __init__(self):
        self.now = 0.0
        self.reaps = self.signals = self.closes = 0
        self.late = False
        self.close_error = False

    def monotonic(self):
        return self.now

    def signal(self, fd, sig):
        self.signals += 1

    def ready_until(self, fd, remaining):
        return True

    def reap(self, fd):
        self.reaps += 1
        if self.late:
            self.now = 2.0
        return SimpleNamespace(si_pid=234, si_code=1, si_status=0)

    def close(self, fd):
        self.closes += 1
        if self.close_error:
            raise OSError("lost close acknowledgment")


def test_late_consumed_reap_is_sticky_unknown_without_second_waitid():
    ops = Ops()
    ops.late = True
    handle = LinuxTdsProcess(IDENTITY, 8, ops)
    with pytest.raises(TdsProcessError):
        handle.contain(deadline=1.0, direct_child=True)
    with pytest.raises(TdsProcessError):
        handle.contain(deadline=10.0, direct_child=True)
    assert ops.reaps == 1
    assert ops.signals == 1
    assert handle._fd == 8


def test_pidfd_close_uncertainty_cannot_become_success_on_retry():
    ops = Ops()
    ops.close_error = True
    handle = LinuxTdsProcess(IDENTITY, 8, ops)
    with pytest.raises(OSError):
        handle.close()
    with pytest.raises(TdsProcessError):
        handle.close()
    assert ops.closes == 1


def test_caught_foreign_owner_poison_preserves_original_cleanup(monkeypatch):
    monkeypatch.setattr("dpone.adapters.mssql_tds_child_process.time.monotonic", lambda: 0.0)
    ops = Ops()
    child = TdsChildProcess(SimpleNamespace(stdout=None), LinuxTdsProcess(IDENTITY, 8, ops), ())
    errors = []

    def foreign():
        try:
            child.check_owner()
        except ValueError as exc:
            errors.append(str(exc))

    thread = threading.Thread(target=foreign)
    thread.start()
    thread.join()
    assert errors
    with pytest.raises(ValueError):
        child.check_owner()
    assert child.terminate(deadline=1.0) == TdsChildExit(IDENTITY, 0, True)
    child.close()
    assert ops.reaps == ops.closes == 1


def test_unknown_descriptor_close_is_not_retried_and_other_resources_close(monkeypatch):
    calls = []

    def close(fd):
        calls.append(fd)
        if fd == 31:
            raise OSError("lost fd acknowledgment")

    monkeypatch.setattr("dpone.adapters.mssql_tds_child_process.os.close", close)
    ops = Ops()
    child = TdsChildProcess(SimpleNamespace(stdout=None), LinuxTdsProcess(IDENTITY, 8, ops), (31, 32))
    child.exit = TdsChildExit(IDENTITY, 0, True)
    with pytest.raises(OSError):
        child.close()
    with pytest.raises(Exception, match="unknown"):
        child.close()
    assert calls == [31, 32]
    assert ops.closes == 1


def test_acquisition_rollback_retains_ambiguous_original_token():
    ops = Ops()
    ops.admit = lambda: None
    identities = iter((IDENTITY, None))
    ops.identity = lambda pid: next(identities)
    ops.open = lambda pid: 83
    ops.close_error = True
    with pytest.raises(TdsProcessError) as caught:
        LinuxTdsProcess.acquire(IDENTITY, _ops=ops)
    child = TdsChildProcess(SimpleNamespace(stdout=None), None, ())
    child.retain_acquisition(caught.value)
    progress = child._resources[-1]
    assert progress.value == 83 and progress.state == "CLOSE_UNKNOWN"
    assert child.handle is None
    assert ops.signals == ops.reaps == 0 and ops.closes == 1
    assert not any(isinstance(value, BaseException) for value in vars(child).values())


def test_failed_budget_clock_is_never_repaired(monkeypatch):
    child = TdsChildProcess(SimpleNamespace(stdout=None), None, ())
    calls = []

    def clock():
        calls.append(True)
        raise OSError("clock failed")

    monkeypatch.setattr("dpone.adapters.mssql_tds_child_process.time.monotonic", clock)
    with pytest.raises(OSError):
        child.capture_budget(allowance=1.0)
    monkeypatch.setattr("dpone.adapters.mssql_tds_child_process.time.monotonic", lambda: 1.0)
    with pytest.raises(Exception, match="budget_unknown"):
        child.capture_budget(deadline=10.0, allowance=1.0)
    assert len(calls) == 1


def test_containment_executor_reuses_custody_and_registers_before_start(monkeypatch):
    import time

    from dpone.adapters.mssql_tds_child_process import TdsChildContainmentExecutor

    child = TdsChildProcess(SimpleNamespace(stdout=None), None, ())
    observed = []

    def start(thread):
        observed.append(child._executor is thread)

    monkeypatch.setattr(threading.Thread, "start", start)
    owner = TdsChildContainmentExecutor(child, IDENTITY, time.monotonic() + 10, 1.0)
    assert owner._resources is child
    assert owner.child is child.process
    assert child._executor is owner._thread
    assert observed == [True]


def test_create_and_departure_share_ambiguous_close_without_forged_compatibility():
    from uuid import uuid4

    from dpone.app.mssql_sqlclient_departure_supervision import (
        SqlClientCreateDepartureUnknown,
        _DepartureRetention,
    )
    from dpone.app.mssql_tds_coordinator_supervision import TdsCoordinatorRetention

    calls = []

    def stop(*, deadline):
        calls.append(("terminate", deadline))
        return TdsChildExit(IDENTITY, -9, True)

    def close():
        calls.append("child.close")
        raise OSError("lost child close acknowledgment")

    gateway = SimpleNamespace(close=lambda **kw: calls.append("gateway.close"))
    create = TdsCoordinatorRetention(
        gateway,
        gateway,
        None,
        None,
        None,
        b"",
        b"",
        child=SimpleNamespace(terminate=stop, close=close),
        process=IDENTITY,
    )
    deadline = create.capture_cleanup_budget(2.0, lambda: 1.0)
    with pytest.raises(OSError):
        create.cleanup_local(deadline, lambda: 1.0)
    # Writable compatibility fields cannot erase the original uncertain effect.
    create.child_close_attempted = False
    create.child_closed = True
    create.containment_deadline = 1000.0
    departure = _DepartureRetention(
        SimpleNamespace(close=lambda **kw: calls.append("attempt.close")),
        None,
        uuid4(),
        None,
        None,
        lambda: 1.0,
        create_retained=create,
    )
    departure.capture_budget(20.0)
    assert departure.containment_deadline == 3.0
    with pytest.raises(SqlClientCreateDepartureUnknown):
        departure.close(deadline=100.0)
    assert calls.count("child.close") == 1
    assert calls.count(("terminate", 3.0)) == 1
    assert calls.count("gateway.close") == 2
    assert calls.count("attempt.close") == 1


def test_observe_shortening_limits_real_loop_and_keeps_unknown_pidfd(monkeypatch):
    import time

    from dpone.adapters.mssql_tds_child_process import TdsChildContainmentExecutor

    entered = threading.Event()
    threads, durations, handles = [], [], []
    ops = Ops()
    ops.monotonic = time.monotonic

    def ready(fd, remaining):
        threads.append(threading.current_thread())
        durations.append(remaining)
        entered.set()
        time.sleep(min(remaining, 0.002))
        return False

    ops.ready_until = ready

    def acquire(identity):
        handle = LinuxTdsProcess(identity, 83, ops)
        handles.append(handle)
        return handle

    monkeypatch.setattr(LinuxTdsProcess, "acquire", acquire)
    owner = TdsChildContainmentExecutor(SimpleNamespace(stdout=None), IDENTITY, time.monotonic() + 10, 2.0)
    assert owner.ready.wait(1)
    original = owner.request()
    assert entered.wait(1)
    shortened = time.monotonic() + 0.02
    assert owner.request(shortened) == shortened < original
    assert owner.done.wait(0.5)
    assert owner.failed and owner.exit is None
    assert ops.signals == 1 and ops.reaps == ops.closes == 0
    assert max(durations) <= 0.01
    assert set(threads) == {owner._thread}
    assert handles[0]._fd == 83


def test_observe_idle_expiry_and_socket_unknown_share_original_ledger(monkeypatch):
    import time

    from dpone.adapters.mssql_sqlclient_observe_process import SqlClientObserveProcess
    from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup

    ops = Ops()
    ops.monotonic = time.monotonic
    monkeypatch.setattr(LinuxTdsProcess, "acquire", lambda identity: LinuxTdsProcess(identity, 83, ops))
    calls = []

    def close_socket():
        calls.append("socket.close")
        raise OSError("socket close ACK lost")

    channel = SimpleNamespace(close=close_socket)
    now = time.monotonic()
    process = SqlClientObserveProcess(
        SimpleNamespace(stdout=None),
        channel,
        TdsCoordinatorStartup(IDENTITY, "b" * 64, "/src", b"n" * 32),
        startup_deadline=now + 0.01,
        operation_deadline=now + 0.02,
        termination_timeout=1.0,
    )
    assert process._containment.done.wait(1)
    assert process._containment.exit == TdsChildExit(IDENTITY, 0, True)
    first = process.cleanup_deadline
    assert first is not None
    for _ in range(2):
        with pytest.raises(Exception, match="unknown"):
            process.close()
    assert process._containment.request(first + 100.0) == first
    assert calls == ["socket.close"] and ops.reaps == ops.closes == 1


@pytest.mark.parametrize("transition", ["ready", "done"])
def test_executor_event_failure_retains_original_handle(monkeypatch, transition):
    import time

    from dpone.adapters.mssql_tds_child_process import TdsChildContainmentExecutor

    acquired, release = threading.Event(), threading.Event()
    ops = Ops()
    ops.monotonic = time.monotonic
    handles = []

    def acquire(identity):
        handle = LinuxTdsProcess(identity, 83, ops)
        handles.append(handle)
        acquired.set()
        assert release.wait(1)
        return handle

    monkeypatch.setattr(LinuxTdsProcess, "acquire", acquire)
    owner = TdsChildContainmentExecutor(SimpleNamespace(stdout=None), IDENTITY, time.monotonic() + 10, 1.0)
    assert acquired.wait(1)

    def fail():
        raise RuntimeError("event acknowledgment lost")

    monkeypatch.setattr(getattr(owner, transition), "set", fail)
    owner.request()
    release.set()
    owner._thread.join(1)
    assert not owner._thread.is_alive() and owner.failed
    assert owner._resources.handle is handles[0]
    if transition == "ready":
        assert handles[0]._fd == 83 and ops.signals == ops.reaps == ops.closes == 0
    else:
        assert owner.exit is not None and ops.reaps == ops.closes == 1
        assert not owner.done.is_set()

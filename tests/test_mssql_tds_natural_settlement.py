"""Executor-owned natural settlement without competing child wait authority."""

import signal
import threading
import time
from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_tds_child_process import TdsChildContainmentExecutor
from dpone.adapters.mssql_tds_natural_settlement import (
    TdsNaturalSettlementCommand,
    TdsNaturalSettlementPolicyError,
    TdsNaturalSettlementState,
)
from dpone.adapters.mssql_tds_process import LinuxTdsProcess
from dpone.contracts.bounded_window import WindowOutcomeUnknown
from dpone.contracts.mssql_tds_worker import TdsProcessIdentity

IDENTITY = TdsProcessIdentity("a" * 64, "11111111-1111-1111-1111-111111111111", 234, 10)


class Ops:
    def __init__(self, exit_code=0):
        self.exit_code = exit_code
        self.signals = self.reaps = self.closes = 0
        self.entered = threading.Event()
        self.release = threading.Event()
        self.block = False

    def monotonic(self):
        return time.monotonic()

    def signal(self, _fd, observed):
        assert observed == signal.SIGKILL
        self.signals += 1

    def ready_until(self, _fd, remaining):
        self.entered.set()
        if self.block:
            self.release.wait(min(remaining, 1.0))
            return False
        if self.signals:
            return True
        time.sleep(min(remaining, 0.002))
        return self.exit_code is not None

    def reap(self, _fd):
        self.reaps += 1
        if self.block:
            self.release.wait(1.0)
        code = self.exit_code if self.exit_code is not None else -signal.SIGKILL
        si_code, status = (1, code) if code >= 0 else (2, -code)
        return SimpleNamespace(si_pid=IDENTITY.pid, si_code=si_code, si_status=status)

    def close(self, _fd):
        self.closes += 1


def owner(monkeypatch, operations, *, operation_deadline=None):
    monkeypatch.setattr(
        LinuxTdsProcess,
        "acquire",
        classmethod(lambda cls, identity: cls(identity, 8, operations)),
    )
    process = SimpleNamespace(stdout=None, returncode=None)
    return TdsChildContainmentExecutor(
        process,
        IDENTITY,
        operation_deadline or time.monotonic() + 2.0,
        0.5,
    )


def resolved_state(*, natural=10.0, containment=None, now=1.0):
    state = TdsNaturalSettlementState()
    state.admit(TdsNaturalSettlementCommand(natural, containment))
    plan = state.resolve(operation_deadline=20.0, allowance=2.0, now=now)
    return state, plan


@pytest.mark.parametrize("value", [True, False, float("nan"), float("inf"), "10", None, 10**1000])
def test_pure_command_rejects_non_exact_natural_deadline(value):
    with pytest.raises(ValueError, match="tds_natural_settlement_deadline"):
        TdsNaturalSettlementCommand(value)


def test_pure_state_preserves_duplicate_and_rejects_prior_force():
    state = TdsNaturalSettlementState()
    accepted = TdsNaturalSettlementCommand(10.0)
    state.admit(accepted)
    with pytest.raises(ValueError, match="already_requested"):
        state.admit(TdsNaturalSettlementCommand(11.0))
    assert state.resolve(operation_deadline=9.0, allowance=2.0, now=1.0).natural_deadline == 9.0

    forced = TdsNaturalSettlementState()
    forced.request_force()
    with pytest.raises(ValueError, match="settlement_unavailable"):
        forced.admit(accepted)


def test_pure_plan_clips_shortens_and_handles_negative_clock_domain():
    state, plan = resolved_state(natural=-1.0, containment=-0.25, now=-10.0)
    assert plan.natural_deadline == -1.0
    assert plan.cleanup_deadline == -0.25
    assert not plan.force_immediately
    state.request_force()
    assert state.natural_ceiling() == -10.0


@pytest.mark.parametrize(
    "error_args,reaped,unknown,permitted",
    [
        (("tds_process_deadline_exceeded",), False, False, True),
        (("tds_process_deadline_exceeded", "extra"), False, False, False),
        (("mssql_native.tds_process_deadline_exceeded",), False, False, False),
        (("tds_process_deadline_exceeded",), True, False, False),
        (("tds_process_deadline_exceeded",), False, True, False),
    ],
)
def test_pure_transition_requires_exact_tuple_and_clean_handle(error_args, reaped, unknown, permitted):
    state, _ = resolved_state()
    assert (
        state.permit_forced_transition(error_args=error_args, reap_consumed=reaped, handle_unknown=unknown) is permitted
    )
    if not permitted:
        with pytest.raises(TdsNaturalSettlementPolicyError):
            state.natural_ceiling()


def test_pure_plan_overflow_expiry_and_unknown_are_irreversible():
    overflow = TdsNaturalSettlementState()
    overflow.admit(TdsNaturalSettlementCommand(1e308))
    with pytest.raises(TdsNaturalSettlementPolicyError):
        overflow.resolve(operation_deadline=1e308, allowance=1e308, now=0.0)
    state, _ = resolved_state()
    state.mark_unknown()
    state.request_force()
    with pytest.raises(TdsNaturalSettlementPolicyError):
        state.natural_ceiling()


def test_natural_settlement_api_is_available(monkeypatch):
    monkeypatch.setattr("dpone.adapters.mssql_tds_child_process.threading.Thread.start", lambda self: None)
    monkeypatch.setattr("dpone.adapters.mssql_tds_child_process.time.monotonic", lambda: 0.0)
    owner = TdsChildContainmentExecutor(SimpleNamespace(stdout=None), object(), 20.0, 2.0)
    assert owner.request_settlement(natural_deadline=10.0) == 12.0


@pytest.mark.parametrize("value", [True, float("nan"), float("inf"), "10"])
def test_natural_deadline_is_strict(value, monkeypatch):
    monkeypatch.setattr("dpone.adapters.mssql_tds_child_process.threading.Thread.start", lambda self: None)
    owner = TdsChildContainmentExecutor(SimpleNamespace(stdout=None), object(), 20.0, 2.0)
    with pytest.raises(ValueError, match="tds_natural_settlement_deadline"):
        owner.request_settlement(natural_deadline=value)


@pytest.mark.parametrize("exit_code", [0, 17])
def test_natural_exit_reaps_and_closes_without_signal(monkeypatch, exit_code):
    operations = Ops(exit_code)
    executor = owner(monkeypatch, operations)
    executor.request_settlement(natural_deadline=time.monotonic() + 0.4)
    assert executor.done.wait(1.0)
    assert not executor.failed
    assert executor.exit is not None and executor.exit.exit_code == exit_code
    assert (operations.signals, operations.reaps, operations.closes) == (0, 1, 1)


def test_natural_deadline_forces_on_same_handle(monkeypatch):
    operations = Ops(None)
    executor = owner(monkeypatch, operations)
    executor.request_settlement(natural_deadline=time.monotonic() + 0.02)
    assert executor.done.wait(1.0)
    assert not executor.failed
    assert executor.exit is not None and executor.exit.exit_code == -signal.SIGKILL
    assert (operations.signals, operations.reaps, operations.closes) == (1, 1, 1)


def test_concurrent_force_shortens_natural_wait(monkeypatch):
    operations = Ops(None)
    operations.block = True
    executor = owner(monkeypatch, operations)
    cleanup = executor.request_settlement(natural_deadline=time.monotonic() + 1.0)
    assert operations.entered.wait(1.0)
    assert executor.request(cleanup - 0.1) == cleanup - 0.1
    operations.block = False
    operations.release.set()
    assert executor.done.wait(1.0)
    assert not executor.failed and operations.signals == operations.reaps == operations.closes == 1


def test_force_during_consumed_reap_is_unknown_without_second_effect(monkeypatch):
    operations = Ops(0)
    original_reap = operations.reap
    reap_entered = threading.Event()

    def blocked_reap(fd):
        reap_entered.set()
        operations.release.wait(1.0)
        return original_reap(fd)

    operations.reap = blocked_reap
    operations.entered.clear()
    executor = owner(monkeypatch, operations)
    executor.request_settlement(natural_deadline=time.monotonic() + 1.0)
    assert reap_entered.wait(1.0)
    executor.request()
    operations.release.set()
    assert executor.done.wait(1.0)
    assert executor.failed and executor.exit is None
    assert (operations.signals, operations.reaps, operations.closes) == (0, 1, 0)


def test_duplicate_and_prior_force_are_deterministic(monkeypatch):
    monkeypatch.setattr("dpone.adapters.mssql_tds_child_process.threading.Thread.start", lambda self: None)
    monkeypatch.setattr("dpone.adapters.mssql_tds_child_process.time.monotonic", lambda: 1.0)
    accepted = TdsChildContainmentExecutor(SimpleNamespace(stdout=None), object(), 20.0, 2.0)
    assert accepted.request_settlement(natural_deadline=10.0) == 12.0
    with pytest.raises(ValueError, match="already_requested"):
        accepted.request_settlement(natural_deadline=9.0)
    assert accepted.cleanup_deadline == 12.0 and not accepted.failed

    forced = TdsChildContainmentExecutor(SimpleNamespace(stdout=None), object(), 20.0, 2.0)
    assert forced.request(8.0) == 3.0
    with pytest.raises(ValueError, match="settlement_unavailable"):
        forced.request_settlement(natural_deadline=10.0)


def test_clock_failure_consumes_natural_command_and_stays_unknown(monkeypatch):
    monkeypatch.setattr("dpone.adapters.mssql_tds_child_process.threading.Thread.start", lambda self: None)
    executor = TdsChildContainmentExecutor(SimpleNamespace(stdout=None), object(), 20.0, 2.0)
    monkeypatch.setattr(
        "dpone.adapters.mssql_tds_child_process.time.monotonic",
        lambda: (_ for _ in ()).throw(OSError("clock")),
    )
    with pytest.raises(OSError, match="clock"):
        executor.request_settlement(natural_deadline=10.0)
    with pytest.raises(ValueError, match="already_requested"):
        executor.request_settlement(natural_deadline=10.0)
    assert executor.failed and executor._resources._unknown


def test_expired_cleanup_and_overflow_are_sticky_unknown(monkeypatch):
    monkeypatch.setattr("dpone.adapters.mssql_tds_child_process.threading.Thread.start", lambda self: None)
    monkeypatch.setattr("dpone.adapters.mssql_tds_child_process.time.monotonic", lambda: 10.0)
    expired = TdsChildContainmentExecutor(SimpleNamespace(stdout=None), object(), 20.0, 2.0)
    with pytest.raises(WindowOutcomeUnknown, match="natural_settlement_unknown"):
        expired.request_settlement(natural_deadline=11.0, containment_deadline=9.0)
    overflow = TdsChildContainmentExecutor(SimpleNamespace(stdout=None), object(), float("1e308"), float("1e308"))
    with pytest.raises(WindowOutcomeUnknown, match="natural_settlement_unknown"):
        overflow.request_settlement(natural_deadline=float("1e308"))


def test_natural_deadline_is_clipped_and_cleanup_only_shortens(monkeypatch):
    monkeypatch.setattr("dpone.adapters.mssql_tds_child_process.threading.Thread.start", lambda self: None)
    monkeypatch.setattr("dpone.adapters.mssql_tds_child_process.time.monotonic", lambda: 1.0)
    executor = TdsChildContainmentExecutor(SimpleNamespace(stdout=None), object(), 7.0, 2.0)
    assert executor.request_settlement(natural_deadline=10.0, containment_deadline=8.0) == 8.0
    assert executor._settlement.plan is not None and executor._settlement.plan.natural_deadline == 7.0
    assert executor.request(100.0) == 8.0
    assert executor.request(6.0) == 6.0


def test_unexpected_natural_error_poison_is_sticky_without_later_effects(monkeypatch):
    operations = Ops(0)
    monkeypatch.setattr("dpone.adapters.mssql_tds_child_process.threading.Thread.start", lambda self: None)
    monkeypatch.setattr(
        LinuxTdsProcess,
        "acquire",
        classmethod(lambda cls, identity: cls(identity, 8, operations)),
    )
    executor = TdsChildContainmentExecutor(
        SimpleNamespace(stdout=None, returncode=None), IDENTITY, time.monotonic() + 2.0, 0.5
    )
    executor.request_settlement(natural_deadline=time.monotonic() + 1.0)
    monkeypatch.setattr(
        executor._resources,
        "_settle",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("unexpected")),
    )
    executor._run()
    assert executor.failed and executor._resources._unknown and executor._resources._poisoned
    assert executor.exit is None
    assert (operations.signals, operations.reaps, operations.closes) == (0, 0, 0)
    executor.request()
    assert (operations.signals, operations.reaps, operations.closes) == (0, 0, 0)


def test_expiry_during_delayed_acquire_skips_natural_and_forces(monkeypatch):
    operations = Ops(None)
    entered, release = threading.Event(), threading.Event()

    def acquire(cls, identity):
        entered.set()
        assert release.wait(1.0)
        return cls(identity, 8, operations)

    monkeypatch.setattr(LinuxTdsProcess, "acquire", classmethod(acquire))
    executor = TdsChildContainmentExecutor(
        SimpleNamespace(stdout=None, returncode=None), IDENTITY, time.monotonic() + 2.0, 0.5
    )
    assert entered.wait(1.0)
    executor.request_settlement(natural_deadline=time.monotonic() + 0.01)
    time.sleep(0.02)
    release.set()
    assert executor.done.wait(1.0) and not executor.failed
    assert executor.exit is not None and executor.exit.exit_code == -signal.SIGKILL
    assert (operations.signals, operations.reaps, operations.closes) == (1, 1, 1)

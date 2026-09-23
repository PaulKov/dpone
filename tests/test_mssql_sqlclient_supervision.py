"""Mocked local ownership faults; no SQL settlement or live-route certification."""

from dataclasses import replace
from hashlib import sha256
from threading import Thread
from types import SimpleNamespace

import pytest

from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import TdsInputReceipt
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity
from dpone.services.mssql_sqlclient_supervision import (
    SqlClientSupervisionUnknown,
    _SqlClientExecution,
    validate_local_exit,
)


@pytest.fixture
def execution():
    calls = []
    now = [10.0]
    identity = TdsProcessIdentity("a" * 64, "11111111-1111-4111-8111-111111111111", 123, 9)
    child = SimpleNamespace(received_result=None)
    child.terminate = lambda **kw: (calls.append(("terminate", kw["deadline"])), TdsChildExit(identity, -9, True))[1]
    child.close = lambda: calls.append(("process_close", None))
    gateway = SimpleNamespace(close=lambda **kw: calls.append(("gateway_close", kw["deadline"])))
    retained = _SqlClientExecution(
        expected_input=TdsInputReceipt(0, 0, sha256(b"").hexdigest()),
        termination_timeout=5,
        clock=lambda: now[0],
        gateways=(gateway,),
        process=child,
        expected_process=identity,
    )
    return SimpleNamespace(retained=retained, child=child, gateway=gateway, calls=calls, now=now, identity=identity)


def test_local_stop_precedes_actor_close_and_has_fixed_deadline(execution):
    execution.retained.close(deadline=100)
    assert execution.calls == [("terminate", 15.0), ("process_close", None), ("gateway_close", 15.0)]
    assert execution.retained.local_exit == TdsChildExit(execution.identity, -9, True)
    execution.now[0] = 11
    execution.retained.close(deadline=1000)
    assert execution.calls[-1] == ("gateway_close", 15.0)
    assert sum(name == "terminate" for name, _ in execution.calls) == 1


def test_original_budget_captured_before_any_fallible_work(execution):
    assert execution.retained.capture_containment_deadline() == 15
    execution.now[0] = 14
    execution.retained.close(deadline=999)
    assert execution.calls[0] == ("terminate", 15)


def test_caller_can_shorten_but_never_extend_teardown(execution):
    execution.retained.close(deadline=12)
    assert execution.calls[0] == ("terminate", 12)
    assert execution.calls[-1] == ("gateway_close", 12)


def test_missing_process_identity_never_permits_descriptor_release(execution):
    execution.retained.expected_process = None
    with pytest.raises(SqlClientSupervisionUnknown):
        execution.retained.close(deadline=100)
    assert ("process_close", None) not in execution.calls
    assert execution.calls[-1] == ("gateway_close", 15)


@pytest.mark.parametrize("failure", ["wrong_identity", "not_reaped", "wrong_exit_type", "terminate"])
def test_unknown_process_still_signals_all_actors(execution, failure):
    def terminate(**kw):
        execution.calls.append(("terminate", kw["deadline"]))
        if failure == "terminate":
            raise OSError("private driver text")
        if failure == "wrong_exit_type":
            return SimpleNamespace(identity=execution.identity, exit_code=0, reaped=True)
        identity = replace(execution.identity, start_ticks=10) if failure == "wrong_identity" else execution.identity
        return TdsChildExit(identity, 0, failure != "not_reaped")

    execution.child.terminate = terminate
    with pytest.raises(SqlClientSupervisionUnknown) as error:
        execution.retained.close(deadline=100)
    assert "private driver text" not in str(error.value)
    assert execution.calls == [("terminate", 15), ("gateway_close", 15)]
    assert execution.retained.local_exit is None


def test_raw_invalid_result_remains_memory_only_and_repr_hidden(execution):
    raw = b'{"password":"PRIVATE_CANARY"}'
    execution.child.received_result = raw
    execution.retained.close(deadline=100)
    assert execution.retained.raw_result is raw
    assert "PRIVATE_CANARY" not in repr(execution.retained)
    assert execution.retained.result is None
    assert execution.retained.receipts == {}


def test_changed_result_cannot_replace_original_or_skip_stop(execution):
    original = b"first"
    execution.retained.raw_result = original
    execution.child.received_result = b"second"
    with pytest.raises(SqlClientSupervisionUnknown):
        execution.retained.close(deadline=100)
    assert execution.retained.raw_result is original
    assert ("process_close", None) in execution.calls


@pytest.mark.parametrize("raw", [b"", b"x" * 16385, bytearray(b"x"), "x"])
def test_invalid_retention_bounds_do_not_skip_containment(execution, raw):
    execution.child.received_result = raw
    with pytest.raises(SqlClientSupervisionUnknown):
        execution.retained.close(deadline=100)
    assert execution.retained.raw_result is None
    assert ("process_close", None) in execution.calls


def test_every_actor_gets_close_even_when_first_fails(execution):
    def fail(**kw):
        execution.calls.append(("bad_gateway", kw["deadline"]))
        raise OSError("private store text")

    execution.retained.gateways = (SimpleNamespace(close=fail), execution.gateway)
    with pytest.raises(SqlClientSupervisionUnknown):
        execution.retained.close(deadline=100)
    assert execution.calls[-2:] == [("bad_gateway", 15), ("gateway_close", 15)]


def test_expired_teardown_signals_actors_without_new_process_budget(execution):
    execution.retained.capture_containment_deadline()
    execution.now[0] = 16
    with pytest.raises(SqlClientSupervisionUnknown):
        execution.retained.close(deadline=100)
    assert execution.calls == [("gateway_close", 15)]


def test_exit_after_deadline_is_retained_without_descriptor_release(execution):
    def terminate(**kw):
        execution.now[0] = 16
        return TdsChildExit(execution.identity, -9, True)

    execution.child.terminate = terminate
    with pytest.raises(SqlClientSupervisionUnknown):
        execution.retained.close(deadline=100)
    assert execution.retained.local_exit == TdsChildExit(execution.identity, -9, True)
    assert execution.calls == [("gateway_close", 15)]


@pytest.mark.parametrize("clock_failure", ["nan", "exception"])
def test_invalid_clock_cannot_capture_replacement_budget(execution, clock_failure):
    if clock_failure == "nan":
        execution.now[0] = float("nan")
    else:

        def broken_clock():
            raise OSError("PRIVATE_CLOCK_CANARY")

        execution.retained.clock = broken_clock
    with pytest.raises(SqlClientSupervisionUnknown):
        execution.retained.close(deadline=100)
    execution.now[0] = 10
    execution.retained.clock = lambda: execution.now[0]
    with pytest.raises(SqlClientSupervisionUnknown):
        execution.retained.close(deadline=100)
    assert execution.calls == [("gateway_close", 0), ("gateway_close", 0)]


def test_unresolved_launch_is_retained_until_containment(execution):
    execution.retained.process = None
    execution.retained.unresolved_launch = SimpleNamespace(
        contain=lambda **kw: execution.calls.append(("contain", kw["deadline"])),
        close=lambda: execution.calls.append(("launch_close", None)),
    )
    error = SqlClientSupervisionUnknown(execution.retained)
    error.close(deadline=100)
    assert execution.calls == [("contain", 15), ("launch_close", None), ("gateway_close", 15)]
    assert not hasattr(error, "send") and not hasattr(error, "process")


def test_cross_thread_close_has_no_effects(execution):
    failures = []

    def run():
        try:
            SqlClientSupervisionUnknown(execution.retained).close(deadline=100)
        except BaseException as error:
            failures.append(error)

    thread = Thread(target=run)
    thread.start()
    thread.join(2)
    assert not thread.is_alive()
    assert len(failures) == 1 and isinstance(failures[0], WindowContractError)
    assert execution.calls == []


def test_fork_identity_and_reentrant_close_have_no_effects(execution, monkeypatch):
    with monkeypatch.context() as patch:
        patch.setattr("dpone.services.mssql_sqlclient_supervision.os.getpid", lambda: -1)
        with pytest.raises(WindowContractError):
            execution.retained.close(deadline=100)
    execution.retained._busy = True
    with pytest.raises(WindowContractError):
        execution.retained.close(deadline=100)
    assert execution.calls == []


@pytest.mark.parametrize("field", ["pid", "start_ticks"])
def test_forged_nested_exit_identity_cannot_use_boolean_alias(execution, field):
    expected = replace(execution.identity, **{field: 1})
    observed_identity = replace(expected)
    object.__setattr__(observed_identity, field, True)
    # Dataclass equality considers True == 1; repeat nested scalar validation.
    assert observed_identity == expected
    with pytest.raises(ValueError):
        validate_local_exit(TdsChildExit(observed_identity, 0, True), expected)

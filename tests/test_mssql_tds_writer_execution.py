from dataclasses import replace

import pytest

from dpone.contracts.mssql_sqlclient_evidence_types import SqlClientEvidenceKind, SqlClientEvidenceObservation
from dpone.contracts.mssql_sqlclient_launch import launch_digest
from dpone.contracts.mssql_sqlclient_result import SqlClientResult, encode_sqlclient_result
from dpone.contracts.mssql_tds_result import TdsAttemptError, TdsWorkerResult
from dpone.contracts.mssql_tds_worker import TdsAttemptPhase, TdsChildExit
from dpone.services.mssql_tds_writer_execution import (
    SqlClientWriterExecutionUnknown,
    SqlClientWriterHandledFailure,
    SqlClientWriterLocallyExited,
    execute_sqlclient_writer,
)
from dpone.services.mssql_tds_writer_observation import prepare_sqlclient_writer_observation
from tests.test_mssql_tds_writer_launch import Process
from tests.test_mssql_tds_writer_launch import setup as setup
from tests.test_mssql_tds_writer_observation import GRANT_ID, _observer, _pregrant


def _ready(setup, monkeypatch):
    pregrant, target, _ = _pregrant(setup, monkeypatch)
    observer, backend = _observer(setup, target, None)
    ready = prepare_sqlclient_writer_observation(pregrant, observer=observer, grant_id=GRANT_ID, now_ns=1)
    return ready, backend


def _result(setup, *, error=None, grant_id=GRANT_ID):
    launch = setup.process.declared_launch
    receipt = None if error is not None else setup.plan.input_descriptor.expected
    return encode_sqlclient_result(
        SqlClientResult(
            1,
            launch_digest(launch),
            launch.attempt_sha256,
            grant_id,
            TdsWorkerResult(launch.attempt_sha256, receipt, error),
        )
    )


def _install_process(monkeypatch, setup, raw: bytes, *, exit_code=0, send_error=False):
    def send_grant(self, body, *, deadline):
        self.calls.append(("send_grant", body, deadline))
        if send_error:
            raise OSError("private partial grant")

    def receive_result(self, *, deadline):
        self.calls.append(("receive_result", deadline))
        self._received_result = raw
        return self._received_result

    def wait(self, *, deadline):
        self.calls.append(("wait", deadline))
        return TdsChildExit(self.identity, exit_code, True)

    monkeypatch.setattr(Process, "send_grant", send_grant, raising=False)
    monkeypatch.setattr(Process, "receive_result", receive_result, raising=False)
    monkeypatch.setattr(Process, "wait", wait, raising=False)
    monkeypatch.setattr(
        Process, "received_result", property(lambda self: getattr(self, "_received_result", None)), raising=False
    )


def test_one_grant_result_exit_and_all_local_closes_precede_success(setup, monkeypatch):
    ready, observer_backend = _ready(setup, monkeypatch)
    raw = _result(setup)
    _install_process(monkeypatch, setup, raw)

    exited = execute_sqlclient_writer(ready, clock_ns=lambda: 1)

    assert isinstance(exited, SqlClientWriterLocallyExited)
    assert repr(exited) == "SqlClientWriterLocallyExited(<opaque>)"
    assert [call[0] for call in setup.calls].count("send_grant") == 1
    assert [call[0] for call in setup.calls[-5:]] == [
        "send_grant",
        "receive_result",
        "evidence",
        "wait",
        "evidence",
    ]
    assert setup.lifecycle.calls[-1] == ("Exited", 3.0)
    assert setup.lifecycle.snapshot.state.phase is TdsAttemptPhase.EXITED
    assert setup.process.closed == 1
    assert setup.evidence.closed == []
    assert setup.lifecycle.closed == []
    assert observer_backend.calls[-2:] == [("contain", 6.0), ("close", 6.0)]
    cleanup = exited._claim_p10f_once(exited)
    owner = exited._assert_p10f_claim(exited, cleanup)
    assert owner.result_bytes is raw
    assert owner.result.result.error is None
    assert owner.local_exit.exit_code == 0
    assert not hasattr(owner, "process") and not hasattr(owner, "credentials")


def test_ambiguous_grant_delivery_is_sticky_unknown_and_never_resends(setup, monkeypatch):
    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup), send_error=True)

    with pytest.raises(SqlClientWriterExecutionUnknown, match="writer_execution_unknown") as caught:
        execute_sqlclient_writer(ready, clock_ns=lambda: 1)

    assert [call[0] for call in setup.calls].count("send_grant") == 1
    assert not hasattr(caught.value, "owner")
    assert not hasattr(caught.value, "process")
    assert not hasattr(caught.value, "send_grant")
    assert "private partial grant" not in str(caught.value)
    with pytest.raises(ValueError):
        execute_sqlclient_writer(ready, clock_ns=lambda: 1)
    assert [call[0] for call in setup.calls].count("send_grant") == 1


@pytest.mark.parametrize(
    ("error", "grant_id", "exit_code", "accepted"),
    [
        (TdsAttemptError.DRIVER, None, 1, True),
        (TdsAttemptError.DRIVER, GRANT_ID, 1, True),
        (TdsAttemptError.DRIVER, None, 0, False),
        (TdsAttemptError.DRIVER, GRANT_ID, 70, False),
        (None, GRANT_ID, 1, False),
        (None, GRANT_ID, -9, False),
    ],
)
def test_exact_result_exit_matrix(setup, monkeypatch, error, grant_id, exit_code, accepted):
    ready, _ = _ready(setup, monkeypatch)
    raw = _result(setup, error=error, grant_id=grant_id)
    _install_process(monkeypatch, setup, raw, exit_code=exit_code)

    if accepted:
        result = execute_sqlclient_writer(ready, clock_ns=lambda: 1)
        assert isinstance(result, SqlClientWriterHandledFailure)
        assert setup.lifecycle.snapshot.state.phase is TdsAttemptPhase.CONTAINED
        assert setup.lifecycle.snapshot.state.error is error
        assert setup.lifecycle.snapshot.state.observation_sha256 is not None
        assert setup.evidence.closed == [6.0]
        assert setup.lifecycle.closed == [6.0]
    else:
        with pytest.raises(SqlClientWriterExecutionUnknown):
            execute_sqlclient_writer(ready, clock_ns=lambda: 1)


@pytest.mark.parametrize("failed_close", ["process", "observer"])
def test_close_failure_after_exited_ack_returns_unknown_without_resend(setup, monkeypatch, failed_close):
    ready, observer_backend = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))
    if failed_close == "process":
        monkeypatch.setattr(Process, "close", lambda self: (_ for _ in ()).throw(OSError("close")))
    elif failed_close == "evidence":
        monkeypatch.setattr(type(setup.evidence), "close", lambda self, **kw: (_ for _ in ()).throw(OSError("close")))
    elif failed_close == "lifecycle":
        monkeypatch.setattr(type(setup.lifecycle), "close", lambda self, **kw: (_ for _ in ()).throw(OSError("close")))
    else:
        monkeypatch.setattr(
            type(observer_backend),
            "close",
            lambda self, **kw: (_ for _ in ()).throw(OSError("close")),
        )

    with pytest.raises(SqlClientWriterExecutionUnknown):
        execute_sqlclient_writer(ready, clock_ns=lambda: 1)

    assert setup.lifecycle.snapshot.state.phase is TdsAttemptPhase.EXITED
    assert [call[0] for call in setup.calls].count("send_grant") == 1
    with pytest.raises(ValueError):
        execute_sqlclient_writer(ready, clock_ns=lambda: 1)
    assert [call[0] for call in setup.calls].count("send_grant") == 1


def test_result_return_must_be_the_exact_eof_retained_object(setup, monkeypatch):
    ready, _ = _ready(setup, monkeypatch)
    raw = _result(setup)
    _install_process(monkeypatch, setup, raw)

    def receive_result(self, *, deadline):
        self.calls.append(("receive_result", deadline))
        self._received_result = raw
        return bytes(bytearray(raw))

    monkeypatch.setattr(Process, "receive_result", receive_result)
    with pytest.raises(SqlClientWriterExecutionUnknown):
        execute_sqlclient_writer(ready, clock_ns=lambda: 1)
    assert [call[0] for call in setup.calls].count("send_grant") == 1


def test_expired_original_deadline_rejects_before_grant_send(setup, monkeypatch):
    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))

    with pytest.raises(SqlClientWriterExecutionUnknown):
        execute_sqlclient_writer(
            ready,
            clock_ns=lambda: setup.process.declared_launch.operation_deadline_ns,
        )

    assert not any(call[0] == "send_grant" for call in setup.calls)


def test_instance_method_shadows_cannot_redirect_effects_or_acks(setup, monkeypatch):
    ready, observer_backend = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))
    callbacks = []

    def shadow(*args, **kwargs):
        callbacks.append((args, kwargs))
        raise AssertionError("instance shadow executed")

    setup.process.send_grant = shadow
    setup.process.receive_result = shadow
    setup.process.wait = shadow
    setup.process.close = shadow
    setup.evidence.write = shadow
    setup.evidence.close = shadow
    setup.lifecycle.advance = shadow
    setup.lifecycle.close = shadow
    observer_backend.contain = shadow
    observer_backend.close = shadow

    execute_sqlclient_writer(ready, clock_ns=lambda: 1)

    assert callbacks == []


@pytest.mark.parametrize("kind", [SqlClientEvidenceKind.RESULT, SqlClientEvidenceKind.LOCAL_EXIT])
def test_replaced_current_evidence_observation_is_unknown(setup, monkeypatch, kind):
    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))
    original = type(setup.evidence).write

    def replace_observation(self, record, *, deadline):
        receipt = original(self, record, deadline=deadline)
        if record.kind is kind:
            self._observation = SqlClientEvidenceObservation(self.attempt_sha256, replace(receipt))
        return receipt

    monkeypatch.setattr(type(setup.evidence), "write", replace_observation)
    with pytest.raises(SqlClientWriterExecutionUnknown):
        execute_sqlclient_writer(ready, clock_ns=lambda: 1)
    assert [call[0] for call in setup.calls].count("send_grant") == 1


def test_lifecycle_ack_identity_loss_after_exit_is_unknown(setup, monkeypatch):
    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))
    original = type(setup.lifecycle).advance

    def lose_ack(self, event, **kwargs):
        observed = original(self, event, **kwargs)
        if type(event).__name__ == "Exited":
            return replace(observed)
        return observed

    monkeypatch.setattr(type(setup.lifecycle), "advance", lose_ack)
    with pytest.raises(SqlClientWriterExecutionUnknown):
        execute_sqlclient_writer(ready, clock_ns=lambda: 1)
    assert setup.lifecycle.snapshot.state.phase is TdsAttemptPhase.EXITED


def test_reentrant_grant_callback_cannot_open_a_second_effect(setup, monkeypatch):
    ready, _ = _ready(setup, monkeypatch)
    raw = _result(setup)
    _install_process(monkeypatch, setup, raw)
    original = Process.send_grant
    nested = []

    def reenter(self, body, *, deadline):
        with pytest.raises(ValueError):
            execute_sqlclient_writer(ready, clock_ns=lambda: 1)
        nested.append("rejected")
        return original(self, body, deadline=deadline)

    monkeypatch.setattr(Process, "send_grant", reenter)
    execute_sqlclient_writer(ready, clock_ns=lambda: 1)
    assert nested == ["rejected"]
    assert [call[0] for call in setup.calls].count("send_grant") == 1


def test_pre_effect_lifecycle_drift_sends_no_grant(setup, monkeypatch):
    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))
    setup.lifecycle.snapshot = replace(setup.lifecycle.snapshot)

    with pytest.raises(SqlClientWriterExecutionUnknown):
        execute_sqlclient_writer(ready, clock_ns=lambda: 1)

    assert not any(call[0] == "send_grant" for call in setup.calls)


def test_locally_exited_provenance_mutation_invalidates_p10f_claim(setup, monkeypatch):
    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))
    exited = execute_sqlclient_writer(ready, clock_ns=lambda: 1)
    claim = exited._claim_p10f_once(exited)
    owner = exited._assert_p10f_claim(exited, claim)
    object.__setattr__(owner.result, "grant_id", None)

    with pytest.raises(ValueError, match="writer_execution_unknown"):
        exited._assert_p10f_claim(exited, claim)

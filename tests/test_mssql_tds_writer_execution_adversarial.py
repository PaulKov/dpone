from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_tds_result import TdsAttemptError
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.services.mssql_tds_writer_execution import (
    SqlClientWriterExecutionUnknown,
    _ExecutionResources,
    execute_sqlclient_writer,
)
from tests.test_mssql_tds_writer_execution import _install_process, _ready, _result
from tests.test_mssql_tds_writer_launch import Process
from tests.test_mssql_tds_writer_launch import setup as setup


def test_eof_retained_before_receive_error_is_preserved_for_unknown(setup, monkeypatch):
    ready, _ = _ready(setup, monkeypatch)
    raw = _result(setup)
    _install_process(monkeypatch, setup, raw)
    reads = []

    def receive_result(self, *, deadline):
        self.calls.append(("receive_result", deadline))
        self._received_result = raw
        raise OSError("private post-eof close error")

    def received_result(self):
        reads.append("read")
        return self._received_result

    monkeypatch.setattr(Process, "receive_result", receive_result)
    monkeypatch.setattr(Process, "received_result", property(received_result))

    with pytest.raises(SqlClientWriterExecutionUnknown) as caught:
        execute_sqlclient_writer(ready, clock_ns=lambda: 1)

    assert reads == ["read"]
    assert caught.value._facts.result_bytes is raw
    assert caught.value._facts.result is None
    assert [call[0] for call in setup.calls].count("send_grant") == 1
    assert [call[0] for call in setup.calls].count("terminate") == 1


@pytest.mark.parametrize("invalid_wait", ["foreign", "unreaped"])
def test_invalid_wait_exit_forces_exact_containment(setup, monkeypatch, invalid_wait):
    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))

    def wait(self, *, deadline):
        self.calls.append(("wait", deadline))
        identity = self.identity
        if invalid_wait == "foreign":
            identity = replace(identity, start_ticks=identity.start_ticks + 1)
        return TdsChildExit(identity, 0, invalid_wait != "unreaped")

    monkeypatch.setattr(Process, "wait", wait)

    with pytest.raises(SqlClientWriterExecutionUnknown) as caught:
        execute_sqlclient_writer(ready, clock_ns=lambda: 1)

    assert [call[0] for call in setup.calls].count("terminate") == 1
    assert caught.value._facts.local_exit == TdsChildExit(setup.process.identity, -9, True)
    assert setup.process.closed == 1


def test_invalid_containment_exit_is_not_retained_as_confirmed(setup, monkeypatch):
    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))

    def wait(self, *, deadline):
        self.calls.append(("wait", deadline))
        return TdsChildExit(self.identity, 0, False)

    def terminate(self, *, deadline):
        self.calls.append(("terminate", deadline))
        return TdsChildExit(replace(self.identity, start_ticks=self.identity.start_ticks + 1), -9, True)

    monkeypatch.setattr(Process, "wait", wait)
    monkeypatch.setattr(Process, "terminate", terminate)

    with pytest.raises(SqlClientWriterExecutionUnknown) as caught:
        execute_sqlclient_writer(ready, clock_ns=lambda: 1)

    assert [call[0] for call in setup.calls].count("terminate") == 1
    assert caught.value._facts.local_exit is None
    assert setup.process.closed == 1


@pytest.mark.parametrize(
    ("error", "exit_code"),
    [(None, 1), (TdsAttemptError.DRIVER, 0)],
)
def test_exact_reaped_exit_is_retained_before_matrix_rejection(setup, monkeypatch, error, exit_code):
    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup, error=error), exit_code=exit_code)

    with pytest.raises(SqlClientWriterExecutionUnknown) as caught:
        execute_sqlclient_writer(ready, clock_ns=lambda: 1)

    assert [call[0] for call in setup.calls].count("terminate") == 0
    assert caught.value._facts.local_exit == TdsChildExit(setup.process.identity, exit_code, True)
    assert setup.process.closed == 1


def test_failure_cleanup_deduplicates_aliased_gateways_by_identity():
    class Gateway:
        def __init__(self):
            self.closed = []

        def close(self, *, deadline):
            self.closed.append(deadline)

    class ProcessResource:
        def __init__(self):
            self.closed = 0

        def close(self):
            self.closed += 1

    class ObserverCleanup:
        def __init__(self):
            self.settled = 0

        def _settle_once(self):
            self.settled += 1

    gateway = Gateway()
    process = ProcessResource()
    observer = ObserverCleanup()
    launch = SimpleNamespace(
        process=process,
        evidence=gateway,
        lifecycle=gateway,
        containment_deadline=7.0,
    )
    owner = SimpleNamespace(refs=SimpleNamespace(launch=launch), observer_cleanup=observer)
    resources = _ExecutionResources(owner, SimpleNamespace())

    resources.close_local_success(close_gateways=True)

    assert gateway.closed == [7.0]
    assert process.closed == 1
    assert observer.settled == 1

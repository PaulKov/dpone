"""Parent supervision for the one-shot P10f settlement helper."""

import pytest

from dpone.adapters.mssql_sqlclient_departure_launch import PythonSqlClientDepartureLauncher
from dpone.app import mssql_sqlclient_writer_settlement_composition as composition
from dpone.app.mssql_sqlclient_writer_settlement_composition import (
    ContainedSqlClientWriterSettlementVerifier,
)
from dpone.contracts.mssql_sqlclient_writer_settlement_ipc_codec import (
    decode_writer_settlement_request,
    encode_writer_settlement_result,
    make_writer_settlement_result,
)
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.test_mssql_sqlclient_writer_settlement_ipc import credentials, observation, request


class Child:
    def __init__(self, *, exit_code=0, startup_impl="e" * 64, close_error=False):
        expected = request().startup
        self.identity = expected.process
        self._startup = TdsCoordinatorStartup(
            self.identity,
            startup_impl,
            expected.package_root,
            expected.launch_nonce,
        )
        self.exit_code = exit_code
        self.close_error = close_error
        self.sent = []
        self.request = None
        self.terminated = 0
        self.closed = 0

    def startup(self, *, deadline):
        return self._startup

    def send_request(self, payload, *, deadline):
        self.sent.append((payload, deadline))
        body = strict_json_object(payload)
        self.request = decode_writer_settlement_request(canonical_json_bytes(body["request"]))

    def receive_result_bounded(self, *, deadline, max_payload):
        assert max_payload == 512 * 1024
        assert self.request is not None
        result = make_writer_settlement_result(self.request, observation(self.request))
        return encode_writer_settlement_result(result, request=self.request)

    def wait(self, *, deadline):
        return TdsChildExit(self.identity, self.exit_code, True)

    def terminate(self, *, deadline):
        self.terminated += 1

    def close(self):
        self.closed += 1
        if self.close_error:
            raise OSError("synthetic close acknowledgement loss")


def verifier(monkeypatch, child, supplier_calls):
    current = request()
    launcher = object.__new__(PythonSqlClientDepartureLauncher)
    monkeypatch.setattr(
        composition,
        "_admission",
        lambda candidate: (None, "f" * 64, "e" * 64, "/synthetic", 8 << 30),
    )
    monkeypatch.setattr(
        PythonSqlClientDepartureLauncher,
        "spawn",
        lambda self, **kwargs: child,
    )

    def supply():
        supplier_calls.append(True)
        return credentials(current).material

    return ContainedSqlClientWriterSettlementVerifier(
        launcher,
        management_admission=current.plan.management_admission,
        management_credentials=supply,
        helper_startup_timeout=5.0,
        cleanup_timeout=1.0,
        clock=lambda: 1.0,
    )


def observe(subject):
    current = request()
    plan = current.plan
    return subject.observe(
        writer_observation=plan.writer_observation,
        writer_admission=plan.writer_admission,
        stage=plan.stage,
        input_descriptor=plan.input_descriptor,
        expectation=plan.expectation,
        operation_deadline=20.0,
    )


def test_parent_authenticates_result_exit_and_provenance(monkeypatch):
    child = Child()
    calls = []
    subject = verifier(monkeypatch, child, calls)

    assert observe(subject) == observation(child.request)
    assert calls == [True]
    assert len(child.sent) == 1
    assert child.closed == 1
    assert child.terminated == 0
    assert subject.provenance.implementation_sha256 == "e" * 64
    with pytest.raises(RuntimeError):
        observe(subject)
    subject.close()


@pytest.mark.parametrize("failure", ["startup", "exit"])
def test_parent_contains_substituted_startup_or_exit(monkeypatch, failure):
    child = Child(startup_impl="0" * 64 if failure == "startup" else "e" * 64, exit_code=1 if failure == "exit" else 0)
    calls = []
    subject = verifier(monkeypatch, child, calls)

    with pytest.raises(RuntimeError):
        observe(subject)
    assert len(calls) <= 1
    assert child.closed == 1
    assert child.terminated == (0 if failure == "exit" else 1)
    with pytest.raises(RuntimeError):
        subject.provenance


def test_ambiguous_close_is_not_repeated(monkeypatch):
    child = Child(close_error=True)
    subject = verifier(monkeypatch, child, [])

    with pytest.raises(RuntimeError):
        observe(subject)
    assert child.closed == 1
    assert child.terminated == 0
    with pytest.raises(RuntimeError):
        subject.close()
    assert child.closed == 1
    assert child.terminated == 0

"""P10f orders remote proof, durable evidence, VERIFIED CAS and cleanup."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_input import input_descriptor_digest
from dpone.contracts.mssql_sqlclient_observation import SqlClientSessionAuthority
from dpone.contracts.mssql_sqlclient_observer_incarnation import observer_incarnation_digest
from dpone.contracts.mssql_sqlclient_writer_session_departure import SqlClientWriterSessionDeparture
from dpone.contracts.mssql_sqlclient_writer_settlement import (
    SqlClientWriterSettlementObservation,
    SqlClientWriterSettlementProvenance,
)
from dpone.contracts.mssql_tds_worker import TdsAttemptPhase
from dpone.services.mssql_tds_writer_execution import execute_sqlclient_writer
from dpone.services.mssql_tds_writer_settlement import (
    SqlClientTerminalProjectionUnknown,
    SqlClientWriterSettlementUnknown,
    SqlClientWriterVerified,
    project_sqlclient_native_chunk,
    settle_sqlclient_writer,
)
from tests.mssql_sqlclient_departure_v2_fixtures import sample
from tests.test_mssql_tds_writer_execution import _install_process, _ready, _result
from tests.test_mssql_tds_writer_launch import setup as setup


def _observation(owner, *, digest=None):
    base = sample(reused=False)
    admission = owner.writer_admission
    original = owner.observation.remote_session
    authority = SqlClientSessionAuthority(
        admission.server,
        admission.database,
        base.observer.authority.login,
        base.observer.authority.transport,
        base.observer.authority.principal_resolution,
    )
    observer = replace(
        base.observer,
        connection_id=UUID(int=original.connection_id.int + 1),
        session_id=original.session_id + 1,
        authority=authority,
        visibility=replace(base.observer.visibility, database_id=admission.database.database_id),
    )
    guard = observer_incarnation_digest(observer)
    departure = SqlClientWriterSessionDeparture(
        original=original,
        writer_admission=admission,
        writer_authority=owner.observation.authority,
        principal=owner.observation.resolved_database_principal,
        observer=observer,
        samples=tuple(replace(value, before_sha256=guard, after_sha256=guard) for value in base.samples),
    )
    return SqlClientWriterSettlementObservation(
        departure=departure,
        stage_before=owner.stage,
        stage_after=owner.stage,
        observer_before=observer,
        observer_after=observer,
        row_count=owner.content_expectation.rows,
        typed_digest=owner.content_expectation.typed_digest if digest is None else digest,
        typed_sum=0,
    )


class Verifier:
    def __init__(self, observation):
        self.observation = observation
        self.calls = []
        self.closed = 0

    def observe(self, **kwargs):
        self.calls.append(kwargs)
        return self.observation

    def close(self):
        self.closed += 1

    @property
    def provenance(self):
        return SqlClientWriterSettlementProvenance(
            startup_sha256="1" * 64,
            request_sha256="2" * 64,
            result_sha256="3" * 64,
            local_exit_sha256="4" * 64,
            implementation_sha256="5" * 64,
            admission_sha256="6" * 64,
        )


def test_exact_p10f_happy_path(setup, monkeypatch):
    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))
    exited = execute_sqlclient_writer(ready, clock_ns=lambda: 1)

    # Derive expected fake observation without consuming custody by reading the
    # closure only inside a temporary method wrapper is deliberately forbidden;
    # the service verifier receives all immutable facts, so it constructs on call.
    class LazyVerifier(Verifier):
        def __init__(self):
            super().__init__(None)

        def observe(self, **kwargs):
            assert (
                input_descriptor_digest(kwargs["input_descriptor"])
                == kwargs["writer_observation"].binding.input_binding_sha256
            )
            owner = type("OwnerView", (), kwargs)()
            owner.observation = kwargs["writer_observation"]
            owner.writer_admission = kwargs["writer_admission"]
            owner.stage = kwargs["stage"]
            owner.content_expectation = kwargs["expectation"]
            self.observation = _observation(owner)
            return super().observe(**kwargs)

    verifier = LazyVerifier()
    terminal = settle_sqlclient_writer(exited, verifier)

    assert isinstance(terminal, SqlClientWriterVerified)
    assert not any(hasattr(terminal, name) for name in ("_record", "_receipt", "_state"))
    assert setup.lifecycle.snapshot.state.phase is TdsAttemptPhase.VERIFIED
    assert setup.evidence.closed == [3.0]
    assert setup.lifecycle.closed == [3.0]
    assert verifier.closed == 1
    with pytest.raises(SqlClientWriterSettlementUnknown):
        settle_sqlclient_writer(exited, verifier)

    projection = project_sqlclient_native_chunk(terminal)
    assert projection.attempt == setup.plan.attempt.state.identity
    with pytest.raises(SqlClientTerminalProjectionUnknown):
        project_sqlclient_native_chunk(terminal)


def test_typed_digest_mismatch_is_unknown_and_closes_custody(setup, monkeypatch):
    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))
    exited = execute_sqlclient_writer(ready, clock_ns=lambda: 1)

    class Mismatch(Verifier):
        def __init__(self):
            super().__init__(None)

        def observe(self, **kwargs):
            owner = type("OwnerView", (), kwargs)()
            owner.observation = kwargs["writer_observation"]
            owner.writer_admission = kwargs["writer_admission"]
            owner.stage = kwargs["stage"]
            owner.content_expectation = kwargs["expectation"]
            return _observation(owner, digest="f" * 64)

    verifier = Mismatch()
    with pytest.raises(SqlClientWriterSettlementUnknown):
        settle_sqlclient_writer(exited, verifier)
    assert setup.lifecycle.snapshot.state.phase is TdsAttemptPhase.EXITED
    assert setup.evidence.closed == [3.0]
    assert setup.lifecycle.closed == [3.0]
    assert verifier.closed == 1


def test_cleanup_failure_after_verified_is_unknown_without_retry(setup, monkeypatch):
    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))
    exited = execute_sqlclient_writer(ready, clock_ns=lambda: 1)

    class FailingClose(Verifier):
        def __init__(self):
            super().__init__(None)

        def observe(self, **kwargs):
            owner = type("OwnerView", (), kwargs)()
            owner.observation = kwargs["writer_observation"]
            owner.writer_admission = kwargs["writer_admission"]
            owner.stage = kwargs["stage"]
            owner.content_expectation = kwargs["expectation"]
            return _observation(owner)

        def close(self):
            self.closed += 1
            raise OSError("synthetic close ACK loss")

    verifier = FailingClose()
    with pytest.raises(SqlClientWriterSettlementUnknown):
        settle_sqlclient_writer(exited, verifier)
    assert setup.lifecycle.snapshot.state.phase is TdsAttemptPhase.VERIFIED
    assert verifier.closed == 1


def test_verified_terminal_cannot_be_constructed_by_callers():
    with pytest.raises(ValueError):
        SqlClientWriterVerified(object(), object(), object(), object())

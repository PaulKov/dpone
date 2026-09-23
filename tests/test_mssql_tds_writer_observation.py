from dataclasses import replace
from datetime import datetime, timedelta
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_evidence_types import SqlClientEvidenceObservation
from dpone.contracts.mssql_sqlclient_launch import launch_digest
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientObserverAdmission,
    SqlClientPrincipalResolution,
    SqlClientSessionAuthority,
    SqlClientWriterObservation,
    session_authority_digest,
)
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity
from dpone.contracts.mssql_tds_validation import deadline_seconds
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity
from dpone.ports.mssql_sqlclient_credentials import preload_sqlclient_credentials
from dpone.ports.mssql_sqlclient_writer_observer import create_sqlclient_writer_observer_custody
from dpone.services.mssql_tds_writer_observation import (
    SqlClientWriterGrantReady,
    SqlClientWriterObservationUnknown,
    SqlClientWriterResultReady,
    prepare_sqlclient_writer_observation,
)
from dpone.services.mssql_tds_writer_pregrant import prepare_sqlclient_writer_pregrant
from tests.mssql_sqlclient_departure_v2_fixtures import sample
from tests.test_mssql_tds_writer_launch import Evidence, Lifecycle
from tests.test_mssql_tds_writer_launch import setup as setup
from tests.test_mssql_tds_writer_pregrant import (
    _bind_p9,
    _credentials,
    _profile,
    _registered,
    _writer_admission,
)

NONCE = b"n" * 32
GRANT_ID = "44444444-4444-4444-8444-444444444444"


def _target_observation(target: SqlClientObserverAdmission) -> SqlClientWriterObservation:
    principal = SqlClientPrincipalResolution("mapped_user", 5, "writer_user", target.login.sid)
    authority = SqlClientSessionAuthority(
        target.server,
        target.database,
        target.login,
        target.transport,
        principal,
    )
    remote = TdsRemoteSessionIdentity(
        UUID("01234567-89ab-cdef-8123-456789abcdef"),
        52,
        datetime(2026, 9, 18, 12),
        datetime(2026, 9, 18, 12) + timedelta(seconds=1),
        NONCE,
        session_authority_digest(authority),
    )
    return SqlClientWriterObservation(remote, authority)


class ObserverBackend:
    def __init__(self, observed):
        self._identity = TdsProcessIdentity(
            "b" * 64,
            "55555555-5555-4555-8555-555555555555",
            654,
            8,
        )
        self.observed = observed
        self.calls = []
        self.closed = 0
        self.contain_error = False

    identity = property(lambda self: self._identity)

    def observe_once(self, *, session_id, nonce, deadline):
        self.calls.append(("observe_once", session_id, nonce, deadline))
        return self.observed

    def contain(self, *, deadline):
        self.calls.append(("contain", deadline))
        if self.contain_error:
            raise OSError("contain failed")
        return TdsChildExit(self.identity, -9, True)

    def close(self, *, deadline):
        self.calls.append(("close", deadline))
        self.closed += 1


def _observer(setup, target, writer_custody, *, backend=None, attempt_sha256=None, launch_sha256=None):
    template = sample().observer
    observer_login = replace(
        target.login,
        principal_id=target.login.principal_id + 1,
        name="observer",
        sid="03",
        original_name="observer",
        original_sid="03",
    )
    observer_authority = SqlClientSessionAuthority(
        target.server,
        target.database,
        observer_login,
        target.transport,
        SqlClientPrincipalResolution("mapped_user", 6, "observer_user", "03"),
    )
    incarnation = replace(
        template,
        authority=observer_authority,
        visibility=replace(template.visibility, database_id=target.database.database_id),
    )
    authority = incarnation.authority
    admitted = SqlClientObserverAdmission(
        authority.server,
        authority.database,
        authority.login,
        authority.transport,
    )
    backend = ObserverBackend(_target_observation(target)) if backend is None else backend
    backend.observer_admission = admitted
    backend.target_admission = target
    backend.incarnation = incarnation
    launch = getattr(setup.process, "declared_launch", None)
    custody = create_sqlclient_writer_observer_custody(
        backend,
        observer_admission=admitted,
        target_admission=target,
        incarnation=incarnation,
        attempt_sha256=("a" * 64 if launch is None else launch.attempt_sha256)
        if attempt_sha256 is None
        else attempt_sha256,
        launch_sha256=("c" * 64 if launch is None else launch_digest(launch))
        if launch_sha256 is None
        else launch_sha256,
        credential_custody=object() if writer_custody is None else writer_custody,
        operation_deadline_ns=setup.process.declared_launch.operation_deadline_ns,
        operation_deadline=deadline_seconds(setup.process.declared_launch.operation_deadline_ns),
        cleanup_deadline=6.0,
    )
    return custody, backend


def _pregrant(setup, monkeypatch, *, response="session"):
    target = _writer_admission(setup.plan.attempt.state.identity.database)
    _bind_p9(setup, target)
    profile = _profile(target)
    registered = _registered(setup, monkeypatch, NONCE, response=response)
    supplier = preload_sqlclient_credentials(profile, _credentials(database=profile.database))
    pregrant = prepare_sqlclient_writer_pregrant(
        registered,
        profile=profile,
        supplier=supplier,
        session_nonce=NONCE,
        clock_ns=lambda: 1,
    )
    return pregrant, target, supplier


def test_session_observation_and_both_ack_pairs_precede_grant_ready(setup, monkeypatch):
    pregrant, target, supplier = _pregrant(setup, monkeypatch)
    observer, backend = _observer(setup, target, None)
    projected_deadline = deadline_seconds(setup.process.declared_launch.operation_deadline_ns)
    assert projected_deadline != setup.plan.operation_deadline

    ready = prepare_sqlclient_writer_observation(
        pregrant,
        observer=observer,
        grant_id=GRANT_ID,
        now_ns=1,
    )

    assert isinstance(ready, SqlClientWriterGrantReady)
    assert repr(ready) == "SqlClientWriterGrantReady(<opaque>)"
    assert backend.calls == [("observe_once", 52, NONCE, projected_deadline)]
    assert setup.lifecycle.calls[-3:] == [
        ("SqlClientCredentialIntent", 3.0),
        ("SqlClientWriterObserved", 3.0),
        ("SqlClientGrantIntent", 3.0),
    ]
    cleanup = ready._prepare_p10e_cleanup(ready)
    claim = ready._claim_p10e_once(ready, cleanup)
    owner = ready._assert_p10e_claim(ready, claim, cleanup)
    assert owner.grant.grant_id == GRANT_ID
    assert len(owner.grant_bytes) == owner.grant_receipt.byte_count
    assert owner.observation_receipt.payload_sha256 == owner.grant.writer_observation_sha256
    assert not any(call[0] in {"send_grant", "receive_result", "bulk"} for call in setup.calls)
    assert supplier is owner.refs.writer_credential_custody


def test_result_route_never_touches_observer_or_new_evidence(setup, monkeypatch):
    pregrant, _, _ = _pregrant(setup, monkeypatch, response="failure")
    effects_before = list(setup.calls)

    ready = prepare_sqlclient_writer_observation(pregrant, observer=None, grant_id=None, now_ns=1)

    assert isinstance(ready, SqlClientWriterResultReady)
    assert setup.calls == effects_before
    assert setup.lifecycle.calls[-1] == ("SqlClientCredentialIntent", 3.0)
    cleanup = ready._prepare_settlement_cleanup(ready)
    claim = ready._claim_settlement_once(ready, cleanup)
    owner = ready._assert_settlement_claim(ready, claim, cleanup)
    assert owner.refs.result is not None and owner.refs.result_bytes is not None


def test_observer_is_one_shot_exact_and_cleanup_is_bounded(setup):
    target = _writer_admission(setup.plan.attempt.state.identity.database)
    custody, backend = _observer(setup, target, None)
    cleanup = custody._prepare_cleanup(custody)
    claim = custody._claim_once(custody, cleanup)

    observed = custody._observe_once(
        custody,
        claim,
        cleanup,
        session_id=52,
        nonce=NONCE,
        deadline=deadline_seconds(setup.process.declared_launch.operation_deadline_ns),
    )

    assert observed == _target_observation(target)
    with pytest.raises(ValueError, match="writer_observer_unknown"):
        custody._observe_once(
            custody,
            claim,
            cleanup,
            session_id=52,
            nonce=NONCE,
            deadline=deadline_seconds(setup.process.declared_launch.operation_deadline_ns),
        )
    cleanup._cleanup_once()
    cleanup._cleanup_once()
    assert backend.calls[-2:] == [("contain", 6.0), ("close", 6.0)]
    assert backend.closed == 1


def test_observer_success_settlement_requires_exact_reap_and_confirmed_close(setup):
    target = _writer_admission(setup.plan.attempt.state.identity.database)
    custody, backend = _observer(setup, target, None)
    cleanup = custody._prepare_cleanup(custody)
    claim = custody._claim_once(custody, cleanup)
    custody._observe_once(
        custody,
        claim,
        cleanup,
        session_id=52,
        nonce=NONCE,
        deadline=deadline_seconds(setup.process.declared_launch.operation_deadline_ns),
    )

    exited = cleanup._settle_once()

    assert exited == TdsChildExit(backend.identity, -9, True)
    assert backend.calls[-2:] == [("contain", 6.0), ("close", 6.0)]
    assert backend.closed == 1
    cleanup._cleanup_once()
    assert backend.closed == 1


@pytest.mark.parametrize("failure", ["identity", "reaped", "contain", "close"])
def test_observer_success_settlement_propagates_every_unconfirmed_outcome(setup, monkeypatch, failure):
    target = _writer_admission(setup.plan.attempt.state.identity.database)
    custody, backend = _observer(setup, target, None)
    cleanup = custody._prepare_cleanup(custody)
    custody._claim_once(custody, cleanup)
    original_contain = type(backend).contain

    def contain(self, *, deadline):
        if failure == "contain":
            raise OSError("private contain")
        value = original_contain(self, deadline=deadline)
        if failure == "identity":
            return replace(value, identity=replace(value.identity, start_ticks=value.identity.start_ticks + 1))
        if failure == "reaped":
            return replace(value, reaped=False)
        return value

    def close(self, *, deadline):
        self.calls.append(("close", deadline))
        if failure == "close":
            raise OSError("private close")
        self.closed += 1

    monkeypatch.setattr(type(backend), "contain", contain)
    monkeypatch.setattr(type(backend), "close", close)

    with pytest.raises(ValueError, match="writer_observer_unknown"):
        cleanup._settle_once()
    assert backend.calls[-1] == ("close", 6.0)


def test_lost_observation_ack_is_unknown_and_cleans_both_exact_processes(setup, monkeypatch):
    pregrant, target, _ = _pregrant(setup, monkeypatch)
    observer, backend = _observer(setup, target, None)
    original = Evidence.write

    def lost_ack(self, record, *, deadline):
        if record.kind.value == "writer_observation":
            return record.receipt
        return original(self, record, deadline=deadline)

    monkeypatch.setattr(Evidence, "write", lost_ack)
    with pytest.raises(SqlClientWriterObservationUnknown, match="writer_observation_unknown") as caught:
        prepare_sqlclient_writer_observation(
            pregrant,
            observer=observer,
            grant_id=GRANT_ID,
            now_ns=1,
        )

    assert caught.value.__context__ is None
    assert repr(caught.value) == "SqlClientWriterObservationUnknown(<opaque>)"
    assert "secret" not in repr(caught.value).lower()
    assert vars(caught.value) == {}
    assert setup.calls[-1] == ("terminate", 6.0)
    assert setup.process.closed == 1
    assert backend.calls[-2:] == [("contain", 6.0), ("close", 6.0)]
    assert not any(name == "SqlClientWriterObserved" for name, _ in setup.lifecycle.calls)
    with pytest.raises(ValueError):
        pregrant._p10d_route(pregrant)


def test_writer_and_observer_credential_custody_alias_is_rejected_before_observation(setup, monkeypatch):
    pregrant, target, writer_custody = _pregrant(setup, monkeypatch)
    observer, backend = _observer(setup, target, writer_custody)

    with pytest.raises(SqlClientWriterObservationUnknown, match="writer_observation_unknown"):
        prepare_sqlclient_writer_observation(
            pregrant,
            observer=observer,
            grant_id=GRANT_ID,
            now_ns=1,
        )

    assert not any(call[0] == "observe_once" for call in backend.calls)
    assert setup.calls[-1] == ("terminate", 6.0)
    assert backend.calls[-2:] == [("contain", 6.0), ("close", 6.0)]


@pytest.mark.parametrize("field", ["attempt", "launch"])
def test_observer_request_binding_mismatch_is_unknown_before_observation(setup, monkeypatch, field):
    pregrant, target, _ = _pregrant(setup, monkeypatch)
    changes = {"attempt_sha256" if field == "attempt" else "launch_sha256": "f" * 64}
    observer, backend = _observer(setup, target, None, **changes)

    with pytest.raises(SqlClientWriterObservationUnknown, match="writer_observation_unknown"):
        prepare_sqlclient_writer_observation(pregrant, observer=observer, grant_id=GRANT_ID, now_ns=1)

    assert not any(call[0] == "observe_once" for call in backend.calls)
    assert setup.calls[-1] == ("terminate", 6.0)
    assert backend.calls[-2:] == [("contain", 6.0), ("close", 6.0)]


def test_observer_equal_value_identity_substitution_is_rejected(setup):
    target = _writer_admission(setup.plan.attempt.state.identity.database)
    custody, backend = _observer(setup, target, None)
    backend._identity = replace(backend.identity)
    cleanup = custody._prepare_cleanup(custody)

    with pytest.raises(ValueError, match="writer_observer_unknown"):
        custody._claim_once(custody, cleanup)
    cleanup._cleanup_once()
    assert backend.calls[-2:] == [("contain", 6.0), ("close", 6.0)]


def test_equal_receipt_substitution_after_lifecycle_ack_is_unknown(setup, monkeypatch):
    pregrant, target, _ = _pregrant(setup, monkeypatch)
    observer, backend = _observer(setup, target, None)
    original = Lifecycle.advance

    def substitute(self, event, *, expected_phase, deadline):
        state = original(self, event, expected_phase=expected_phase, deadline=deadline)
        if type(event).__name__ == "SqlClientWriterObserved":
            observed = setup.evidence.observation
            setup.evidence._observation = SqlClientEvidenceObservation(
                observed.attempt_sha256, replace(observed.receipt)
            )
        return state

    monkeypatch.setattr(Lifecycle, "advance", substitute)
    with pytest.raises(SqlClientWriterObservationUnknown, match="writer_observation_unknown"):
        prepare_sqlclient_writer_observation(pregrant, observer=observer, grant_id=GRANT_ID, now_ns=1)

    assert not any(name == "SqlClientGrantIntent" for name, _ in setup.lifecycle.calls)
    assert backend.calls[-2:] == [("contain", 6.0), ("close", 6.0)]


def test_result_successor_claim_is_bound_to_precaptured_cleanup(setup, monkeypatch):
    pregrant, _, _ = _pregrant(setup, monkeypatch, response="failure")
    ready = prepare_sqlclient_writer_observation(pregrant, observer=None, grant_id=None, now_ns=1)
    assert isinstance(ready, SqlClientWriterResultReady)
    cleanup = ready._prepare_settlement_cleanup(ready)
    reconstructed = object.__new__(type(cleanup))

    with pytest.raises(ValueError, match="writer_observation_unknown"):
        ready._claim_settlement_once(ready, reconstructed)
    with pytest.raises(ValueError, match="writer_observation_unknown"):
        reconstructed._cleanup_once()
    assert setup.process.closed == 0


def test_invalid_process_mutation_before_capture_is_rejected(setup):
    target = _writer_admission(setup.plan.attempt.state.identity.database)
    backend = ObserverBackend(_target_observation(target))
    object.__setattr__(backend._identity, "pid", 0)

    with pytest.raises(ValueError, match="writer_observer_unknown"):
        _observer(setup, target, None, backend=backend)
    assert backend.calls == []


@pytest.mark.parametrize("subject", ["process", "incarnation", "observer_admission", "target_admission"])
def test_after_capture_origin_mutation_rejects_claim_but_cleanup_still_closes(setup, subject):
    target = _writer_admission(setup.plan.attempt.state.identity.database)
    custody, backend = _observer(setup, target, None)
    cleanup = custody._prepare_cleanup(custody)
    if subject == "process":
        object.__setattr__(backend._identity, "pid", 0)
    elif subject == "incarnation":
        object.__setattr__(backend.incarnation, "session_id", 0)
    elif subject == "observer_admission":
        object.__setattr__(backend.observer_admission.login, "name", "")
    else:
        object.__setattr__(backend.target_admission.database, "database_id", 0)

    with pytest.raises(ValueError, match="writer_observer_unknown"):
        custody._claim_once(custody, cleanup)
    cleanup._cleanup_once()
    assert backend.calls[-1] == ("close", 6.0)
    assert backend.closed == 1


def test_contain_failure_still_attempts_bounded_close_once(setup):
    target = _writer_admission(setup.plan.attempt.state.identity.database)
    custody, backend = _observer(setup, target, None)
    cleanup = custody._prepare_cleanup(custody)
    custody._claim_once(custody, cleanup)
    backend.contain_error = True

    cleanup._cleanup_once()
    cleanup._cleanup_once()

    assert backend.calls[-2:] == [("contain", 6.0), ("close", 6.0)]
    assert backend.closed == 1


def test_grant_evidence_ack_loss_prevents_grant_lifecycle(setup, monkeypatch):
    pregrant, target, _ = _pregrant(setup, monkeypatch)
    observer, backend = _observer(setup, target, None)
    original = Evidence.write

    def lost_ack(self, record, *, deadline):
        if record.kind.value == "grant_intent":
            return record.receipt
        return original(self, record, deadline=deadline)

    monkeypatch.setattr(Evidence, "write", lost_ack)
    with pytest.raises(SqlClientWriterObservationUnknown, match="writer_observation_unknown"):
        prepare_sqlclient_writer_observation(pregrant, observer=observer, grant_id=GRANT_ID, now_ns=1)

    assert not any(name == "SqlClientGrantIntent" for name, _ in setup.lifecycle.calls)
    assert backend.calls[-2:] == [("contain", 6.0), ("close", 6.0)]


def test_grant_lifecycle_commit_then_ack_loss_is_sticky_unknown(setup, monkeypatch):
    pregrant, target, _ = _pregrant(setup, monkeypatch)
    observer, backend = _observer(setup, target, None)
    original = Lifecycle.advance

    def ambiguous(self, event, *, expected_phase, deadline):
        state = original(self, event, expected_phase=expected_phase, deadline=deadline)
        if type(event).__name__ == "SqlClientGrantIntent":
            raise TimeoutError("ACK lost after commit")
        return state

    monkeypatch.setattr(Lifecycle, "advance", ambiguous)
    with pytest.raises(SqlClientWriterObservationUnknown, match="writer_observation_unknown"):
        prepare_sqlclient_writer_observation(pregrant, observer=observer, grant_id=GRANT_ID, now_ns=1)

    assert setup.lifecycle.calls[-1] == ("SqlClientGrantIntent", 3.0)
    assert backend.calls[-2:] == [("contain", 6.0), ("close", 6.0)]

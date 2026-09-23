from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace

import pytest

from dpone.app.mssql_sqlclient_writer_observer_request import sqlclient_writer_observer_request
from dpone.contracts.mssql_native_chunks import TdsInputReceipt
from dpone.contracts.mssql_sqlclient_credential_admission import SqlClientCredentialProfile, validate_credentials
from dpone.contracts.mssql_sqlclient_credentials import SqlClientCredentials
from dpone.contracts.mssql_sqlclient_evidence_types import SqlClientEvidenceObservation
from dpone.contracts.mssql_sqlclient_input import input_descriptor_digest
from dpone.contracts.mssql_sqlclient_job import decode_job
from dpone.contracts.mssql_sqlclient_launch import launch_digest
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
    SqlClientTransportAuthority,
)
from dpone.contracts.mssql_sqlclient_registration import encode_credential_intent
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement import RestrictedWriterSettlementReceipt
from dpone.contracts.mssql_sqlclient_result import SqlClientResult, encode_sqlclient_result
from dpone.contracts.mssql_sqlclient_session_control import SqlClientSessionAnnouncement, encode_session_announcement
from dpone.contracts.mssql_tds_result import TdsWorkerResult, attempt_identity_digest
from dpone.contracts.mssql_tds_worker import TdsAttemptError
from dpone.ports.mssql_sqlclient_credentials import preload_sqlclient_credentials
from dpone.services.mssql_tds_writer_authority import _ADMITTED_TOKEN, _create_sqlclient_writer_admitted
from dpone.services.mssql_tds_writer_authority_proof import mint_writer_authority_proof
from dpone.services.mssql_tds_writer_launch import (
    _REGISTERED_TOKEN,
    SqlClientWriterRegistered,
    _create_registered_writer,
)
from dpone.services.mssql_tds_writer_pregrant import (
    _PREGRANT_CLASS_TOKEN,
    SqlClientWriterPreGrant,
    SqlClientWriterPreGrantUnknown,
    prepare_sqlclient_writer_pregrant,
)
from dpone.services.mssql_tds_writer_registration_custody import _REGISTERED_CLASS_TOKEN, _LaunchOwner
from tests.mssql_sqlclient_evidence_fixtures import registration
from tests.test_mssql_sqlclient_launch_contract import ready
from tests.test_mssql_tds_writer_launch import Evidence, Lifecycle, Process, run
from tests.test_mssql_tds_writer_launch import setup as setup


def _writer_admission(database: str = "warehouse") -> SqlClientObserverAdmission:
    return SqlClientObserverAdmission(
        SqlClientServerAuthority("server", "machine", "instance", "physical"),
        SqlClientDatabaseAuthority(7, database, "11111111-1111-4111-8111-111111111111", "01"),
        SqlClientLoginAuthority(9, "writer", "02", "writer", "02", 1, False),
        SqlClientTransportAuthority("TCP", "TSQL", "SQL", "TRUE"),
    )


def _profile(writer: SqlClientObserverAdmission | None = None, **changes) -> SqlClientCredentialProfile:
    writer = _writer_admission() if writer is None else writer
    values = dict(
        writer_admission=writer,
        host="sql.test",
        port=1433,
        database=writer.database.database_name,
        username="writer",
        tls_profile="verified",
        allow_disposable_test=False,
    )
    values.update(changes)
    return SqlClientCredentialProfile(**values)


def _claim_p10d(pregrant: SqlClientWriterPreGrant):
    cleanup = pregrant._prepare_p10d_cleanup(pregrant)
    claim = pregrant._claim_p10d_once(pregrant, cleanup)
    return pregrant._assert_p10d_claim(pregrant, claim, cleanup)


def _credentials(**changes) -> SqlClientCredentials:
    values = dict(
        host="sql.test",
        port=1433,
        database="warehouse",
        username="writer",
        password="secret-canary",
        tls_profile="verified",
    )
    values.update(changes)
    return SqlClientCredentials(**values)


def _bind_p9(h, writer: SqlClientObserverAdmission) -> None:
    receipt = RestrictedWriterSettlementReceipt(
        "a" * 64,
        "b" * 64,
        "c" * 64,
        f"tds-restricted-writer-settlement-v1-{'a' * 64}-{'b' * 64}-{'c' * 64}.json",
        10,
    )
    owner = h.plan.p9_owner
    terminal_values = list(h.plan.terminal)
    terminal_values[1] = receipt
    terminal = tuple.__new__(type(h.plan.terminal), terminal_values)
    owner._terminal, owner._receipt = terminal, receipt
    owner._completion = SimpleNamespace(request=SimpleNamespace(plan=SimpleNamespace(writer_admission=writer)))
    proof = mint_writer_authority_proof(
        terminal,
        owner,
        owner.retained._owner,
        h.plan.transition,
        fence=h.plan.authority_fence,
    )
    h.plan = h.plan._replace(terminal=terminal, authority_proof=proof)
    h.admitted = _create_sqlclient_writer_admitted(_ADMITTED_TOKEN, h.plan)


def _registered(
    h, monkeypatch, nonce: bytes, *, send_error: bool = False, response: str = "session"
) -> SqlClientWriterRegistered:
    def send_credentials(self, body, *, deadline):
        decoded = decode_job(body)
        assert decoded.credentials.password == "secret-canary"
        self.calls.append(("send_credentials", deadline))
        if send_error:
            raise OSError("partial")

    def receive_session_or_result(self, *, deadline):
        self.calls.append(("receive_session_or_result", deadline))
        launch = self.declared_launch
        if response == "failure":
            failed = SqlClientResult(
                1,
                launch_digest(launch),
                launch.attempt_sha256,
                None,
                TdsWorkerResult(launch.attempt_sha256, None, TdsAttemptError.DRIVER),
            )
            return "result", encode_sqlclient_result(failed)
        announcement = SqlClientSessionAnnouncement(1, launch_digest(launch), launch.attempt_sha256, 52, nonce.hex())
        return "session", encode_session_announcement(announcement)

    monkeypatch.setattr(Process, "send_credentials", send_credentials, raising=False)
    monkeypatch.setattr(Process, "receive_session_or_result", receive_session_or_result, raising=False)
    registered = run(h)
    h.lifecycle.snapshot = replace(
        h.lifecycle.snapshot,
        state=replace(h.lifecycle.snapshot.state, schema_version=2, backend="mssql_sqlclient"),
    )
    return registered


def test_profile_and_preloaded_holder_are_exact_one_shot():
    profile = _profile()
    credentials = _credentials()
    supplier = preload_sqlclient_credentials(profile, credentials)

    assert "secret-canary" not in repr(supplier)
    assert supplier.release_once(profile) is credentials
    with pytest.raises(ValueError, match="credentials_unavailable"):
        supplier.release_once(profile)
    with pytest.raises(ValueError, match="credential_admission_invalid"):
        validate_credentials(profile, replace(credentials, host="other.test"))
    with pytest.raises(ValueError, match="credential_admission_invalid"):
        _profile(tls_profile="disposable_test")


def test_intent_and_lifecycle_ack_precede_release_send_and_session(setup, monkeypatch):
    writer, nonce = _writer_admission(setup.plan.attempt.state.identity.database), b"n" * 32
    _bind_p9(setup, writer)
    profile = _profile(writer)
    supplier = preload_sqlclient_credentials(profile, _credentials(database=profile.database))
    registered = _registered(setup, monkeypatch, nonce)

    pregrant = prepare_sqlclient_writer_pregrant(
        registered, profile=profile, supplier=supplier, session_nonce=nonce, clock_ns=lambda: 1
    )

    assert repr(pregrant) == "SqlClientWriterPreGrant(<opaque>)"
    assert type(pregrant)._p10d_route(pregrant, pregrant) == "session"
    assert setup.lifecycle.calls[-1] == ("SqlClientCredentialIntent", 3.0)
    assert setup.calls[-2:] == [("send_credentials", 3.0), ("receive_session_or_result", 3.0)]
    custody = _claim_p10d(pregrant)
    assert custody.session is not None and custody.result is None
    assert custody.writer_credential_custody is supplier
    assert custody.state.state.sqlclient is not None
    assert (
        custody.state.state.sqlclient.credential_intent_sha256
        == sha256(encode_credential_intent(custody.intent)).hexdigest()
    )
    with pytest.raises(ValueError, match="credentials_unavailable"):
        supplier.release_once(profile)
    with pytest.raises(ValueError, match="writer_pregrant_unknown"):
        pregrant._claim_p10d_once(pregrant, pregrant._prepare_p10d_cleanup(pregrant))


def test_registered_and_successor_capabilities_reject_subclass_proxies(setup, monkeypatch):
    writer, nonce = _writer_admission(setup.plan.attempt.state.identity.database), b"n" * 32
    _bind_p9(setup, writer)
    profile = _profile(writer)
    registered = _registered(setup, monkeypatch, nonce)

    with pytest.raises(TypeError, match="writer_launch_unknown"):
        type("RegisteredProxy", (type(registered),), {})

    class RegisteredBaseProxy(SqlClientWriterRegistered, _token=_REGISTERED_CLASS_TOKEN):
        __slots__ = ("target",)

        def __init__(self, target):
            self.target = target

        def _p10c_identity(self):
            return self

        def _prepare_p10c_cleanup(self, candidate):
            return self.target._prepare_p10c_cleanup(candidate)

        def _claim_p10c_once(self, candidate, cleanup):
            return self.target._claim_p10c_once(candidate, cleanup)

        def _assert_p10c_claim(self, candidate, claim, cleanup):
            return self.target._assert_p10c_claim(candidate, claim, cleanup)

    supplier = preload_sqlclient_credentials(profile, _credentials(database=profile.database))
    with pytest.raises(ValueError, match="writer_launch_unknown"):
        prepare_sqlclient_writer_pregrant(
            RegisteredBaseProxy(registered),
            profile=profile,
            supplier=supplier,
            session_nonce=nonce,
            clock_ns=lambda: 1,
        )
    assert supplier.release_once(profile).password == "secret-canary"

    pregrant = prepare_sqlclient_writer_pregrant(
        registered,
        profile=profile,
        supplier=preload_sqlclient_credentials(profile, _credentials(database=profile.database)),
        session_nonce=nonce,
        clock_ns=lambda: 1,
    )
    with pytest.raises(TypeError, match="writer_pregrant_unknown"):
        type("PreGrantProxy", (type(pregrant),), {})

    class PreGrantBaseProxy(SqlClientWriterPreGrant, _token=_PREGRANT_CLASS_TOKEN):
        __slots__ = ("target",)

        def __init__(self, target):
            self.target = target

        def _p10d_identity(self):
            return self.target._p10d_identity()

        def _p10d_route(self, candidate):
            return self.target._p10d_route(candidate)

        def _prepare_p10d_cleanup(self, candidate):
            return self.target._prepare_p10d_cleanup(candidate)

        def _claim_p10d_once(self, candidate, cleanup):
            return self.target._claim_p10d_once(candidate, cleanup)

        def _assert_p10d_claim(self, candidate, claim, cleanup):
            return self.target._assert_p10d_claim(candidate, claim, cleanup)

    proxy = PreGrantBaseProxy(pregrant)
    assert proxy._p10d_identity() is not proxy
    with pytest.raises(ValueError, match="writer_pregrant_unknown"):
        proxy._prepare_p10d_cleanup(proxy)


def test_partial_job_delivery_is_unknown_contained_and_not_replayable(setup, monkeypatch):
    writer, nonce = _writer_admission(setup.plan.attempt.state.identity.database), b"n" * 32
    _bind_p9(setup, writer)
    profile = _profile(writer)
    registered = _registered(setup, monkeypatch, nonce, send_error=True)

    with pytest.raises(SqlClientWriterPreGrantUnknown, match="writer_pregrant_unknown"):
        prepare_sqlclient_writer_pregrant(
            registered,
            profile=profile,
            supplier=preload_sqlclient_credentials(profile, _credentials(database=profile.database)),
            session_nonce=nonce,
            clock_ns=lambda: 1,
        )

    assert setup.calls[-2:] == [("send_credentials", 3.0), ("terminate", 6.0)]
    assert setup.process.closed == 1
    with pytest.raises(ValueError):
        registered._claim_p10c_once(registered, registered._prepare_p10c_cleanup(registered))


def test_claim_failure_after_irreversible_claim_uses_preclaimed_cleanup_custody(setup, monkeypatch):
    writer, nonce = _writer_admission(setup.plan.attempt.state.identity.database), b"n" * 32
    _bind_p9(setup, writer)
    profile = _profile(writer)
    supplier = preload_sqlclient_credentials(profile, _credentials(database=profile.database))
    registered = _registered(setup, monkeypatch, nonce)
    exact_type = type(registered)
    original = exact_type._claim_p10c_once

    def claimed_then_cancelled(self, candidate, cleanup):
        original(self, candidate, cleanup)
        raise KeyboardInterrupt

    monkeypatch.setattr(exact_type, "_claim_p10c_once", claimed_then_cancelled)
    with pytest.raises(SqlClientWriterPreGrantUnknown, match="writer_pregrant_unknown") as caught:
        prepare_sqlclient_writer_pregrant(
            registered,
            profile=profile,
            supplier=supplier,
            session_nonce=nonce,
            clock_ns=lambda: 1,
        )

    assert caught.value.__context__ is None
    assert setup.calls[-1] == ("terminate", 6.0)
    assert setup.process.closed == 1
    assert supplier.release_once(profile).password == "secret-canary"


def test_lost_evidence_ack_prevents_lifecycle_and_secret_release(setup, monkeypatch):
    writer, nonce = _writer_admission(setup.plan.attempt.state.identity.database), b"n" * 32
    _bind_p9(setup, writer)
    profile = _profile(writer)
    supplier = preload_sqlclient_credentials(profile, _credentials(database=profile.database))
    registered = _registered(setup, monkeypatch, nonce)

    def lost_ack(self, record, *, deadline):
        self.calls.append(("evidence_lost", deadline))
        return record.receipt

    monkeypatch.setattr(Evidence, "write", lost_ack)
    lifecycle_before = list(setup.lifecycle.calls)
    with pytest.raises(SqlClientWriterPreGrantUnknown, match="writer_pregrant_unknown") as caught:
        prepare_sqlclient_writer_pregrant(
            registered, profile=profile, supplier=supplier, session_nonce=nonce, clock_ns=lambda: 1
        )

    assert setup.lifecycle.calls == lifecycle_before
    assert not any(call[0] == "send_credentials" for call in setup.calls)
    trace = caught.value.__traceback__
    while trace is not None:
        if trace.tb_frame.f_code.co_name == "prepare_sqlclient_writer_pregrant":
            assert "supplier" not in trace.tb_frame.f_locals
            assert "secret-canary" not in repr(trace.tb_frame.f_locals)
        trace = trace.tb_next
    assert supplier.release_once(profile).password == "secret-canary"


@pytest.mark.parametrize("replaced", ["process", "evidence", "lifecycle"])
def test_preclaim_resource_replacement_contains_only_exact_original(setup, monkeypatch, replaced):
    writer, nonce = _writer_admission(setup.plan.attempt.state.identity.database), b"n" * 32
    _bind_p9(setup, writer)
    profile = _profile(writer)
    original = _registered(setup, monkeypatch, nonce)
    cleanup = original._prepare_p10c_cleanup(original)
    claim = original._claim_p10c_once(original, cleanup)
    refs = original._assert_p10c_claim(original, claim, cleanup)
    owner = _LaunchOwner(
        refs.admitted,
        refs.claim,
        refs.plan,
        refs.lifecycle,
        refs.containment_deadline,
        refs.evidence,
        refs.process,
        None,
        refs.registration,
        refs.receipt,
    )
    registered = _create_registered_writer(_REGISTERED_TOKEN, owner)
    replacement_calls = []
    if replaced == "process":
        replacement = Process(owner.registration.launch, owner.registration.input, replacement_calls)
        replacement._startup_receipt = owner.registration.ready
    elif replaced == "evidence":
        replacement = Evidence(owner.registration.binding.attempt_sha256, replacement_calls)
        replacement._observation = owner.evidence.observation
    else:
        replacement = Lifecycle()
        replacement.snapshot = owner.lifecycle.snapshot
    setattr(owner, replaced, replacement)
    supplier = preload_sqlclient_credentials(profile, _credentials(database=profile.database))

    with pytest.raises(SqlClientWriterPreGrantUnknown, match="writer_pregrant_unknown"):
        prepare_sqlclient_writer_pregrant(
            registered, profile=profile, supplier=supplier, session_nonce=nonce, clock_ns=lambda: 1
        )

    assert setup.calls[-1] == ("terminate", 6.0)
    assert setup.process.closed == 1
    assert setup.evidence.closed == [6.0]
    assert setup.lifecycle.closed == [6.0]
    assert replacement_calls == []
    if replaced == "process":
        assert replacement.closed == 0
    else:
        assert replacement.closed == []
    assert supplier.release_once(profile).password == "secret-canary"


def test_release_failure_happens_only_after_both_acks_and_never_sends(setup, monkeypatch):
    writer, nonce = _writer_admission(setup.plan.attempt.state.identity.database), b"n" * 32
    _bind_p9(setup, writer)
    profile = _profile(writer)
    registered = _registered(setup, monkeypatch, nonce)

    with pytest.raises(SqlClientWriterPreGrantUnknown, match="writer_pregrant_unknown"):
        prepare_sqlclient_writer_pregrant(
            registered,
            profile=profile,
            supplier=preload_sqlclient_credentials(profile, None),
            session_nonce=nonce,
            clock_ns=lambda: 1,
        )

    assert setup.lifecycle.calls[-1] == ("SqlClientCredentialIntent", 3.0)
    assert not any(call[0] == "send_credentials" for call in setup.calls)


def test_nonempty_early_failure_is_retained_without_grant(setup, monkeypatch):
    writer, nonce = _writer_admission(setup.plan.attempt.state.identity.database), b"n" * 32
    _bind_p9(setup, writer)
    profile = _profile(writer)
    registered = _registered(setup, monkeypatch, nonce, response="failure")

    pregrant = prepare_sqlclient_writer_pregrant(
        registered,
        profile=profile,
        supplier=preload_sqlclient_credentials(profile, _credentials(database=profile.database)),
        session_nonce=nonce,
        clock_ns=lambda: 1,
    )

    assert type(pregrant)._p10d_route(pregrant, pregrant) == "result"
    custody = _claim_p10d(pregrant)
    assert custody.session is None
    assert custody.result.result.error is TdsAttemptError.DRIVER
    assert custody.result_bytes is not None


def test_observer_request_projection_is_read_only_and_exact(setup, monkeypatch):
    writer, nonce = _writer_admission(setup.plan.attempt.state.identity.database), b"n" * 32
    _bind_p9(setup, writer)
    profile = _profile(writer)
    registered = _registered(setup, monkeypatch, nonce)
    pregrant = prepare_sqlclient_writer_pregrant(
        registered,
        profile=profile,
        supplier=preload_sqlclient_credentials(profile, _credentials(database=profile.database)),
        session_nonce=nonce,
        clock_ns=lambda: 1,
    )
    observer = replace(
        writer,
        login=replace(
            writer.login, principal_id=10, name="observer", sid="03", original_name="observer", original_sid="03"
        ),
    )

    first = sqlclient_writer_observer_request(pregrant, observer_admission=observer)
    second = sqlclient_writer_observer_request(pregrant, observer_admission=observer)

    assert first == second
    assert first is not None
    assert first.target_admission is writer
    assert type(pregrant)._p10d_route(pregrant, pregrant) == "session"
    assert _claim_p10d(pregrant).session is not None


def test_observer_request_projection_returns_none_for_early_result(setup, monkeypatch):
    writer, nonce = _writer_admission(setup.plan.attempt.state.identity.database), b"n" * 32
    _bind_p9(setup, writer)
    profile = _profile(writer)
    registered = _registered(setup, monkeypatch, nonce, response="failure")
    pregrant = prepare_sqlclient_writer_pregrant(
        registered,
        profile=profile,
        supplier=preload_sqlclient_credentials(profile, _credentials(database=profile.database)),
        session_nonce=nonce,
        clock_ns=lambda: 1,
    )
    observer = replace(
        writer,
        login=replace(
            writer.login, principal_id=10, name="observer", sid="03", original_name="observer", original_sid="03"
        ),
    )

    assert sqlclient_writer_observer_request(pregrant, observer_admission=observer) is None
    assert type(pregrant)._p10d_route(pregrant, pregrant) == "result"


def test_observer_request_projection_rejects_alias_and_mismatch(setup, monkeypatch):
    writer, nonce = _writer_admission(setup.plan.attempt.state.identity.database), b"n" * 32
    _bind_p9(setup, writer)
    profile = _profile(writer)
    registered = _registered(setup, monkeypatch, nonce)
    pregrant = prepare_sqlclient_writer_pregrant(
        registered,
        profile=profile,
        supplier=preload_sqlclient_credentials(profile, _credentials(database=profile.database)),
        session_nonce=nonce,
        clock_ns=lambda: 1,
    )

    class AdmissionAlias(SqlClientObserverAdmission):
        pass

    with pytest.raises(ValueError, match="observer_request_invalid"):
        sqlclient_writer_observer_request(
            pregrant,
            observer_admission=AdmissionAlias(
                writer.server,
                writer.database,
                writer.login,
                writer.transport,
            ),
        )
    with pytest.raises(ValueError, match="observer_request_invalid"):
        sqlclient_writer_observer_request(pregrant, observer_admission=writer)
    assert type(pregrant)._p10d_route(pregrant, pregrant) == "session"
    assert not any(call[0] == "send_grant" for call in setup.calls)


def test_replaced_p9_receipt_fails_before_release_or_new_evidence(setup, monkeypatch):
    writer, nonce = _writer_admission(setup.plan.attempt.state.identity.database), b"n" * 32
    _bind_p9(setup, writer)
    profile = _profile(writer)
    supplier = preload_sqlclient_credentials(profile, _credentials(database=profile.database))
    registered = _registered(setup, monkeypatch, nonce)
    setup.plan.p9_owner._receipt = replace(setup.plan.p9_owner._receipt)
    calls_before = list(setup.calls)

    with pytest.raises(SqlClientWriterPreGrantUnknown, match="writer_pregrant_unknown"):
        prepare_sqlclient_writer_pregrant(
            registered, profile=profile, supplier=supplier, session_nonce=nonce, clock_ns=lambda: 1
        )

    assert setup.calls[: len(calls_before)] == calls_before
    assert not any(call[0] == "send_credentials" for call in setup.calls)
    assert supplier.release_once(profile).password == "secret-canary"


def test_reconstructed_registration_fails_before_release(setup):
    writer = _writer_admission(setup.plan.attempt.state.identity.database)
    _bind_p9(setup, writer)
    profile = _profile(writer)
    supplier = preload_sqlclient_credentials(profile, _credentials(database=profile.database))
    reconstructed = object.__new__(type(run(setup)))

    with pytest.raises(ValueError, match="writer_launch_unknown"):
        prepare_sqlclient_writer_pregrant(
            reconstructed,
            profile=profile,
            supplier=supplier,
            session_nonce=b"n" * 32,
            clock_ns=lambda: 1,
        )

    assert supplier.release_once(profile).password == "secret-canary"


def test_empty_input_never_releases_supplier_and_requires_exact_success(setup, monkeypatch):
    import dpone.services.mssql_tds_writer_pregrant as module

    original = run(setup)
    original_cleanup = original._prepare_p10c_cleanup(original)
    original_claim = original._claim_p10c_once(original, original_cleanup)
    refs = original._assert_p10c_claim(original, original_claim, original_cleanup)
    owner = _LaunchOwner(
        refs.admitted,
        refs.claim,
        refs.plan,
        refs.lifecycle,
        refs.containment_deadline,
        refs.evidence,
        refs.process,
        None,
        refs.registration,
        refs.receipt,
    )
    setup.lifecycle.snapshot = replace(
        setup.lifecycle.snapshot,
        state=replace(setup.lifecycle.snapshot.state, schema_version=2, backend="mssql_sqlclient"),
    )
    base = registration()
    empty_receipt = TdsInputReceipt(0, 0, sha256(b"").hexdigest())
    source = replace(base.input, expected=empty_receipt, file_identity=replace(base.input.file_identity, size=0))
    identity = replace(base.binding.identity, file_sha256=empty_receipt.file_sha256)
    launch = replace(
        base.launch,
        attempt_sha256=attempt_identity_digest(identity),
        input_binding_sha256=input_descriptor_digest(source),
    )
    binding = replace(
        base.binding,
        launch_sha256=launch_digest(launch),
        attempt_sha256=launch.attempt_sha256,
        identity=identity,
        input_binding_sha256=launch.input_binding_sha256,
    )
    owner.registration = replace(
        base,
        binding=binding,
        launch=launch,
        ready=ready(launch),
        input=source,
        input_mode=owner.require_plan().policy.input,
        batch_rows=owner.require_plan().policy.batch_rows,
        max_input_batch_bytes=owner.require_plan().policy.max_input_batch_bytes,
    )
    owner.evidence.attempt_sha256 = binding.attempt_sha256
    owner.evidence._observation = SqlClientEvidenceObservation(binding.attempt_sha256)
    registered = _create_registered_writer(_REGISTERED_TOKEN, owner)
    monkeypatch.setattr(
        module,
        "validate_pregrant_origin",
        lambda registered, claim, cleanup, exact_refs, profile: (
            owner.require_plan(),
            setup.lifecycle.snapshot,
            "c" * 64,
        ),
    )
    monkeypatch.setattr(module, "reassert_pregrant_custody", lambda *args: None)

    def send_credentials(self, body, *, deadline):
        assert decode_job(body).credentials is None
        self.calls.append(("send_credentials", deadline))

    def receive_session_or_result(self, *, deadline):
        reg = owner.registration
        success = SqlClientResult(
            1,
            launch_digest(reg.launch),
            reg.launch.attempt_sha256,
            None,
            TdsWorkerResult(reg.launch.attempt_sha256, reg.input.expected, None),
        )
        return "result", encode_sqlclient_result(success)

    monkeypatch.setattr(Process, "send_credentials", send_credentials, raising=False)
    monkeypatch.setattr(Process, "receive_session_or_result", receive_session_or_result, raising=False)
    writer = _writer_admission(setup.plan.attempt.state.identity.database)
    profile = _profile(writer)
    supplier = preload_sqlclient_credentials(profile, _credentials(database=profile.database))

    pregrant = prepare_sqlclient_writer_pregrant(
        registered, profile=profile, supplier=supplier, session_nonce=None, clock_ns=lambda: 1
    )

    assert type(pregrant)._p10d_route(pregrant, pregrant) == "result"
    custody = _claim_p10d(pregrant)
    assert custody.input_empty and custody.result.result.receipt == empty_receipt
    assert custody.writer_credential_custody is None
    with pytest.raises(ValueError, match="writer_pregrant_unknown"):
        type(pregrant)._p10d_route(pregrant, pregrant)
    assert supplier.release_once(profile).password == "secret-canary"

"""One-shot credential intent, Job delivery and pre-grant session custody."""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Lock, get_ident
from typing import Literal, cast

from dpone.ports.mssql_sqlclient_credentials import PreloadedSqlClientCredentials
from dpone.services import mssql_tds_writer_pregrant_support as _support
from dpone.services.mssql_tds_writer_contracts import (
    SqlClientAttemptEvidence,
    SqlClientCredentialIntent,
    SqlClientCredentialIntentRecord,
    SqlClientCredentialProfile,
    SqlClientCredentials,
    SqlClientEvidenceKind,
    SqlClientEvidenceObservation,
    SqlClientEvidenceReceipt,
    SqlClientEvidenceRecord,
    SqlClientJob,
    SqlClientResult,
    SqlClientSessionAnnouncement,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    advance_state,
    build_credential_intent,
    decode_session_announcement,
    decode_sqlclient_result,
    encode_credential_intent,
    encode_job,
    job_binding_digest,
    validate_credential_intent,
    validate_credentials,
    validate_job,
    validate_session_announcement,
)
from dpone.services.mssql_tds_writer_pregrant_validation import reassert_pregrant_custody, validate_pregrant_origin
from dpone.services.mssql_tds_writer_registration_custody import (
    SqlClientWriterRegistered,
    _P10cCleanupCustody,
    _P10cLaunchRefs,
)

ERROR = "mssql_native.sqlclient_writer_pregrant_unknown"
_PREGRANT_CLASS_TOKEN = object()
_call, _invalid, _property = _support.call, _support.invalid, _support.property_value


@dataclass(slots=True, repr=False)
class _PreGrantOwner:
    launch: _P10cLaunchRefs
    cleanup: _P10cCleanupCustody
    claim: object
    profile: SqlClientCredentialProfile
    intent: SqlClientCredentialIntentRecord
    state: TdsAttemptSnapshot
    input_empty: bool
    writer_credential_custody: object | None = field(default=None, repr=False)
    session: SqlClientSessionAnnouncement | None = None
    result: SqlClientResult | None = None
    result_bytes: bytes | None = field(default=None, repr=False)
    _pid: int = field(default_factory=os.getpid)
    _thread_id: int = field(default_factory=get_ident)

    def assert_owner(self) -> None:
        if os.getpid() != self._pid or get_ident() != self._thread_id:
            _invalid()


class SqlClientWriterPreGrant:
    """Opaque one-shot custody of a validated session locator or early result."""

    __slots__ = ()

    def __init_subclass__(cls, *, _token: object = None, **kwargs: object) -> None:
        if _token is not _PREGRANT_CLASS_TOKEN:
            raise TypeError(ERROR)
        super().__init_subclass__(**kwargs)

    def __repr__(self) -> str:
        return "SqlClientWriterPreGrant(<opaque>)"

    def _p10d_identity(self) -> SqlClientWriterPreGrant:
        _invalid()

    def _p10d_route(self, candidate: SqlClientWriterPreGrant) -> Literal["session", "result"]:
        """Reveal only the sealed successor route before P10d acquires resources."""
        _invalid()

    def _p10d_observer_facts(self, candidate: SqlClientWriterPreGrant) -> tuple | None:
        _invalid()

    def _prepare_p10d_cleanup(self, candidate: SqlClientWriterPreGrant) -> _P10cCleanupCustody:
        _invalid()

    def _claim_p10d_once(self, candidate: SqlClientWriterPreGrant, cleanup: _P10cCleanupCustody) -> object:
        _invalid()

    def _assert_p10d_claim(
        self, candidate: SqlClientWriterPreGrant, claim: object, cleanup: _P10cCleanupCustody
    ) -> _PreGrantOwner:
        _invalid()


def _create_pregrant(owner: _PreGrantOwner) -> SqlClientWriterPreGrant:
    lock, pid, thread_id = Lock(), os.getpid(), get_ident()
    state, exact_claim = "AVAILABLE", None
    exact: SqlClientWriterPreGrant

    class _ExactPreGrant(SqlClientWriterPreGrant, _token=_PREGRANT_CLASS_TOKEN):
        __slots__ = ()

        def __init_subclass__(cls, **kwargs: object) -> None:
            raise TypeError(ERROR)

        def _p10d_identity(self) -> SqlClientWriterPreGrant:
            if self is not exact or os.getpid() != pid or get_ident() != thread_id:
                _invalid()
            return exact

        def _p10d_route(self, candidate: SqlClientWriterPreGrant) -> Literal["session", "result"]:
            if candidate is not self or self is not exact or os.getpid() != pid or get_ident() != thread_id:
                _invalid()
            with lock:
                if state != "AVAILABLE" or exact_claim is not None:
                    _invalid()
                if (owner.session is None) == (owner.result is None):
                    _invalid()
                return "session" if owner.session is not None else "result"

        def _p10d_observer_facts(self, candidate):
            if candidate is not self or self is not exact or (os.getpid(), get_ident()) != (pid, thread_id):
                _invalid()
            with lock:
                if state != "AVAILABLE" or exact_claim is not None:
                    _invalid()
                if (owner.session is None) == (owner.result is None):
                    _invalid()
                if owner.result is not None:
                    return None
                registration = owner.launch.registration
                return (
                    registration.binding.attempt_sha256,
                    registration.binding.launch_sha256,
                    owner.profile.writer_admission,
                    registration.launch.operation_deadline_ns,
                )

        def _prepare_p10d_cleanup(self, candidate: SqlClientWriterPreGrant) -> _P10cCleanupCustody:
            if candidate is not self or self is not exact or os.getpid() != pid or get_ident() != thread_id:
                _invalid()
            with lock:
                if state != "AVAILABLE" or exact_claim is not None:
                    _invalid()
                return owner.cleanup

        def _claim_p10d_once(self, candidate: SqlClientWriterPreGrant, cleanup: _P10cCleanupCustody) -> object:
            nonlocal state, exact_claim
            if (
                candidate is not self
                or self is not exact
                or cleanup is not owner.cleanup
                or os.getpid() != pid
                or get_ident() != thread_id
            ):
                _invalid()
            with lock:
                if state != "AVAILABLE" or exact_claim is not None:
                    _invalid()
                exact_claim, state = object(), "CLAIMED"
                return exact_claim

        def _assert_p10d_claim(
            self, candidate: SqlClientWriterPreGrant, claim: object, cleanup: _P10cCleanupCustody
        ) -> _PreGrantOwner:
            if (
                candidate is not self
                or self is not exact
                or cleanup is not owner.cleanup
                or os.getpid() != pid
                or get_ident() != thread_id
            ):
                _invalid()
            with lock:
                if state != "CLAIMED" or claim is not exact_claim:
                    _invalid()
                owner.assert_owner()
                return owner

    exact = _ExactPreGrant()
    return exact


class SqlClientWriterPreGrantUnknown(RuntimeError):
    """Claimed launch cannot be replayed; exact cleanup custody is retained."""

    def __init__(self, cleanup: _P10cCleanupCustody) -> None:
        self._cleanup = cleanup
        super().__init__(ERROR)

    def __repr__(self) -> str:
        return "SqlClientWriterPreGrantUnknown(<opaque>)"


def _advance(
    refs: _P10cLaunchRefs, previous: TdsAttemptSnapshot, event: SqlClientCredentialIntent
) -> TdsAttemptSnapshot:
    predicted = advance_state(previous.state, event, expected_phase=TdsAttemptPhase.SPAWNED_WAITING)
    observed = _call(
        refs.lifecycle,
        "advance",
        event,
        expected_phase=TdsAttemptPhase.SPAWNED_WAITING,
        deadline=refs.plan.operation_deadline,
    )
    if (
        type(observed) is not TdsAttemptSnapshot
        or observed.state != predicted
        or observed.revision <= previous.revision
        or _property(refs.lifecycle, "snapshot") is not observed
    ):
        _invalid()
    return observed


def _persist_intent(refs: _P10cLaunchRefs, current: TdsAttemptSnapshot, intent) -> TdsAttemptSnapshot:
    payload = encode_credential_intent(intent)
    record = SqlClientEvidenceRecord(intent.binding.attempt_sha256, SqlClientEvidenceKind.CREDENTIAL_INTENT, payload)
    receipt = _call(refs.evidence, "write", record, deadline=refs.plan.operation_deadline)
    observation = _property(refs.evidence, "observation")
    if (
        type(receipt) is not SqlClientEvidenceReceipt
        or receipt != record.receipt
        or type(observation) is not SqlClientEvidenceObservation
        or observation.attempt_sha256 != intent.binding.attempt_sha256
        or observation.receipt is not receipt
    ):
        _invalid()
    observation.__post_init__()
    event = SqlClientCredentialIntent(
        SqlClientAttemptEvidence(
            launch_sha256=intent.binding.launch_sha256,
            credential_intent_sha256=receipt.payload_sha256,
            input_empty=intent.input_empty,
        )
    )
    return _advance(refs, current, event)


def _job(registration, credentials: SqlClientCredentials | None, nonce: bytes | None) -> SqlClientJob:
    binding = registration.binding
    return SqlClientJob(
        1,
        binding.launch_sha256,
        binding.identity,
        binding.ownership,
        binding.object_identity,
        registration.input,
        cast(Literal["rows", "arrow"], registration.input_mode),
        registration.batch_rows,
        registration.max_input_batch_bytes,
        credentials,
        None if nonce is None else nonce.hex(),
    )


def prepare_sqlclient_writer_pregrant(
    registered: SqlClientWriterRegistered,
    *,
    profile: SqlClientCredentialProfile,
    supplier: PreloadedSqlClientCredentials,
    session_nonce: bytes | None,
    clock_ns: Callable[[], int],
) -> SqlClientWriterPreGrant:
    """Consume P10b and stop after one validated Job response, before grant."""
    if (
        not isinstance(registered, SqlClientWriterRegistered)
        or type(profile) is not SqlClientCredentialProfile
        or type(supplier) is not PreloadedSqlClientCredentials
        or (session_nonce is not None and (type(session_nonce) is not bytes or len(session_nonce) != 32))
        or not callable(clock_ns)
    ):
        _invalid()
    if type(registered)._p10c_identity(registered) is not registered:
        _invalid()
    profile.__post_init__()
    _call(supplier, "assert_profile", profile)
    cleanup = type(registered)._prepare_p10c_cleanup(registered, registered)
    refs: _P10cLaunchRefs | None = None
    try:
        claim = type(registered)._claim_p10c_once(registered, registered, cleanup)
        refs = type(registered)._assert_p10c_claim(registered, registered, claim, cleanup)
        plan, current, capability_sha256 = validate_pregrant_origin(registered, claim, cleanup, refs, profile)
        registration = refs.registration
        process = refs.process
        empty = registration.input.expected.rows == 0
        if (empty and session_nonce is not None) or (not empty and session_nonce is None):
            _invalid()
        intent = build_credential_intent(
            registration,
            session_nonce=session_nonce,
            tls_profile=None if empty else profile.tls_profile,
            capability_evidence_sha256=capability_sha256,
        )
        validate_credential_intent(intent, registration)
        running = _persist_intent(refs, current, intent)
        reassert_pregrant_custody(registered, claim, cleanup, refs, profile, running)
        now_ns = clock_ns()
        if type(now_ns) is not int or now_ns < 0 or now_ns >= registration.launch.operation_deadline_ns:
            _invalid()
        credentials: SqlClientCredentials | None = None
        job: SqlClientJob | None = None
        body: bytes | None = None
        try:
            credentials = None if empty else _call(supplier, "release_once", profile)
            if credentials is not None:
                validate_credentials(profile, credentials)
            job = _job(registration, credentials, session_nonce)
            validate_job(
                job,
                launch=registration.launch,
                ownership=registration.binding.ownership,
                object_identity=registration.binding.object_identity,
                policy=plan.policy,
                session_nonce=intent.session_nonce,
                tls_profile=cast(Literal["verified", "disposable_test"] | None, intent.tls_profile),
                allow_disposable_test=profile.allow_disposable_test,
                now_ns=now_ns,
            )
            if job_binding_digest(job) != intent.job_binding_sha256:
                _invalid()
            body = encode_job(job)
            _call(process, "send_credentials", body, deadline=plan.operation_deadline)
        finally:
            body = None
            credentials = None
            job = None
        branch = _call(process, "receive_session_or_result", deadline=plan.operation_deadline)
        if type(branch) is not tuple or len(branch) != 2 or type(branch[0]) is not str or type(branch[1]) is not bytes:
            _invalid()
        pregrant = _PreGrantOwner(
            launch=refs,
            cleanup=cleanup,
            claim=claim,
            profile=profile,
            intent=intent,
            state=running,
            input_empty=empty,
            writer_credential_custody=None if empty else supplier,
        )
        if branch[0] == "session" and not empty:
            if session_nonce is None:
                _invalid()
            announced = decode_session_announcement(branch[1])
            validate_session_announcement(announced, launch=registration.launch, nonce=session_nonce, now_ns=clock_ns())
            pregrant.session = announced
        elif branch[0] == "result":
            result = decode_sqlclient_result(
                branch[1],
                launch=registration.launch,
                expected_input=registration.input.expected,
                expected_grant_id=None,
            )
            if (empty and result.result.receipt is None) or (not empty and result.result.error is None):
                _invalid()
            pregrant.result, pregrant.result_bytes = result, branch[1]
        else:
            _invalid()
        return _create_pregrant(pregrant)
    except BaseException:
        type(cleanup)._cleanup_once(cleanup)
    # Raise outside the handler so the opaque UNKNOWN does not retain a causal
    # traceback whose frames could contain the transient credential value.
    del supplier
    raise SqlClientWriterPreGrantUnknown(cleanup)


__all__ = (
    "SqlClientWriterPreGrant",
    "SqlClientWriterPreGrantUnknown",
    "prepare_sqlclient_writer_pregrant",
)

"""Exact-origin validation and record construction for the P10d boundary."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from inspect import getattr_static
from typing import Any, Never

from dpone.services.mssql_tds_writer_authority_proof import assert_writer_authority
from dpone.services.mssql_tds_writer_contracts import (
    SqlClientBulkGrant,
    SqlClientCredentialIntentRecord,
    SqlClientCredentialProfile,
    SqlClientEvidenceKind,
    SqlClientEvidenceObservation,
    SqlClientEvidenceReceipt,
    SqlClientEvidenceRecord,
    SqlClientGrantIntent,
    SqlClientRegistration,
    SqlClientResult,
    SqlClientSessionAnnouncement,
    SqlClientWriterObservation,
    SqlClientWriterObservationRecord,
    SqlClientWriterObserved,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    advance_state,
    encode_bulk_grant,
    encode_credential_intent,
    encode_registration,
    encode_writer_observation,
    validate_bulk_grant,
    validate_credential_intent,
    validate_writer_observation,
)
from dpone.services.mssql_tds_writer_pregrant import SqlClientWriterPreGrant, _PreGrantOwner
from dpone.services.mssql_tds_writer_registration_custody import _P10cCleanupCustody, _P10cLaunchRefs

ERROR = "mssql_native.sqlclient_writer_observation_unknown"


def _invalid() -> Never:
    raise ValueError(ERROR)


def _property(value: object, name: str) -> Any:
    descriptor = getattr_static(type(value), name)
    if type(descriptor) is not property:
        _invalid()
    return descriptor.__get__(value, type(value))


def _call(value: object, name: str, /, *args: object, **kwargs: object):
    descriptor = getattr_static(type(value), name)
    return descriptor.__get__(value, type(value))(*args, **kwargs)


@dataclass(frozen=True, slots=True)
class _ClaimedRefs:
    pregrant: SqlClientWriterPreGrant
    claim: object
    predecessor_claim: object
    cleanup: _P10cCleanupCustody
    launch: _P10cLaunchRefs
    profile: SqlClientCredentialProfile
    intent: SqlClientCredentialIntentRecord
    state: TdsAttemptSnapshot
    input_empty: bool
    writer_credential_custody: object | None
    session: SqlClientSessionAnnouncement | None
    result: SqlClientResult | None
    result_bytes: bytes | None


def capture_claimed_refs(
    pregrant: SqlClientWriterPreGrant,
    claim: object,
    cleanup: _P10cCleanupCustody,
) -> _ClaimedRefs:
    """Snapshot and validate the exact P10c owner immediately after claim."""
    owner = type(pregrant)._assert_p10d_claim(pregrant, pregrant, claim, cleanup)
    if type(owner) is not _PreGrantOwner or type(owner.launch) is not _P10cLaunchRefs or owner.cleanup is not cleanup:
        _invalid()
    refs = _ClaimedRefs(
        pregrant,
        claim,
        owner.claim,
        cleanup,
        owner.launch,
        owner.profile,
        owner.intent,
        owner.state,
        owner.input_empty,
        owner.writer_credential_custody,
        owner.session,
        owner.result,
        owner.result_bytes,
    )
    reassert_claimed_refs(refs)
    return refs


def reassert_claimed_refs(refs: _ClaimedRefs, expected: TdsAttemptSnapshot | None = None) -> None:
    """Reject mutable-owner, actor, process or durable-snapshot substitution."""
    owner = type(refs.pregrant)._assert_p10d_claim(refs.pregrant, refs.pregrant, refs.claim, refs.cleanup)
    sqlclient = refs.state.state.sqlclient
    if (
        type(owner) is not _PreGrantOwner
        or owner.launch is not refs.launch
        or owner.cleanup is not refs.cleanup
        or owner.claim is not refs.predecessor_claim
        or owner.profile is not refs.profile
        or owner.intent is not refs.intent
        or owner.state is not refs.state
        or owner.input_empty is not refs.input_empty
        or owner.writer_credential_custody is not refs.writer_credential_custody
        or owner.session is not refs.session
        or owner.result is not refs.result
        or owner.result_bytes is not refs.result_bytes
    ):
        _invalid()
    launch_refs, registration = refs.launch, refs.launch.registration
    state = refs.state if expected is None else expected
    if (
        type(registration) is not SqlClientRegistration
        or type(refs.intent) is not SqlClientCredentialIntentRecord
        or type(refs.state) is not TdsAttemptSnapshot
        or type(state) is not TdsAttemptSnapshot
        or sqlclient is None
        or state.state.phase is not TdsAttemptPhase.RUNNING
        or _property(launch_refs.lifecycle, "snapshot") is not state
        or _property(launch_refs.process, "declared_launch") is not registration.launch
        or _property(launch_refs.process, "bound_input") is not registration.input
        or _property(launch_refs.process, "startup_receipt") is not registration.ready
        or _property(launch_refs.process, "identity") != registration.launch.process
        or refs.intent is not owner.intent
        or sha256(encode_credential_intent(refs.intent)).hexdigest() != sqlclient.credential_intent_sha256
        or refs.input_empty != refs.intent.input_empty
        or refs.input_empty != (registration.input.expected.rows == 0)
    ):
        _invalid()
    validate_credential_intent(refs.intent, registration)
    observation = _property(launch_refs.evidence, "observation")
    if expected is None and (
        type(observation) is not SqlClientEvidenceObservation
        or type(observation.receipt) is not SqlClientEvidenceReceipt
        or observation.receipt.kind is not SqlClientEvidenceKind.CREDENTIAL_INTENT
        or observation.receipt.payload_sha256 != sqlclient.credential_intent_sha256
    ):
        _invalid()
    if refs.input_empty:
        if refs.session is not None or type(refs.result) is not SqlClientResult or type(refs.result_bytes) is not bytes:
            _invalid()
    elif (refs.session is None) == (refs.result is None):
        _invalid()
    if refs.session is not None and (refs.result_bytes is not None or refs.writer_credential_custody is None):
        _invalid()
    if refs.result is not None and type(refs.result_bytes) is not bytes:
        _invalid()


def validate_observer_refs(refs: _ClaimedRefs, observer: Any) -> None:
    """Bind the independently contained observer to admitted source and target."""
    if refs.session is None or refs.writer_credential_custody is None:
        _invalid()
    plan = refs.launch.plan
    _, writer_admission, _, _ = assert_writer_authority(
        plan.authority_proof,
        terminal=plan.terminal,
        owner=plan.p9_owner,
        transition=plan.transition,
        fence=plan.authority_fence,
        require_writer=True,
    )
    if writer_admission is None:
        _invalid()
    authority = observer.incarnation.authority
    if (
        observer.target_admission is not writer_admission
        or refs.profile.writer_admission is not writer_admission
        or observer.attempt_sha256 != refs.intent.binding.attempt_sha256
        or observer.launch_sha256 != refs.intent.binding.launch_sha256
        or observer.credential_custody is refs.writer_credential_custody
        or observer.operation_deadline_ns != refs.launch.registration.launch.operation_deadline_ns
        or observer.observer_admission.server != writer_admission.server
        or observer.observer_admission.database != writer_admission.database
        or observer.observer_admission.login == writer_admission.login
        or observer.observer_admission.login.sid == writer_admission.login.sid
        or authority.server != observer.observer_admission.server
        or authority.database != observer.observer_admission.database
        or authority.login != observer.observer_admission.login
        or authority.transport != observer.observer_admission.transport
    ):
        _invalid()


def reassert_evidence_receipt(
    refs: _ClaimedRefs, receipt: SqlClientEvidenceReceipt, kind: SqlClientEvidenceKind
) -> None:
    """Require the actor's exact last ACK after its dependent lifecycle CAS."""
    observation = _property(refs.launch.evidence, "observation")
    if (
        type(receipt) is not SqlClientEvidenceReceipt
        or receipt.kind is not kind
        or type(observation) is not SqlClientEvidenceObservation
        or observation.attempt_sha256 != refs.launch.registration.binding.attempt_sha256
        or observation.receipt is not receipt
    ):
        _invalid()
    observation.__post_init__()


def persist_evidence(refs: _ClaimedRefs, kind: SqlClientEvidenceKind, payload: bytes) -> SqlClientEvidenceReceipt:
    """Persist once and require the exact actor ACK identity."""
    record = SqlClientEvidenceRecord(refs.launch.registration.binding.attempt_sha256, kind, payload)
    receipt = _call(refs.launch.evidence, "write", record, deadline=refs.launch.plan.operation_deadline)
    if type(receipt) is not SqlClientEvidenceReceipt or receipt != record.receipt:
        _invalid()
    reassert_evidence_receipt(refs, receipt, kind)
    return receipt


def advance_lifecycle(
    refs: _ClaimedRefs,
    previous: TdsAttemptSnapshot,
    event: SqlClientWriterObserved | SqlClientGrantIntent,
) -> TdsAttemptSnapshot:
    """Require predicted state, increasing revision and exact gateway snapshot."""
    predicted = advance_state(previous.state, event, expected_phase=TdsAttemptPhase.RUNNING)
    observed = _call(
        refs.launch.lifecycle,
        "advance",
        event,
        expected_phase=TdsAttemptPhase.RUNNING,
        deadline=refs.launch.plan.operation_deadline,
    )
    if (
        type(observed) is not TdsAttemptSnapshot
        or observed.state != predicted
        or observed.revision <= previous.revision
        or _property(refs.launch.lifecycle, "snapshot") is not observed
    ):
        _invalid()
    return observed


def validate_writer_result(refs: _ClaimedRefs) -> None:
    """Accept only P10c's already decoded terminal result branch."""
    reassert_claimed_refs(refs)
    result = refs.result
    if (
        refs.session is not None
        or type(result) is not SqlClientResult
        or type(refs.result_bytes) is not bytes
        or (refs.input_empty and (result.result.receipt is None or result.result.error is not None))
        or (not refs.input_empty and result.result.error is None)
    ):
        _invalid()


def writer_observation_record(
    refs: _ClaimedRefs, observation: SqlClientWriterObservation
) -> SqlClientWriterObservationRecord:
    """Build the canonical observation solely from retained originals."""
    if type(observation) is not SqlClientWriterObservation or refs.session is None:
        _invalid()
    target = refs.profile.writer_admission
    authority = observation.authority
    if (
        authority.server != target.server
        or authority.database != target.database
        or authority.login != target.login
        or authority.transport != target.transport
    ):
        _invalid()
    registration = refs.launch.registration
    record = SqlClientWriterObservationRecord(
        binding=registration.binding,
        registration_sha256=sha256(encode_registration(registration)).hexdigest(),
        credential_intent_sha256=sha256(encode_credential_intent(refs.intent)).hexdigest(),
        capability_evidence_sha256=refs.intent.capability_evidence_sha256,
        announcement=refs.session,
        remote_session=observation.remote_session,
        authority=observation.authority,
        resolved_database_principal=observation.resolved_database_principal,
    )
    validate_writer_observation(record, registration, refs.intent)
    encode_writer_observation(record)
    return record


def bulk_grant_record(
    refs: _ClaimedRefs,
    record: SqlClientWriterObservationRecord,
    observation_sha256: str,
    *,
    grant_id: str,
    now_ns: int,
) -> SqlClientBulkGrant:
    """Build and validate the exact grant without delivering it."""
    registration = refs.launch.registration
    binding, launch = registration.binding, registration.launch
    grant = SqlClientBulkGrant(
        1,
        grant_id,
        binding.launch_sha256,
        binding.attempt_sha256,
        binding.ownership,
        launch.process,
        binding.object_identity,
        launch.input_binding_sha256,
        launch.build_sha256,
        record.remote_session,
        observation_sha256,
        launch.operation_deadline_ns,
        record.resolved_database_principal,
    )
    validate_bulk_grant(
        grant,
        launch=launch,
        ownership=binding.ownership,
        object_identity=binding.object_identity,
        remote_session=record.remote_session,
        writer_observation_sha256=observation_sha256,
        resolved_database_principal=record.resolved_database_principal,
        now_ns=now_ns,
    )
    encode_bulk_grant(grant)
    return grant


__all__ = (
    "_ClaimedRefs",
    "advance_lifecycle",
    "bulk_grant_record",
    "capture_claimed_refs",
    "persist_evidence",
    "reassert_evidence_receipt",
    "reassert_claimed_refs",
    "validate_observer_refs",
    "validate_writer_result",
    "writer_observation_record",
)

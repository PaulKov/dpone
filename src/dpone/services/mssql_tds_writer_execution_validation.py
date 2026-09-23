"""Exact origin, result, evidence and lifecycle checks for P10e."""

from __future__ import annotations

from dataclasses import asdict
from hashlib import sha256
from inspect import getattr_static
from typing import Any, Never

from dpone.contracts.strict_json import canonical_json_bytes
from dpone.services.mssql_tds_writer_contracts import (
    Contained,
    ContainmentRequired,
    Exited,
    SqlClientEvidenceKind,
    SqlClientEvidenceObservation,
    SqlClientEvidenceRecord,
    SqlClientLocalExit,
    SqlClientResult,
    SqlClientResultContext,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    TdsChildExit,
    advance_state,
    decode_sqlclient_result,
    encode_bulk_grant,
    encode_worker_local_exit,
)
from dpone.services.mssql_tds_writer_observation_custody import _GrantReadyOwner

ERROR = "mssql_native.sqlclient_writer_execution_unknown"


def invalid() -> Never:
    raise ValueError(ERROR)


def class_call(value: object, name: str, /, *args: object, **kwargs: object):
    descriptor = getattr_static(type(value), name)
    return descriptor.__get__(value, type(value))(*args, **kwargs)


def class_property(value: object, name: str):
    descriptor = getattr_static(type(value), name)
    if type(descriptor) is not property:
        invalid()
    return descriptor.__get__(value, type(value))


def validate_ready(owner: _GrantReadyOwner, *, now_ns: int) -> None:
    """Reassert the complete P10d barrier immediately before the grant effect."""
    from dpone.services.mssql_tds_writer_observation_validation import (
        reassert_claimed_refs,
        reassert_evidence_receipt,
    )

    if type(owner) is not _GrantReadyOwner or type(now_ns) is not int or now_ns < 0:
        invalid()
    refs = owner.refs
    sqlclient = owner.state.state.sqlclient
    reassert_claimed_refs(refs, owner.state)
    if (
        sqlclient is None
        or now_ns >= refs.launch.registration.launch.operation_deadline_ns
        or encode_bulk_grant(owner.grant) != owner.grant_bytes
        or owner.grant_receipt.payload_sha256 != sqlclient.grant_intent_sha256
        or class_property(refs.launch.process, "identity") != refs.launch.registration.launch.process
        or class_property(refs.launch.lifecycle, "snapshot") is not owner.state
    ):
        invalid()
    reassert_evidence_receipt(refs, owner.grant_receipt, SqlClientEvidenceKind.GRANT_INTENT)
    current = type(owner.observer)._assert_claim(
        owner.observer, owner.observer, owner.observer_claim, owner.observer_cleanup
    )
    if current is not owner.observer_refs:
        invalid()


def decode_exact_result(owner: _GrantReadyOwner, raw: bytes) -> SqlClientResult:
    registration = owner.refs.launch.registration
    context = SqlClientResultContext(registration.launch, registration.input.expected, owner.grant.grant_id)
    return decode_sqlclient_result(
        raw,
        launch=context.launch,
        expected_input=context.expected_input,
        expected_grant_id=context.expected_grant_id,
    )


def persist_result(owner: _GrantReadyOwner, raw: bytes):
    refs = owner.refs
    registration = refs.launch.registration
    context = SqlClientResultContext(registration.launch, registration.input.expected, owner.grant.grant_id)
    record = SqlClientEvidenceRecord(
        refs.launch.registration.binding.attempt_sha256,
        SqlClientEvidenceKind.RESULT,
        raw,
        context,
    )
    receipt = class_call(refs.launch.evidence, "write", record, deadline=refs.launch.plan.operation_deadline)
    _require_observation(owner, receipt, SqlClientEvidenceKind.RESULT)
    if receipt != record.receipt:
        invalid()
    return receipt


def validate_result_exit(owner: _GrantReadyOwner, result: SqlClientResult, exit_value: TdsChildExit) -> None:
    validate_local_exit(owner, exit_value)
    worker = result.result
    expected = owner.refs.launch.registration.input.expected
    success = (
        worker.error is None
        and worker.receipt == expected
        and result.grant_id == owner.grant.grant_id
        and exit_value.exit_code == 0
    )
    handled = (
        worker.error is not None
        and worker.receipt is None
        and result.grant_id in (None, owner.grant.grant_id)
        and exit_value.exit_code == 1
    )
    if not (success or handled):
        invalid()


def validate_local_exit(owner: _GrantReadyOwner, exit_value: object) -> TdsChildExit:
    """Require an exact, reaped exit for the original managed writer."""
    if type(exit_value) is not TdsChildExit:
        invalid()
    exit_value.__post_init__()
    if exit_value.identity != owner.refs.launch.registration.launch.process or exit_value.reaped is not True:
        invalid()
    return exit_value


def persist_local_exit(owner: _GrantReadyOwner, result_receipt: Any, exit_value: TdsChildExit):
    refs = owner.refs
    record = SqlClientLocalExit(
        binding=refs.launch.registration.binding,
        registration_sha256=refs.launch.receipt.payload_sha256,
        result_sha256=result_receipt.payload_sha256,
        exit=exit_value,
    )
    payload = encode_worker_local_exit(record)
    request = SqlClientEvidenceRecord(
        refs.launch.registration.binding.attempt_sha256,
        SqlClientEvidenceKind.LOCAL_EXIT,
        payload,
    )
    receipt = class_call(refs.launch.evidence, "write", request, deadline=refs.launch.plan.operation_deadline)
    _require_observation(owner, receipt, SqlClientEvidenceKind.LOCAL_EXIT)
    if receipt != request.receipt:
        invalid()
    return receipt


def advance_exited(owner: _GrantReadyOwner, previous: TdsAttemptSnapshot, exit_value: TdsChildExit, result_sha: str):
    refs = owner.refs
    event = Exited(exit_value.exit_code, result_sha)
    predicted = advance_state(previous.state, event, expected_phase=TdsAttemptPhase.RUNNING)
    observed = class_call(
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
        or class_property(refs.launch.lifecycle, "snapshot") is not observed
    ):
        invalid()
    return observed


def advance_containment_required(owner: _GrantReadyOwner, previous: TdsAttemptSnapshot, error):
    """Persist a handled worker failure; it can never enter P10f verification."""
    refs = owner.refs
    event = ContainmentRequired(error)
    predicted = advance_state(previous.state, event, expected_phase=TdsAttemptPhase.EXITED)
    observed = class_call(
        refs.launch.lifecycle,
        "advance",
        event,
        expected_phase=TdsAttemptPhase.EXITED,
        deadline=refs.launch.plan.operation_deadline,
    )
    if (
        type(observed) is not TdsAttemptSnapshot
        or observed.state != predicted
        or observed.revision <= previous.revision
        or class_property(refs.launch.lifecycle, "snapshot") is not observed
    ):
        invalid()
    return observed


def advance_contained(
    owner: _GrantReadyOwner,
    previous: TdsAttemptSnapshot,
    local_exit: TdsChildExit,
) -> TdsAttemptSnapshot:
    """Persist known process/channel containment before exposing a retry candidate."""
    refs = owner.refs
    proof = sha256(canonical_json_bytes(asdict(local_exit))).hexdigest()
    event = Contained(proof)
    predicted = advance_state(previous.state, event, expected_phase=TdsAttemptPhase.CONTAINMENT_REQUIRED)
    observed = class_call(
        refs.launch.lifecycle,
        "advance",
        event,
        expected_phase=TdsAttemptPhase.CONTAINMENT_REQUIRED,
        deadline=refs.launch.plan.operation_deadline,
    )
    if (
        type(observed) is not TdsAttemptSnapshot
        or observed.state != predicted
        or observed.revision <= previous.revision
        or class_property(refs.launch.lifecycle, "snapshot") is not observed
    ):
        invalid()
    return observed


def _require_observation(owner: _GrantReadyOwner, receipt: Any, kind: SqlClientEvidenceKind) -> None:
    observation = class_property(owner.refs.launch.evidence, "observation")
    if (
        type(observation) is not SqlClientEvidenceObservation
        or observation.attempt_sha256 != owner.refs.launch.registration.binding.attempt_sha256
        or observation.receipt is not receipt
        or getattr(receipt, "kind", None) is not kind
    ):
        invalid()
    observation.__post_init__()


def reassert_final(owner: _GrantReadyOwner, local_receipt: Any, state: TdsAttemptSnapshot) -> None:
    _require_observation(owner, local_receipt, SqlClientEvidenceKind.LOCAL_EXIT)
    if class_property(owner.refs.launch.lifecycle, "snapshot") is not state:
        invalid()


__all__ = (
    "advance_exited",
    "advance_containment_required",
    "advance_contained",
    "class_call",
    "class_property",
    "decode_exact_result",
    "invalid",
    "persist_local_exit",
    "persist_result",
    "reassert_final",
    "validate_ready",
    "validate_local_exit",
    "validate_result_exit",
)

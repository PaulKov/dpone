"""One-shot managed SqlClient launch and durable registration boundary.

This private service owns no credential supplier and cannot release SQL work.
It retains the exact child, lifecycle and evidence gateways for the next phase.
"""

from __future__ import annotations

from collections.abc import Callable
from hashlib import sha256
from inspect import getattr_static
from typing import Never, Protocol

from dpone.ports.mssql_sqlclient_process import SqlClientProcess
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown
from dpone.services.mssql_tds_writer_authority import SqlClientWriterAdmitted
from dpone.services.mssql_tds_writer_contracts import (
    LaunchIntent,
    ProcessRegistered,
    SqlClientEvidenceKind,
    SqlClientEvidenceObservation,
    SqlClientEvidenceReceipt,
    SqlClientEvidenceRecord,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    TdsLifecycleEvent,
    advance_state,
    attempt_identity_digest,
    encode_registration,
)
from dpone.services.mssql_tds_writer_launch_validation import build_registration, validate_launch_plan
from dpone.services.mssql_tds_writer_registration_custody import (
    _REGISTERED_TOKEN,
    EvidenceGateway,
    SqlClientWriterRegistered,
    _create_registered_writer,
    _LaunchOwner,
)

ERROR = "mssql_native.sqlclient_writer_launch_unknown"


def _class_call(value: object, name: str, /, *args: object, **kwargs: object):
    descriptor = getattr_static(type(value), name)
    bound = descriptor.__get__(value, type(value))
    return bound(*args, **kwargs)


def _class_property(value: object, name: str):
    descriptor = getattr_static(type(value), name)
    if type(descriptor) is not property:
        raise ValueError(ERROR)
    return descriptor.__get__(value, type(value))


class Launcher(Protocol):
    def spawn(self, *, startup_deadline: float, operation_deadline: float) -> SqlClientProcess: ...


class SqlClientEvidenceOpenUnknown(RuntimeError):
    """Composition-normalized actor-open uncertainty with optional orphan."""

    def __init__(self, gateway: EvidenceGateway | None = None) -> None:
        self.gateway = gateway
        super().__init__(ERROR)


class SqlClientWriterLaunchUnknown(RuntimeError):
    """Sticky unknown outcome retaining exact cleanup observations only."""

    def __init__(self, owner: _LaunchOwner) -> None:
        self._owner = owner
        super().__init__(ERROR)

    def __repr__(self) -> str:
        return "SqlClientWriterLaunchUnknown(<opaque>)"


def _invalid() -> Never:
    raise ValueError(ERROR)


def _advance(
    owner: _LaunchOwner,
    event: TdsLifecycleEvent,
    expected_phase: TdsAttemptPhase,
    previous: TdsAttemptSnapshot,
) -> TdsAttemptSnapshot:
    if owner.lifecycle is None:
        _invalid()
    predicted = advance_state(previous.state, event, expected_phase=expected_phase)
    observed = _class_call(
        owner.lifecycle,
        "advance",
        event,
        expected_phase=expected_phase,
        deadline=owner.require_plan().operation_deadline,
    )
    if (
        type(observed) is not TdsAttemptSnapshot
        or observed.state != predicted
        or observed.revision <= previous.revision
        or _class_property(owner.lifecycle, "snapshot") is not observed
    ):
        _invalid()
    return observed


def launch_and_register_sqlclient_writer(
    admitted: SqlClientWriterAdmitted,
    *,
    clock: Callable[[], float],
    open_evidence: Callable[[str, float], EvidenceGateway],
    build_launcher: Callable[..., Launcher],
) -> SqlClientWriterRegistered:
    """Consume P10a once and stop with the managed child before credentials."""
    if (
        not isinstance(admitted, SqlClientWriterAdmitted)
        or not callable(clock)
        or not callable(open_evidence)
        or not callable(build_launcher)
    ):
        _invalid()
    owner = _LaunchOwner(admitted, None, None)
    try:
        claim = type(admitted)._claim_p10b_once(admitted)
        owner.claim = claim
        plan = type(admitted)._assert_p10b_claim(admitted, claim)
        owner.plan = plan
        owner.lifecycle = plan.transition.attempt._lifecycle
        owner.capture_deadline(clock)
        attempt = validate_launch_plan(admitted, claim, plan)
        attempt_sha256 = attempt_identity_digest(attempt.state.identity)
        launch_intent = _advance(owner, LaunchIntent(plan.grant_result_sha256), TdsAttemptPhase.PREPARED, attempt)
        try:
            owner.evidence = open_evidence(attempt_sha256, plan.operation_deadline)
        except SqlClientEvidenceOpenUnknown as error:
            owner.evidence = error.gateway
            raise
        launcher = build_launcher(
            installation=plan.installation,
            attempt_sha256=attempt_sha256,
            input_descriptor=plan.input_descriptor,
            address_space_bytes=plan.max_worker_address_space_bytes,
        )
        try:
            owner.process = _class_call(
                launcher,
                "spawn",
                startup_deadline=plan.startup_deadline,
                operation_deadline=plan.operation_deadline,
            )
        except TdsLaunchUnknown as error:
            owner.unresolved = error.launch
            raise
        _class_call(owner.process, "startup", deadline=plan.startup_deadline)
        if owner.process is None:
            _invalid()
        registration = build_registration(owner.process, plan, attempt)
        launch = registration.launch
        payload = encode_registration(registration)
        record = SqlClientEvidenceRecord(attempt_sha256, SqlClientEvidenceKind.REGISTRATION, payload)
        if owner.evidence is None:
            _invalid()
        receipt = _class_call(owner.evidence, "write", record, deadline=plan.operation_deadline)
        observation = _class_property(owner.evidence, "observation")
        if (
            type(receipt) is not SqlClientEvidenceReceipt
            or receipt != record.receipt
            or type(observation) is not SqlClientEvidenceObservation
            or observation.attempt_sha256 != attempt_sha256
            or observation.receipt != receipt
        ):
            _invalid()
        SqlClientEvidenceObservation.__post_init__(observation)
        # The actor boundary decodes its durable ACK into an equal, distinct
        # value. Retain that exact observed instance so P10c can prove it still
        # owns the actor's current registration acknowledgement.
        observed_receipt = observation.receipt
        if type(observed_receipt) is not SqlClientEvidenceReceipt:
            _invalid()
        receipt = observed_receipt
        owner.registration, owner.receipt = registration, receipt
        registered = _advance(
            owner,
            ProcessRegistered(launch.process),
            TdsAttemptPhase.LAUNCH_INTENT,
            launch_intent,
        )
        if registered.state.process != launch.process or sha256(payload).hexdigest() != receipt.payload_sha256:
            _invalid()
        type(admitted)._mark_p10b_registered(admitted, claim)
        return _create_registered_writer(_REGISTERED_TOKEN, owner)
    except BaseException:
        recovered_plan = None
        try:
            recovered_plan = type(admitted)._mark_p10b_unknown(admitted, owner.claim)
        except BaseException:
            pass
        if recovered_plan is None:
            raise
        if owner.plan is None:
            owner.plan = recovered_plan
        try:
            owner.lifecycle = recovered_plan.transition.attempt._lifecycle
        except BaseException:
            pass
        owner.cleanup_once()
        raise SqlClientWriterLaunchUnknown(owner) from None


__all__ = (
    "SqlClientEvidenceOpenUnknown",
    "SqlClientWriterLaunchUnknown",
    "SqlClientWriterRegistered",
    "launch_and_register_sqlclient_writer",
)

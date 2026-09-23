"""Exact live-origin and protocol validation for the P10c pre-grant boundary."""

from __future__ import annotations

from hashlib import sha256
from inspect import getattr_static
from typing import Any, Never

from dpone.services.mssql_tds_writer_authority import _AdmissionPlan
from dpone.services.mssql_tds_writer_authority_proof import assert_writer_authority
from dpone.services.mssql_tds_writer_contracts import (
    SqlClientCredentialProfile,
    SqlClientEvidenceKind,
    SqlClientEvidenceObservation,
    SqlClientEvidenceReceipt,
    SqlClientRegistration,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    attempt_identity_digest,
    encode_registration,
    input_descriptor_digest,
    launch_digest,
)
from dpone.services.mssql_tds_writer_registration_custody import (
    SqlClientWriterRegistered,
    _P10cCleanupCustody,
    _P10cLaunchRefs,
)

ERROR = "mssql_native.sqlclient_writer_pregrant_unknown"


def _invalid() -> Never:
    raise ValueError(ERROR)


def _property(value: object, name: str) -> Any:
    descriptor = getattr_static(type(value), name)
    if type(descriptor) is not property:
        _invalid()
    return descriptor.__get__(value, type(value))


def _writer_authority(plan: _AdmissionPlan):
    return assert_writer_authority(
        plan.authority_proof,
        terminal=plan.terminal,
        owner=plan.p9_owner,
        transition=plan.transition,
        fence=plan.authority_fence,
        require_writer=True,
    )


def validate_pregrant_origin(
    registered: SqlClientWriterRegistered,
    claim: object,
    cleanup: _P10cCleanupCustody,
    refs: _P10cLaunchRefs,
    profile: SqlClientCredentialProfile,
) -> tuple[_AdmissionPlan, TdsAttemptSnapshot, str]:
    """Revalidate exact P7-P10 ownership and return current durable custody."""
    if type(refs) is not _P10cLaunchRefs or type(profile) is not SqlClientCredentialProfile:
        _invalid()
    if type(registered)._p10c_identity(registered) is not registered:
        _invalid()
    exact = type(registered)._assert_p10c_claim(registered, registered, claim, cleanup)
    plan = refs.plan
    if exact is not refs or type(plan) is not _AdmissionPlan:
        _invalid()
    admitted_plan = type(refs.admitted)._assert_p10b_registered(refs.admitted, refs.claim)
    if admitted_plan is not plan:
        _invalid()
    receipt, writer, grant_receipt, grant_digest = _writer_authority(plan)
    if receipt is None or writer is None:
        _invalid()
    profile.__post_init__()
    if profile.writer_admission is not writer:
        _invalid()
    registration, registration_receipt = refs.registration, refs.receipt
    process, lifecycle, evidence = refs.process, refs.lifecycle, refs.evidence
    if (
        type(registration) is not SqlClientRegistration
        or type(registration_receipt) is not SqlClientEvidenceReceipt
        or registration_receipt.kind is not SqlClientEvidenceKind.REGISTRATION
        or lifecycle is None
        or evidence is None
    ):
        _invalid()
    raw = encode_registration(registration)
    current = _property(lifecycle, "snapshot")
    observation = _property(evidence, "observation")
    launch = registration.launch
    if (
        type(current) is not TdsAttemptSnapshot
        or current.state.phase is not TdsAttemptPhase.SPAWNED_WAITING
        or current.state.identity != registration.binding.identity
        or current.state.identity != plan.attempt.state.identity
        or current.state.ownership != registration.binding.ownership
        or current.state.ownership != plan.attempt.state.ownership
        or current.state.object_identity != registration.binding.object_identity
        or current.state.object_identity != plan.attempt.state.object_identity
        or current.state.process != launch.process
        or attempt_identity_digest(current.state.identity) != registration.binding.attempt_sha256
        or launch_digest(launch) != registration.binding.launch_sha256
        or input_descriptor_digest(registration.input) != launch.input_binding_sha256
        or sha256(raw).hexdigest() != registration_receipt.payload_sha256
        or type(observation) is not SqlClientEvidenceObservation
        or observation.receipt is not registration_receipt
        or _property(process, "declared_launch") is not launch
        or _property(process, "bound_input") is not registration.input
        or _property(process, "startup_receipt") is not registration.ready
        or _property(process, "identity") != launch.process
    ):
        _invalid()
    return plan, current, receipt.payload_sha256


def reassert_pregrant_custody(
    registered: SqlClientWriterRegistered,
    claim: object,
    cleanup: _P10cCleanupCustody,
    refs: _P10cLaunchRefs,
    profile: SqlClientCredentialProfile,
    running: TdsAttemptSnapshot,
) -> None:
    """Recheck exact owners after both durable ACKs and before secret release."""
    if type(registered)._assert_p10c_claim(registered, registered, claim, cleanup) is not refs:
        _invalid()
    if type(refs.admitted)._assert_p10b_registered(refs.admitted, refs.claim) is not refs.plan:
        _invalid()
    _, writer, _, _ = _writer_authority(refs.plan)
    registration, process = refs.registration, refs.process
    if (
        profile.writer_admission is not writer
        or type(running) is not TdsAttemptSnapshot
        or running.state.phase is not TdsAttemptPhase.RUNNING
        or _property(refs.lifecycle, "snapshot") is not running
        or _property(process, "declared_launch") is not registration.launch
        or _property(process, "bound_input") is not registration.input
        or _property(process, "startup_receipt") is not registration.ready
        or _property(process, "identity") != registration.launch.process
    ):
        _invalid()


__all__ = ("reassert_pregrant_custody", "validate_pregrant_origin")

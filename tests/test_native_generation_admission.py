"""A trusted invocation identity cannot be substituted or expanded on replay."""

from dataclasses import replace
from hashlib import sha256
from uuid import UUID

import pytest

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceGuardEpoch
from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptRequest
from dpone.contracts.native_delivery import NativeGenerationContractError
from dpone.contracts.native_generation_admission import VerifiedGenerationRequest, generation_admission_request_bytes
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativeGenerationOriginalSubject
from dpone.contracts.native_source_custody import (
    NativeSourceCustodyError,
    SourceCustodySnapshot,
    SourceExecutorBinding,
    SourceReadGrant,
)
from dpone.contracts.native_source_custody_codec import decode_source_executor_binding, encode_source_executor_binding
from tests.test_native_original_subjects import authority


def executor():
    return SourceExecutorBinding(
        generation_id=UUID("12345678-1234-5678-1234-567812345678"),
        guard_epoch=1,
        invocation_id=UUID("22345678-1234-5678-1234-567812345678"),
        reservation=OriginalRef("generation/reservation", "sha256:" + "a" * 64),
        profile=OriginalRef("generation/trusted-profile", "sha256:" + "b" * 64),
        command=OriginalRef("generation/build-command", "sha256:" + "c" * 64),
    )


def generation_request(*, subject=None, attempt=None, guard=None, requested_bytes=100, profile=None):
    value = executor()
    arguments = dict(
        subject=subject or NativeGenerationOriginalSubject(authority(), value.generation_id),
        workspace_attempt=attempt
        or DbtWorkspaceAttemptRequest.build(
            activation_id="42345678-1234-5678-1234-567812345678",
            attempt_id=value.command.sha256,
            workflow_id="orders",
            write_subjects=(value.profile.sha256,),
        ),
        guard=guard or DbtWorkspaceGuardEpoch("mssql://source/orders", 1),
        profile=profile or value.profile,
        command=value.command,
        requested_bytes=requested_bytes,
    )
    body = generation_admission_request_bytes(**arguments)
    reference = OriginalRef(
        "generation/" + str(arguments["subject"].generation_id), "sha256:" + sha256(body).hexdigest()
    )
    return VerifiedGenerationRequest(reservation=reference, **arguments)


def test_canonical_request_producer_precedes_original_reference_construction():
    value = generation_request()
    assert value.reservation.sha256 == "sha256:" + sha256(value.request_bytes()).hexdigest()
    with pytest.raises(NativeGenerationContractError):
        replace(value, requested_bytes=value.requested_bytes + 1)


def test_writer_identity_roundtrip_keeps_the_complete_original_tuple():
    value = executor()
    encoded = encode_source_executor_binding(value)
    assert decode_source_executor_binding(encoded) == value
    assert b"dpone.native-source-executor-binding.v1" in encoded


@pytest.mark.parametrize("epoch", [True, 0, -1, 9223372036854775808, "1"])
def test_invalid_epoch_never_forms_a_writer_binding(epoch):
    with pytest.raises((TypeError, ValueError)):
        replace(executor(), guard_epoch=epoch)


@pytest.mark.parametrize("field", ["generation_id", "invocation_id", "reservation", "profile", "command"])
def test_missing_coordinate_is_not_a_partial_authority(field):
    with pytest.raises((TypeError, ValueError)):
        replace(executor(), **{field: None})


def test_codec_rejects_changed_or_unknown_wire_coordinates():
    encoded = encode_source_executor_binding(executor())
    with pytest.raises(ValueError):
        decode_source_executor_binding(b" " + encoded)
    with pytest.raises(ValueError):
        decode_source_executor_binding(encoded.replace(b'"guard_epoch":1', b'"guard_epoch":true'))
    with pytest.raises(ValueError):
        decode_source_executor_binding(encoded[:-1] + b',"success":true}')


def test_frozen_alias_mutation_cannot_evade_validation_at_the_ledger_boundary():
    value = executor()
    object.__setattr__(value.profile, "sha256", "not-a-digest")
    with pytest.raises(DbtPublishingError):
        encode_source_executor_binding(value)


def export_snapshot(outcome="ACTIVE"):
    value = executor()
    plan = OriginalRef("generation/export-plan", "sha256:" + "d" * 64)
    grant = SourceReadGrant(UUID("32345678-1234-5678-1234-567812345678"), value, "EXPORT", plan, 3, value.command)
    return SourceCustodySnapshot(
        generation_id=value.generation_id,
        guard_epoch=value.guard_epoch,
        revision=3,
        executor=value,
        reservation=value.reservation,
        closure=value.command,
        frozen=value.profile,
        quality=value.command,
        export_plan=plan,
        active_reads=(grant,),
        state="FROZEN",
        writer_admission="CLOSED",
        outcome=outcome,
    )


@pytest.mark.parametrize("outcome", ["ACTIVE", "UNKNOWN"])
def test_export_read_requires_quality_and_its_exact_retained_plan(outcome):
    snapshot = export_snapshot(outcome)
    with pytest.raises(NativeSourceCustodyError):
        replace(snapshot, quality=None, export_plan=None)
    with pytest.raises(NativeSourceCustodyError):
        replace(snapshot, export_plan=None)
    with pytest.raises(NativeSourceCustodyError):
        replace(snapshot, export_plan=snapshot.executor.command)


def test_unknown_retains_the_valid_export_grant():
    active = export_snapshot()
    unknown = replace(active, outcome="UNKNOWN")
    assert unknown.active_reads == active.active_reads
    assert unknown.reservation == active.reservation

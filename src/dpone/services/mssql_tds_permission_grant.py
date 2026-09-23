"""Retain one SQL permission process through the first verified READY hold."""

# ruff: noqa: I001

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict
from hashlib import sha256
from uuid import UUID

# fmt: off
from dpone.services.mssql_tds_writer_contracts import (
    decode_permission_grant_evidence,
    encode_permission_grant_evidence, encode_permission_grant_request, validate_permission_binding,
)
from dpone.services.mssql_tds_writer_contracts import (
    PermissionGrantHeldReadyEvidence, encode_permission_grant_held_ready_evidence,
)
from dpone.services.mssql_tds_writer_contracts import (
    PermissionGrantParentEvidenceKind as EvidenceKind,
)
from dpone.services.mssql_tds_writer_contracts import (
    PermissionBoundary, PermissionWireBinding, PermissionWireKind, encode_permission_message,
)
from dpone.services.mssql_tds_writer_contracts import (
    CoordinatorCredentialIntent, CoordinatorGrantIntent, CoordinatorProcessRegistered,
    CoordinatorResultReceived, CoordinatorSessionRegistered, TdsCoordinatorGrant, TdsCoordinatorPhase,
    TdsCoordinatorResult, TdsCoordinatorResultKind, TdsCoordinatorSnapshot, coordinator_grant_digest, coordinator_identity_digest,
)
from dpone.services.mssql_tds_writer_contracts import (
    authority_digest, decode_authority,
)
# fmt: on
from dpone.services.mssql_tds_writer_contracts import (
    decode_startup,
    encode_coordinator_registration as encode_registration,
)
from dpone.services.mssql_tds_writer_contracts import TdsRemoteSessionIdentity, encode_session_identity
from dpone.services.mssql_tds_writer_contracts import deadline_nanoseconds
from dpone.services.mssql_tds_writer_contracts import canonical_json_bytes, strict_json_object
from dpone.ports.mssql_tds_coordinator import TdsCoordinatorGateway
from dpone.services.mssql_tds_permission_grant_owner import (
    PermissionAssociation as _Association,
    PermissionEvidence as _Evidence,
    PermissionGrantHeldOwner,
    PermissionGrantHeldUnknown as PermissionGrantHeldUnknown,
    PermissionLauncher as _Launcher,
    permission_evidence_subject as permission_evidence_subject,
)

ERROR = "mssql_native.sqlclient_permission_held_unknown"


def _grant_body(value: TdsCoordinatorGrant) -> dict:
    if type(value.session) is not TdsRemoteSessionIdentity:
        raise ValueError(ERROR)
    return dict(
        asdict(value), grant_id=str(value.grant_id), session=strict_json_object(encode_session_identity(value.session))
    )


def _wire_body(owner: PermissionGrantHeldOwner, message):
    kind = owner._read(lambda: message.kind)
    ordinal = owner._read(lambda: message.ordinal)
    body = owner._read(lambda: message.body)
    return kind, ordinal, body


def hold_permission_grant(
    association: _Association,
    coordinator: TdsCoordinatorGateway,
    launcher: _Launcher,
    launch_inputs,
    admission: bytes,
    credential_payload: Callable[[PermissionWireBinding, bytes], bytes],
    evidence_factory: Callable[[PermissionWireBinding], _Evidence],
    *,
    grant_id: Callable[[], UUID],
    deadline: float,
    clock: Callable[[], float],
) -> PermissionGrantHeldOwner:
    """Execute the one permitted transcript and stop with the remote hold live."""
    owner = PermissionGrantHeldOwner(association, None, coordinator, None)
    owner._busy = True
    try:
        association.assert_ready()
        owner._check_refs()
        previous_owner = owner._read(lambda: getattr(association, "_held_ready_owner", None))
        if previous_owner is not None:
            previous_owner._fail()
        setattr(association, "_held_ready_owner", owner)
        owner._check_refs()
        if owner._read(lambda: getattr(association, "_held_ready_owner", None)) is not owner:
            owner._fail()
        owner._identity = owner._read(lambda: association.identity)
        owner._reservation = owner._read(lambda: association.reservation)
        observation = owner._read(lambda: coordinator.observation)
        current = owner._read(lambda: observation.snapshot)
        operation = owner._read(lambda: launch_inputs.operation)
        execution_owner = owner._read(lambda: launch_inputs.execution_owner)
        request = owner._read(lambda: launch_inputs.request)
        operation_deadline = owner._read(lambda: launch_inputs.operation_deadline)
        if (
            type(current) is not TdsCoordinatorSnapshot
            or current.state.phase is not TdsCoordinatorPhase.INTENT
            or current.state.identity != owner._identity
            or operation != owner._identity
            or execution_owner != current.state.execution_owner
        ):
            owner._fail()
        owner._current = current
        process = owner._effect(lambda: launcher.launch(launch_inputs), deadline, clock)
        owner.process = owner._process_ref = process

        def receive(kind, ordinal):
            return owner._effect(lambda: process.receive_public(kind, ordinal, deadline=deadline), deadline, clock)

        def send(kind, ordinal, body):
            owner._effect(lambda: process.send_public(kind, ordinal, body, deadline=deadline), deadline, clock)

        def send_credentials(payload):
            owner._effect(lambda: process.send_credentials(payload, deadline=deadline), deadline, clock)

        startup_message = receive(PermissionWireKind.STARTUP, 0)
        startup_body = owner._read(lambda: startup_message.body)
        startup = decode_startup(canonical_json_bytes(strict_json_object(startup_body)["startup"]))
        binding = PermissionWireBinding(
            request, operation, startup, execution_owner, deadline_nanoseconds(operation_deadline)
        )
        owner.binding = owner._binding_ref = binding
        owner._operation_ref = operation
        owner._binding_snapshot = owner._read(binding.snapshot)
        owner._startup_ref, owner._execution_owner_ref = binding.startup, binding.execution_owner
        evidence = owner._read(lambda: evidence_factory(binding))
        owner.evidence = owner._evidence_ref = evidence
        owner._persist(EvidenceKind.REQUEST, encode_permission_grant_request(binding.request), deadline, clock)
        owner._persist(EvidenceKind.ADMISSION, admission, deadline, clock)
        registration = encode_registration(startup, sha256(admission).hexdigest())
        owner._persist(EvidenceKind.REGISTRATION, registration, deadline, clock)
        owner._advance(CoordinatorProcessRegistered(startup.process, sha256(registration).hexdigest()), deadline, clock)
        request_body = {
            "operation": strict_json_object(
                canonical_json_bytes(dict(asdict(binding.operation), operation_id=str(binding.operation.operation_id)))
            ),
            "execution_owner": asdict(binding.execution_owner),
            "request": strict_json_object(encode_permission_grant_request(binding.request)),
        }
        send(PermissionWireKind.REQUEST, 1, request_body)
        request_payload = encode_permission_message(binding, PermissionWireKind.REQUEST, 1, request_body)
        accepted = receive(PermissionWireKind.REQUEST_ACCEPTED, 1)
        accepted_kind, accepted_ordinal, accepted_body = _wire_body(owner, accepted)
        accepted_payload = encode_permission_message(
            binding, accepted_kind, accepted_ordinal, strict_json_object(accepted_body)
        )
        owner._persist(EvidenceKind.REQUEST_ACCEPTED, accepted_payload, deadline, clock)
        owner._advance(CoordinatorCredentialIntent(), deadline, clock)
        private = owner._effect(lambda: credential_payload(binding, request_payload), deadline, clock)
        try:
            send_credentials(private)
        finally:
            del private
        authority_message = receive(PermissionWireKind.AUTHORITY, 2)
        authority_kind, authority_ordinal, authority_body = _wire_body(owner, authority_message)
        authority_payload = encode_permission_message(
            binding, authority_kind, authority_ordinal, strict_json_object(authority_body)
        )
        authority = decode_authority(canonical_json_bytes(strict_json_object(authority_body)["authority"]))
        owner.authority = owner._authority_ref = authority
        owner._persist(EvidenceKind.AUTHORITY, authority_payload, deadline, clock)
        owner._advance(CoordinatorSessionRegistered(authority.session, authority_digest(authority)), deadline, clock)
        generated_id = owner._read(grant_id)
        grant = TdsCoordinatorGrant(
            coordinator_identity_digest(binding.operation),
            binding.execution_owner,
            startup.process,
            authority.session,
            authority_digest(authority),
            generated_id,
        )
        validate_permission_binding(binding.request, binding.operation, grant, authority)
        owner.grant = owner._grant_ref = grant
        execute_body = {"grant": _grant_body(grant)}
        execute_payload = encode_permission_message(binding, PermissionWireKind.EXECUTE, 3, execute_body)
        owner._persist(EvidenceKind.EXECUTION_INTENT, execute_payload, deadline, clock)
        owner._advance(CoordinatorGrantIntent(grant), deadline, clock)
        send(PermissionWireKind.EXECUTE, 3, execute_body)
        held_result = receive(PermissionWireKind.PERMISSION_HELD, 3)
        result_body = owner._read(lambda: held_result.body)
        result = decode_permission_grant_evidence(canonical_json_bytes(strict_json_object(result_body)["evidence"]))
        validate_permission_binding(binding.request, binding.operation, result.grant, result.authority)
        result_receipt = owner._persist(EvidenceKind.RESULT, encode_permission_grant_evidence(result), deadline, clock)
        owner.result = owner._result_ref = result
        owner.result_receipt = owner._result_receipt_ref = result_receipt
        coordinator_result = TdsCoordinatorResult(
            coordinator_identity_digest(binding.operation),
            coordinator_grant_digest(grant),
            TdsCoordinatorResultKind.SUCCEEDED,
            result_receipt.payload_sha256,
        )
        owner._advance(CoordinatorResultReceived(coordinator_result), deadline, clock)
        check_body = {"boundary": PermissionBoundary.READY.value, "evidence_sha256": result_receipt.payload_sha256}
        check_payload = encode_permission_message(binding, PermissionWireKind.CHECK_HELD, 4, check_body)
        send(PermissionWireKind.CHECK_HELD, 4, check_body)
        held = receive(PermissionWireKind.HELD, 4)
        held_kind, held_ordinal, held_body = _wire_body(owner, held)
        held_payload = encode_permission_message(binding, held_kind, held_ordinal, strict_json_object(held_body))
        ready = encode_permission_grant_held_ready_evidence(
            PermissionGrantHeldReadyEvidence(
                coordinator_identity_digest(binding.operation),
                result_receipt.payload_sha256,
                sha256(check_payload).hexdigest(),
                check_payload,
                sha256(held_payload).hexdigest(),
                held_payload,
            ),
            binding=binding,
        )
        owner._persist(EvidenceKind.HELD_READY, ready, deadline, clock)
        owner.phase, owner._busy = "HELD_READY", False
        owner.assert_held_ready()
        return owner
    except BaseException:
        owner._fail()

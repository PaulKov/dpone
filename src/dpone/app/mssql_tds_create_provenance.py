"""Pure consistency of retained CREATE components, never proof of authentic origin.

The composition root must retain these inputs from the actual successful CREATE
call. Fabricated equivalent objects cannot authenticate SQL effects, reaping or
actor ACKs. No supervisor import, credential material, or journal policy is added.
"""

from dataclasses import dataclass, fields
from hashlib import sha256
from typing import Any

from dpone.adapters.mssql_tds_coordinator_connection import decode_connection_admission, encode_connection_admission
from dpone.app.mssql_tds_coordinator_request import (
    TdsCreateResponse,
    decode_create_response,
    encode_create_response,
    validate_grant,
)
from dpone.contracts.mssql_tds_api import (
    authority_digest,
    create_command_digest,
    decode_authority,
    decode_coordinator_state,
    decode_create_request,
    decode_local_exit,
    encode_authority,
    encode_coordinator_state,
    encode_create_request,
    encode_local_exit,
    local_exit_observation,
)
from dpone.contracts.mssql_tds_coordinator import coordinator_identity_digest
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup, encode_registration
from dpone.contracts.mssql_tds_operation_models import (
    TdsAttemptIdentity,
    TdsAttemptOwnership,
    TdsChildExit,
    TdsCoordinatorAuthority,
    TdsCoordinatorCommand,
    TdsCoordinatorEvidenceReceipt,
    TdsCoordinatorEvidenceRecord,
    TdsCoordinatorGrant,
    TdsCoordinatorIdentity,
    TdsCoordinatorLocalExit,
    TdsCoordinatorLocalObservation,
    TdsCoordinatorPhase,
    TdsCoordinatorResult,
    TdsCoordinatorResultKind,
    TdsCoordinatorSnapshot,
    TdsCoordinatorState,
    TdsCreateColumn,
    TdsCreateEvidence,
    TdsCreateObservedColumn,
    TdsCreateRequest,
    TdsDatabaseObservation,
    TdsLockObservation,
    TdsProcessIdentity,
    TdsRemoteSessionIdentity,
    TdsSchemaObservation,
)
from dpone.contracts.mssql_tds_operation_models import (
    TdsCoordinatorEvidenceKind as Kind,
)

_ERROR = "mssql_native.tds_create_provenance_invalid"
_FAILURES = (ValueError, TypeError, OverflowError, RecursionError, UnicodeError, AttributeError)


def _check(value: Any, cls: type) -> None:
    if type(value) is not cls:
        raise ValueError(_ERROR)
    cls(**{f.name: getattr(value, f.name) for f in fields(cls)})


def _identity(value: TdsCoordinatorIdentity) -> None:
    _check(value, TdsCoordinatorIdentity)
    _check(value.parent, TdsAttemptIdentity)


def _request(value: TdsCreateRequest) -> None:
    _check(value, TdsCreateRequest)
    _check(value.parent, TdsAttemptIdentity)
    for column in value.columns:
        _check(column, TdsCreateColumn)
    decode_create_request(encode_create_request(value))


def _grant(value: TdsCoordinatorGrant) -> None:
    _check(value, TdsCoordinatorGrant)
    _check(value.ownership, TdsAttemptOwnership)
    _check(value.process, TdsProcessIdentity)
    _check(value.session, TdsRemoteSessionIdentity)


def _exit(value: TdsChildExit) -> None:
    _check(value, TdsChildExit)
    _check(value.identity, TdsProcessIdentity)


def _authority(value: TdsCoordinatorAuthority) -> None:
    _check(value, TdsCoordinatorAuthority)
    for item, cls in (
        (value.execution_owner, TdsAttemptOwnership),
        (value.process, TdsProcessIdentity),
        (value.session, TdsRemoteSessionIdentity),
        (value.database, TdsDatabaseObservation),
        (value.schema_observation, TdsSchemaObservation),
        (value.lock, TdsLockObservation),
    ):
        _check(item, cls)
    for name in ("resource", "principal", "owner", "mode"):
        if type(getattr(value.lock, name)) is not str:
            raise ValueError(_ERROR)
    decode_authority(encode_authority(value))


def _snapshot(value: TdsCoordinatorSnapshot) -> None:
    if type(value) is not TdsCoordinatorSnapshot or type(value.state) is not TdsCoordinatorState:
        raise ValueError(_ERROR)
    state = value.state
    _identity(state.identity)
    _check(state.execution_owner, TdsAttemptOwnership)
    _check(state.ownership, TdsAttemptOwnership)
    _check(state.process, TdsProcessIdentity)
    _check(state.session, TdsRemoteSessionIdentity)
    if state.grant is None or state.local is None:
        raise ValueError(_ERROR)
    _grant(state.grant)
    _check(state.result, TdsCoordinatorResult)
    _check(state.local, TdsCoordinatorLocalObservation)
    _check(state.local.process, TdsProcessIdentity)
    _check(state, TdsCoordinatorState)
    _check(value, TdsCoordinatorSnapshot)
    decode_coordinator_state(encode_coordinator_state(state), identity=state.identity)


@dataclass(frozen=True, kw_only=True)
class TdsCoordinatorCreateProvenance:
    """Nonsecret retained originals; no new wire schema or root authentication."""

    request: TdsCreateRequest
    admission: bytes
    startup: TdsCoordinatorStartup
    authority: TdsCoordinatorAuthority
    grant: TdsCoordinatorGrant
    local_proof: TdsCoordinatorLocalExit
    snapshot: TdsCoordinatorSnapshot

    def __post_init__(self) -> None:
        try:
            _request(self.request)
            if encode_connection_admission(*decode_connection_admission(self.admission)) != self.admission:
                raise ValueError
            _check(self.startup, TdsCoordinatorStartup)
            _check(self.startup.process, TdsProcessIdentity)
            _authority(self.authority)
            _grant(self.grant)
            _check(self.local_proof, TdsCoordinatorLocalExit)
            _check(self.local_proof.process, TdsProcessIdentity)
            _exit(self.local_proof.exit)
            decode_local_exit(encode_local_exit(self.local_proof))
            _snapshot(self.snapshot)
        except _FAILURES:
            raise ValueError(_ERROR) from None


def validate_create_provenance(
    provenance: TdsCoordinatorCreateProvenance,
    *,
    response: TdsCreateResponse,
    local_exit: TdsChildExit,
    receipts: tuple[TdsCoordinatorEvidenceReceipt, ...],
    request: TdsCreateRequest,
    identity: TdsCoordinatorIdentity,
    ownership: TdsAttemptOwnership,
) -> None:
    """Compare separately held originals and all six exact producer byte receipts."""
    try:
        _check(provenance, TdsCoordinatorCreateProvenance)
        _request(request)
        _identity(identity)
        _check(ownership, TdsAttemptOwnership)
        _exit(local_exit)
        if type(response) is not TdsCreateResponse:
            raise ValueError(_ERROR)
        _check(response.result, TdsCoordinatorResult)
        _check(response.evidence, TdsCreateEvidence)
        evidence = response.evidence
        assert evidence is not None
        for item, cls in (
            (evidence.session, TdsRemoteSessionIdentity),
            (evidence.database, TdsDatabaseObservation),
            (evidence.schema_observation, TdsSchemaObservation),
        ):
            _check(item, cls)
        for column in evidence.columns:
            _check(column, TdsCreateObservedColumn)
        _check(response, TdsCreateResponse)
        p = provenance
        operation = coordinator_identity_digest(identity)
        registration = encode_registration(p.startup, sha256(p.admission).hexdigest())
        authentication = sha256(registration).hexdigest()
        if (
            identity.command is not TdsCoordinatorCommand.CREATE
            or identity.parent != request.parent
            or identity.command_sha256 != create_command_digest(request)
            or identity.original_fence != ownership.fence
            or p.request != request
            or p.authority.execution_owner != ownership
            or p.grant.ownership != ownership
            or p.authority.operation_sha256 != operation
            or p.startup.implementation_sha256 != identity.implementation_sha256
            or p.authority.implementation_sha256 != identity.implementation_sha256
            or p.authority.database.name != request.parent.database
            or p.authority.schema_observation.name != request.parent.schema
            or p.authority.process != p.startup.process
            or local_exit.identity != p.startup.process
            or local_exit.reaped is not True
            or local_exit.exit_code != 0
            or response.result.outcome is not TdsCoordinatorResultKind.SUCCEEDED
        ):
            raise ValueError
        validate_grant(identity, p.grant, p.authority)
        result_bytes = encode_create_response(response)
        decode_create_response(result_bytes, request=request, identity=identity, grant=p.grant, authority=p.authority)
        expected_local = TdsCoordinatorLocalExit(operation, p.startup.process, local_exit, authentication)
        if p.local_proof != expected_local:
            raise ValueError
        expected_state = TdsCoordinatorState(
            identity,
            ownership,
            ownership,
            TdsCoordinatorPhase.RESULT_RECEIVED,
            6,
            process=p.startup.process,
            authentication_sha256=authentication,
            session=p.authority.session,
            authority_sha256=authority_digest(p.authority),
            grant=p.grant,
            result=response.result,
            local=local_exit_observation(expected_local),
        )
        if p.snapshot.state != expected_state:
            raise ValueError
        payloads = {
            Kind.CREATE_REQUEST: encode_create_request(request),
            Kind.ADMISSION: p.admission,
            Kind.REGISTRATION: registration,
            Kind.AUTHORITY: encode_authority(p.authority),
            Kind.RESULT: result_bytes,
            Kind.LOCAL_EXIT: encode_local_exit(expected_local),
        }
        if type(receipts) is not tuple or len(receipts) != len(payloads):
            raise ValueError
        seen = set()
        for receipt in receipts:
            _check(receipt, TdsCoordinatorEvidenceReceipt)
            if (
                receipt.kind in seen
                or receipt != TdsCoordinatorEvidenceRecord(operation, receipt.kind, payloads[receipt.kind]).receipt
            ):
                raise ValueError
            seen.add(receipt.kind)
    except _FAILURES:
        raise ValueError(_ERROR) from None

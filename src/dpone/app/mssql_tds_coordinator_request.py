"""Closed child/supervisor envelopes binding concrete admission to CREATE effects.

Only credential bytes contain secrets. They must remain in private memory/pipes;
no envelope or decoded material belongs in diagnostics or durable state. Driver
profile caps replace caller timeout knobs; obsolete timeout fields are rejected.
"""

from dataclasses import asdict, dataclass, field
from hashlib import sha256

from dpone.contracts.mssql_tds_api import (
    _construct,
    _enum,
    _identity,
    _identity_body,
    _shape,
    _uuid,
    authority_digest,
    canonical_json_bytes,
    create_command_digest,
    create_evidence_digest,
    decode_create_evidence,
    decode_create_request,
    decode_session_identity,
    encode_create_evidence,
    encode_create_request,
    encode_session_identity,
    require_session_nonce,
    strict_json_object,
)
from dpone.contracts.mssql_tds_coordinator import (
    coordinator_grant_digest,
    coordinator_identity_digest,
)
from dpone.contracts.mssql_tds_operation_models import (
    TdsAttemptError,
    TdsAttemptOwnership,
    TdsConnectionMaterial,
    TdsConnectionProfile,
    TdsCoordinatorAuthority,
    TdsCoordinatorCommand,
    TdsCoordinatorGrant,
    TdsCoordinatorIdentity,
    TdsCoordinatorResult,
    TdsCoordinatorResultKind,
    TdsCoordinatorStartup,
    TdsCreateColumn,
    TdsCreateEvidence,
    TdsCreateRequest,
    TdsProcessIdentity,
    TdsRemoteSessionIdentity,
)
from dpone.contracts.mssql_tds_worker import _hash

_INVALID = "mssql_native.tds_coordinator_envelope_invalid"


def _read(payload: bytes, schema: str, names: set[str], limit: int) -> dict:
    if type(payload) is not bytes or len(payload) > limit:
        raise ValueError(_INVALID)
    value = strict_json_object(payload)
    if set(value) != names | {"schema"} or value.pop("schema") != schema:
        raise ValueError(_INVALID)
    return value


def _write(value: dict, limit: int) -> bytes:
    payload = canonical_json_bytes(value)
    if len(payload) > limit:
        raise ValueError(_INVALID)
    return payload


@dataclass(frozen=True)
class TdsCoordinatorCredentials:
    identity: TdsCoordinatorIdentity
    execution_owner: TdsAttemptOwnership
    process: TdsProcessIdentity
    launch_nonce: bytes
    request: TdsCreateRequest
    connection_material: TdsConnectionMaterial = field(repr=False)
    session_nonce: bytes
    driver_profile: TdsConnectionProfile

    def __post_init__(self) -> None:
        for value, cls in (
            (self.identity, TdsCoordinatorIdentity),
            (self.execution_owner, TdsAttemptOwnership),
            (self.process, TdsProcessIdentity),
            (self.request, TdsCreateRequest),
            (self.connection_material, TdsConnectionMaterial),
            (self.driver_profile, TdsConnectionProfile),
        ):
            if type(value) is not cls:
                raise ValueError(_INVALID)
        require_session_nonce(self.launch_nonce)
        require_session_nonce(self.session_nonce)
        if (
            self.identity.command is not TdsCoordinatorCommand.CREATE
            or self.request.parent != self.identity.parent
            or create_command_digest(self.request) != self.identity.command_sha256
            or self.execution_owner.fence != self.identity.original_fence
        ):
            raise ValueError(_INVALID)


def encode_credentials(value: TdsCoordinatorCredentials) -> bytes:
    if type(value) is not TdsCoordinatorCredentials:
        raise ValueError(_INVALID)
    body = asdict(value)
    body.update(
        schema="dpone.tds.coordinator-credentials.v1",
        identity=_identity_body(value.identity),
        launch_nonce=value.launch_nonce.hex(),
        session_nonce=value.session_nonce.hex(),
        request=strict_json_object(encode_create_request(value.request)),
    )
    return _write(body, 1048576)


def decode_credentials(
    payload: bytes, *, startup: TdsCoordinatorStartup, profile: TdsConnectionProfile
) -> TdsCoordinatorCredentials:
    try:
        body = _read(
            payload,
            "dpone.tds.coordinator-credentials.v1",
            set(TdsCoordinatorCredentials.__dataclass_fields__),
            1048576,
        )
        body["identity"] = _identity(body["identity"])
        body["execution_owner"] = _construct(TdsAttemptOwnership, body["execution_owner"])
        body["process"] = _construct(TdsProcessIdentity, body["process"])
        body["request"] = decode_create_request(canonical_json_bytes(body["request"]))
        body["connection_material"] = _construct(TdsConnectionMaterial, body["connection_material"])
        body["driver_profile"] = _enum(TdsConnectionProfile, body["driver_profile"])
        for name in ("launch_nonce", "session_nonce"):
            if type(body[name]) is not str:
                raise ValueError
            body[name] = bytes.fromhex(body[name])
        value = TdsCoordinatorCredentials(**body)
        if (
            type(startup) is not TdsCoordinatorStartup
            or type(profile) is not TdsConnectionProfile
            or value.process != startup.process
            or value.launch_nonce != startup.launch_nonce
            or value.identity.implementation_sha256 != startup.implementation_sha256
            or value.driver_profile is not profile
        ):
            raise ValueError
        if strict_json_object(encode_credentials(value)) != strict_json_object(payload):
            raise ValueError
        return value
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError(_INVALID) from None


def validate_grant(
    identity: TdsCoordinatorIdentity, grant: TdsCoordinatorGrant, authority: TdsCoordinatorAuthority
) -> None:
    """Validate the full original binding, not a subset of caller digests."""
    if (
        type(identity) is not TdsCoordinatorIdentity
        or type(grant) is not TdsCoordinatorGrant
        or type(authority) is not TdsCoordinatorAuthority
    ):
        raise ValueError(_INVALID)
    if (
        grant.operation_sha256 != coordinator_identity_digest(identity)
        or authority.operation_sha256 != grant.operation_sha256
        or authority.implementation_sha256 != identity.implementation_sha256
        or grant.ownership != authority.execution_owner
        or grant.ownership.fence != identity.original_fence
        or grant.process != authority.process
        or grant.session != authority.session
        or grant.authority_sha256 != authority_digest(authority)
    ):
        raise ValueError(_INVALID)


def encode_grant(identity: TdsCoordinatorIdentity, grant: TdsCoordinatorGrant) -> bytes:
    if (
        type(identity) is not TdsCoordinatorIdentity
        or type(grant) is not TdsCoordinatorGrant
        or grant.operation_sha256 != coordinator_identity_digest(identity)
        or type(grant.session) is not TdsRemoteSessionIdentity
    ):
        raise ValueError(_INVALID)
    body = dict(
        asdict(grant), grant_id=str(grant.grant_id), session=strict_json_object(encode_session_identity(grant.session))
    )
    return _write(dict(schema="dpone.tds.coordinator-grant.v1", identity=_identity_body(identity), grant=body), 16384)


def decode_grant(
    payload: bytes, *, identity: TdsCoordinatorIdentity, authority: TdsCoordinatorAuthority
) -> TdsCoordinatorGrant:
    try:
        body = _read(payload, "dpone.tds.coordinator-grant.v1", {"identity", "grant"}, 16384)
        if _identity(body["identity"]) != identity:
            raise ValueError
        value = dict(_shape(TdsCoordinatorGrant, body["grant"]))
        value["ownership"] = _construct(TdsAttemptOwnership, value["ownership"])
        value["process"] = _construct(TdsProcessIdentity, value["process"])
        value["session"] = decode_session_identity(canonical_json_bytes(value["session"]))
        value["grant_id"] = _uuid(value["grant_id"])
        grant = TdsCoordinatorGrant(**value)
        validate_grant(identity, grant, authority)
        return grant
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError(_INVALID) from None


@dataclass(frozen=True)
class TdsCreateFailure:
    operation_sha256: str
    grant_sha256: str
    authority_sha256: str
    error: TdsAttemptError

    def __post_init__(self) -> None:
        for digest in (self.operation_sha256, self.grant_sha256, self.authority_sha256):
            _hash(digest)
        if type(self.error) is not TdsAttemptError:
            raise ValueError(_INVALID)


def _failure_body(failure: TdsCreateFailure) -> dict:
    return dict(asdict(failure), schema="dpone.tds.create-failure.v1")


@dataclass(frozen=True)
class TdsCreateResponse:
    result: TdsCoordinatorResult
    evidence: TdsCreateEvidence | None
    failure: TdsCreateFailure | None

    def __post_init__(self) -> None:
        if type(self.result) is not TdsCoordinatorResult:
            raise ValueError(_INVALID)
        bound: TdsCreateEvidence | TdsCreateFailure
        if self.result.outcome is TdsCoordinatorResultKind.SUCCEEDED:
            if type(self.evidence) is not TdsCreateEvidence or self.failure is not None:
                raise ValueError(_INVALID)
            expected = create_evidence_digest(self.evidence)
            bound = self.evidence
        else:
            if (
                type(self.failure) is not TdsCreateFailure
                or self.evidence is not None
                or self.failure.error is not self.result.error
            ):
                raise ValueError(_INVALID)
            expected = sha256(canonical_json_bytes(_failure_body(self.failure))).hexdigest()
            bound = self.failure
        if (
            self.result.proof_sha256 != expected
            or self.result.operation_sha256 != bound.operation_sha256
            or self.result.grant_sha256 != bound.grant_sha256
        ):
            raise ValueError(_INVALID)


def successful_response(evidence: TdsCreateEvidence) -> TdsCreateResponse:
    """Observed SQL evidence; overall success still requires zero child exit/ACKs."""
    return TdsCreateResponse(
        TdsCoordinatorResult(
            evidence.operation_sha256,
            evidence.grant_sha256,
            TdsCoordinatorResultKind.SUCCEEDED,
            create_evidence_digest(evidence),
        ),
        evidence,
        None,
    )


def failed_response(
    identity: TdsCoordinatorIdentity,
    grant: TdsCoordinatorGrant,
    authority: TdsCoordinatorAuthority,
    error: TdsAttemptError,
) -> TdsCreateResponse:
    validate_grant(identity, grant, authority)
    failure = TdsCreateFailure(
        coordinator_identity_digest(identity), coordinator_grant_digest(grant), authority_digest(authority), error
    )
    proof = sha256(canonical_json_bytes(_failure_body(failure))).hexdigest()
    return TdsCreateResponse(
        TdsCoordinatorResult(
            failure.operation_sha256, failure.grant_sha256, TdsCoordinatorResultKind.FAILED, proof, error
        ),
        None,
        failure,
    )


def encode_create_response(value: TdsCreateResponse) -> bytes:
    if type(value) is not TdsCreateResponse:
        raise ValueError(_INVALID)
    return _write(
        dict(
            schema="dpone.tds.coordinator-create-result.v1",
            result=asdict(value.result),
            evidence=None if value.evidence is None else strict_json_object(encode_create_evidence(value.evidence)),
            failure=None if value.failure is None else _failure_body(value.failure),
        ),
        262144,
    )


def decode_create_response(
    payload: bytes,
    *,
    request: TdsCreateRequest,
    identity: TdsCoordinatorIdentity,
    grant: TdsCoordinatorGrant,
    authority: TdsCoordinatorAuthority,
) -> TdsCreateResponse:
    try:
        validate_grant(identity, grant, authority)
        if request.parent != identity.parent or create_command_digest(request) != identity.command_sha256:
            raise ValueError
        body = _read(payload, "dpone.tds.coordinator-create-result.v1", {"result", "evidence", "failure"}, 262144)
        result = dict(_shape(TdsCoordinatorResult, body["result"]))
        result["outcome"] = _enum(TdsCoordinatorResultKind, result["outcome"])
        if result["error"] is not None:
            result["error"] = _enum(TdsAttemptError, result["error"])
        evidence = None if body["evidence"] is None else decode_create_evidence(canonical_json_bytes(body["evidence"]))
        failure = None
        if body["failure"] is not None:
            if type(body["failure"]) is not dict:
                raise ValueError
            raw = dict(body["failure"])
            if raw.pop("schema", None) != "dpone.tds.create-failure.v1":
                raise ValueError
            _shape(TdsCreateFailure, raw)
            raw["error"] = _enum(TdsAttemptError, raw["error"])
            failure = TdsCreateFailure(**raw)
        response = TdsCreateResponse(TdsCoordinatorResult(**result), evidence, failure)
        if response.result.operation_sha256 != coordinator_identity_digest(
            identity
        ) or response.result.grant_sha256 != coordinator_grant_digest(grant):
            raise ValueError
        if failure is not None and failure.authority_sha256 != authority_digest(authority):
            raise ValueError
        if evidence is not None and (
            evidence.command_sha256 != identity.command_sha256
            or evidence.authority_sha256 != authority_digest(authority)
            or evidence.session != authority.session
            or evidence.database != authority.database
            or evidence.schema_observation != authority.schema_observation
            or evidence.table_name != request.parent.table
            or evidence.object_nonce != request.object_nonce
            or evidence.owner_binding != request.parent.owner_binding
            or tuple(TdsCreateColumn(c.name, c.type, c.nullable) for c in evidence.columns) != request.columns
        ):
            raise ValueError
        return response
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError(_INVALID) from None

"""Typed parent request and private credentials for one permission child."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from hashlib import sha256
from typing import TypedDict

from dpone.contracts.mssql_tds_api import connection_contract, permission_wire

SqlClientPermissionGrantRequest = permission_wire.SqlClientPermissionGrantRequest
PermissionWireBinding = permission_wire.PermissionWireBinding
TdsCoordinatorIdentity = permission_wire.TdsCoordinatorIdentity
TdsAttemptOwnership = permission_wire.TdsAttemptOwnership
TdsConnectionMaterial = connection_contract.TdsConnectionMaterial
TdsConnectionProfile = connection_contract.TdsConnectionProfile
TdsCoordinatorCommand = permission_wire.TdsCoordinatorCommand
encode_permission_grant_request = permission_wire.encode_permission_grant_request
decode_permission_grant_request = permission_wire.decode_permission_grant_request
permission_grant_digest = permission_wire.permission_grant_digest
coordinator_identity_body = permission_wire.coordinator_identity_body
coordinator_identity_from_body = permission_wire.coordinator_identity_from_body
canonical_json_bytes = permission_wire.canonical_json_bytes
strict_json_object = permission_wire.strict_json_object
_hash = permission_wire._hash

ERROR = "mssql_native.sqlclient_permission_launch_request_invalid"
PUBLIC_LIMIT = 131072
PRIVATE_LIMIT = 196608


class PermissionGrantPublicRequest(TypedDict):
    schema: str
    request: SqlClientPermissionGrantRequest
    operation: TdsCoordinatorIdentity
    execution_owner: TdsAttemptOwnership
    admission_sha256: str
    startup_deadline: float
    operation_deadline: float
    termination_timeout: float


def _nonce(value: bytes) -> None:
    if type(value) is not bytes or len(value) != 32:
        raise ValueError(ERROR)


def _deadline(value: float) -> int:
    if type(value) is not float or not math.isfinite(value) or value <= 0:
        raise ValueError(ERROR)
    numerator, denominator = value.as_integer_ratio()
    converted = numerator * 1_000_000_000 // denominator
    if not 1 <= converted <= 2**63 - 1:
        raise ValueError(ERROR)
    return converted


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientPermissionGrantLaunchRequest:
    """Immutable launch intent; secret-bearing fields are excluded from repr."""

    request: SqlClientPermissionGrantRequest
    operation: TdsCoordinatorIdentity
    execution_owner: TdsAttemptOwnership
    connection_material: TdsConnectionMaterial = field(repr=False)
    profile: TdsConnectionProfile
    admission_sha256: str
    startup_deadline: float
    operation_deadline: float
    termination_timeout: float
    session_nonce: bytes = field(repr=False)
    schema: str = "dpone.sqlclient.permission-grant-launch.v1"

    def __post_init__(self) -> None:
        try:
            if (
                self.schema != "dpone.sqlclient.permission-grant-launch.v1"
                or type(self.request) is not SqlClientPermissionGrantRequest
                or type(self.operation) is not TdsCoordinatorIdentity
                or type(self.execution_owner) is not TdsAttemptOwnership
                or type(self.connection_material) is not TdsConnectionMaterial
                or type(self.profile) is not TdsConnectionProfile
            ):
                raise ValueError
            _hash(self.admission_sha256)
            _nonce(self.session_nonce)
            start = _deadline(self.startup_deadline)
            operation = _deadline(self.operation_deadline)
            if (
                start > operation
                or type(self.termination_timeout) not in (int, float)
                or not math.isfinite(self.termination_timeout)
                or self.termination_timeout <= 0
            ):
                raise ValueError
            if (
                self.operation.parent != self.request.parent
                or self.operation.command is not TdsCoordinatorCommand.GRANT
                or self.operation.command_sha256 != permission_grant_digest(self.request)
                or self.operation.original_fence != self.execution_owner.fence
                or self.connection_material.database != self.request.stage.database_name
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise ValueError(ERROR) from None

    def public_payload(self) -> bytes:
        return encode_permission_launch_request(self)

    def credential_payload(self, binding: PermissionWireBinding, request_payload: bytes) -> bytes:
        return encode_permission_credentials(self, binding=binding, request_payload=request_payload)


def encode_permission_launch_request(value: SqlClientPermissionGrantLaunchRequest) -> bytes:
    """Encode only public facts required before REQUEST acceptance."""
    try:
        if type(value) is not SqlClientPermissionGrantLaunchRequest:
            raise ValueError
        value.__post_init__()
        payload = canonical_json_bytes(
            {
                "schema": value.schema,
                "request": strict_json_object(encode_permission_grant_request(value.request)),
                "operation": coordinator_identity_body(value.operation),
                "execution_owner": asdict(value.execution_owner),
                "admission_sha256": value.admission_sha256,
                "startup_deadline": value.startup_deadline,
                "operation_deadline": value.operation_deadline,
                "termination_timeout": value.termination_timeout,
            }
        )
        if not 0 < len(payload) <= PUBLIC_LIMIT:
            raise ValueError
        return payload
    except (ValueError, TypeError, AttributeError, OverflowError, UnicodeError):
        raise ValueError(ERROR) from None


def decode_permission_launch_request(payload: bytes) -> PermissionGrantPublicRequest:
    """Decode canonical public launch facts without constructing private material."""
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= PUBLIC_LIMIT:
            raise ValueError
        body = strict_json_object(payload)
        if (
            set(body)
            != {
                "schema",
                "request",
                "operation",
                "execution_owner",
                "admission_sha256",
                "startup_deadline",
                "operation_deadline",
                "termination_timeout",
            }
            or body["schema"] != "dpone.sqlclient.permission-grant-launch.v1"
        ):
            raise ValueError
        result: PermissionGrantPublicRequest = {
            "schema": body["schema"],
            "request": decode_permission_grant_request(canonical_json_bytes(body["request"])),
            "operation": coordinator_identity_from_body(body["operation"]),
            "execution_owner": TdsAttemptOwnership(**body["execution_owner"]),
            "admission_sha256": body["admission_sha256"],
            "startup_deadline": body["startup_deadline"],
            "operation_deadline": body["operation_deadline"],
            "termination_timeout": body["termination_timeout"],
        }
        _hash(result["admission_sha256"])
        if (
            _deadline(result["startup_deadline"]) > _deadline(result["operation_deadline"])
            or type(result["termination_timeout"]) not in (int, float)
            or not math.isfinite(result["termination_timeout"])
            or result["termination_timeout"] <= 0
        ):
            raise ValueError
        if canonical_json_bytes(body) != payload:
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, UnicodeError):
        raise ValueError(ERROR) from None


def encode_permission_credentials(
    value: SqlClientPermissionGrantLaunchRequest,
    *,
    binding: PermissionWireBinding,
    request_payload: bytes,
) -> bytes:
    """Encode the sole private frame after the child accepted REQUEST."""
    try:
        value.__post_init__()
        if (
            binding.request != value.request
            or binding.operation != value.operation
            or binding.execution_owner != value.execution_owner
        ):
            raise ValueError
        payload = canonical_json_bytes(
            {
                "schema": "dpone.sqlclient.permission-grant-credentials.v1",
                "request_payload_sha256": sha256(request_payload).hexdigest(),
                "process": asdict(binding.startup.process),
                "launch_nonce": binding.startup.launch_nonce.hex(),
                "session_nonce": value.session_nonce.hex(),
                "profile": value.profile.value,
                "connection_material": asdict(value.connection_material),
            }
        )
        if not 0 < len(payload) <= PRIVATE_LIMIT:
            raise ValueError
        return payload
    except (ValueError, TypeError, AttributeError, OverflowError, UnicodeError):
        raise ValueError(ERROR) from None


def decode_permission_credentials(
    payload: bytes,
    *,
    binding: PermissionWireBinding,
    request_payload: bytes,
    profile: TdsConnectionProfile,
) -> tuple[TdsConnectionMaterial, bytes]:
    """Authenticate private material to the accepted public request and child."""
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= PRIVATE_LIMIT:
            raise ValueError
        body = strict_json_object(payload)
        if (
            set(body)
            != {
                "schema",
                "request_payload_sha256",
                "process",
                "launch_nonce",
                "session_nonce",
                "profile",
                "connection_material",
            }
            or body["schema"] != "dpone.sqlclient.permission-grant-credentials.v1"
        ):
            raise ValueError
        material = TdsConnectionMaterial(**body["connection_material"])
        nonce = bytes.fromhex(body["session_nonce"])
        _nonce(nonce)
        if (
            body["request_payload_sha256"] != sha256(request_payload).hexdigest()
            or body["process"] != asdict(binding.startup.process)
            or body["launch_nonce"] != binding.startup.launch_nonce.hex()
            or body["profile"] != profile.value
            or material.database != binding.request.stage.database_name
            or canonical_json_bytes(body) != payload
        ):
            raise ValueError
        return material, nonce
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, UnicodeError):
        raise ValueError(ERROR) from None

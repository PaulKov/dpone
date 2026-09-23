"""Closed retained OBSERVE handshake and original SQL authority bindings.

Pure wire validation only. Request acceptance releases no credentials and grants
no SQL authority. The parent acknowledges registration events independently.
"""

from dataclasses import asdict, dataclass, field

from dpone.contracts.mssql_sqlclient_grant_inventory import validate_inventory_original
from dpone.contracts.mssql_sqlclient_observe import ERROR, SqlClientObserveRequest
from dpone.contracts.mssql_sqlclient_observe_wire import MAX_CREDENTIAL_PAYLOAD_BYTES, canonical, decode
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial, TdsConnectionProfile
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorIdentity, coordinator_identity_digest
from dpone.contracts.mssql_tds_coordinator_authority import TdsCoordinatorAuthority, decode_authority
from dpone.contracts.mssql_tds_coordinator_codec import (
    coordinator_identity_body as _identity_body,
)
from dpone.contracts.mssql_tds_coordinator_codec import (
    coordinator_identity_from_body as _identity,
)
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from dpone.contracts.mssql_tds_session import require_session_nonce
from dpone.contracts.mssql_tds_worker import TdsAttemptOwnership, TdsProcessIdentity
from dpone.contracts.strict_record import construct_record, record_shape, string_enum


@dataclass(frozen=True)
class SqlClientObserveCredentials:
    request_sha256: str
    identity: TdsCoordinatorIdentity
    execution_owner: TdsAttemptOwnership
    process: TdsProcessIdentity
    launch_nonce: bytes
    session_nonce: bytes
    connection_material: TdsConnectionMaterial = field(repr=False)
    driver_profile: TdsConnectionProfile
    schema: str = "dpone.sqlclient.observe.credentials.v1"


def encode_credentials(value: SqlClientObserveCredentials, request: SqlClientObserveRequest) -> bytes:
    if type(value) is not SqlClientObserveCredentials:
        raise ValueError(ERROR)
    for item, cls in (
        (value.identity, TdsCoordinatorIdentity),
        (value.execution_owner, TdsAttemptOwnership),
        (value.process, TdsProcessIdentity),
        (value.connection_material, TdsConnectionMaterial),
    ):
        validate_inventory_original(item, cls)
    require_session_nonce(value.launch_nonce)
    require_session_nonce(value.session_nonce)
    if (
        type(value.driver_profile) is not TdsConnectionProfile
        or type(value.request_sha256) is not str
        or value.request_sha256 != request.command_sha256
        or value.identity.command is not TdsCoordinatorCommand.OBSERVE
        or value.identity.command_sha256 != value.request_sha256
        or value.identity.parent != request.parent
        or value.execution_owner.fence != value.identity.original_fence
        or value.schema != "dpone.sqlclient.observe.credentials.v1"
    ):
        raise ValueError(ERROR)
    body = asdict(value)
    body.update(
        identity=_identity_body(value.identity),
        launch_nonce=value.launch_nonce.hex(),
        session_nonce=value.session_nonce.hex(),
    )
    return canonical(body, MAX_CREDENTIAL_PAYLOAD_BYTES)


def decode_credentials(
    payload: bytes, request: SqlClientObserveRequest, startup: TdsCoordinatorStartup, profile: TdsConnectionProfile
) -> SqlClientObserveCredentials:
    body = record_shape(SqlClientObserveCredentials, decode(payload, MAX_CREDENTIAL_PAYLOAD_BYTES))
    body["identity"] = _identity(body["identity"])
    for name, cls in (
        ("execution_owner", TdsAttemptOwnership),
        ("process", TdsProcessIdentity),
        ("connection_material", TdsConnectionMaterial),
    ):
        body[name] = construct_record(cls, body[name])
    for name in ("launch_nonce", "session_nonce"):
        if type(body[name]) is not str:
            raise ValueError(ERROR)
        body[name] = bytes.fromhex(body[name])
    body["driver_profile"] = string_enum(TdsConnectionProfile, body["driver_profile"])
    value = SqlClientObserveCredentials(**body)
    if (
        encode_credentials(value, request) != payload
        or value.process != startup.process
        or value.launch_nonce != startup.launch_nonce
        or value.driver_profile is not profile
        or value.identity.implementation_sha256 != startup.implementation_sha256
    ):
        raise ValueError(ERROR)
    return value


def encode_request_accepted(request: SqlClientObserveRequest, startup: TdsCoordinatorStartup) -> bytes:
    """The sole closed acceptance projection; contains no private material."""
    validate_inventory_original(startup, TdsCoordinatorStartup)
    return canonical(
        dict(
            schema="dpone.sqlclient.observe.request-accepted.v1",
            request_sha256=request.command_sha256,
            launch_nonce=startup.launch_nonce.hex(),
        )
    )


def validate_request_accepted(payload: bytes, request: SqlClientObserveRequest, startup: TdsCoordinatorStartup) -> None:
    if type(payload) is not bytes or payload != encode_request_accepted(request, startup):
        raise ValueError(ERROR)


def validate_authority(
    payload: bytes,
    request: SqlClientObserveRequest,
    identity: TdsCoordinatorIdentity,
    owner: TdsAttemptOwnership,
    startup: TdsCoordinatorStartup,
    nonce: bytes,
) -> TdsCoordinatorAuthority:
    """Return actual decoded authority only when every original binding matches."""
    authority = decode_authority(payload)
    if (
        authority.operation_sha256 != coordinator_identity_digest(identity)
        or authority.execution_owner != owner
        or authority.process != startup.process
        or authority.implementation_sha256 != identity.implementation_sha256
        or authority.session.nonce != nonce
        or (authority.database.database_id, authority.database.name, str(authority.database.database_guid))
        != (
            request.selected_stage.database_id,
            request.selected_stage.database_name,
            str(request.selected_stage.database_guid),
        )
        or authority.schema_observation.name != request.parent.schema
    ):
        raise ValueError(ERROR)
    return authority

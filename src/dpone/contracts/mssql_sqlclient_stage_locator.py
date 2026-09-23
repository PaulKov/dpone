"""Immutable SqlClient discovery records; neither UUID nor locator grants authority.

The state marker belongs to one durable authoritative store. Clones, rollback,
legacy adoption and arbitrary credential holders are outside that premise.
Canonical bytes bind original coordinates without normalizing SQL identifiers.
"""

from dataclasses import asdict, dataclass, fields
from hashlib import sha256
from typing import Any
from uuid import UUID

from dpone.contracts.bounded_window import WindowRecord
from dpone.contracts.mssql_sqlclient_observation import SqlClientServerAuthority
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorIdentity
from dpone.contracts.mssql_tds_coordinator_authority import TdsDatabaseObservation
from dpone.contracts.mssql_tds_create import TdsCreateColumn, TdsCreateRequest, create_command_digest
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand, TdsDirectoryLimits
from dpone.contracts.mssql_tds_validation import _hash, _integer, _text
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsAttemptOwnership
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid, construct_record, record_shape, string_enum

STATE_DOMAIN_KEY = "sqlclient-state-domain/v1"
MAX_STATE_DOMAIN_BYTES = 256
MAX_STAGE_LOCATOR_BYTES = 16384
_DOMAIN_SCHEMA = "dpone.sqlclient.state-domain.v1"
_LOCATOR_SCHEMA = "dpone.sqlclient.stage-locator.v1"
_ERROR = "mssql_native.sqlclient_stage_locator_invalid"


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError(_ERROR)


def _uuid(value: UUID) -> None:
    _require(type(value) is UUID and type(value.int) is int and 1 <= value.int <= 2**128 - 1)


def _original(value: Any, cls: type) -> None:
    # Validate actual original fields before any equality, deepcopy or asdict.
    _require(type(value) is cls)
    originals = {field.name: getattr(value, field.name) for field in fields(cls)}
    if cls is SqlClientServerAuthority:
        for scalar in originals.values():
            _text(scalar, 128)
    elif cls is TdsDatabaseObservation:
        _uuid(value.database_guid)
    elif cls is TdsDirectoryLimits:
        for scalar in originals.values():
            _integer(scalar, 1)
    cls(**originals)


def _operation(value: TdsCoordinatorIdentity) -> None:
    _require(type(value) is TdsCoordinatorIdentity)
    _original(value.parent, TdsAttemptIdentity)
    _uuid(value.operation_id)
    _original(value, TdsCoordinatorIdentity)
    _require(value.command is TdsCoordinatorCommand.CREATE)


def _payload(record: WindowRecord, cap: int) -> bytes:
    _require(type(record) is WindowRecord)
    _integer(record.revision, 1)
    _require(type(record.payload) is str and len(record.payload) <= cap)
    encoded = record.payload.encode("utf-8")
    _require(len(encoded) <= cap)
    return encoded


def encode_state_domain(domain_id: UUID) -> bytes:
    """Encode the fixed create-once marker without target or current ownership."""
    _uuid(domain_id)
    encoded = canonical_json_bytes({"schema": _DOMAIN_SCHEMA, "domain_id": str(domain_id)})
    _require(len(encoded) <= MAX_STATE_DOMAIN_BYTES)
    return encoded


def validate_state_domain_record(record: WindowRecord) -> UUID:
    """Validate exact revision, closed duplicate-free bytes and canonical UUID."""
    payload = _payload(record, MAX_STATE_DOMAIN_BYTES)
    value = strict_json_object(payload)
    _require(set(value) == {"schema", "domain_id"} and value["schema"] == _DOMAIN_SCHEMA)
    domain_id = canonical_uuid(value["domain_id"])
    _require(encode_state_domain(domain_id) == payload)
    return domain_id


@dataclass(frozen=True)
class SqlClientStageLookup:
    """Independent domain/server/database expectation and observed SQL properties."""

    state_domain_id: UUID
    server: SqlClientServerAuthority
    database: TdsDatabaseObservation
    owner_binding: str
    object_nonce: UUID

    def __post_init__(self) -> None:
        _uuid(self.state_domain_id)
        _uuid(self.object_nonce)
        _original(self.server, SqlClientServerAuthority)
        _original(self.database, TdsDatabaseObservation)
        _hash(self.owner_binding)


def stage_locator_key(lookup: SqlClientStageLookup) -> str:
    """One point lookup; database name/ID drift is rejected by record binding."""
    _original(lookup, SqlClientStageLookup)
    body = [
        str(lookup.state_domain_id),
        asdict(lookup.server),
        str(lookup.database.database_guid),
        lookup.owner_binding,
        str(lookup.object_nonce),
    ]
    return (
        "sqlclient-stage-locator/v1/"
        + sha256(b"dpone.sqlclient.stage-locator-key.v1\0" + canonical_json_bytes(body)).hexdigest()
    )


@dataclass(frozen=True)
class SqlClientStageLocator:
    """Original immutable recovery coordinates, with no execution capability."""

    state_domain_id: UUID
    server: SqlClientServerAuthority
    database: TdsDatabaseObservation
    object_nonce: UUID
    create_operation: TdsCoordinatorIdentity
    execution_owner: TdsAttemptOwnership
    directory_limits: TdsDirectoryLimits

    def __post_init__(self) -> None:
        _operation(self.create_operation)
        _original(self.execution_owner, TdsAttemptOwnership)
        _original(self.directory_limits, TdsDirectoryLimits)
        self.lookup()
        _require(self.create_operation.original_fence == self.execution_owner.fence)
        _require(self.create_operation.parent.database == self.database.name)

    def lookup(self) -> SqlClientStageLookup:
        """Discovery key inputs; callers still need independently admitted values."""
        _operation(self.create_operation)
        return SqlClientStageLookup(
            self.state_domain_id,
            self.server,
            self.database,
            self.create_operation.parent.owner_binding,
            self.object_nonce,
        )


def validate_locator_request(locator: SqlClientStageLocator, request: TdsCreateRequest) -> None:
    """Bind the full strictly original request, including nonce and typed columns."""
    _original(locator, SqlClientStageLocator)
    _require(type(request) is TdsCreateRequest)
    _original(request.parent, TdsAttemptIdentity)
    _uuid(request.object_nonce)
    _require(type(request.columns) is tuple)
    for column in request.columns:
        _original(column, TdsCreateColumn)
    _original(request, TdsCreateRequest)
    _require(request.parent == locator.create_operation.parent and request.object_nonce == locator.object_nonce)
    _require(create_command_digest(request) == locator.create_operation.command_sha256)


def encode_stage_locator(value: SqlClientStageLocator) -> bytes:
    """Closed canonical encoding, bounded before persistence or hashing."""
    _original(value, SqlClientStageLocator)
    body = asdict(value)
    body["schema"] = _LOCATOR_SCHEMA
    body["state_domain_id"] = str(value.state_domain_id)
    body["object_nonce"] = str(value.object_nonce)
    body["database"]["database_guid"] = str(value.database.database_guid)
    body["create_operation"]["operation_id"] = str(value.create_operation.operation_id)
    payload = canonical_json_bytes(body)
    _require(len(payload) <= MAX_STAGE_LOCATOR_BYTES)
    return payload


def decode_stage_locator(payload: bytes) -> SqlClientStageLocator:
    """Reject aliases rather than silently repairing previously persisted bytes."""
    try:
        _require(type(payload) is bytes and len(payload) <= MAX_STAGE_LOCATOR_BYTES)
        body = strict_json_object(payload)
        _require(body.pop("schema", None) == _LOCATOR_SCHEMA)
        record_shape(SqlClientStageLocator, body)
        for name in ("state_domain_id", "object_nonce"):
            body[name] = canonical_uuid(body[name])
        body["server"] = construct_record(SqlClientServerAuthority, body["server"])
        database = record_shape(TdsDatabaseObservation, body["database"])
        database["database_guid"] = canonical_uuid(database["database_guid"])
        body["database"] = TdsDatabaseObservation(**database)
        operation = record_shape(TdsCoordinatorIdentity, body["create_operation"])
        operation["parent"] = construct_record(TdsAttemptIdentity, operation["parent"])
        operation["operation_id"] = canonical_uuid(operation["operation_id"])
        operation["command"] = string_enum(TdsCoordinatorCommand, operation["command"])
        body["create_operation"] = TdsCoordinatorIdentity(**operation)
        body["execution_owner"] = construct_record(TdsAttemptOwnership, body["execution_owner"])
        body["directory_limits"] = construct_record(TdsDirectoryLimits, body["directory_limits"])
        value = SqlClientStageLocator(**body)
        _require(encode_stage_locator(value) == payload)
        return value
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError(_ERROR) from None


@dataclass(frozen=True)
class SqlClientStageLocatorSnapshot:
    """A validated point-read or CAS acknowledgement; still discovery metadata."""

    locator: SqlClientStageLocator
    revision: int

    def __post_init__(self) -> None:
        _original(self.locator, SqlClientStageLocator)
        _integer(self.revision, 1)


def decode_stage_locator_record(record: WindowRecord) -> SqlClientStageLocatorSnapshot:
    return SqlClientStageLocatorSnapshot(
        decode_stage_locator(_payload(record, MAX_STAGE_LOCATOR_BYTES)), record.revision
    )

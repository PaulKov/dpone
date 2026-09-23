"""Exact SQL authority observations; caller hashes never acquire a database lock."""

from dataclasses import asdict, dataclass
from hashlib import sha256
from uuid import UUID

from dpone.contracts.mssql_tds_session import (
    TdsRemoteSessionIdentity,
    TdsRestrictedRemoteSessionIdentity,
    TdsSessionIdentity,
    decode_session_continuity_identity,
    encode_session_continuity_identity,
)
from dpone.contracts.mssql_tds_worker import TdsAttemptOwnership, TdsProcessIdentity, _hash, _integer
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid as _uuid
from dpone.contracts.strict_record import construct_record as _construct
from dpone.contracts.strict_record import record_shape as _shape

LOCK_RESOURCE = "dpone.tds.coordinator.ddl.v1"
AUTHORITY_SCHEMA = "dpone.tds.coordinator-authority.v1"


def identifier(value: str) -> None:
    """Admit bounded names without inventing SQL collation canonicalization."""
    if type(value) is not str or not value or any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in value):
        raise ValueError("mssql_native.tds_create_identifier_invalid")
    try:
        if len(value.encode("utf-16-le")) > 256:
            raise ValueError
    except (UnicodeError, ValueError):
        raise ValueError("mssql_native.tds_create_identifier_invalid") from None


@dataclass(frozen=True)
class TdsDatabaseObservation:
    name: str
    database_id: int
    database_guid: UUID

    def __post_init__(self) -> None:
        identifier(self.name)
        _integer(self.database_id, 1, 2**31 - 1)
        if type(self.database_guid) is not UUID or not self.database_guid.int:
            raise ValueError("mssql_native.tds_database_observation_invalid")


@dataclass(frozen=True)
class TdsSchemaObservation:
    schema_id: int
    name: str

    def __post_init__(self) -> None:
        _integer(self.schema_id, 1, 2**31 - 1)
        identifier(self.name)


@dataclass(frozen=True)
class TdsLockObservation:
    acquisition_result: int
    resource: str = LOCK_RESOURCE
    principal: str = "public"
    owner: str = "Session"
    mode: str = "Exclusive"

    def __post_init__(self) -> None:
        _integer(self.acquisition_result, 0, 1)
        if (self.resource, self.principal, self.owner, self.mode) != (LOCK_RESOURCE, "public", "Session", "Exclusive"):
            raise ValueError("mssql_native.tds_lock_observation_invalid")


@dataclass(frozen=True)
class TdsCoordinatorAuthority:
    operation_sha256: str
    execution_owner: TdsAttemptOwnership
    process: TdsProcessIdentity
    implementation_sha256: str
    session: TdsSessionIdentity
    database: TdsDatabaseObservation
    schema_observation: TdsSchemaObservation
    lock: TdsLockObservation
    transaction_count: int = 0
    implicit_transactions: bool = False

    def __post_init__(self) -> None:
        _hash(self.operation_sha256)
        _hash(self.implementation_sha256)
        for value, cls in (
            (self.execution_owner, TdsAttemptOwnership),
            (self.process, TdsProcessIdentity),
            (self.database, TdsDatabaseObservation),
            (self.schema_observation, TdsSchemaObservation),
            (self.lock, TdsLockObservation),
        ):
            if type(value) is not cls:
                raise ValueError("mssql_native.tds_authority_invalid")
        if type(self.session) not in (TdsRemoteSessionIdentity, TdsRestrictedRemoteSessionIdentity):
            raise ValueError("mssql_native.tds_authority_invalid")
        if (
            type(self.transaction_count) is not int
            or self.transaction_count != 0
            or self.implicit_transactions is not False
        ):
            raise ValueError("mssql_native.tds_authority_invalid")


def encode_authority(value: TdsCoordinatorAuthority) -> bytes:
    if type(value) is not TdsCoordinatorAuthority:
        raise ValueError("mssql_native.tds_authority_invalid")
    body = asdict(value)
    body["schema"] = AUTHORITY_SCHEMA
    body["database"]["database_guid"] = str(value.database.database_guid)
    body["session"] = strict_json_object(encode_session_continuity_identity(value.session))
    payload = canonical_json_bytes(body)
    if len(payload) > 16384:
        raise ValueError("mssql_native.tds_authority_invalid")
    return payload


def authority_digest(value: TdsCoordinatorAuthority) -> str:
    return sha256(encode_authority(value)).hexdigest()


def decode_authority(payload: bytes) -> TdsCoordinatorAuthority:
    try:
        if type(payload) is not bytes or len(payload) > 16384:
            raise ValueError
        body = strict_json_object(payload)
        if body.pop("schema", None) != AUTHORITY_SCHEMA:
            raise ValueError
        _shape(TdsCoordinatorAuthority, body)
        database = _shape(TdsDatabaseObservation, body["database"])
        database["database_guid"] = _uuid(database["database_guid"])
        body["database"] = TdsDatabaseObservation(**database)
        for name, cls in (
            ("execution_owner", TdsAttemptOwnership),
            ("process", TdsProcessIdentity),
            ("schema_observation", TdsSchemaObservation),
            ("lock", TdsLockObservation),
        ):
            body[name] = _construct(cls, body[name])
        body["session"] = decode_session_continuity_identity(canonical_json_bytes(body["session"]))
        return TdsCoordinatorAuthority(**body)
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError("mssql_native.tds_authority_invalid") from None

"""Bounded CREATE models and wire contract, with no credential or SQL text encoding.

The component profile does not narrow general native/TDS admission. Keeping the
closed wire representation with its models avoids duplicate contract dependencies.
"""

from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from hashlib import sha256
from uuid import UUID

from dpone.contracts.mssql_tds_coordinator_authority import TdsDatabaseObservation, TdsSchemaObservation, identifier
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity, decode_session_identity, encode_session_identity
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, _hash, _integer
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid as _uuid
from dpone.contracts.strict_record import construct_record as _construct
from dpone.contracts.strict_record import record_shape as _shape
from dpone.contracts.strict_record import string_enum as _enum

CREATE_COMPONENT_MAX_COLUMNS = 100


class TdsCreateType(StrEnum):
    BIGINT = "bigint"
    FLOAT53 = "float53"
    NVARCHARMAX = "nvarcharmax"
    DATETIME2_6 = "datetime2_6"


@dataclass(frozen=True)
class TdsCreateColumn:
    name: str
    type: TdsCreateType
    nullable: bool

    def __post_init__(self) -> None:
        identifier(self.name)
        if type(self.type) is not TdsCreateType or type(self.nullable) is not bool:
            raise ValueError("mssql_native.tds_create_column_invalid")


def _columns(columns: tuple, cls: type) -> None:
    if type(columns) is not tuple or not 1 <= len(columns) <= CREATE_COMPONENT_MAX_COLUMNS:
        raise ValueError("mssql_native.tds_create_component_width_unsupported")
    if any(type(column) is not cls for column in columns) or len({column.name for column in columns}) != len(columns):
        raise ValueError("mssql_native.tds_create_columns_invalid")


@dataclass(frozen=True)
class TdsCreateRequest:
    parent: TdsAttemptIdentity
    object_nonce: UUID
    columns: tuple[TdsCreateColumn, ...]

    def __post_init__(self) -> None:
        if (
            type(self.parent) is not TdsAttemptIdentity
            or type(self.object_nonce) is not UUID
            or not self.object_nonce.int
        ):
            raise ValueError("mssql_native.tds_create_request_invalid")
        for name in (self.parent.database, self.parent.schema, self.parent.table):
            identifier(name)
        if self.parent.table.startswith("#"):
            raise ValueError("mssql_native.tds_create_temporary_table_forbidden")
        _columns(self.columns, TdsCreateColumn)


@dataclass(frozen=True)
class TdsCreateObservedColumn:
    ordinal: int
    name: str
    type: TdsCreateType
    nullable: bool
    max_length: int
    precision: int
    scale: int
    collation: str | None

    def __post_init__(self) -> None:
        TdsCreateColumn(self.name, self.type, self.nullable)
        _integer(self.ordinal, 1, CREATE_COMPONENT_MAX_COLUMNS)
        _integer(self.max_length, -1, 8000)
        _integer(self.precision, 0, 53)
        _integer(self.scale, 0, 38)
        profiles = {
            TdsCreateType.BIGINT: (8, 19, 0),
            TdsCreateType.FLOAT53: (8, 53, 0),
            TdsCreateType.NVARCHARMAX: (-1, 0, 0),
            TdsCreateType.DATETIME2_6: (8, 26, 6),
        }
        if (self.max_length, self.precision, self.scale) != profiles[self.type]:
            raise ValueError("mssql_native.tds_create_storage_profile_invalid")
        if self.type is TdsCreateType.NVARCHARMAX:
            if self.collation is None:
                raise ValueError("mssql_native.tds_create_collation_invalid")
            identifier(self.collation)
        elif self.collation is not None:
            raise ValueError("mssql_native.tds_create_collation_invalid")


@dataclass(frozen=True)
class TdsCreateEvidence:
    operation_sha256: str
    command_sha256: str
    grant_sha256: str
    authority_sha256: str
    session: TdsRemoteSessionIdentity
    database: TdsDatabaseObservation
    schema_observation: TdsSchemaObservation
    table_name: str
    object_id: int
    create_date: datetime
    owner_binding: str
    object_nonce: UUID
    columns: tuple[TdsCreateObservedColumn, ...]
    empty: bool = True
    committed: bool = True

    def __post_init__(self) -> None:
        for digest in (
            self.operation_sha256,
            self.command_sha256,
            self.grant_sha256,
            self.authority_sha256,
            self.owner_binding,
        ):
            _hash(digest)
        if (
            type(self.session) is not TdsRemoteSessionIdentity
            or type(self.database) is not TdsDatabaseObservation
            or type(self.schema_observation) is not TdsSchemaObservation
        ):
            raise ValueError("mssql_native.tds_create_evidence_invalid")
        identifier(self.table_name)
        _integer(self.object_id, 1, 2**31 - 1)
        if (
            type(self.create_date) is not datetime
            or self.create_date.tzinfo is not None
            or self.create_date < datetime(1900, 1, 1)
        ):
            raise ValueError("mssql_native.tds_create_date_invalid")
        if (
            type(self.object_nonce) is not UUID
            or not self.object_nonce.int
            or self.empty is not True
            or self.committed is not True
        ):
            raise ValueError("mssql_native.tds_create_evidence_invalid")
        _columns(self.columns, TdsCreateObservedColumn)
        if tuple(column.ordinal for column in self.columns) != tuple(range(1, len(self.columns) + 1)):
            raise ValueError("mssql_native.tds_create_column_order_invalid")


MAX_CREATE_BYTES = 131072
REQUEST_SCHEMA = "dpone.tds.create-request.v1"
EVIDENCE_SCHEMA = "dpone.tds.create-evidence.v1"


def _encode(value: TdsCreateRequest | TdsCreateEvidence) -> bytes:
    if type(value) not in (TdsCreateRequest, TdsCreateEvidence):
        raise ValueError("mssql_native.tds_create_record_invalid")
    body = asdict(value)
    body["object_nonce"] = str(value.object_nonce)
    body["schema"] = REQUEST_SCHEMA if type(value) is TdsCreateRequest else EVIDENCE_SCHEMA
    if isinstance(value, TdsCreateEvidence):
        body["session"] = strict_json_object(encode_session_identity(value.session))
        body["database"]["database_guid"] = str(value.database.database_guid)
        body["create_date"] = value.create_date.isoformat(timespec="microseconds")
    payload = canonical_json_bytes(body)
    if len(payload) > MAX_CREATE_BYTES:
        raise ValueError("mssql_native.tds_create_record_too_large")
    return payload


def encode_create_request(value: TdsCreateRequest) -> bytes:
    if type(value) is not TdsCreateRequest:
        raise ValueError("mssql_native.tds_create_record_invalid")
    return _encode(value)


def encode_create_evidence(value: TdsCreateEvidence) -> bytes:
    if type(value) is not TdsCreateEvidence:
        raise ValueError("mssql_native.tds_create_record_invalid")
    return _encode(value)


def create_command_digest(value: TdsCreateRequest) -> str:
    """Hash only the effect request, with no coordinator identity/self-reference."""
    return sha256(encode_create_request(value)).hexdigest()


def create_evidence_digest(value: TdsCreateEvidence) -> str:
    return sha256(encode_create_evidence(value)).hexdigest()


def _decode(payload: bytes, *, evidence: bool):
    try:
        if type(payload) is not bytes or len(payload) > MAX_CREATE_BYTES:
            raise ValueError
        original = strict_json_object(payload)
        body = dict(original)
        if body.pop("schema", None) != (EVIDENCE_SCHEMA if evidence else REQUEST_SCHEMA):
            raise ValueError
        cls = TdsCreateEvidence if evidence else TdsCreateRequest
        _shape(cls, body)
        body["object_nonce"] = _uuid(body["object_nonce"])
        if type(body["columns"]) is not list or not 1 <= len(body["columns"]) <= 100:
            raise ValueError
        columns = []
        for value in body["columns"]:
            value = dict(_shape(TdsCreateObservedColumn if evidence else TdsCreateColumn, value))
            value["type"] = _enum(TdsCreateType, value["type"])
            columns.append((TdsCreateObservedColumn if evidence else TdsCreateColumn)(**value))
        body["columns"] = tuple(columns)
        if evidence:
            database = dict(_shape(TdsDatabaseObservation, body["database"]))
            database["database_guid"] = _uuid(database["database_guid"])
            body["database"] = TdsDatabaseObservation(**database)
            body["schema_observation"] = _construct(TdsSchemaObservation, body["schema_observation"])
            body["session"] = decode_session_identity(canonical_json_bytes(body["session"]))
            if type(body["create_date"]) is not str:
                raise ValueError
            body["create_date"] = datetime.fromisoformat(body["create_date"])
        else:
            body["parent"] = _construct(TdsAttemptIdentity, body["parent"])
        result = cls(**body)
        if strict_json_object(_encode(result)) != original:
            raise ValueError
        return result
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError("mssql_native.tds_create_record_invalid") from None


def decode_create_request(payload: bytes) -> TdsCreateRequest:
    return _decode(payload, evidence=False)


def decode_create_evidence(payload: bytes) -> TdsCreateEvidence:
    return _decode(payload, evidence=True)

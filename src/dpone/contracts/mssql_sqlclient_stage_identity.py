"""Stable catalog fingerprint for an owned stage, without SQL authority claims.

The semantic fingerprint excludes transient operation/session observations and
artifact references. Producers must independently observe the current catalog;
constructing or decoding this record proves neither exclusion nor admission.
"""

from dataclasses import asdict, dataclass
from datetime import datetime
from hashlib import sha256
from uuid import UUID

from dpone.contracts.mssql_tds_coordinator_authority import TdsDatabaseObservation, TdsSchemaObservation, identifier
from dpone.contracts.mssql_tds_create import (
    CREATE_COMPONENT_MAX_COLUMNS,
    TdsCreateEvidence,
    TdsCreateObservedColumn,
    TdsCreateType,
    decode_create_evidence,
    encode_create_evidence,
)
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity
from dpone.contracts.mssql_tds_validation import _hash, _integer
from dpone.contracts.mssql_tds_worker import TdsObjectIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid, record_shape, string_enum

_SCHEMA = "dpone.sqlclient.stage-identity.v1"
_DOMAIN = b"dpone.sqlclient.stage-identity.v1\0"
_LIMIT = 131072
_ERROR = "mssql_native.sqlclient_stage_identity_invalid"


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientStageIdentity:
    """Same stage incarnation across CREATE and later independent observations.

    SQL timestamps have no timezone conversion. Names and collations preserve
    their exact case; this record does not guess a server's equality semantics.
    """

    database_guid: UUID
    database_id: int
    database_name: str
    schema_id: int
    schema_name: str
    table_name: str
    object_id: int
    create_date: datetime
    owner_binding: str
    object_nonce: UUID
    columns: tuple[TdsCreateObservedColumn, ...]
    schema: str = _SCHEMA

    def __post_init__(self) -> None:
        try:
            if type(self.schema) is not str or self.schema != _SCHEMA:
                raise ValueError(_ERROR)
            for value in (self.database_guid, self.object_nonce):
                if type(value) is not UUID or not value.int:
                    raise ValueError(_ERROR)
            for number in (self.database_id, self.schema_id, self.object_id):
                _integer(number, 1, 2**31 - 1)
            for name in (self.database_name, self.schema_name, self.table_name):
                identifier(name)
            if self.table_name.startswith("#"):
                raise ValueError(_ERROR)
            _hash(self.owner_binding)
            if (
                type(self.create_date) is not datetime
                or self.create_date.tzinfo is not None
                or self.create_date < datetime(1900, 1, 1)
                or type(self.columns) is not tuple
                or not 1 <= len(self.columns) <= CREATE_COMPONENT_MAX_COLUMNS
            ):
                raise ValueError(_ERROR)
            for ordinal, column in enumerate(self.columns, 1):
                if type(column) is not TdsCreateObservedColumn:
                    raise ValueError(_ERROR)
                # Reconstruct every nested scalar before hash/equality use.
                TdsCreateObservedColumn(**asdict(column))
                if column.ordinal != ordinal:
                    raise ValueError(_ERROR)
            if len({column.name for column in self.columns}) != len(self.columns):
                raise ValueError(_ERROR)
        except (ValueError, TypeError, UnicodeError, OverflowError, RecursionError):
            raise ValueError(_ERROR) from None


def encode_stage_identity(record: SqlClientStageIdentity) -> bytes:
    """Canonical bounded bytes; repeated validation precedes serialization."""
    try:
        if type(record) is not SqlClientStageIdentity:
            raise ValueError(_ERROR)
        record.__post_init__()
        body = asdict(record)
        body["database_guid"] = str(record.database_guid)
        body["object_nonce"] = str(record.object_nonce)
        body["create_date"] = record.create_date.isoformat(timespec="microseconds")
        payload = canonical_json_bytes(body)
        if len(payload) > _LIMIT:
            raise ValueError(_ERROR)
        return payload
    except (ValueError, TypeError, UnicodeError, OverflowError, RecursionError):
        raise ValueError(_ERROR) from None


def decode_stage_identity(payload: bytes) -> SqlClientStageIdentity:
    """Closed strict reconstruction; reject ambiguous or noncanonical bodies."""
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= _LIMIT:
            raise ValueError(_ERROR)
        data = record_shape(SqlClientStageIdentity, strict_json_object(payload))
        for key in ("database_guid", "object_nonce"):
            data[key] = canonical_uuid(data[key])
        if type(data["create_date"]) is not str:
            raise ValueError(_ERROR)
        data["create_date"] = datetime.fromisoformat(data["create_date"])
        if type(data["columns"]) is not list or not 1 <= len(data["columns"]) <= CREATE_COMPONENT_MAX_COLUMNS:
            raise ValueError(_ERROR)
        columns = []
        for raw in data["columns"]:
            column = record_shape(TdsCreateObservedColumn, raw)
            column["type"] = string_enum(TdsCreateType, column["type"])
            columns.append(TdsCreateObservedColumn(**column))
        data["columns"] = tuple(columns)
        result = SqlClientStageIdentity(**data)
        if encode_stage_identity(result) != payload:
            raise ValueError(_ERROR)
        return result
    except (ValueError, TypeError, UnicodeError, OverflowError, RecursionError):
        raise ValueError(_ERROR) from None


def stage_identity_from_create(evidence: TdsCreateEvidence) -> SqlClientStageIdentity:
    """Project validated CREATE fields without asserting current catalog facts.

    Do not construct a fake CREATE result for fresh observations: an observer
    builds SqlClientStageIdentity directly from its independently observed facts.
    """
    try:
        if type(evidence) is not TdsCreateEvidence:
            raise ValueError(_ERROR)
        evidence.__post_init__()
        # Wire encoding can normalize enum/UUID strings or bytearrays. Require
        # exact original nested types before any such representation change.
        TdsDatabaseObservation(**asdict(evidence.database))
        TdsSchemaObservation(**asdict(evidence.schema_observation))
        TdsRemoteSessionIdentity(**asdict(evidence.session))
        for column in evidence.columns:
            TdsCreateObservedColumn(**asdict(column))
        original = decode_create_evidence(encode_create_evidence(evidence))
        return SqlClientStageIdentity(
            database_guid=original.database.database_guid,
            database_id=original.database.database_id,
            database_name=original.database.name,
            schema_id=original.schema_observation.schema_id,
            schema_name=original.schema_observation.name,
            table_name=original.table_name,
            object_id=original.object_id,
            create_date=original.create_date,
            owner_binding=original.owner_binding,
            object_nonce=original.object_nonce,
            columns=original.columns,
        )
    except (ValueError, TypeError, UnicodeError, OverflowError, RecursionError):
        raise ValueError(_ERROR) from None


def stage_fingerprint(record: SqlClientStageIdentity) -> str:
    """Semantic catalog identity; not a persisted artifact's byte SHA256."""
    return sha256(_DOMAIN + encode_stage_identity(record)).hexdigest()


def stage_object_identity(record: SqlClientStageIdentity) -> TdsObjectIdentity:
    """Existing object DTO with a canonical producer-defined fingerprint."""
    fingerprint = stage_fingerprint(record)
    return TdsObjectIdentity(record.object_id, fingerprint)

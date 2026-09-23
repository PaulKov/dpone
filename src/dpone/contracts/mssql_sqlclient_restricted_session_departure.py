"""Truthful P9 departure contract and codec for a restricted session."""

from dataclasses import asdict, dataclass, fields
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
    SqlClientTransportAuthority,
)
from dpone.contracts.mssql_sqlclient_observer_incarnation import (
    _FAILURES,
    SqlClientObserverIncarnation,
    _context_records,
    _record,
    observer_incarnation_digest,
)
from dpone.contracts.mssql_sqlclient_observer_incarnation_codec import (
    _decode_timestamp,
    decode_observer_incarnation,
    encode_observer_incarnation,
)
from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
from dpone.contracts.mssql_tds_coordinator_authority import TdsDatabaseObservation
from dpone.contracts.mssql_tds_session import (
    TdsRestrictedRemoteSessionIdentity,
    coordinator_authority_digest,
    decode_restricted_session_identity,
    encode_restricted_session_identity,
)
from dpone.contracts.mssql_tds_validation import _hash, _integer
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid, construct_record, record_shape, string_enum

ERROR = "mssql_native.sqlclient_restricted_session_departure_invalid"


def _copy(value: object, kind: type) -> None:
    if type(value) is not kind:
        raise ValueError(ERROR)
    kind(**{field.name: getattr(value, field.name) for field in fields(kind)})


def validate_restricted_departure_inputs(
    original: TdsRestrictedRemoteSessionIdentity,
    database: TdsDatabaseObservation,
    admission: SqlClientObserverAdmission,
    principal: SqlClientDatabasePrincipal,
) -> None:
    """Bind restricted identity authority without inventing a server UUID."""
    try:
        for value, kind in (
            (original, TdsRestrictedRemoteSessionIdentity),
            (database, TdsDatabaseObservation),
            (admission, SqlClientObserverAdmission),
            (principal, SqlClientDatabasePrincipal),
        ):
            _copy(value, kind)
        _context_records(admission)
        server, db, login = admission.server, admission.database, admission.login
        if (database.name, database.database_id, str(database.database_guid)) != (
            db.database_name,
            db.database_id,
            db.database_guid,
        ):
            raise ValueError
        digest = coordinator_authority_digest(
            (
                server.server_name,
                server.machine_name,
                server.instance_name,
                server.physical_machine_name,
                db.database_name,
                db.database_id,
                UUID(db.database_guid),
                login.name,
                bytes.fromhex(login.sid),
                login.original_name,
                bytes.fromhex(login.original_sid),
                principal.name,
                principal.principal_id,
                bytes.fromhex(principal.sid),
            )
        )
        if digest != original.authority_sha256:
            raise ValueError
    except _FAILURES:
        raise ValueError(ERROR) from None


class RestrictedDepartureSampleKind(StrEnum):
    CONNECTIONS = "connections"
    SESSIONS = "sessions"
    REQUESTS = "requests"
    TRANSACTIONS = "transactions"


@dataclass(frozen=True, slots=True, kw_only=True)
class RestrictedObserverRequest:
    connection_id: UUID
    session_id: int
    request_id: int
    start_time: datetime

    def __post_init__(self) -> None:
        from dpone.contracts.mssql_sqlclient_observer_incarnation import _connection, _timestamp

        try:
            _connection(self.connection_id)
            _integer(self.session_id, 1, 32767)
            _integer(self.request_id, 0, 2**31 - 1)
            _timestamp(self.start_time)
        except _FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class RestrictedDepartureSample:
    kind: RestrictedDepartureSampleKind
    raw_count: int
    own_count: int
    original_epoch_count: int | None
    request: RestrictedObserverRequest | None
    before_sha256: str
    after_sha256: str

    def __post_init__(self) -> None:
        try:
            if type(self.kind) is not RestrictedDepartureSampleKind:
                raise ValueError
            _integer(self.raw_count, 0, 2**63 - 1)
            _integer(self.own_count, 0, 1)
            if self.own_count > self.raw_count:
                raise ValueError
            if self.kind is RestrictedDepartureSampleKind.SESSIONS:
                _integer(self.original_epoch_count, 0, self.raw_count)
            elif self.original_epoch_count is not None:
                raise ValueError
            if self.kind is RestrictedDepartureSampleKind.REQUESTS and self.own_count == 1:
                _record(self.request, RestrictedObserverRequest)
            elif self.request is not None:
                raise ValueError
            _hash(self.before_sha256)
            _hash(self.after_sha256)
        except _FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class RestrictedSessionDeparture:
    """Six guarded SPID partitions with exact old login-epoch absence."""

    original: TdsRestrictedRemoteSessionIdentity
    database: TdsDatabaseObservation
    admission: SqlClientObserverAdmission
    principal: SqlClientDatabasePrincipal
    observer: SqlClientObserverIncarnation
    samples: tuple[RestrictedDepartureSample, ...]

    def __post_init__(self) -> None:
        try:
            validate_restricted_departure_inputs(self.original, self.database, self.admission, self.principal)
            _record(self.observer, SqlClientObserverIncarnation)
            if type(self.samples) is not tuple or len(self.samples) != 6:
                raise ValueError
            for sample in self.samples:
                _record(sample, RestrictedDepartureSample)
            own = self.observer
            if (
                own.authority.server != self.admission.server
                or own.authority.database != self.admission.database
                or own.login_time <= self.original.login_time
            ):
                raise ValueError
            kind = RestrictedDepartureSampleKind
            if tuple(sample.kind for sample in self.samples) != (
                kind.CONNECTIONS,
                kind.SESSIONS,
                kind.REQUESTS,
                kind.TRANSACTIONS,
                kind.CONNECTIONS,
                kind.SESSIONS,
            ):
                raise ValueError
            reused = int(own.session_id == self.original.session_id)
            for sample in self.samples:
                expected = 0 if sample.kind is kind.TRANSACTIONS else reused
                if sample.raw_count != expected or sample.own_count != expected:
                    raise ValueError
                if sample.kind is kind.SESSIONS and sample.original_epoch_count != 0:
                    raise ValueError
                request = sample.request
                if request is not None and (
                    request.connection_id != own.connection_id
                    or request.session_id != own.session_id
                    or request.start_time < own.login_time
                ):
                    raise ValueError
            digest = observer_incarnation_digest(own)
            if any(sample.before_sha256 != digest or sample.after_sha256 != digest for sample in self.samples):
                raise ValueError
        except _FAILURES:
            raise ValueError(ERROR) from None


LIMIT = 16384


SCHEMA = "dpone.sqlclient.restricted-session-departure.v1"


def encode_restricted_session_departure(value: RestrictedSessionDeparture) -> bytes:
    try:
        _record(value, RestrictedSessionDeparture)
        database = asdict(value.database)
        database["database_guid"] = str(value.database.database_guid)
        samples = []
        for sample in value.samples:
            item = asdict(sample)
            item["kind"] = sample.kind.value
            if sample.request is not None:
                item["request"]["connection_id"] = str(sample.request.connection_id)
                item["request"]["start_time"] = sample.request.start_time.isoformat(timespec="microseconds")
            samples.append(item)
        payload = canonical_json_bytes(
            {
                "schema": SCHEMA,
                "original": strict_json_object(encode_restricted_session_identity(value.original)),
                "database": database,
                "admission": asdict(value.admission),
                "principal": asdict(value.principal),
                "observer": strict_json_object(encode_observer_incarnation(value.observer)),
                "samples": samples,
            }
        )
        if not 0 < len(payload) <= LIMIT:
            raise ValueError
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_restricted_session_departure(payload: bytes) -> RestrictedSessionDeparture:
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= LIMIT:
            raise ValueError
        data = strict_json_object(payload)
        if data.pop("schema", None) != SCHEMA:
            raise ValueError
        data = record_shape(RestrictedSessionDeparture, data)
        data["original"] = decode_restricted_session_identity(canonical_json_bytes(data["original"]))
        database = record_shape(TdsDatabaseObservation, data["database"])
        database["database_guid"] = canonical_uuid(database["database_guid"])
        data["database"] = TdsDatabaseObservation(**database)
        admission = record_shape(SqlClientObserverAdmission, data["admission"])
        for name, kind in (
            ("server", SqlClientServerAuthority),
            ("database", SqlClientDatabaseAuthority),
            ("login", SqlClientLoginAuthority),
            ("transport", SqlClientTransportAuthority),
        ):
            admission[name] = construct_record(kind, admission[name])
        data["admission"] = SqlClientObserverAdmission(**admission)
        data["principal"] = construct_record(SqlClientDatabasePrincipal, data["principal"])
        data["observer"] = decode_observer_incarnation(canonical_json_bytes(data["observer"]))
        if type(data["samples"]) is not list or len(data["samples"]) != 6:
            raise ValueError
        samples = []
        for raw in data["samples"]:
            item = record_shape(RestrictedDepartureSample, raw)
            item["kind"] = string_enum(RestrictedDepartureSampleKind, item["kind"])
            if item["request"] is not None:
                request = record_shape(RestrictedObserverRequest, item["request"])
                request["connection_id"] = canonical_uuid(request["connection_id"])
                request["start_time"] = _decode_timestamp(request["start_time"])
                item["request"] = RestrictedObserverRequest(**request)
            samples.append(RestrictedDepartureSample(**item))
        data["samples"] = tuple(samples)
        result = RestrictedSessionDeparture(**data)
        if encode_restricted_session_departure(result) != payload:
            raise ValueError
        return result
    except _FAILURES:
        raise ValueError(ERROR) from None

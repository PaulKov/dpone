"""P10 writer-session departure contract and codec."""

from dataclasses import asdict, dataclass

from dpone.contracts.mssql_sqlclient_create_departure_v2 import (
    SqlClientDepartureSampleKind,
    SqlClientDepartureSampleV2,
    SqlClientObserverRequestV2,
)
from dpone.contracts.mssql_sqlclient_create_departure_v2 import (
    SqlClientDepartureSampleKind as Kind,
)
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientPrincipalResolution,
    SqlClientServerAuthority,
    SqlClientSessionAuthority,
    SqlClientTransportAuthority,
    session_authority_digest,
)
from dpone.contracts.mssql_sqlclient_observer_incarnation import (
    SqlClientObserverIncarnation,
    observer_incarnation_digest,
)
from dpone.contracts.mssql_sqlclient_observer_incarnation_codec import (
    _decode_timestamp,
    decode_observer_incarnation,
    encode_observer_incarnation,
)
from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity, decode_session_identity, encode_session_identity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid, construct_record, record_shape, string_enum

ERROR = "mssql_native.sqlclient_writer_session_departure_invalid"


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientWriterSessionDeparture:
    """Six guarded partitions proving absence of the observed P10 session."""

    original: TdsRemoteSessionIdentity
    writer_admission: SqlClientObserverAdmission
    writer_authority: SqlClientSessionAuthority
    principal: SqlClientDatabasePrincipal
    observer: SqlClientObserverIncarnation
    samples: tuple[SqlClientDepartureSampleV2, ...]

    def __post_init__(self) -> None:
        try:
            for value, kind in (
                (self.original, TdsRemoteSessionIdentity),
                (self.writer_admission, SqlClientObserverAdmission),
                (self.writer_authority, SqlClientSessionAuthority),
                (self.principal, SqlClientDatabasePrincipal),
                (self.observer, SqlClientObserverIncarnation),
            ):
                if type(value) is not kind:
                    raise ValueError
                value.__post_init__()
            authority = self.writer_authority
            if (
                self.original.authority_sha256 != session_authority_digest(authority)
                or (authority.server, authority.database, authority.login, authority.transport)
                != (
                    self.writer_admission.server,
                    self.writer_admission.database,
                    self.writer_admission.login,
                    self.writer_admission.transport,
                )
                or authority.principal_resolution.principal != self.principal
                or self.observer.authority.server != authority.server
                or self.observer.authority.database != authority.database
                or self.observer.connection_id == self.original.connection_id
                or self.observer.connect_time <= self.original.connect_time
                or self.observer.login_time <= self.original.login_time
                or type(self.samples) is not tuple
                or len(self.samples) != 6
            ):
                raise ValueError
            kinds = (Kind.CONNECTIONS, Kind.SESSIONS, Kind.REQUESTS, Kind.TRANSACTIONS, Kind.CONNECTIONS, Kind.SESSIONS)
            if tuple(sample.kind for sample in self.samples) != kinds:
                raise ValueError
            reused = int(self.observer.session_id == self.original.session_id)
            digest = observer_incarnation_digest(self.observer)
            for sample in self.samples:
                if type(sample) is not SqlClientDepartureSampleV2:
                    raise ValueError
                sample.__post_init__()
                expected = 0 if sample.kind is Kind.TRANSACTIONS else reused
                if sample.raw_count != expected or sample.own_count != expected:
                    raise ValueError
                if sample.kind in (Kind.CONNECTIONS, Kind.REQUESTS) and sample.original_uuid_count != 0:
                    raise ValueError
                if sample.before_sha256 != digest or sample.after_sha256 != digest:
                    raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise ValueError(ERROR) from None


SCHEMA = "dpone.sqlclient.writer-session-departure.v1"


LIMIT = 32768


def _admission(raw: dict) -> SqlClientObserverAdmission:
    value = record_shape(SqlClientObserverAdmission, raw)
    for name, kind in (
        ("server", SqlClientServerAuthority),
        ("database", SqlClientDatabaseAuthority),
        ("login", SqlClientLoginAuthority),
        ("transport", SqlClientTransportAuthority),
    ):
        value[name] = construct_record(kind, value[name])
    return SqlClientObserverAdmission(**value)


def _authority(raw: dict) -> SqlClientSessionAuthority:
    value = record_shape(SqlClientSessionAuthority, raw)
    for name, kind in (
        ("server", SqlClientServerAuthority),
        ("database", SqlClientDatabaseAuthority),
        ("login", SqlClientLoginAuthority),
        ("transport", SqlClientTransportAuthority),
        ("principal_resolution", SqlClientPrincipalResolution),
    ):
        value[name] = construct_record(kind, value[name])
    return SqlClientSessionAuthority(**value)


def encode_writer_session_departure(value: SqlClientWriterSessionDeparture) -> bytes:
    try:
        if type(value) is not SqlClientWriterSessionDeparture:
            raise ValueError
        value.__post_init__()
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
                "original": strict_json_object(encode_session_identity(value.original)),
                "writer_admission": asdict(value.writer_admission),
                "writer_authority": asdict(value.writer_authority),
                "principal": asdict(value.principal),
                "observer": strict_json_object(encode_observer_incarnation(value.observer)),
                "samples": samples,
            }
        )
        if not 0 < len(payload) <= LIMIT:
            raise ValueError
        return payload
    except (ValueError, TypeError, AttributeError, OverflowError, RecursionError):
        raise ValueError(ERROR) from None


def decode_writer_session_departure(payload: bytes) -> SqlClientWriterSessionDeparture:
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= LIMIT:
            raise ValueError
        body = strict_json_object(payload)
        if body.pop("schema", None) != SCHEMA:
            raise ValueError
        body = record_shape(SqlClientWriterSessionDeparture, body)
        body["original"] = decode_session_identity(canonical_json_bytes(body["original"]))
        body["writer_admission"] = _admission(body["writer_admission"])
        body["writer_authority"] = _authority(body["writer_authority"])
        body["principal"] = construct_record(SqlClientDatabasePrincipal, body["principal"])
        body["observer"] = decode_observer_incarnation(canonical_json_bytes(body["observer"]))
        if type(body["samples"]) is not list or len(body["samples"]) != 6:
            raise ValueError
        samples = []
        for raw in body["samples"]:
            item = record_shape(SqlClientDepartureSampleV2, raw)
            item["kind"] = string_enum(SqlClientDepartureSampleKind, item["kind"])
            if item["request"] is not None:
                request = record_shape(SqlClientObserverRequestV2, item["request"])
                request["connection_id"] = canonical_uuid(request["connection_id"])
                request["start_time"] = _decode_timestamp(request["start_time"])
                item["request"] = SqlClientObserverRequestV2(**request)
            samples.append(SqlClientDepartureSampleV2(**item))
        body["samples"] = tuple(samples)
        result = SqlClientWriterSessionDeparture(**body)
        if encode_writer_session_departure(result) != payload:
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError):
        raise ValueError(ERROR) from None

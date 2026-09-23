"""Private departure envelope: ephemeral material, never durable evidence.

The parent acknowledges only the nonsecret request digest before delivery. This
codec does not authenticate those ACKs or supply binary admission. Independently
held startup/admission/deadlines must be provided by the owning composition root.
"""

from dataclasses import asdict, dataclass, field, fields

from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_sqlclient_observe_departure import (
    SqlClientObserveDepartureRequest,
    validate_observe_departure_binding,
)
from dpone.contracts.mssql_tds_api import (
    SqlClientDepartureRequest,
    SqlClientDepartureRequestV2,
    canonical_json_bytes,
    construct_record,
    decode_departure_request,
    decode_departure_request_v2,
    decode_observe_departure_request,
    encode_departure_request,
    encode_departure_request_v2,
    encode_observe_departure_request,
    record_shape,
    strict_json_object,
    validate_departure_request_binding,
    validate_departure_request_binding_v2,
)
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup

_LIMIT = 196608
_ERROR = "mssql_native.sqlclient_departure_credentials_invalid"
_FAILURES = (ValueError, TypeError, OverflowError, RecursionError, UnicodeError, AttributeError)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientDepartureCredentials:
    """Observer credentials may differ from creator credentials; database may not."""

    request: SqlClientDepartureRequest
    connection_material: TdsConnectionMaterial = field(repr=False)
    schema: str = "dpone.sqlclient.departure-credentials.v1"

    def __post_init__(self) -> None:
        try:
            if (
                type(self.schema) is not str
                or self.schema != "dpone.sqlclient.departure-credentials.v1"
                or type(self.request) is not SqlClientDepartureRequest
                or type(self.connection_material) is not TdsConnectionMaterial
            ):
                raise ValueError
            self.request.__post_init__()
            # Preserve scalar types before JSON serialization could normalize them.
            TdsConnectionMaterial(
                **{f.name: getattr(self.connection_material, f.name) for f in fields(TdsConnectionMaterial)}
            )
            if self.connection_material.database != self.request.plan.database.name:
                raise ValueError
        except _FAILURES:
            raise ValueError(_ERROR) from None


def encode_departure_credentials(value: SqlClientDepartureCredentials) -> bytes:
    """Return private pipe bytes only; no digest, storage or diagnostic operation."""
    try:
        if type(value) is not SqlClientDepartureCredentials:
            raise ValueError
        value.__post_init__()
        body = canonical_json_bytes(
            dict(
                schema=value.schema,
                request=strict_json_object(encode_departure_request(value.request)),
                connection_material=asdict(value.connection_material),
            )
        )
        if not 0 < len(body) <= _LIMIT:
            raise ValueError
        return body
    except _FAILURES:
        raise ValueError(_ERROR) from None


def decode_departure_credentials(
    payload: bytes,
    *,
    startup: TdsCoordinatorStartup,
    admission_sha256: str,
    startup_deadline: float,
    operation_deadline: float,
    max_address_space_bytes: int,
) -> SqlClientDepartureCredentials:
    """Bind to separately held launch facts; profile is fixed by admission digest."""
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= _LIMIT:
            raise ValueError
        data = record_shape(SqlClientDepartureCredentials, strict_json_object(payload))
        data["request"] = decode_departure_request(canonical_json_bytes(data["request"]))
        data["connection_material"] = construct_record(TdsConnectionMaterial, data["connection_material"])
        value = SqlClientDepartureCredentials(**data)
        validate_departure_request_binding(
            value.request,
            startup=startup,
            admission_sha256=admission_sha256,
            startup_deadline=startup_deadline,
            operation_deadline=operation_deadline,
            max_address_space_bytes=max_address_space_bytes,
        )
        if encode_departure_credentials(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(_ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientDepartureCredentialsV2:
    """Observer credentials may differ from creator credentials; database may not."""

    request: SqlClientDepartureRequestV2
    connection_material: TdsConnectionMaterial = field(repr=False)
    schema: str = "dpone.sqlclient.departure-credentials.v2"

    def __post_init__(self) -> None:
        try:
            if (
                type(self.schema) is not str
                or self.schema != "dpone.sqlclient.departure-credentials.v2"
                or type(self.request) is not SqlClientDepartureRequestV2
                or type(self.connection_material) is not TdsConnectionMaterial
            ):
                raise ValueError
            self.request.__post_init__()
            # Preserve scalar types before JSON serialization could normalize them.
            TdsConnectionMaterial(
                **{f.name: getattr(self.connection_material, f.name) for f in fields(TdsConnectionMaterial)}
            )
            if (
                self.connection_material.database != self.request.plan.database.name
                or self.connection_material.username != self.request.plan.observer_admission.login.name
            ):
                raise ValueError
        except _FAILURES:
            raise ValueError(_ERROR) from None


def encode_departure_credentials_v2(value: SqlClientDepartureCredentialsV2) -> bytes:
    """Return private pipe bytes only; no digest, storage or diagnostic operation."""
    try:
        if type(value) is not SqlClientDepartureCredentialsV2:
            raise ValueError
        value.__post_init__()
        body = canonical_json_bytes(
            dict(
                schema=value.schema,
                request=strict_json_object(encode_departure_request_v2(value.request)),
                connection_material=asdict(value.connection_material),
            )
        )
        if not 0 < len(body) <= _LIMIT:
            raise ValueError
        return body
    except _FAILURES:
        raise ValueError(_ERROR) from None


def decode_departure_credentials_v2(
    payload: bytes,
    *,
    startup: TdsCoordinatorStartup,
    admission_sha256: str,
    startup_deadline: float,
    operation_deadline: float,
    max_address_space_bytes: int,
) -> SqlClientDepartureCredentialsV2:
    """Bind to separately held launch facts; profile is fixed by admission digest."""
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= _LIMIT:
            raise ValueError
        data = record_shape(SqlClientDepartureCredentialsV2, strict_json_object(payload))
        data["request"] = decode_departure_request_v2(canonical_json_bytes(data["request"]))
        data["connection_material"] = construct_record(TdsConnectionMaterial, data["connection_material"])
        value = SqlClientDepartureCredentialsV2(**data)
        validate_departure_request_binding_v2(
            value.request,
            startup=startup,
            admission_sha256=admission_sha256,
            startup_deadline=startup_deadline,
            operation_deadline=operation_deadline,
            max_address_space_bytes=max_address_space_bytes,
        )
        if encode_departure_credentials_v2(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(_ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientObserveDepartureCredentials:
    """Private verifier material is never a durable observation or diagnostic value."""

    request: SqlClientObserveDepartureRequest
    connection_material: TdsConnectionMaterial = field(repr=False)
    schema: str = "dpone.sqlclient.observe-departure-credentials.v1"

    def __post_init__(self) -> None:
        try:
            if (
                type(self.schema) is not str
                or self.schema != "dpone.sqlclient.observe-departure-credentials.v1"
                or type(self.request) is not SqlClientObserveDepartureRequest
                or type(self.connection_material) is not TdsConnectionMaterial
            ):
                raise ValueError
            self.request.__post_init__()
            # Preserve scalar types before JSON serialization could normalize them.
            TdsConnectionMaterial(
                **{f.name: getattr(self.connection_material, f.name) for f in fields(TdsConnectionMaterial)}
            )
            if (
                self.connection_material.database != self.request.plan.database.name
                or self.connection_material.username != self.request.plan.observer_admission.login.name
            ):
                raise ValueError
        except _FAILURES:
            raise ValueError(_ERROR) from None


def encode_observe_departure_credentials(value: SqlClientObserveDepartureCredentials) -> bytes:
    """Return private pipe bytes only; no digest, storage or diagnostic operation."""
    try:
        if type(value) is not SqlClientObserveDepartureCredentials:
            raise ValueError
        value.__post_init__()
        body = canonical_json_bytes(
            dict(
                schema=value.schema,
                request=strict_json_object(encode_observe_departure_request(value.request)),
                connection_material=asdict(value.connection_material),
            )
        )
        if not 0 < len(body) <= _LIMIT:
            raise ValueError
        return body
    except _FAILURES:
        raise ValueError(_ERROR) from None


def decode_observe_departure_credentials(
    payload: bytes,
    *,
    startup: TdsCoordinatorStartup,
    admission_sha256: str,
    startup_deadline: float,
    operation_deadline: float,
    max_address_space_bytes: int,
    observer_admission: SqlClientObserverAdmission,
) -> SqlClientObserveDepartureCredentials:
    """Bind launcher facts and explicit verifier expectation.

    Parents supply their independently retained admission. The child supplies the
    parent plan admission for consistency against actual SQL, not a second source
    of independent verifier configuration.
    """
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= _LIMIT:
            raise ValueError
        data = record_shape(SqlClientObserveDepartureCredentials, strict_json_object(payload))
        data["request"] = decode_observe_departure_request(canonical_json_bytes(data["request"]))
        data["connection_material"] = construct_record(TdsConnectionMaterial, data["connection_material"])
        value = SqlClientObserveDepartureCredentials(**data)
        validate_observe_departure_binding(
            value.request,
            startup=startup,
            admission_sha256=admission_sha256,
            startup_deadline=startup_deadline,
            operation_deadline=operation_deadline,
            max_address_space_bytes=max_address_space_bytes,
            observer_admission=observer_admission,
        )
        if encode_observe_departure_credentials(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(_ERROR) from None

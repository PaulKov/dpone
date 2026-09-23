"""Full observer identity/context commitments, without acquisition authentication.

One initial record and twelve independently acquired guards are required by the
future producer. A caller can fabricate matching digests; these pure records do
not establish physical connection continuity, SQL exclusion, or permission.
"""

from dataclasses import asdict, dataclass, fields
from datetime import datetime
from hashlib import sha256
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientPrincipalResolution,
    SqlClientServerAuthority,
    SqlClientSessionAuthority,
    SqlClientTransportAuthority,
)
from dpone.contracts.mssql_tds_validation import _integer
from dpone.contracts.strict_json import canonical_json_bytes

_ERROR = "mssql_native.sqlclient_observer_incarnation_invalid"
_FAILURES = (ValueError, TypeError, OverflowError, RecursionError, UnicodeError, AttributeError)
_LIMIT = 16384
_SCHEMA = "dpone.sqlclient.observer-incarnation.v2"
_DOMAIN = b"dpone.sqlclient.observer-incarnation.v2\0"


def _record(value: Any, cls: type) -> None:
    """Reconstruct exact original fields, never a serialized or coerced copy."""
    if type(value) is not cls:
        raise ValueError(_ERROR)
    cls(**{f.name: getattr(value, f.name) for f in fields(cls)})


def _timestamp(value: datetime) -> None:
    if type(value) is not datetime or value.tzinfo is not None or value <= datetime(1900, 1, 1):
        raise ValueError(_ERROR)


def _connection(value: UUID) -> None:
    if type(value) is not UUID or not value.int:
        raise ValueError(_ERROR)


def _context_records(value: SqlClientObserverAdmission) -> None:
    """Reject text subclasses before legacy constructors use asdict internally."""
    if type(value) not in (SqlClientObserverAdmission, SqlClientSessionAuthority):
        raise ValueError(_ERROR)
    for item, cls in (
        (value.server, SqlClientServerAuthority),
        (value.database, SqlClientDatabaseAuthority),
        (value.login, SqlClientLoginAuthority),
        (value.transport, SqlClientTransportAuthority),
    ):
        if cls in (SqlClientServerAuthority, SqlClientTransportAuthority):
            if type(item) is not cls or any(type(getattr(item, f.name)) is not str for f in fields(cls)):
                raise ValueError(_ERROR)
        _record(item, cls)


def _authority(value: SqlClientSessionAuthority) -> None:
    if type(value) is not SqlClientSessionAuthority:
        raise ValueError(_ERROR)
    _context_records(value)
    _record(value.principal_resolution, SqlClientPrincipalResolution)
    # Existing authority policy checks profile equality, so v2 also requires
    # its exact original string type before projection can normalize an alias.
    if type(value.profile) is not str:
        raise ValueError(_ERROR)
    _record(value, SqlClientSessionAuthority)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientDepartureVisibilityV2:
    """Actual raw permissions, including an explicitly nullable unused privilege."""

    server_major_version: int
    engine_edition: int
    view_server_state: int | None
    view_server_performance_state: int | None
    database_id: int

    def __post_init__(self) -> None:
        try:
            _integer(self.server_major_version, 13, 17)
            _integer(self.engine_edition, 2, 4)
            _integer(self.database_id, 1, 2**31 - 1)
            for value in (self.view_server_state, self.view_server_performance_state):
                if value is not None:
                    _integer(value, 0, 1)
            required = self.view_server_state if self.server_major_version <= 15 else self.view_server_performance_state
            if required != 1:
                raise ValueError
        except _FAILURES:
            raise ValueError(_ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientObserverIncarnation:
    """Stable own incarnation and complete actual initial SQL context.

    Request IDs/times intentionally live on R samples, since each statement is
    a different request. No nonce or original-creator authority is substituted.
    """

    connection_id: UUID
    session_id: int
    connect_time: datetime
    login_time: datetime
    parent_connection_id: None
    mars_child_count: int
    transaction_count: int
    xact_state: int
    authority: SqlClientSessionAuthority
    visibility: SqlClientDepartureVisibilityV2

    def __post_init__(self) -> None:
        try:
            _connection(self.connection_id)
            _integer(self.session_id, 1, 32767)
            _timestamp(self.connect_time)
            _timestamp(self.login_time)
            if self.parent_connection_id is not None or self.login_time < self.connect_time:
                raise ValueError
            for value in (self.mars_child_count, self.transaction_count, self.xact_state):
                _integer(value, 0, 0)
            _authority(self.authority)
            _record(self.visibility, SqlClientDepartureVisibilityV2)
            if self.visibility.database_id != self.authority.database.database_id:
                raise ValueError
        except _FAILURES:
            raise ValueError(_ERROR) from None


def _observer_projection(value: SqlClientObserverIncarnation) -> dict:
    """Sole canonical projection; validate originals before asdict or formatting."""
    _record(value, SqlClientObserverIncarnation)
    data = asdict(value)
    data.update(
        schema=_SCHEMA,
        connection_id=str(value.connection_id),
        connect_time=value.connect_time.isoformat(timespec="microseconds"),
        login_time=value.login_time.isoformat(timespec="microseconds"),
    )
    return data


def _observer_bytes(value: SqlClientObserverIncarnation) -> bytes:
    """Model-owned canonical body keeps digest and wire codec byte-identical."""
    try:
        body = canonical_json_bytes(_observer_projection(value))
        if not 0 < len(body) <= _LIMIT:
            raise ValueError
        return body
    except _FAILURES:
        raise ValueError(_ERROR) from None


def observer_incarnation_digest(value: SqlClientObserverIncarnation) -> str:
    """Hash validated full context with the v2 domain and its terminating NUL."""
    return sha256(_DOMAIN + _observer_bytes(value)).hexdigest()

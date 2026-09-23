"""Truthful raw/own departure partitions bound to twelve full-context guards.

These records validate consistency only. Independent physical acquisitions,
original CREATE/reap, helper authentication and durable ACKs remain execution
premises. v1 literal-zero sweeps are neither accepted nor synthesized here.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from dpone.contracts.mssql_sqlclient_create_departure import validate_create_departure_inputs
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_sqlclient_observer_incarnation import (
    _FAILURES,
    SqlClientObserverIncarnation,
    _connection,
    _context_records,
    _record,
    _timestamp,
    observer_incarnation_digest,
)
from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
from dpone.contracts.mssql_tds_coordinator_authority import TdsDatabaseObservation
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity
from dpone.contracts.mssql_tds_validation import _hash, _integer

_ERROR = "mssql_native.sqlclient_create_departure_v2_invalid"


class SqlClientDepartureSampleKind(StrEnum):
    """Four predicates, observed in the fixed C/S/R/T/C/S order."""

    CONNECTIONS = "connections"
    SESSIONS = "sessions"
    REQUESTS = "requests"
    TRANSACTIONS = "transactions"


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientObserverRequestV2:
    """The own R statement's actual request identity, without SQL text/handles."""

    connection_id: UUID
    session_id: int
    request_id: int
    start_time: datetime

    def __post_init__(self) -> None:
        try:
            _connection(self.connection_id)
            _integer(self.session_id, 1, 32767)
            _integer(self.request_id, 0, 2**31 - 1)
            _timestamp(self.start_time)
        except _FAILURES:
            raise ValueError(_ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientDepartureSampleV2:
    """One broad raw predicate count, its own partition and two guard digests."""

    kind: SqlClientDepartureSampleKind
    raw_count: int
    own_count: int
    original_uuid_count: int | None
    request: SqlClientObserverRequestV2 | None
    before_sha256: str
    after_sha256: str

    def __post_init__(self) -> None:
        try:
            if type(self.kind) is not SqlClientDepartureSampleKind:
                raise ValueError
            _integer(self.raw_count, 0, 2**63 - 1)
            _integer(self.own_count, 0, 1)
            if self.own_count > self.raw_count:
                raise ValueError
            if self.kind in (SqlClientDepartureSampleKind.CONNECTIONS, SqlClientDepartureSampleKind.REQUESTS):
                _integer(self.original_uuid_count, 0, self.raw_count)
            elif self.original_uuid_count is not None:
                raise ValueError
            if self.kind is SqlClientDepartureSampleKind.REQUESTS and self.own_count == 1:
                _record(self.request, SqlClientObserverRequestV2)
            elif self.request is not None:
                raise ValueError
            _hash(self.before_sha256)
            _hash(self.after_sha256)
        except _FAILURES:
            raise ValueError(_ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientCreateDepartureV2:
    """One complete own record and six distinct samples; no permission to mutate."""

    original: TdsRemoteSessionIdentity
    database: TdsDatabaseObservation
    admission: SqlClientObserverAdmission
    principal: SqlClientDatabasePrincipal
    observer: SqlClientObserverIncarnation
    samples: tuple[SqlClientDepartureSampleV2, ...]

    def __post_init__(self) -> None:
        try:
            # Validate all new original nested values before either creator or
            # own semantic hashing; byte projection cannot launder aliases.
            _record(self.observer, SqlClientObserverIncarnation)
            if type(self.samples) is not tuple or len(self.samples) != 6:
                raise ValueError
            for sample in self.samples:
                _record(sample, SqlClientDepartureSampleV2)
            _context_records(self.admission)
            validate_create_departure_inputs(self.original, self.database, self.admission, self.principal)
            own = self.observer
            if (
                own.authority.server != self.admission.server
                or own.authority.database != self.admission.database
                or own.connection_id == self.original.connection_id
                or own.connect_time <= self.original.connect_time
                or own.login_time <= self.original.login_time
            ):
                raise ValueError
            kind = SqlClientDepartureSampleKind
            if tuple(s.kind for s in self.samples) != (
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
                if sample.kind in (kind.CONNECTIONS, kind.REQUESTS) and sample.original_uuid_count != 0:
                    raise ValueError
                request = sample.request
                if request is not None and (
                    request.connection_id != own.connection_id
                    or request.session_id != own.session_id
                    or request.start_time < own.login_time
                ):
                    raise ValueError
            digest = observer_incarnation_digest(own)
            if any(s.before_sha256 != digest or s.after_sha256 != digest for s in self.samples):
                raise ValueError
        except _FAILURES:
            raise ValueError(_ERROR) from None

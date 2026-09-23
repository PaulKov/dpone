"""Management observation of least-privilege restricted-session departure."""

from collections.abc import Callable
from typing import Any

from dpone.adapters.mssql_sqlclient_observer import SqlClientObservationError, _Cursor
from dpone.adapters.mssql_sqlclient_observer_incarnation import capture_observer_incarnation
from dpone.adapters.mssql_sqlclient_restricted_session_departure_sql import (
    CONNECTIONS_SQL,
    REQUESTS_SQL,
    SESSIONS_SQL,
    TRANSACTIONS_SQL,
)
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_tds_api import (
    RestrictedDepartureSample,
    RestrictedDepartureSampleKind,
    RestrictedObserverRequest,
    RestrictedSessionDeparture,
    SqlClientDatabasePrincipal,
    TdsDatabaseObservation,
    TdsRestrictedRemoteSessionIdentity,
    _integer,
    observer_incarnation_digest,
    validate_restricted_departure_inputs,
)

ERROR = "mssql_native.sqlclient_restricted_session_departure_unavailable"


class SqlClientRestrictedSessionDepartureObserver:
    """Prove old login-epoch absence without applying a client GUID to DMVs."""

    def __init__(
        self,
        cursor: _Cursor,
        *,
        admission: SqlClientObserverAdmission,
        observer_admission: SqlClientObserverAdmission,
        operation_deadline_ns: int,
        monotonic_ns: Callable[[], int],
    ) -> None:
        from dpone.adapters.mssql_sqlclient_create_exclusion import SqlClientCreateExclusionObserver

        self._delegate = SqlClientCreateExclusionObserver(
            cursor,
            admission=admission,
            operation_deadline_ns=operation_deadline_ns,
            monotonic_ns=monotonic_ns,
        )
        if type(observer_admission) is not SqlClientObserverAdmission or (
            observer_admission.server != admission.server or observer_admission.database != admission.database
        ):
            raise ValueError(ERROR)
        self._observer_admission = observer_admission

    def observe(
        self,
        *,
        original: TdsRestrictedRemoteSessionIdentity,
        database: TdsDatabaseObservation,
        principal: SqlClientDatabasePrincipal,
    ) -> RestrictedSessionDeparture:
        delegate = self._delegate
        delegate._begin()
        try:
            validate_restricted_departure_inputs(original, database, delegate._admission, principal)
            delegate._creator(principal)
            observer = capture_observer_incarnation(delegate._query, admission=self._observer_admission)
            digest = observer_incarnation_digest(observer)
            old, own = original.session_id, observer.session_id
            queries = (
                (
                    RestrictedDepartureSampleKind.CONNECTIONS,
                    CONNECTIONS_SQL,
                    (old, str(observer.connection_id), own, observer.connect_time),
                ),
                (
                    RestrictedDepartureSampleKind.SESSIONS,
                    SESSIONS_SQL,
                    (old, own, observer.login_time, old, original.login_time),
                ),
                (RestrictedDepartureSampleKind.REQUESTS, REQUESTS_SQL, (old, str(observer.connection_id), own)),
                (RestrictedDepartureSampleKind.TRANSACTIONS, TRANSACTIONS_SQL, (old,)),
                (
                    RestrictedDepartureSampleKind.CONNECTIONS,
                    CONNECTIONS_SQL,
                    (old, str(observer.connection_id), own, observer.connect_time),
                ),
                (
                    RestrictedDepartureSampleKind.SESSIONS,
                    SESSIONS_SQL,
                    (old, own, observer.login_time, old, original.login_time),
                ),
            )
            samples = []
            for kind, sql, parameters in queries:
                before = self._guard(digest)
                rows = delegate._query(sql, *parameters)
                after = self._guard(digest)
                samples.append(self._sample(kind, rows, before, after))
            delegate._creator(principal)
            result = RestrictedSessionDeparture(
                original=original,
                database=database,
                admission=delegate._admission,
                principal=principal,
                observer=observer,
                samples=tuple(samples),
            )
            delegate._current()
            delegate._finish()
            return result
        except BaseException as error:
            delegate._fail()
            if not isinstance(error, Exception):
                raise
            raise SqlClientObservationError(ERROR) from None

    def _guard(self, expected: str) -> str:
        actual = capture_observer_incarnation(self._delegate._query, admission=self._observer_admission)
        digest = observer_incarnation_digest(actual)
        if digest != expected:
            raise ValueError(ERROR)
        return digest

    def _sample(
        self,
        kind: RestrictedDepartureSampleKind,
        rows: list[Any],
        before: str,
        after: str,
    ) -> RestrictedDepartureSample:
        widths = {
            RestrictedDepartureSampleKind.CONNECTIONS: 2,
            RestrictedDepartureSampleKind.SESSIONS: 3,
            RestrictedDepartureSampleKind.REQUESTS: 6,
            RestrictedDepartureSampleKind.TRANSACTIONS: 1,
        }
        if len(rows) != 1 or len(rows[0]) != widths[kind]:
            raise ValueError(ERROR)
        row = rows[0]
        _integer(row[0], 0, 2**63 - 1)
        own = 0 if kind is RestrictedDepartureSampleKind.TRANSACTIONS else row[1]
        _integer(own, 0, 1)
        epoch = row[2] if kind is RestrictedDepartureSampleKind.SESSIONS else None
        if epoch is not None:
            _integer(epoch, 0, row[0])
        request = None
        if kind is RestrictedDepartureSampleKind.REQUESTS:
            if own == 0 and any(value is not None for value in row[2:]):
                raise ValueError(ERROR)
            if own == 1:
                request = RestrictedObserverRequest(
                    connection_id=row[2], session_id=row[3], request_id=row[4], start_time=row[5]
                )
        return RestrictedDepartureSample(
            kind=kind,
            raw_count=row[0],
            own_count=own,
            original_epoch_count=epoch,
            request=request,
            before_sha256=before,
            after_sha256=after,
        )

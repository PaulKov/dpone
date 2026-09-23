"""Explicit v2 departure observation; no entrypoint or v1 behavior is changed.

Thirteen independently acquired incarnations bracket six broad raw partitions.
Same-handle execution and the immutable deadline use the existing lifecycle;
SQL failures poison it permanently. Each capture samples transaction state
at batch entry, then metadata in the SELECT: 27 execute calls, 40 statements.
This is sequential sampling, not an atomic snapshot, parent-origin or route proof.
"""

from collections.abc import Callable
from typing import Any, Protocol

from dpone.adapters.mssql_sqlclient_create_exclusion import SqlClientCreateExclusionObserver
from dpone.adapters.mssql_sqlclient_observer import SqlClientObservationError, _Cursor
from dpone.adapters.mssql_sqlclient_observer_incarnation import capture_observer_incarnation
from dpone.contracts.mssql_sqlclient_create_departure_v2 import (
    SqlClientDepartureSampleKind as Kind,
)
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_tds_api import (
    SqlClientCreateDepartureV2,
    SqlClientDatabasePrincipal,
    SqlClientDepartureSampleV2,
    SqlClientObserverRequestV2,
    TdsDatabaseObservation,
    TdsRemoteSessionIdentity,
    _integer,
    observer_incarnation_digest,
    validate_create_departure_inputs,
)


def _partition(rows: list[Any], kind: Kind, before: str, after: str) -> SqlClientDepartureSampleV2:
    width = {Kind.CONNECTIONS: 3, Kind.SESSIONS: 2, Kind.REQUESTS: 7, Kind.TRANSACTIONS: 1}[kind]
    if len(rows) != 1 or len(rows[0]) != width:
        raise ValueError("departure_partition_invalid")
    row = rows[0]
    _integer(row[0], 0, 2**63 - 1)
    own = 0 if kind is Kind.TRANSACTIONS else row[1]
    _integer(own, 0, 1)
    original = row[2] if kind in (Kind.CONNECTIONS, Kind.REQUESTS) else None
    if original is not None:
        _integer(original, 0, row[0])
    request = None
    if kind is Kind.REQUESTS:
        if own == 0:
            if any(value is not None for value in row[3:]):
                raise ValueError("departure_request_invalid")
        else:
            request = SqlClientObserverRequestV2(
                connection_id=row[3], session_id=row[4], request_id=row[5], start_time=row[6]
            )
    return SqlClientDepartureSampleV2(
        kind=kind,
        raw_count=row[0],
        own_count=own,
        original_uuid_count=original,
        request=request,
        before_sha256=before,
        after_sha256=after,
    )


SqlClientCreateExclusionQuery = tuple[Kind, str, tuple[object, ...]]


class SqlClientCreateExclusionQueryProvider(Protocol):
    """Supply the reviewed raw sweep without owning observation lifecycle."""

    def queries(
        self,
        *,
        original: TdsRemoteSessionIdentity,
        observer: Any,
    ) -> tuple[SqlClientCreateExclusionQuery, ...]: ...


class SqlClientCreateExclusionObserverV2(SqlClientCreateExclusionObserver):
    """Require creator and separately root-admitted helper expectations explicitly."""

    _sample_kind = Kind

    def __init__(
        self,
        cursor: _Cursor,
        *,
        admission: SqlClientObserverAdmission,
        observer_admission: SqlClientObserverAdmission,
        operation_deadline_ns: int,
        monotonic_ns: Callable[[], int],
    ) -> None:
        sql_module = __import__(
            "dpone.adapters.mssql_sqlclient_create_exclusion_v2_sql",
            fromlist=("FixedSqlClientCreateExclusionQueries",),
        )
        self._initialize_v2(
            cursor,
            admission=admission,
            observer_admission=observer_admission,
            operation_deadline_ns=operation_deadline_ns,
            monotonic_ns=monotonic_ns,
            query_provider=sql_module.FixedSqlClientCreateExclusionQueries(Kind),
        )

    @classmethod
    def _composed(
        cls,
        cursor: _Cursor,
        *,
        admission: SqlClientObserverAdmission,
        observer_admission: SqlClientObserverAdmission,
        operation_deadline_ns: int,
        monotonic_ns: Callable[[], int],
        query_provider: SqlClientCreateExclusionQueryProvider,
    ) -> "SqlClientCreateExclusionObserverV2":
        """Internal composition seam; public construction remains compatible."""
        subject = cls.__new__(cls)
        subject._initialize_v2(
            cursor,
            admission=admission,
            observer_admission=observer_admission,
            operation_deadline_ns=operation_deadline_ns,
            monotonic_ns=monotonic_ns,
            query_provider=query_provider,
        )
        return subject

    def _initialize_v2(
        self,
        cursor: _Cursor,
        *,
        admission: SqlClientObserverAdmission,
        observer_admission: SqlClientObserverAdmission,
        operation_deadline_ns: int,
        monotonic_ns: Callable[[], int],
        query_provider: SqlClientCreateExclusionQueryProvider,
    ) -> None:
        super().__init__(
            cursor, admission=admission, operation_deadline_ns=operation_deadline_ns, monotonic_ns=monotonic_ns
        )
        if type(observer_admission) is not SqlClientObserverAdmission or (
            observer_admission.server != admission.server or observer_admission.database != admission.database
        ):
            raise ValueError("mssql_native.sqlclient_observer_admission_invalid")
        self._observer_admission = observer_admission
        self._query_provider = query_provider

    def observe_departure_v2(
        self,
        *,
        original: TdsRemoteSessionIdentity,
        database: TdsDatabaseObservation,
        principal: SqlClientDatabasePrincipal,
    ) -> SqlClientCreateDepartureV2:
        """Return actual partitions only after all independent guards and creator checks."""
        self._begin()
        try:
            validate_create_departure_inputs(original, database, self._admission, principal)
            self._creator(principal)
            initial = capture_observer_incarnation(self._query, admission=self._observer_admission)
            digest = observer_incarnation_digest(initial)
            queries = self._query_provider.queries(original=original, observer=initial)
            samples = []
            for kind, sql, parameters in queries:
                before = self._guard(digest)
                rows = self._query(sql, *parameters)
                after = self._guard(digest)
                samples.append(_partition(rows, kind, before, after))
            self._creator(principal)
            result = SqlClientCreateDepartureV2(
                original=original,
                database=database,
                admission=self._admission,
                principal=principal,
                observer=initial,
                samples=tuple(samples),
            )
            self._current()
            self._finish()
            return result
        except BaseException as error:
            self._fail()
            if not isinstance(error, Exception):
                raise
            raise SqlClientObservationError("mssql_native.sqlclient_create_departure_unavailable") from None

    def _guard(self, expected: str) -> str:
        actual = capture_observer_incarnation(self._query, admission=self._observer_admission)
        digest = observer_incarnation_digest(actual)
        if digest != expected:
            raise ValueError("observer_incarnation_changed")
        return digest

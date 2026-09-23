"""Mocked departure observations do not certify remote exclusion or live SQL."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.adapters.mssql_sqlclient_create_exclusion import SqlClientCreateExclusionObserver
from dpone.adapters.mssql_sqlclient_observer import SqlClientObservationError
from dpone.contracts.mssql_sqlclient_create_departure import SqlClientCreateDeparture
from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
from dpone.contracts.mssql_tds_coordinator_authority import TdsDatabaseObservation
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity, coordinator_authority_digest
from tests.test_mssql_sqlclient_observation import GUID, NONCE, STAMP, admission, observation
from tests.test_mssql_sqlclient_observer import Cursor


def originals():
    digest = coordinator_authority_digest(
        [
            "server",
            "machine",
            "MSSQLSERVER",
            "physical",
            "target",
            7,
            GUID,
            "writer",
            b"\xaa",
            "writer",
            b"\xaa",
            "writer_user",
            5,
            b"\xaa",
        ]
    )
    return dict(
        original=TdsRemoteSessionIdentity(GUID, 72, STAMP, STAMP, NONCE, digest),
        database=TdsDatabaseObservation("target", 7, GUID),
        principal=SqlClientDatabasePrincipal(5, "writer_user", "aa"),
    )


class DepartureCursor(Cursor):
    def __init__(self, bad_at=None, rows=None):
        super().__init__()
        self.bad_at, self.bad_rows, self.scalars = bad_at, rows, 0
        self.extra = False

    def execute(self, sql, *parameters):
        if "COUNT_BIG" in sql:
            self.calls.append((sql, parameters))
            self.scalars += 1
            self.current = iter(self.bad_rows if self.scalars == self.bad_at else [(0,)])
        else:
            super().execute(sql, *parameters)

    def nextset(self):
        return self.extra


def handle(cursor, clock=lambda: 1):
    return SqlClientCreateExclusionObserver(
        cursor, admission=admission(), operation_deadline_ns=100, monotonic_ns=clock
    )


def test_original_create_authority_and_six_absence_queries():
    cursor = DepartureCursor()
    result = handle(cursor).observe_departure(**originals())
    assert result == SqlClientCreateDeparture(**originals(), admission=admission(), counts=(0,) * 6)
    assert cursor.scalars == 6
    calls = [(sql, params) for sql, params in cursor.calls if "COUNT_BIG" in sql]
    assert calls[0] == calls[4] and calls[1] == calls[5]
    assert calls[0][1] == (str(GUID), str(GUID), 72)
    assert "parent_connection_id" in calls[0][0]
    assert calls[2][1] == (72, str(GUID))
    assert all("JOIN" not in sql and "context_info" not in sql for sql, _ in calls)


@pytest.mark.parametrize("index", range(1, 7))
@pytest.mark.parametrize("rows", [[(1,)], [(None,)], [(False,)], [(0.0,)], [], [(0,), (0,)], [(0, 0)]])
def test_any_nonzero_or_ambiguous_count_poisoned(index, rows):
    cursor = DepartureCursor(index, rows)
    observer = handle(cursor)
    with pytest.raises(SqlClientObservationError):
        observer.observe_departure(**originals())
    count = len(cursor.calls)
    with pytest.raises(SqlClientObservationError):
        observer.observe_departure(**originals())
    assert len(cursor.calls) == count and cursor.scalars == index


@pytest.mark.parametrize(
    "change",
    [
        {"original": replace(originals()["original"], authority_sha256=observation().remote_session.authority_sha256)},
        {"database": TdsDatabaseObservation("target", 7, UUID(int=2))},
        {"principal": SqlClientDatabasePrincipal(6, "writer_user", "aa")},
    ],
)
def test_wrong_original_rejected_before_sql(change):
    cursor = DepartureCursor()
    with pytest.raises(SqlClientObservationError):
        handle(cursor).observe_departure(**(originals() | change))
    assert not cursor.calls


def test_extra_count_result_set_rejected():
    cursor = DepartureCursor()
    cursor.extra = True
    with pytest.raises(SqlClientObservationError):
        handle(cursor).observe_departure(**originals())


def test_original_deadline_prevents_sql():
    cursor = DepartureCursor()
    with pytest.raises(SqlClientObservationError):
        handle(cursor, lambda: 100).observe_departure(**originals())
    assert not cursor.calls


@pytest.mark.parametrize("value", [True, 0.0, None, 1, -1])
def test_pure_record_never_accepts_zero_aliases(value):
    with pytest.raises(ValueError):
        SqlClientCreateDeparture(**originals(), admission=admission(), counts=(0, 0, value, 0, 0, 0))


def test_denied_visibility_stops_before_absence_queries():
    from dpone.adapters.mssql_sqlclient_observer_sql import VISIBILITY_SQL

    cursor = DepartureCursor()
    cursor.results[VISIBILITY_SQL] = [(16, 3, 1, 0, 7)]
    with pytest.raises(SqlClientObservationError):
        handle(cursor).observe_departure(**originals())
    assert cursor.scalars == 0


def test_principal_changed_after_sweep_rejects_success():
    from dpone.adapters.mssql_sqlclient_observer_sql import PRINCIPALS_SQL

    class Changed(DepartureCursor):
        def execute(self, sql, *parameters):
            if self.scalars == 6:
                self.results[PRINCIPALS_SQL] = [(1, "dbo", b"\xbb", "SQL_USER", "INSTANCE")]
            super().execute(sql, *parameters)

    with pytest.raises(SqlClientObservationError):
        handle(Changed()).observe_departure(**originals())


def test_process_control_interruption_faults_inherited_handle():
    class Interrupted(DepartureCursor):
        def execute(self, sql, *parameters):
            if "COUNT_BIG" in sql:
                raise KeyboardInterrupt
            super().execute(sql, *parameters)

    cursor = Interrupted()
    observer = handle(cursor)
    with pytest.raises(KeyboardInterrupt):
        observer.observe_departure(**originals())
    calls = len(cursor.calls)
    with pytest.raises(SqlClientObservationError, match="poisoned"):
        observer.preflight()
    assert len(cursor.calls) == calls


def test_reentrant_observation_poisoning_prevents_outer_success():
    class Reentrant(DepartureCursor):
        def execute(self, sql, *parameters):
            if "COUNT_BIG" in sql:
                with pytest.raises(SqlClientObservationError, match="poisoned"):
                    observer.observe_departure(**originals())
            super().execute(sql, *parameters)

    cursor = Reentrant()
    observer = handle(cursor)
    with pytest.raises(SqlClientObservationError):
        observer.observe_departure(**originals())
    assert cursor.scalars == 1


def test_wrong_thread_permanently_faults_without_sql():
    import threading

    cursor = DepartureCursor()
    observer = handle(cursor)
    errors = []

    def call():
        try:
            observer.observe_departure(**originals())
        except SqlClientObservationError as error:
            errors.append(error)

    thread = threading.Thread(target=call)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive() and len(errors) == 1 and not cursor.calls
    with pytest.raises(SqlClientObservationError, match="poisoned"):
        observer.observe_departure(**originals())


@pytest.mark.parametrize("boundary", [0, 1, 3, 6])
def test_expired_clock_during_observation_never_returns_success(boundary):
    def clock():
        return 100 if cursor.scalars >= boundary else 1

    cursor = DepartureCursor()
    observer = handle(cursor, clock)
    with pytest.raises(SqlClientObservationError):
        observer.observe_departure(**originals())
    count = len(cursor.calls)
    with pytest.raises(SqlClientObservationError, match="poisoned"):
        observer.observe_departure(**originals())
    assert len(cursor.calls) == count


@pytest.mark.parametrize("field,value", [("session_id", 72.0), ("nonce", bytearray(NONCE))])
def test_forged_nested_original_is_not_normalized(field, value):
    inputs = originals()
    object.__setattr__(inputs["original"], field, value)
    cursor = DepartureCursor()
    with pytest.raises(SqlClientObservationError):
        handle(cursor).observe_departure(**inputs)
    assert not cursor.calls


@pytest.mark.parametrize("query", ["VISIBILITY_SQL", "ADMISSION_SQL", "PRINCIPALS_SQL"])
@pytest.mark.parametrize("departure", [True, False])
def test_shared_reader_rejects_extra_sets_at_every_admission_query(query, departure):
    from dpone.adapters import mssql_sqlclient_observer_sql

    target = getattr(mssql_sqlclient_observer_sql, query)

    class Extra(DepartureCursor):
        def execute(self, sql, *parameters):
            self.extra = sql == target
            super().execute(sql, *parameters)

    cursor = Extra()
    observer = handle(cursor)
    with pytest.raises(SqlClientObservationError):
        if departure:
            observer.observe_departure(**originals())
        else:
            observer.observe(session_id=72, nonce=NONCE)
    assert cursor.scalars == 0

"""The production retirement DB-API session is bounded and containing."""

from uuid import UUID

import pytest

from dpone.adapters.mssql_sqlclient_native_retirement_dbapi import (
    DbApiSqlClientRetirementSession,
    open_dbapi_retirement_session,
)
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
    SqlClientTransportAuthority,
)
from dpone.contracts.mssql_tds_coordinator_authority import LOCK_RESOURCE, TdsLockObservation


def _admission() -> SqlClientObserverAdmission:
    return SqlClientObserverAdmission(
        SqlClientServerAuthority("server", "machine", "instance", "physical"),
        SqlClientDatabaseAuthority(7, "database", str(UUID(int=1)), "aa"),
        SqlClientLoginAuthority(5, "login", "aa", "login", "aa", 1, True),
        SqlClientTransportAuthority("TCP", "TSQL", "SQL", "TRUE"),
    )


class _Cursor:
    def __init__(self, rows=()) -> None:
        self.rows = list(rows)
        self.timeout = None
        self.executed = None
        self.closed = False

    def execute(self, sql, *parameters):
        self.executed = (sql, parameters)
        return self

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchmany(self, size):
        values, self.rows = self.rows[:size], self.rows[size:]
        return values

    def nextset(self):
        return None

    def close(self):
        self.closed = True


class _Connection:
    def __init__(self, *cursors) -> None:
        self.cursors = list(cursors)
        self.committed = self.rolled_back = self.closed = False

    def cursor(self):
        return self.cursors.pop(0)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def test_session_acquires_canonical_lock_and_returns_bounded_rows() -> None:
    lock_cursor, query_cursor = _Cursor([(0,)]), _Cursor([(1, "value")])
    connection = _Connection(lock_cursor, query_cursor)
    session = DbApiSqlClientRetirementSession(connection, _admission(), monotonic=lambda: 10.0)

    assert session.acquire_exclusive(LOCK_RESOURCE, deadline=12.0) == TdsLockObservation(0)
    assert session.query("SELECT", (1,), deadline=12.0) == ((1, "value"),)
    assert lock_cursor.timeout == query_cursor.timeout == 2
    assert lock_cursor.closed and query_cursor.closed


def test_session_rejects_expired_or_unbounded_observation() -> None:
    session = DbApiSqlClientRetirementSession(_Connection(_Cursor()), _admission(), monotonic=lambda: 10.0)
    with pytest.raises(TimeoutError, match="retirement_session_unknown"):
        session.query("SELECT", (), deadline=10.0)

    cursor = _Cursor([(1,), (2,)])
    session = DbApiSqlClientRetirementSession(_Connection(cursor), _admission(), monotonic=lambda: 10.0)
    with pytest.raises(RuntimeError, match="retirement_session_unknown"):
        session.query("SELECT", (), deadline=11.0)


def test_session_commits_rolls_back_and_connection_close_contains_lock() -> None:
    connection = _Connection(_Cursor(), _Cursor())
    session = DbApiSqlClientRetirementSession(connection, _admission(), monotonic=lambda: 10.0)
    session.commit(deadline=11.0)
    session.rollback(deadline=11.0)
    session.contain(deadline=11.0)
    session.contain(deadline=11.0)

    assert connection.committed and connection.rolled_back and connection.closed


def test_factory_observes_admission_on_exact_connection_and_always_contains() -> None:
    connection = _Connection()
    observed = []

    with open_dbapi_retirement_session(
        lambda: connection,
        lambda candidate: observed.append(candidate) or _admission(),
        monotonic=lambda: 10.0,
    ) as session:
        assert session.admission == _admission()

    assert observed == [connection]
    assert connection.closed

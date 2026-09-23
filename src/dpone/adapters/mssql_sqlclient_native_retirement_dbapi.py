"""Bounded DB-API session for exact SqlClient stage retirement."""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, Protocol

from dpone.contracts.mssql_tds_coordinator_authority import TdsLockObservation
from dpone.ports.mssql_sqlclient_writer_settlement import SqlClientObserverAdmission

_ERROR = "mssql_native.sqlclient_retirement_session_unknown"
_ACQUIRE = """
SET NOCOUNT ON;
DECLARE @result int;
EXEC @result=sys.sp_getapplock @Resource=?,@LockMode=N'Exclusive',
 @LockOwner=N'Session',@DbPrincipal=N'public',@LockTimeout=?;
SELECT @result;
"""


class DbApiRetirementConnection(Protocol):
    """Minimal independent SQL connection owned by one retirement effect."""

    def cursor(self) -> Any: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


class DbApiSqlClientRetirementSession:
    """Hold one admitted connection, DDL lock and transaction to containment."""

    def __init__(
        self,
        connection: DbApiRetirementConnection,
        admission: SqlClientObserverAdmission,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if type(admission) is not SqlClientObserverAdmission or not callable(monotonic):
            raise ValueError(_ERROR)
        self._connection = connection
        self._admission = admission
        self._clock = monotonic
        self._closed = False

    @property
    def admission(self) -> SqlClientObserverAdmission:
        """Return the authority observed on this exact connection."""
        return self._admission

    def _cursor(self, deadline: float) -> Any:
        if self._closed or type(deadline) not in (int, float):
            raise RuntimeError(_ERROR)
        remaining = deadline - self._clock()
        if not math.isfinite(remaining) or remaining <= 0:
            raise TimeoutError(_ERROR)
        cursor = self._connection.cursor()
        try:
            cursor.timeout = max(1, min(2**31 - 1, math.ceil(remaining)))
        except (AttributeError, TypeError, ValueError, OverflowError):
            cursor.close()
            raise RuntimeError(_ERROR) from None
        return cursor

    @staticmethod
    def _finish(cursor: Any) -> None:
        more = cursor.nextset()
        if more is not None and more is not False:
            raise RuntimeError(_ERROR)

    def acquire_exclusive(self, resource: str, *, deadline: float) -> TdsLockObservation:
        """Acquire the canonical session lock once within the shared deadline."""
        remaining = deadline - self._clock()
        if remaining <= 0:
            raise TimeoutError(_ERROR)
        milliseconds = max(0, min(2**31 - 1, math.floor(remaining * 1000)))
        cursor = self._cursor(deadline)
        try:
            cursor.execute(_ACQUIRE, resource, milliseconds)
            row = cursor.fetchone()
            if row is None or cursor.fetchone() is not None:
                raise RuntimeError(_ERROR)
            self._finish(cursor)
            return TdsLockObservation(row[0], resource=resource)
        finally:
            cursor.close()

    def query(self, sql: str, parameters: tuple[object, ...], *, deadline: float) -> tuple[tuple[Any, ...], ...]:
        """Execute the bounded retirement observation and reject cardinality drift."""
        cursor = self._cursor(deadline)
        try:
            cursor.execute(sql, parameters)
            rows = cursor.fetchmany(2)
            if len(rows) == 2 or cursor.fetchone() is not None:
                raise RuntimeError(_ERROR)
            self._finish(cursor)
            return tuple(tuple(row) for row in rows)
        finally:
            cursor.close()

    def execute(self, sql: str, parameters: tuple[object, ...], *, deadline: float) -> None:
        """Execute the guarded DROP inside the owned transaction."""
        cursor = self._cursor(deadline)
        try:
            cursor.execute(sql, parameters)
            self._finish(cursor)
        finally:
            cursor.close()

    def commit(self, *, deadline: float) -> None:
        """Commit only while the operation deadline remains valid."""
        cursor = self._cursor(deadline)
        cursor.close()
        self._connection.commit()

    def rollback(self, *, deadline: float) -> None:
        """Rollback only while the operation deadline remains valid."""
        cursor = self._cursor(deadline)
        cursor.close()
        self._connection.rollback()

    def contain(self, *, deadline: float) -> None:
        """Close the connection; SQL Server releases its session-owned lock."""
        if self._closed:
            return
        self._closed = True
        self._connection.close()


@contextmanager
def open_dbapi_retirement_session(
    connect: Callable[[], DbApiRetirementConnection],
    observe_admission: Callable[[DbApiRetirementConnection], SqlClientObserverAdmission],
    *,
    monotonic: Callable[[], float] = time.monotonic,
) -> Iterator[DbApiSqlClientRetirementSession]:
    """Open, admit and contain one independent management connection."""
    if not callable(connect) or not callable(observe_admission) or not callable(monotonic):
        raise ValueError(_ERROR)
    connection = connect()
    session: DbApiSqlClientRetirementSession | None = None
    try:
        admission = observe_admission(connection)
        session = DbApiSqlClientRetirementSession(connection, admission, monotonic=monotonic)
        yield session
    finally:
        if session is None:
            connection.close()
        else:
            session.contain(deadline=monotonic())


__all__ = (
    "DbApiRetirementConnection",
    "DbApiSqlClientRetirementSession",
    "open_dbapi_retirement_session",
)

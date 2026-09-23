"""Complete-result acquisition on the caller's already locked SQL handle.

Deadline checks do not interrupt a blocked driver. Production use requires the
external process supervisor; this adapter opens no connection or lock.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from dpone.adapters.mssql_sqlclient_grant_catalog_projection import (
    BATCH_COLUMNS_SQL,
    BATCH_FEATURES_SQL,
    BATCH_MEMBERS_SQL,
    BATCH_OBJECTS_SQL,
    MEMBER_SQL,
    PERMISSION_COUNT_SQL,
    PERMISSIONS_SQL,
    PRINCIPALS_SQL,
    SqlClientGrantCatalogProjection,
    _permission_row,
)
from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor
from dpone.adapters.mssql_tds_coordinator_sql import TdsCoordinatorSql

_ERROR = "mssql_native.sqlclient_grant_catalog_unavailable"


class SqlClientGrantCatalog(SqlClientGrantCatalogProjection):
    """Own the observation lease, authority checks, and complete-result reads."""

    def __init__(self, sql: TdsCoordinatorSql, *, deadline: float) -> None:
        self._initialize(
            sql,
            deadline,
            ObservationCursor(
                sql.cursor, deadline=deadline, check_deadline=lambda: sql.check_deadline(deadline=deadline)
            ),
        )

    def _initialize(self, sql: TdsCoordinatorSql, deadline: float, session: ObservationCursor) -> None:
        self.sql, self.deadline = sql, deadline
        self.identity, self.execution_owner, self.process = sql.identity, sql.execution_owner, sql.process
        if sql.authority is None:
            raise ValueError(_ERROR)
        self.authority = sql.authority
        self._session = session
        self._lease: object | None = None

    @classmethod
    def _sharing(
        cls, sql: TdsCoordinatorSql, *, deadline: float, session: ObservationCursor
    ) -> "SqlClientGrantCatalog":
        result = cls.__new__(cls)
        result._initialize(sql, deadline, session)
        return result

    _owner = property(lambda self: self._session.owner)
    _busy = property(lambda self: self._session.busy)
    _faulted = property(lambda self: self._session.faulted)

    @contextmanager
    def _operation(self) -> Iterator[object]:
        try:
            lease = self._session.begin()
        except ValueError:
            raise RuntimeError(_ERROR) from None
        self._lease = lease
        try:
            yield lease
            self._current()
            self._session.finish(lease)
        except BaseException as error:
            self._session.fail(lease)
            if isinstance(error, ValueError) and str(error) == _ERROR:
                raise
            raise RuntimeError(_ERROR) from None

    def _current(self) -> None:
        self._session.current(self._lease)

    def guard(self) -> Any:
        with self._operation() as lease:
            return self._guard_within(lease)

    def _guard_within(self, lease: object) -> Any:
        self._session.healthy(lease)
        self._lease = lease
        try:
            return self._authority()
        except BaseException:
            self._session.fail(lease)
            raise RuntimeError(_ERROR) from None

    def _authority(self) -> Any:
        self._current()
        authority = self.sql.require_authority(deadline=self.deadline)
        self._current()
        return authority

    def _rows(self, statement: str, parameters: tuple[Any, ...] = (), *, limit: int = 1) -> list[Any]:
        with self._operation() as lease:
            return self._rows_within(lease, statement, parameters, limit=limit)

    def _rows_within(
        self, lease: object, statement: str, parameters: tuple[Any, ...] = (), *, limit: int = 1
    ) -> list[Any]:
        self._session.healthy(lease)
        self._lease = lease
        try:
            self._authority()
            rows = self._session.rows(
                lease,
                statement,
                parameters,
                max_rows=limit,
                nextset_required=True,
                detach_rows=True,
                current=self._current,
            )
            self._authority()
            return rows
        except BaseException:
            self._session.fail(lease)
            raise RuntimeError(_ERROR) from None

    def _permissions_within(self, lease: object, writer_id: int, public_id: int, *, limit: int) -> list[Any]:
        try:
            if type(limit) is not int or not 1 <= limit <= 4096:
                raise ValueError(_ERROR)
            count = self._rows_within(lease, PERMISSION_COUNT_SQL, (writer_id, public_id))
            if len(count) != 1 or len(count[0]) != 1 or type(count[0][0]) is not int or not 0 <= count[0][0] <= limit:
                raise ValueError(_ERROR)
            rows = self._rows_within(lease, PERMISSIONS_SQL, (limit + 1, writer_id, public_id), limit=limit)
            after = self._rows_within(lease, PERMISSION_COUNT_SQL, (writer_id, public_id))
            if after != count or count[0][0] != len(rows):
                raise ValueError(_ERROR)
            return [_permission_row(row) for row in rows]
        except BaseException:
            self._session.fail(lease)
            raise


__all__ = [
    "BATCH_COLUMNS_SQL",
    "BATCH_FEATURES_SQL",
    "BATCH_MEMBERS_SQL",
    "BATCH_OBJECTS_SQL",
    "MEMBER_SQL",
    "PERMISSION_COUNT_SQL",
    "PERMISSIONS_SQL",
    "PRINCIPALS_SQL",
    "SqlClientGrantCatalog",
]

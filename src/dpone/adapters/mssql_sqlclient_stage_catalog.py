"""Bounded, read-only stage catalog acquisition on an exclusive injected cursor.

The candidate profile requires independently admitted management, full
metadata/security-policy visibility and SQL Server 16–17. It is not a writer
profile. Every driver boundary checks the original deadline and owner; this does
not interrupt blocked driver calls. Callers supply external process containment.
"""

import os as os
import threading as threading
from collections.abc import Callable
from typing import Any, Protocol
from uuid import UUID

from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor
from dpone.adapters.mssql_sqlclient_observer_incarnation import capture_observer_incarnation
from dpone.adapters.mssql_sqlclient_stage_catalog_sql import (
    COLUMNS_SQL,
    FEATURES_SQL,
    OBJECT_SQL,
    SCHEMA_SQL,
    STAGE_VISIBILITY_SQL,
)
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_tds_api import (
    SqlClientStageIdentity,
    SqlClientStageObservation,
    parse_stage_columns,
    parse_stage_object,
    quote_stage_identifier,
    snapshot_stage_admission,
    snapshot_stage_identity,
)

_ERROR = "mssql_native.sqlclient_stage_observation_unavailable"


class StageCursor(Protocol):
    """Dedicated complete-result cursor, supplied by an external composition."""

    def execute(self, sql: str, *parameters: Any) -> Any: ...
    def fetchone(self) -> Any: ...
    def nextset(self) -> Any: ...


class SqlClientStageObservationError(RuntimeError):
    """Constant diagnostic and permanent poison; never remote settlement."""


class SqlClientStageObserver:
    """One process/thread, one cursor, one absolute budget; no reconnect/retry.

    Management admission is supplied independently and detached before SQL. The
    successful result contains observed identities only, never CREATE evidence.
    Reentrant/overlapping calls poison the active call even if rejection is caught.
    """

    def __init__(
        self,
        cursor: StageCursor,
        *,
        admission: SqlClientObserverAdmission,
        operation_deadline_ns: int,
        monotonic_ns: Callable[[], int],
    ) -> None:
        self._initialize(admission, operation_deadline_ns)
        self._session = ObservationCursor(cursor, deadline=operation_deadline_ns, clock=monotonic_ns)

    def _initialize(self, admission: SqlClientObserverAdmission, deadline: int) -> None:
        self._admission = snapshot_stage_admission(admission)
        if type(deadline) is not int or deadline < 1:
            raise ValueError(_ERROR)
        self._lease: object | None = None

    @classmethod
    def _sharing(
        cls, session: ObservationCursor, *, admission: SqlClientObserverAdmission, operation_deadline_ns: int
    ) -> "SqlClientStageObserver":
        result = cls.__new__(cls)
        result._initialize(admission, operation_deadline_ns)
        result._session = session
        return result

    _cursor = property(lambda self: self._session.cursor)
    _deadline = property(lambda self: self._session.deadline)
    _clock = property(lambda self: self._session.clock, lambda self, value: setattr(self._session, "clock", value))
    _owner = property(lambda self: self._session.owner)
    _lock = property(lambda self: self._session.lock)
    _busy = property(lambda self: self._session.busy)
    _faulted = property(lambda self: self._session.faulted)

    def _state(self, *, begin: bool = False, finish: bool = False) -> None:
        try:
            if begin:
                self._lease = self._session.begin()
            elif finish:
                self._session.finish(self._lease)
            else:
                self._session.healthy(self._lease)
        except ValueError:
            raise SqlClientStageObservationError(_ERROR) from None

    def _current(self) -> None:
        self._session.current(self._lease)

    def _query(self, sql: str, *parameters: Any, limit: int = 1) -> list[Any]:
        return self._session.rows(
            self._lease,
            sql,
            parameters,
            max_rows=limit,
            nextset_required=True,
            detach_rows=False,
            current=self._current,
        )

    def _one(self, sql: str, *parameters: Any) -> Any:
        rows = self._query(sql, *parameters)
        if len(rows) != 1:
            raise ValueError(_ERROR)
        return rows[0]

    def _catalog(
        self, expected: SqlClientStageIdentity, *, database_guid: str, database_id: int, database_name: str
    ) -> tuple[SqlClientStageIdentity, tuple[int, int, int]]:
        target = quote_stage_identifier(expected.schema_name) + "." + quote_stage_identifier(expected.table_name)
        visibility = self._one(STAGE_VISIBILITY_SQL, target)
        if (
            len(visibility) != 4
            or type(visibility[0]) is not int
            or visibility[0] not in (16, 17)
            or any(type(p) is not int or p != 1 for p in visibility[1:])
        ):
            raise ValueError(_ERROR)
        schema = self._one(SCHEMA_SQL, expected.schema_id)
        if len(schema) != 2:
            raise ValueError(_ERROR)
        if type(schema[0]) is not int or not 1 <= schema[0] <= 2**31 - 1:
            raise ValueError(_ERROR)
        quote_stage_identifier(schema[1])
        observed = parse_stage_object(self._one(OBJECT_SQL, schema[0], expected.table_name))
        features = self._one(FEATURES_SQL, observed[0])
        if len(features) != 12 or any(type(v) is not int or v != 0 for v in features):
            raise ValueError(_ERROR)
        columns = parse_stage_columns(self._query(COLUMNS_SQL, observed[0], limit=100))
        result = SqlClientStageIdentity(
            database_guid=UUID(database_guid),
            database_id=database_id,
            database_name=database_name,
            schema_id=schema[0],
            schema_name=schema[1],
            table_name=observed[1],
            object_id=observed[0],
            create_date=observed[2],
            owner_binding=observed[3],
            object_nonce=UUID(observed[4]),
            columns=columns,
        )
        if result != expected:
            raise ValueError(_ERROR)
        return result, (visibility[1], visibility[2], visibility[3])

    def catalog_in_existing_session(
        self,
        expected: SqlClientStageIdentity,
        *,
        lease: object,
        database_guid: str,
        database_id: int,
        database_name: str,
    ) -> tuple[SqlClientStageIdentity, tuple[int, int, int]]:
        """Reuse the caller's active cursor lease without starting a nested observation."""
        if self._lease is not None or lease is None:
            raise SqlClientStageObservationError(_ERROR)
        self._lease = lease
        try:
            return self._catalog(
                expected,
                database_guid=database_guid,
                database_id=database_id,
                database_name=database_name,
            )
        finally:
            self._lease = None

    def observe(self, expected_stage_identity: SqlClientStageIdentity) -> SqlClientStageObservation:
        """Observe identity, actual emptiness and identity again; return no grant."""
        self._state(begin=True)
        try:
            result = self._observe_within(self._lease, expected_stage_identity)
            self._state(finish=True)
            return result
        except BaseException as error:
            self._session.fail(self._lease)
            if not isinstance(error, Exception):
                raise
            raise SqlClientStageObservationError(_ERROR) from None

    def _observe_within(
        self, lease: object | None, expected_stage_identity: SqlClientStageIdentity
    ) -> SqlClientStageObservation:
        self._session.healthy(lease)
        self._lease = lease
        try:
            expected = snapshot_stage_identity(expected_stage_identity)
            management = capture_observer_incarnation(self._query, admission=self._admission)
            db = management.authority.database
            before, permissions = self._catalog(
                expected, database_guid=db.database_guid, database_id=db.database_id, database_name=db.database_name
            )
            if capture_observer_incarnation(self._query, admission=self._admission) != management:
                raise ValueError(_ERROR)
            target = quote_stage_identifier(before.schema_name) + "." + quote_stage_identifier(before.table_name)
            empty = self._one(
                "SELECT CASE WHEN EXISTS(SELECT 1 FROM "
                + target
                + " WITH(READCOMMITTEDLOCK)) THEN CONVERT(int,0) ELSE CONVERT(int,1) END;"
            )
            if len(empty) != 1 or type(empty[0]) is not int or empty[0] != 1:
                raise ValueError(_ERROR)
            final_management = capture_observer_incarnation(self._query, admission=self._admission)
            db = final_management.authority.database
            after, final_permissions = self._catalog(
                expected, database_guid=db.database_guid, database_id=db.database_id, database_name=db.database_name
            )
            if capture_observer_incarnation(self._query, admission=self._admission) != final_management:
                raise ValueError(_ERROR)
            if permissions != final_permissions:
                raise ValueError(_ERROR)
            result = SqlClientStageObservation(
                before=before,
                after=after,
                management_before=management,
                management_after=final_management,
                empty=empty[0],
                metadata_permissions=permissions,
            )
            self._current()
            return result
        except BaseException as error:
            self._session.fail(self._lease)
            if not isinstance(error, Exception):
                raise
            raise SqlClientStageObservationError(_ERROR) from None

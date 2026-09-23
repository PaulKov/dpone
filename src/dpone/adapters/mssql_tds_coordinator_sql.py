"""One child-owned SQL session continuously holds the database-wide DDL lock.

The lock is cooperative with other framework coordinators, not a defence against
external privileged DDL. No caller hash, session nonce or local exit acquires it.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from time import monotonic
from typing import Any

from dpone.adapters.mssql_tds_coordinator_connection import TdsSqlConnection, _before
from dpone.adapters.mssql_tds_session import TdsCoordinatorSession
from dpone.contracts.mssql_tds_api import (
    LOCK_RESOURCE,
    TdsAttemptOwnership,
    TdsCoordinatorAuthority,
    TdsDatabaseObservation,
    TdsLockObservation,
    TdsProcessIdentity,
    TdsRemoteSessionIdentity,
    TdsSchemaObservation,
)
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorIdentity, coordinator_identity_digest


class TdsAuthorityError(RuntimeError):
    """Continuity/lock authority is unknown; this SQL handle cannot resume."""


_DATABASE = """
SELECT DB_NAME(), DB_ID(), d.database_guid, s.schema_id, s.name
FROM sys.database_recovery_status d CROSS JOIN sys.schemas s
WHERE d.database_id=DB_ID() AND DB_ID(?)=DB_ID() AND s.name=?;
"""
_ACQUIRE = """
SET NOCOUNT ON;
DECLARE @result int;
EXEC @result = sys.sp_getapplock @Resource=?, @LockMode=N'Exclusive',
 @LockOwner=N'Session', @DbPrincipal=N'public', @LockTimeout=?;
SELECT @result;
"""
_LOCK = "SELECT APPLOCK_MODE(N'public', ?, N'Session');"


def _one(cursor: Any, sql: str, parameters: tuple = ()) -> Any:
    """Internal bounded-cardinality SQL observation, never a supervisor port."""
    cursor.execute(sql, *parameters)
    if cursor.description is None:
        raise ValueError("mssql_native.tds_sql_result_missing")
    row = cursor.fetchone()
    if row is None or cursor.fetchone() is not None:
        raise ValueError("mssql_native.tds_sql_cardinality_invalid")
    completion = cursor.nextset()
    if completion is not None and completion is not False:
        raise ValueError("mssql_native.tds_sql_result_extra")
    return row


class TdsCoordinatorSql:
    """Initialize once; any failed check poisons without reconnection/reacquisition."""

    def __init__(
        self,
        connection: TdsSqlConnection,
        identity: TdsCoordinatorIdentity,
        execution_owner: TdsAttemptOwnership,
        process: TdsProcessIdentity,
        *,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if (
            type(identity) is not TdsCoordinatorIdentity
            or type(execution_owner) is not TdsAttemptOwnership
            or type(process) is not TdsProcessIdentity
            or execution_owner.fence != identity.original_fence
        ):
            raise ValueError("mssql_native.tds_sql_binding_invalid")
        self.connection, self.identity, self.execution_owner, self.process = (
            connection,
            identity,
            execution_owner,
            process,
        )
        self._clock = clock
        self._session = TdsCoordinatorSession[TdsRemoteSessionIdentity](connection.cursor)
        self._attempted = self._poisoned = False
        self.authority: TdsCoordinatorAuthority | None = None

    @property
    def cursor(self) -> Any:
        self.connection.check_owner()
        return self.connection.cursor

    def check_deadline(self, *, deadline: float) -> None:
        """Check the same child clock before another SQL statement may start."""
        self.connection.check_owner()
        _before(deadline, self._clock)

    def _database(self) -> tuple[TdsDatabaseObservation, TdsSchemaObservation]:
        row = _one(self.cursor, _DATABASE, (self.identity.parent.database, self.identity.parent.schema))
        if len(row) != 5:
            raise ValueError
        return TdsDatabaseObservation(*row[:3]), TdsSchemaObservation(*row[3:])

    def _lock(self) -> None:
        row = _one(self.cursor, _LOCK, (LOCK_RESOURCE,))
        if len(row) != 1 or row[0] != "Exclusive":
            raise ValueError

    def acquire(self, nonce: bytes, *, deadline: float) -> TdsCoordinatorAuthority:
        self.connection.check_owner()
        if self._attempted or self._poisoned:
            raise TdsAuthorityError("mssql_native.tds_sql_authority_unavailable")
        self._attempted = self._poisoned = True
        try:
            _before(deadline, self._clock)
            session = self._session.initialize(nonce)
            database, schema = self._database()
            milliseconds = max(0, min(2**31 - 1, math.floor(_before(deadline, self._clock) * 1000)))
            row = _one(self.cursor, _ACQUIRE, (LOCK_RESOURCE, milliseconds))
            if len(row) != 1:
                raise ValueError
            lock = TdsLockObservation(row[0])
            self._session.require_same(session)
            self._lock()
            if self._database() != (database, schema):
                raise ValueError
            _before(deadline, self._clock)
            receipt = TdsCoordinatorAuthority(
                coordinator_identity_digest(self.identity),
                self.execution_owner,
                self.process,
                self.identity.implementation_sha256,
                session,
                database,
                schema,
                lock,
            )
            self.authority = receipt
            self._poisoned = False
            return receipt
        except BaseException:
            raise TdsAuthorityError("mssql_native.tds_sql_authority_unknown") from None

    def require_authority(self, *, deadline: float, in_transaction: bool = False) -> TdsCoordinatorAuthority:
        self.connection.check_owner()
        if self._poisoned or self.authority is None:
            raise TdsAuthorityError("mssql_native.tds_sql_authority_unavailable")
        if type(self.authority.session) is not TdsRemoteSessionIdentity:
            raise TdsAuthorityError("mssql_native.tds_sql_authority_unavailable")
        self._poisoned = True
        try:
            _before(deadline, self._clock)
            self._session.require_same(self.authority.session, in_transaction=in_transaction)
            self._lock()
            if self._database() != (self.authority.database, self.authority.schema_observation):
                raise ValueError
            _before(deadline, self._clock)
            self._poisoned = False
            return self.authority
        except BaseException:
            raise TdsAuthorityError("mssql_native.tds_sql_authority_unknown") from None

    def close(self) -> None:
        """Connection teardown releases the session lock; never reacquire it."""
        self._poisoned = True
        self.connection.close()

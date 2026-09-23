"""Single-cursor SQL Server session observation for the guarded coordinator child.

No connection creation, retry, reconnection, transaction cleanup or mutation
authorization lives here. The caller owns the dedicated connection and cursor
and bounds this synchronous work through the isolated coordinator process.
"""

from __future__ import annotations

import os
import threading
from typing import Any, Generic, Protocol, TypeVar, cast
from uuid import UUID

from dpone.contracts.mssql_tds_api import (
    TdsRemoteSessionIdentity,
    TdsRestrictedRemoteSessionIdentity,
    coordinator_authority_digest,
    require_session_nonce,
)


class _Cursor(Protocol):
    @property
    def description(self) -> Any: ...

    def nextset(self) -> Any: ...

    def execute(self, sql: str, *parameters: Any) -> Any: ...

    def fetchone(self) -> Any: ...


class TdsSessionObservationError(RuntimeError):
    """Continuity is unproved; the handle cannot be reused or repaired."""


_INITIALIZE = """
IF @@TRANCOUNT <> 0 OR XACT_STATE() NOT IN (0, 1) OR (@@OPTIONS & 2) <> 0
    THROW 51000, 'mssql_native.tds_session_transaction_invalid', 1;
IF (SELECT COUNT(*) FROM sys.dm_exec_sessions
    WHERE session_id = @@SPID AND DATALENGTH(context_info) = 0) <> 1
    THROW 51000, 'mssql_native.tds_session_context_present', 1;
DECLARE @nonce varbinary(128) = ?;
SET CONTEXT_INFO @nonce;
"""

# Do not filter away competing connections/requests before checking cardinality.
# A database GUID distinguishes recreation under the same database name and ID.
_OBSERVE = """
SELECT c.connection_id, c.session_id, c.connect_time, s.login_time,
       c.parent_connection_id, c.net_transport, c.protocol_type, r.connection_id,
       (SELECT COUNT(*) FROM sys.dm_exec_connections child
        WHERE child.parent_connection_id = c.connection_id),
       s.context_info, @@TRANCOUNT, XACT_STATE(), (@@OPTIONS & 2),
       CONVERT(nvarchar(128), SERVERPROPERTY('ServerName')),
       CONVERT(nvarchar(128), SERVERPROPERTY('MachineName')),
       COALESCE(CONVERT(nvarchar(128), SERVERPROPERTY('InstanceName')), N'MSSQLSERVER'),
       COALESCE(CONVERT(nvarchar(128), SERVERPROPERTY('ComputerNamePhysicalNetBIOS')),
                CONVERT(nvarchar(128), SERVERPROPERTY('MachineName'))),
       DB_NAME(), DB_ID(), d.database_guid,
       SUSER_SNAME(), SUSER_SID(), ORIGINAL_LOGIN(), s.original_security_id,
       USER_NAME(), DATABASE_PRINCIPAL_ID(),
       (SELECT sid FROM sys.database_principals WHERE principal_id = DATABASE_PRINCIPAL_ID())
FROM sys.dm_exec_connections c
JOIN sys.dm_exec_sessions s ON s.session_id = c.session_id
JOIN sys.dm_exec_requests r ON r.session_id = c.session_id
CROSS JOIN sys.database_recovery_status d
WHERE c.session_id = @@SPID AND d.database_id = DB_ID();
"""


_SessionIdentity = TypeVar("_SessionIdentity", TdsRemoteSessionIdentity, TdsRestrictedRemoteSessionIdentity)


class TdsCoordinatorSession(Generic[_SessionIdentity]):
    """Observe one exclusive child-owned session, poisoning it after ambiguity.

    ``initialize`` installs a one-use nonce once. ``require_same`` only reads;
    even restored context cannot revive a poisoned handle. The supplied cursor
    must never be used concurrently or backed by a reconnecting abstraction.
    The guarded child owns all cursor and connection cleanup on its own thread.
    """

    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor
        self._owner = (os.getpid(), threading.current_thread())
        self._attempted = False
        self._poisoned = False
        self._identity: _SessionIdentity | None = None

    def _require_owner(self) -> None:
        if self._owner != (os.getpid(), threading.current_thread()):
            raise TdsSessionObservationError("mssql_native.tds_session_owner_mismatch")
        if self._poisoned:
            raise TdsSessionObservationError("mssql_native.tds_session_poisoned")

    def initialize(self, nonce: bytes) -> _SessionIdentity:
        """Install context before business work; lost SET/read ACK forbids retry."""
        self._require_owner()
        require_session_nonce(nonce)
        if self._attempted:
            raise TdsSessionObservationError("mssql_native.tds_session_already_initialized")
        self._attempted = True
        self._poisoned = True
        try:
            self._cursor.execute(_INITIALIZE, nonce)
            if self._cursor.description is not None:
                raise ValueError("initialization_result_unexpected")
            completion = self._cursor.nextset()
            if completion is not None and completion is not False:
                raise ValueError("initialization_result_extra")
            identity = self._observe(in_transaction=False)
            if identity.nonce != nonce:
                raise ValueError("nonce_mismatch")
        except Exception:
            raise TdsSessionObservationError("mssql_native.tds_session_unavailable") from None
        self._identity = identity
        self._poisoned = False
        return identity

    def require_same(self, expected: _SessionIdentity, *, in_transaction: bool = False) -> None:
        """Read exact continuity; transaction state is checked separately.

        Set ``in_transaction=True`` inside the coordinator's one explicit SQL
        transaction. This checks transaction state, not ownership/fence locks.
        """
        self._require_owner()
        self._poisoned = True
        if type(expected) is not TdsRemoteSessionIdentity or expected != self._identity:
            raise TdsSessionObservationError("mssql_native.tds_session_binding_mismatch")
        if type(in_transaction) is not bool:
            raise TdsSessionObservationError("mssql_native.tds_session_transaction_invalid")
        try:
            observed = self._observe(in_transaction=in_transaction)
            if observed != expected:
                raise ValueError("identity_mismatch")
        except Exception:
            raise TdsSessionObservationError("mssql_native.tds_session_continuity_unproved") from None
        self._poisoned = False

    def _observe(self, *, in_transaction: bool) -> _SessionIdentity:
        self._cursor.execute(_OBSERVE)
        if self._cursor.description is None:
            raise ValueError("observation_result_missing")
        row = self._cursor.fetchone()
        if row is None or len(row) != 27 or self._cursor.fetchone() is not None:
            raise ValueError("observation_cardinality")
        completion = self._cursor.nextset()
        if completion is not None and completion is not False:
            raise ValueError("observation_result_extra")
        if (
            row[4] is not None
            or row[5] != "TCP"
            or row[6] != "TSQL"
            or type(row[7]) is not UUID
            or row[7] != row[0]
            or type(row[8]) is not int
            or row[8] != 0
        ):
            raise ValueError("connection_ambiguous")
        if any(type(row[i]) is not int for i in (10, 11, 12)):
            raise ValueError("transaction_invalid")
        # A committable autocommit request may report XACT_STATE=1 with no
        # explicit transaction. Never accept doomed or implicit transactions.
        allowed = {(1, 1, 0)} if in_transaction else {(0, 0, 0), (0, 1, 0)}
        if tuple(row[10:13]) not in allowed:
            raise ValueError("transaction_invalid")
        return cast(
            _SessionIdentity,
            TdsRemoteSessionIdentity(row[0], row[1], row[2], row[3], row[9], _authority_digest(row)),
        )


def _authority_digest(row: Any) -> bytes:
    """Preserve the coordinator row projection onto its original authority codec."""
    return coordinator_authority_digest(row[13:])

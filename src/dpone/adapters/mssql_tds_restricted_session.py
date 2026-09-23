"""Least-privilege continuity for the restricted SQL Server writer session."""

from __future__ import annotations

from dpone.adapters.mssql_tds_coordinator_connection import TdsSqlConnection
from dpone.adapters.mssql_tds_session import TdsCoordinatorSession, TdsSessionObservationError
from dpone.contracts.mssql_tds_api import (
    TdsRestrictedRemoteSessionIdentity,
    coordinator_authority_digest,
    require_session_nonce,
)

_INITIALIZE = """
IF @@TRANCOUNT <> 0 OR XACT_STATE() NOT IN (0, 1) OR (@@OPTIONS & 2) <> 0
    THROW 51000, 'mssql_native.tds_session_transaction_invalid', 1;
IF (SELECT COUNT(*) FROM sys.dm_exec_sessions
    WHERE session_id = @@SPID AND DATALENGTH(context_info) = 0) <> 1
    THROW 51000, 'mssql_native.tds_session_context_present', 1;
DECLARE @nonce varbinary(128) = ?;
SET CONTEXT_INFO @nonce;
"""

_OBSERVE = """
SELECT s.session_id, s.login_time, s.context_info, @@TRANCOUNT, XACT_STATE(), (@@OPTIONS & 2),
       CONVERT(nvarchar(128), SERVERPROPERTY('ServerName')),
       CONVERT(nvarchar(128), SERVERPROPERTY('MachineName')),
       COALESCE(CONVERT(nvarchar(128), SERVERPROPERTY('InstanceName')), N'MSSQLSERVER'),
       COALESCE(CONVERT(nvarchar(128), SERVERPROPERTY('ComputerNamePhysicalNetBIOS')),
                CONVERT(nvarchar(128), SERVERPROPERTY('MachineName'))),
       DB_NAME(), DB_ID(), d.database_guid,
       SUSER_SNAME(), SUSER_SID(), ORIGINAL_LOGIN(), s.original_security_id,
       USER_NAME(), DATABASE_PRINCIPAL_ID(),
       (SELECT sid FROM sys.database_principals WHERE principal_id = DATABASE_PRINCIPAL_ID())
FROM sys.dm_exec_sessions s
CROSS JOIN sys.database_recovery_status d
WHERE s.session_id = @@SPID AND d.database_id = DB_ID();
"""


class TdsRestrictedWriterSession(TdsCoordinatorSession[TdsRestrictedRemoteSessionIdentity]):
    """Bind client connection ID to the writer's universally visible own session.

    SQL Server 2022 intentionally protects connection DMVs with the broad
    ``VIEW SERVER PERFORMANCE STATE`` permission. The writer never receives it.
    Microsoft ODBC supplies its immutable client connection ID and SPID; SQL
    independently confirms the SPID, login epoch, nonce, authority and clean
    transaction state on the same exclusive, non-reconnecting handle.
    """

    def __init__(self, connection: TdsSqlConnection) -> None:
        if type(connection) is not TdsSqlConnection:
            raise ValueError("mssql_native.tds_restricted_session_invalid")
        super().__init__(connection.cursor)
        self._connection_id, self._client_session_id = connection.require_client_identity()

    def initialize(self, nonce: bytes) -> TdsRestrictedRemoteSessionIdentity:
        self._require_owner()
        require_session_nonce(nonce)
        if self._attempted:
            raise TdsSessionObservationError("mssql_native.tds_session_already_initialized")
        self._attempted = True
        self._poisoned = True
        try:
            self._cursor.execute(_INITIALIZE, nonce)
            if self._cursor.description is not None or self._cursor.nextset() not in (None, False):
                raise ValueError
            identity = self._observe(in_transaction=False)
            if identity.nonce != nonce:
                raise ValueError
        except Exception:
            raise TdsSessionObservationError("mssql_native.tds_session_unavailable") from None
        self._identity = identity
        self._poisoned = False
        return identity

    def require_same(self, expected: TdsRestrictedRemoteSessionIdentity, *, in_transaction: bool = False) -> None:
        """Reobserve the exact restricted identity without reinterpreting its UUID."""
        self._require_owner()
        self._poisoned = True
        if type(expected) is not TdsRestrictedRemoteSessionIdentity or expected != self._identity:
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

    def _observe(self, *, in_transaction: bool) -> TdsRestrictedRemoteSessionIdentity:
        self._cursor.execute(_OBSERVE)
        if self._cursor.description is None:
            raise ValueError
        row = self._cursor.fetchone()
        if row is None or len(row) != 20 or self._cursor.fetchone() is not None:
            raise ValueError
        if self._cursor.nextset() not in (None, False):
            raise ValueError
        if (
            self._connection_id is None
            or self._client_session_id is None
            or row[0] != self._client_session_id
            or any(type(row[index]) is not int for index in (3, 4, 5))
        ):
            raise ValueError
        allowed = {(1, 1, 0)} if in_transaction else {(0, 0, 0), (0, 1, 0)}
        if tuple(row[3:6]) not in allowed:
            raise ValueError
        return TdsRestrictedRemoteSessionIdentity(
            self._connection_id,
            row[0],
            row[1],
            row[2],
            coordinator_authority_digest(row[6:]),
        )

"""Read-only CREATE departure sweep on the existing bounded observation handle.

Caller must contain the original successful one-shot CREATE process, admit its
no-reconnect profile and retain authentic original receipts. Synchronous SQL still
requires external process containment; a local timeout is not remote settlement.
"""

from dpone.adapters.mssql_sqlclient_observer import SqlClientObservationError, SqlClientWriterObserver
from dpone.adapters.mssql_sqlclient_observer_sql import PRINCIPALS_SQL
from dpone.contracts.mssql_tds_api import (
    SqlClientCreateDeparture,
    SqlClientDatabasePrincipal,
    TdsDatabaseObservation,
    TdsRemoteSessionIdentity,
    resolve_database_principal,
    validate_create_departure_inputs,
)

_CONNECTIONS = """SELECT COUNT_BIG(*) FROM sys.dm_exec_connections
WHERE connection_id = ? OR parent_connection_id = ? OR session_id = ?;"""
_SESSIONS = "SELECT COUNT_BIG(*) FROM sys.dm_exec_sessions WHERE session_id = ?;"
_REQUESTS = """SELECT COUNT_BIG(*) FROM sys.dm_exec_requests
WHERE session_id = ? OR connection_id = ?;"""
_TRANSACTIONS = "SELECT COUNT_BIG(*) FROM sys.dm_tran_session_transactions WHERE session_id = ?;"


class SqlClientCreateExclusionObserver(SqlClientWriterObserver):
    """Reuse bounded SQL lifecycle while preserving the original CREATE authority.

    The inherited writer observation remains unchanged. This additional operation
    rejects every live session at the old SPID, including a reused incarnation.
    No nonce or identity join can filter a competing row out of the sweep.
    """

    def _creator(self, principal: SqlClientDatabasePrincipal) -> None:
        self._preflight()
        resolved = resolve_database_principal(
            self._query(PRINCIPALS_SQL, bytes.fromhex(self._admission.login.sid)), admission=self._admission
        ).principal
        if resolved != principal:
            raise ValueError("creator_principal_changed")

    def _zero(self, sql: str, *parameters: object) -> int:
        rows = self._query(sql, *parameters)
        if len(rows) != 1 or len(rows[0]) != 1 or type(rows[0][0]) is not int or rows[0][0] != 0:
            raise ValueError("departure_unproved")
        return 0

    def observe_departure(
        self,
        *,
        original: TdsRemoteSessionIdentity,
        database: TdsDatabaseObservation,
        principal: SqlClientDatabasePrincipal,
    ) -> SqlClientCreateDeparture:
        """Return SQL facts only after matching authority before and after a sweep."""
        self._begin()
        try:
            validate_create_departure_inputs(original, database, self._admission, principal)
            self._creator(principal)
            connection, session = str(original.connection_id), original.session_id
            queries = (
                (_CONNECTIONS, (connection, connection, session)),
                (_SESSIONS, (session,)),
                (_REQUESTS, (session, connection)),
                (_TRANSACTIONS, (session,)),
                (_CONNECTIONS, (connection, connection, session)),
                (_SESSIONS, (session,)),
            )
            counts = tuple(self._zero(sql, *parameters) for sql, parameters in queries)
            self._creator(principal)
            result = SqlClientCreateDeparture(
                original=original,
                database=database,
                admission=self._admission,
                principal=principal,
                counts=counts,
            )
            self._current()
            self._finish()
            return result
        except BaseException as error:
            self._fail()
            if not isinstance(error, Exception):
                raise
            raise SqlClientObservationError("mssql_native.sqlclient_create_departure_unavailable") from None

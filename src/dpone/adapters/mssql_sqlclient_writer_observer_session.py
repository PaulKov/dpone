"""One admitted connection serving one fixed P10d writer observation."""

from __future__ import annotations

from time import monotonic_ns
from typing import Any

from dpone.adapters import mssql_tds_coordinator_connection as connection_adapter
from dpone.adapters.mssql_sqlclient_observer import SqlClientWriterObserver
from dpone.adapters.mssql_sqlclient_observer_incarnation import capture_observer_incarnation

ERROR = "mssql_native.sqlclient_writer_observer_session_unknown"


class SqlClientWriterObserverSession:
    """Own the observer SQL connection and permit exactly one target read."""

    def __init__(
        self,
        request: Any,
        credentials: Any,
        admission: bytes,
        request_sha256: str,
    ) -> None:
        request.__post_init__()
        credentials.__post_init__()
        build, profile = connection_adapter.decode_connection_admission(admission)
        expected_profile = (
            connection_adapter.TdsConnectionProfile.VERIFIED_TLS
            if credentials.tls_profile == "verified"
            else connection_adapter.TdsConnectionProfile.SYNTHETIC_LOCAL
        )
        if profile is not expected_profile:
            raise ValueError(ERROR)
        material = connection_adapter.TdsConnectionMaterial(
            credentials.host,
            credentials.port,
            credentials.database,
            credentials.username,
            credentials.password,
        )
        try:
            self._connection = connection_adapter.TdsCoordinatorConnection(build, profile).connect(
                material, deadline=request.operation_deadline
            )
        finally:
            del material, credentials
        self._request = request
        self._request_sha256 = request_sha256
        self._used = self._closed = False
        self._observer = SqlClientWriterObserver.for_target(
            self._connection.cursor,
            admission=request.observer_admission,
            target_admission=request.target_admission,
            operation_deadline_ns=request.operation_deadline_ns,
            monotonic_ns=monotonic_ns,
        )
        self.incarnation = capture_observer_incarnation(self._own_rows, admission=request.observer_admission)

    def _own_rows(self, statement: str) -> list[object]:
        cursor = self._connection.cursor
        cursor.execute(statement)
        rows = list(cursor.fetchmany(2))
        if len(rows) != 1 or cursor.fetchone() is not None:
            raise ValueError(ERROR)
        return rows

    def observe_once(self, command: Any):
        command.__post_init__()
        if self._used or self._closed or command.request_sha256 != self._request_sha256:
            raise ValueError(ERROR)
        self._used = True
        return self._observer.observe(session_id=command.session_id, nonce=bytes.fromhex(command.nonce))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._connection.close()


__all__ = ("SqlClientWriterObserverSession",)

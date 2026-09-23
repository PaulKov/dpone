"""Dedicated injected cursor for independent pregrant SqlClient observations.

Synchronous driver calls must run within externally isolated containment. Checks
before and after each call cannot interrupt a blocked driver. The original
absolute monotonic deadline is never renewed. No connection creation, grants,
transactions, checkpointing, cleanup, retries or settlement inference occur.
"""

import os as os  # Retained module seam for owner monkeypatch compatibility.
import threading as threading
from collections.abc import Callable
from typing import Any, Protocol

from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor, ObservationOwnerMismatch
from dpone.adapters.mssql_sqlclient_observer_sql import ADMISSION_SQL, PRINCIPALS_SQL, VISIBILITY_SQL, WRITER_SQL
from dpone.contracts.mssql_tds_api import observation_contracts


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: Any) -> Any: ...
    def fetchone(self) -> Any: ...


class SqlClientObservationError(RuntimeError):
    """Constant diagnostic; an ambiguous observation permanently poisons reuse."""


class SqlClientWriterObserver:
    """Own one exclusive cursor in one process/thread under one original budget.

    Call preflight before source acquisition and observe after worker nonce
    installation. Preflight is repeated before each observation so revoked
    visibility cannot silently narrow DMV results. Busy state and a permanent
    fault latch have synchronized transitions; no state lock covers driver or
    clock calls. A rejected overlap also invalidates the in-flight operation.
    Root capability admission
    remains independent, including rejecting privileged bulk-worker credentials.
    """

    def __init__(
        self,
        cursor: _Cursor,
        *,
        admission: observation_contracts.SqlClientObserverAdmission,
        operation_deadline_ns: int,
        monotonic_ns: Callable[[], int],
    ) -> None:
        self._initialize(
            cursor,
            admission=admission,
            target_admission=admission,
            operation_deadline_ns=operation_deadline_ns,
            monotonic_ns=monotonic_ns,
        )

    @classmethod
    def for_target(
        cls,
        cursor: _Cursor,
        *,
        admission: observation_contracts.SqlClientObserverAdmission,
        target_admission: observation_contracts.SqlClientObserverAdmission,
        operation_deadline_ns: int,
        monotonic_ns: Callable[[], int],
    ) -> "SqlClientWriterObserver":
        """Build the P10 observer without changing the original constructor contract."""
        subject = cls.__new__(cls)
        subject._initialize(
            cursor,
            admission=admission,
            target_admission=target_admission,
            operation_deadline_ns=operation_deadline_ns,
            monotonic_ns=monotonic_ns,
        )
        return subject

    def _initialize(
        self,
        cursor: _Cursor,
        *,
        admission: observation_contracts.SqlClientObserverAdmission,
        target_admission: observation_contracts.SqlClientObserverAdmission,
        operation_deadline_ns: int,
        monotonic_ns: Callable[[], int],
    ) -> None:
        if type(admission) is not observation_contracts.SqlClientObserverAdmission:
            raise ValueError("mssql_native.sqlclient_observer_admission_invalid")
        if type(target_admission) is not observation_contracts.SqlClientObserverAdmission:
            raise ValueError("mssql_native.sqlclient_observer_admission_invalid")
        admission.__post_init__()
        target_admission.__post_init__()
        if target_admission is not admission and (
            target_admission.server != admission.server
            or target_admission.database != admission.database
            or target_admission.login == admission.login
            or target_admission.login.sid == admission.login.sid
        ):
            raise ValueError("mssql_native.sqlclient_observer_admission_invalid")
        observation_contracts._integer(operation_deadline_ns, 1)
        self._admission = admission
        self._target_admission = target_admission
        self._session = ObservationCursor(cursor, deadline=operation_deadline_ns, clock=monotonic_ns)
        self._lease: object | None = None

    _cursor = property(lambda self: self._session.cursor)
    _deadline = property(lambda self: self._session.deadline)
    _clock = property(lambda self: self._session.clock, lambda self, value: setattr(self._session, "clock", value))
    _owner = property(lambda self: self._session.owner)
    _state_lock = property(lambda self: self._session.lock)
    _busy = property(lambda self: self._session.busy)
    _faulted = property(lambda self: self._session.faulted)

    def _begin(self) -> None:
        try:
            self._lease = self._session.begin()
        except ObservationOwnerMismatch:
            raise SqlClientObservationError("mssql_native.sqlclient_observer_owner_mismatch") from None
        except ValueError:
            raise SqlClientObservationError("mssql_native.sqlclient_observer_poisoned") from None

    def _healthy(self) -> None:
        self._session.healthy(self._lease)

    def _finish(self) -> None:
        self._session.finish(self._lease)

    def _fail(self) -> None:
        self._session.fail(self._lease)

    def _current(self) -> None:
        self._session.current(self._lease)

    def _query(self, sql: str, *parameters: Any) -> list[Any]:
        return self._session.rows(
            self._lease, sql, parameters, max_rows=3, nextset_required=False, detach_rows=False, current=self._current
        )

    def _preflight(self) -> None:
        observation_contracts.validate_visibility(self._query(VISIBILITY_SQL), self._admission)
        observation_contracts.validate_catalog_admission(
            self._query(ADMISSION_SQL, self._admission.database.database_id, self._admission.login.name),
            self._admission,
        )
        observation_contracts.resolve_database_principal(
            self._query(PRINCIPALS_SQL, bytes.fromhex(self._admission.login.sid)), admission=self._admission
        )

    def preflight(self) -> None:
        """Admit DMV visibility/version before any source data acquisition."""
        self._begin()
        try:
            self._preflight()
            self._current()
            self._finish()
        except BaseException as error:
            self._fail()
            if not isinstance(error, Exception):
                raise
            raise SqlClientObservationError("mssql_native.sqlclient_observer_unavailable") from None

    def observe(
        self,
        *,
        session_id: int,
        nonce: bytes,
        expected: observation_contracts.TdsRemoteSessionIdentity | None = None,
    ) -> observation_contracts.SqlClientWriterObservation:
        """Bind locator and exact nonce to admitted facts; optionally recheck incarnation.

        The returned full authority is private evidence. It is neither an
        effective-token proof nor an authorization to send the one-shot grant.
        """
        self._begin()
        try:
            observation_contracts._integer(session_id, 1, 32767)
            observation_contracts.require_session_nonce(nonce)
            self._preflight()
            row = observation_contracts.validate_writer_rows(
                self._query(WRITER_SQL, session_id),
                session_id=session_id,
                nonce=nonce,
                admission=self._target_admission,
            )
            observation = observation_contracts.resolve_writer_observation(
                row,
                self._query(PRINCIPALS_SQL, bytes.fromhex(self._target_admission.login.sid)),
                admission=self._target_admission,
            )
            if expected is not None and (
                type(expected) is not observation_contracts.TdsRemoteSessionIdentity
                or expected != observation.remote_session
            ):
                raise ValueError("continuity")
            self._current()
            self._finish()
        except BaseException as error:
            self._fail()
            if not isinstance(error, Exception):
                raise
            raise SqlClientObservationError("mssql_native.sqlclient_observer_unavailable") from None
        return observation

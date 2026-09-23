"""Deadline-bound SQL Server management-session authority for retirement."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any, Protocol

from dpone.ports.mssql_native_chunk_retirement import NativeChunkRetirementRequest
from dpone.ports.mssql_sqlclient_writer_settlement import (
    LOCK_RESOURCE,
    SqlClientObserverAdmission,
    TdsLockObservation,
)

_ERROR = "mssql_native.sqlclient_retirement_effect_invalid"


class SqlClientRetirementSession(Protocol):
    """One independently admitted management session, owned by one effect."""

    @property
    def admission(self) -> SqlClientObserverAdmission: ...

    def acquire_exclusive(self, resource: str, *, deadline: float) -> TdsLockObservation: ...
    def query(self, sql: str, parameters: tuple[object, ...], *, deadline: float) -> tuple[tuple[Any, ...], ...]: ...
    def execute(self, sql: str, parameters: tuple[object, ...], *, deadline: float) -> None: ...
    def commit(self, *, deadline: float) -> None: ...
    def rollback(self, *, deadline: float) -> None: ...
    def contain(self, *, deadline: float) -> None: ...


OpenManagementSession = Callable[[], AbstractContextManager[SqlClientRetirementSession]]


ValidateCurrentAuthority = Callable[[NativeChunkRetirementRequest], None]


class SqlClientRetirementSessionAuthority:
    """Own one canonical DDL authority and contain every terminal session."""

    def __init__(
        self,
        *,
        open_management_session: OpenManagementSession,
        admission: SqlClientObserverAdmission,
        monotonic: Callable[[], float],
        operation_timeout: float,
    ) -> None:
        self._open = open_management_session
        self._admission = admission
        self._clock = monotonic
        self._timeout = operation_timeout
        self._context: AbstractContextManager[SqlClientRetirementSession] | None = None
        self._session: SqlClientRetirementSession | None = None
        self._deadline: float | None = None
        self._request_sha: str | None = None

    @property
    def deadline(self) -> float:
        """Return the absolute deadline of the currently held authority."""
        if self._deadline is None:
            raise RuntimeError("mssql_native.sqlclient_retirement_deadline")
        return self._deadline

    def acquire(
        self,
        request: NativeChunkRetirementRequest,
        validate_current: ValidateCurrentAuthority,
    ) -> SqlClientRetirementSession:
        """Acquire or reuse one request-bound canonical DDL authority."""
        key = request.projection.projection_sha256
        if self._session is None:
            context = self._open()
            session = context.__enter__()
            deadline = self._clock() + self._timeout
            self._context, self._session, self._deadline, self._request_sha = context, session, deadline, key
            try:
                if session.admission != self._admission:
                    raise ValueError(_ERROR)
                lock = session.acquire_exclusive(LOCK_RESOURCE, deadline=deadline)
                if type(lock) is not TdsLockObservation:
                    raise ValueError(_ERROR)
                lock.__post_init__()
            except BaseException as error:
                self.close(primary=error)
                raise
        if self._request_sha != key or self._deadline is None or self._clock() >= self._deadline:
            raise RuntimeError("mssql_native.sqlclient_retirement_deadline")
        validate_current(request)
        return self._session

    def close(self, *, primary: BaseException | None = None) -> None:
        """Contain and release the held session without replacing a primary error."""
        context, self._context = self._context, None
        session, deadline = self._session, self._deadline
        self._session = self._deadline = self._request_sha = None
        failures: list[BaseException] = [] if primary is None else [primary]
        if session is not None and deadline is not None:
            try:
                session.contain(deadline=deadline)
            except BaseException as error:
                failures.append(error)
        if context is not None:
            try:
                context.__exit__(
                    type(primary) if primary is not None else None,
                    primary,
                    primary.__traceback__ if primary is not None else None,
                )
            except BaseException as error:
                failures.append(error)
        if len(failures) <= (1 if primary is not None else 0):
            return
        if primary is not None:
            for failure in failures[1:]:
                primary.add_note(f"retirement session containment failed: {type(failure).__name__}")
            return
        if len(failures) == 1:
            raise failures[0]
        raise BaseExceptionGroup("mssql_native.sqlclient_retirement_containment_failed", failures)


__all__ = (
    "OpenManagementSession",
    "SqlClientRetirementSession",
    "SqlClientRetirementSessionAuthority",
)

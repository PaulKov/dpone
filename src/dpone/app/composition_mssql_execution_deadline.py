"""Compose resolved MSSQL connectors with one request's absolute I/O budget.

Timeouts are integer seconds, capped at ten and rounded down; less than one
second cannot admit ordinary I/O. In pyodbc 5.3, connection.timeout updates the
connection attribute, but statement query timeouts are copied only at cursor
creation. Existing statement execution/fetch and SQLEndTran (commit/rollback)
therefore remain subject to driver limitations. Pre/post checks reject late
results; they do not cancel driver work. The caller retains its concurrency slot
until I/O unwinds and reconciles uncertain commits through the durable journal.

Rollback and close always attempt cleanup, even after expiry or timeout-setting
failure. They neither start a new budget nor create a connection. Their errors
remain available to existing lifecycle callers, which preserve the primary error.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from typing import Any, Protocol, TypeVar

from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.runtime_connection import ResolvedBindingConnection
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.state.mssql_database_authority_support import bounded_authority_connection

_T = TypeVar("_T")
_DRIVER_TIMEOUT_SECONDS = 10


class _ConnectorFactory(Protocol):
    def __call__(self, connection: ResolvedBindingConnection, *, autocommit: bool) -> Any: ...


class _Budget:
    def __init__(self, deadline: Callable[[], float], clock: Callable[[], float], execution: Callable[[], None] | None):
        self.deadline = deadline
        self.clock = clock
        self.execution = execution

    def start(self) -> tuple[float, int]:
        if self.execution is not None:
            self.execution()
        deadline = self.deadline()
        remaining = deadline - self.clock()
        if not math.isfinite(remaining) or remaining < 1:
            raise CompositionAdmissionError("mssql_execution_deadline")
        return deadline, min(_DRIVER_TIMEOUT_SECONDS, math.floor(remaining))

    def finish(self, deadline: float) -> None:
        if self.execution is not None:
            self.execution()
        remaining = min(deadline, self.deadline()) - self.clock()
        if not math.isfinite(remaining) or remaining <= 0:
            raise CompositionAdmissionError("mssql_execution_deadline")


class BudgetedMssqlConnectorFactory:
    """Inject into control verification or source construction, without discovery.

    ``io_deadline`` supplies an absolute monotonic deadline, including an already
    established cleanup phase when appropriate. Source-only construction also
    supplies ``require_execution`` so cleanup can never admit a source replay.
    No connector is constructed until its connection property is first accessed.
    """

    def __init__(
        self,
        connector_factory: _ConnectorFactory = ResolvedConnectorFactory.create,
        *,
        io_deadline: Callable[[], float],
        clock: Callable[[], float] = time.monotonic,
        require_execution: Callable[[], None] | None = None,
    ) -> None:
        self._factory = connector_factory
        self._budget = _Budget(io_deadline, clock, require_execution)

    def __call__(self, connection: ResolvedBindingConnection, *, autocommit: bool = True) -> _Connector:
        descriptor = connection.descriptor
        if descriptor is None or descriptor.connection_type.strip().lower() != "mssql":
            raise CompositionAdmissionError("mssql_execution_connector")
        return _Connector(self._factory, connection, autocommit, self._budget)


class _Connector:
    def __init__(
        self, factory: _ConnectorFactory, resolved: ResolvedBindingConnection, autocommit: bool, budget: _Budget
    ):
        self._factory, self._resolved, self._autocommit, self._budget = factory, resolved, autocommit, budget
        self._connection: _Connection | None = None
        self._attempted = False
        self._closed = False

    @property
    def connection(self) -> _Connection:
        if self._closed:
            raise CompositionAdmissionError("mssql_execution_connection_closed")
        if self._connection is not None:
            return self._connection
        if self._attempted:
            raise CompositionAdmissionError("mssql_execution_connection_unknown")
        self._attempted = True
        connector = None
        try:
            deadline, seconds = self._budget.start()
            resolved = bounded_authority_connection(self._resolved, connect_timeout_seconds=seconds)
            connector = self._factory(resolved, autocommit=self._autocommit)
            raw = connector.connection
            self._budget.finish(deadline)
            self._connection = _Connection(raw, connector.close, self._budget, resolved.credentials.query_timeout)
            return self._connection
        except BaseException:
            if connector is not None:
                try:
                    connector.close()
                except Exception:
                    pass
            raise

    def close(self) -> None:
        self._closed = True
        if self._connection is not None:
            self._connection.close()


class _Connection:
    def __init__(self, raw: Any, close: Callable[[], None], budget: _Budget, configured_timeout: int | None):
        self._raw, self._close, self._budget = raw, close, budget
        self._closed = False
        self._cap = min(
            [_DRIVER_TIMEOUT_SECONDS]
            + [value for value in (configured_timeout, raw.timeout) if type(value) is int and value > 0]
        )

    def _start(self) -> float:
        if self._closed:
            raise CompositionAdmissionError("mssql_execution_connection_closed")
        deadline, seconds = self._budget.start()
        self._raw.timeout = min(self._cap, seconds)
        self._budget.finish(deadline)
        return deadline

    def run(self, operation: Callable[[], _T]) -> _T:
        deadline = self._start()
        result = operation()
        self._budget.finish(deadline)
        return result

    def cleanup(self, operation: Callable[[], None]) -> None:
        # A failed timeout request must not prevent rollback/disconnect attempts.
        try:
            remaining = self._budget.deadline() - self._budget.clock()
            seconds = max(1, math.floor(remaining)) if math.isfinite(remaining) else 1
            self._raw.timeout = min(self._cap, seconds)
        except Exception:
            pass
        operation()

    @property
    def autocommit(self) -> bool:
        return bool(self._raw.autocommit)

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self.run(lambda: setattr(self._raw, "autocommit", value))

    def cursor(self) -> _Cursor:
        deadline = self._start()
        raw = self._raw.cursor()
        try:
            self._budget.finish(deadline)
        except BaseException:
            try:
                self.cleanup(raw.close)
            except Exception:
                pass
            raise
        return _Cursor(raw, self)

    def commit(self) -> None:
        self.run(self._raw.commit)

    def rollback(self) -> None:
        self.cleanup(self._raw.rollback)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self.cleanup(self._close)


class _Cursor:
    def __init__(self, raw: Any, connection: _Connection):
        self._raw, self._connection = raw, connection
        self._closed = False

    def _run(self, operation: Callable[[], _T]) -> _T:
        if self._closed:
            raise CompositionAdmissionError("mssql_execution_cursor_closed")
        return self._connection.run(operation)

    def execute(self, sql: str, *parameters: object) -> _Cursor:
        self._run(lambda: self._raw.execute(sql, *parameters))
        return self

    def fetchone(self) -> Any:
        return self._run(self._raw.fetchone)

    def fetchmany(self, size: int | None = None) -> Any:
        return self._run(self._raw.fetchmany if size is None else lambda: self._raw.fetchmany(size))

    def fetchall(self) -> Any:
        return self._run(self._raw.fetchall)

    @property
    def description(self) -> Any:
        return self._raw.description

    @property
    def rowcount(self) -> int:
        return int(self._raw.rowcount)

    @property
    def arraysize(self) -> int:
        return int(self._raw.arraysize)

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._raw.arraysize = value

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._connection.cleanup(self._raw.close)

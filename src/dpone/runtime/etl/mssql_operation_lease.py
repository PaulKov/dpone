"""Runtime-owned heartbeat for a finite generic MSSQL operation lease."""

from __future__ import annotations

from datetime import datetime, timezone
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission


LEASE_OPTION = "__dpone_mssql_transaction_lease"
_UTC = timezone.utc  # noqa: UP017 - mypy target may be older than datetime.UTC.


class MssqlOperationLeaseHeartbeatError(RuntimeError):
    """Lease renewal failed before the target transaction could fence it."""

    code = "mssql_transaction.operation_lease_heartbeat_lost"

    def __init__(self) -> None:
        super().__init__(self.code)


class MssqlOperationLeaseControllerError(RuntimeError):
    """An admission-injected runtime controller does not implement the lease port."""

    code = "mssql_transaction.operation_lease_controller_invalid"

    def __init__(self) -> None:
        super().__init__(self.code)


MssqlOperationLeaseControllerContractError = MssqlOperationLeaseControllerError


@runtime_checkable
class MssqlOperationLeaseController(Protocol):
    """Idempotent lifecycle shared by thread and process-safe controllers."""

    def start(self) -> None: ...

    def assert_healthy(self) -> None: ...

    def stop(self) -> None: ...


class MssqlOperationLeaseHeartbeat:
    """Renew a finite owner lease through independent state sessions."""

    def __init__(self, state: Any, admission: MssqlTransactionAdmission) -> None:
        operation = admission.operation
        if operation is None or operation.lease_expires_at_utc is None:
            raise ValueError("mssql_transaction.finite_operation_lease_required")
        now = datetime.now(_UTC)
        ttl = operation.lease_expires_at_utc - now
        if ttl.total_seconds() <= 0:
            raise MssqlOperationLeaseHeartbeatError()
        self._state = state
        self._operation = operation
        self._ttl = ttl
        self._interval = min(30.0, max(0.1, ttl.total_seconds() / 3.0))
        self._stop = Event()
        self._failed = Event()
        self._thread = Thread(target=self._run, name="dpone-mssql-operation-lease", daemon=True)
        self._start_called = False

    def start(self) -> None:
        if self._start_called:
            return
        self._start_called = True
        try:
            self._thread.start()
        except BaseException:
            self._start_called = False
            raise

    def stop(self) -> None:
        if not self._start_called:
            return
        self._stop.set()
        self._thread.join(timeout=max(1.0, self._interval + 1.0))

    def assert_healthy(self) -> None:
        if self._failed.is_set():
            raise MssqlOperationLeaseHeartbeatError()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                active = self._state.renew_operation_lease(
                    self._operation,
                    lease_expires_at_utc=datetime.now(_UTC) + self._ttl,
                )
            except Exception:  # The processor observes only a stable typed error.
                self._failed.set()
                return
            if not active:
                self._stop.set()
                return


def lease_from_config(load_config: Any) -> MssqlOperationLeaseController | None:
    """Return a complete injected controller and reject silent lease loss."""

    value = (getattr(load_config, "options", {}) or {}).get(LEASE_OPTION)
    if value is None:
        return None
    lifecycle = ("start", "assert_healthy", "stop")
    if not isinstance(value, MssqlOperationLeaseController) or not all(
        callable(getattr(value, method, None)) for method in lifecycle
    ):
        raise MssqlOperationLeaseControllerError()
    return value


__all__ = [
    "LEASE_OPTION",
    "MssqlOperationLeaseController",
    "MssqlOperationLeaseControllerContractError",
    "MssqlOperationLeaseControllerError",
    "MssqlOperationLeaseHeartbeat",
    "MssqlOperationLeaseHeartbeatError",
    "lease_from_config",
]

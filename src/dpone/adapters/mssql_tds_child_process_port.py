"""Injected Linux process operations used by child-process custody."""

from __future__ import annotations

from typing import Any, Protocol

from dpone.adapters.mssql_tds_process import LinuxTdsProcess, TdsProcessError


class TdsChildProcessPort(Protocol):
    """Narrow process boundary required by the custody executor."""

    def acquire(self, identity: Any) -> object: ...

    def is_native_handle(self, handle: object) -> bool: ...

    def settle_effective(
        self,
        handle: object,
        *,
        deadline: float,
        direct_child: bool,
        kill: bool,
        current_deadline: Any,
    ) -> Any: ...

    def permits_forced_transition(self, settlement: Any, error: BaseException, handle: object | None) -> bool: ...


class LinuxTdsChildProcessPort:
    """Thin adapter around the admitted Linux pidfd implementation."""

    def acquire(self, identity: Any) -> LinuxTdsProcess:
        return LinuxTdsProcess.acquire(identity)

    def is_native_handle(self, handle: object) -> bool:
        return isinstance(handle, LinuxTdsProcess)

    def settle_effective(self, handle: object, **arguments: Any) -> object:
        assert isinstance(handle, LinuxTdsProcess)
        return handle._settle_effective(**arguments)

    def permits_forced_transition(self, settlement: Any, error: BaseException, handle: object | None) -> bool:
        return (
            isinstance(error, TdsProcessError)
            and isinstance(handle, LinuxTdsProcess)
            and settlement.permit_forced_transition(
                error_args=error.args,
                reap_consumed=handle._consumed_reap is not None,
                handle_unknown=handle._unknown,
            )
        )


LINUX_TDS_CHILD_PROCESS = LinuxTdsChildProcessPort()

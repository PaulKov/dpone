"""Bounded process-tree cleanup for dbt subprocess adapters."""

from __future__ import annotations

import math
import os
import signal
import subprocess
import time
from collections.abc import Callable
from typing import BinaryIO, Protocol

DEFAULT_TERMINATE_GRACE_SECONDS = 5.0
DEFAULT_KILL_GRACE_SECONDS = 5.0
_POLL_INTERVAL_SECONDS = 0.01


class ManagedProcess(Protocol):
    """Minimum subprocess lifecycle used by the supervisor."""

    pid: int
    stdout: BinaryIO | None
    stderr: BinaryIO | None

    def wait(self, timeout: float | None = None) -> int: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...


class ProcessSupervisionError(RuntimeError):
    """Raised when a process tree cannot be stopped within its deadline."""


class DbtProcessSupervisor:
    """Stop a POSIX process group or a non-POSIX parent within fixed bounds."""

    def __init__(
        self,
        *,
        posix: bool | None = None,
        terminate_grace_seconds: float = DEFAULT_TERMINATE_GRACE_SECONDS,
        kill_grace_seconds: float = DEFAULT_KILL_GRACE_SECONDS,
        kill_process_group: Callable[[int, int], None] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._posix = os.name == "posix" if posix is None else _require_bool(posix, "posix")
        self._terminate_grace_seconds = _require_positive_duration(
            terminate_grace_seconds,
            "terminate_grace_seconds",
        )
        self._kill_grace_seconds = _require_positive_duration(
            kill_grace_seconds,
            "kill_grace_seconds",
        )
        self._kill_process_group = _kill_process_group if kill_process_group is None else kill_process_group
        self._monotonic = monotonic
        self._sleep = sleep

    @property
    def start_new_session(self) -> bool:
        """Whether Popen must isolate the command in a new process session."""

        return self._posix

    def terminate(self, process: ManagedProcess) -> None:
        """Terminate all owned processes and reap the direct child."""

        if self._posix:
            self._terminate_posix_group(process)
            return
        self._terminate_parent(process)

    def _terminate_posix_group(self, process: ManagedProcess) -> None:
        process_group_id = getattr(process, "pid", None)
        if isinstance(process_group_id, bool) or not isinstance(process_group_id, int) or process_group_id <= 0:
            raise ProcessSupervisionError("dbt process group identity is unavailable")

        self._signal_group(process_group_id, signal.SIGTERM)
        if not self._wait_for_process_group(
            process,
            process_group_id,
            self._terminate_grace_seconds,
        ):
            self._signal_group(process_group_id, signal.SIGKILL)
            if not self._wait_for_process_group(
                process,
                process_group_id,
                self._kill_grace_seconds,
            ):
                raise ProcessSupervisionError("dbt process group did not terminate")

    def _terminate_parent(self, process: ManagedProcess) -> None:
        try:
            process.terminate()
        except (OSError, ValueError) as exc:
            raise ProcessSupervisionError("dbt process could not be terminated") from exc
        if self._wait_for_parent(process, self._terminate_grace_seconds):
            return
        try:
            process.kill()
        except (OSError, ValueError) as exc:
            raise ProcessSupervisionError("dbt process could not be killed") from exc
        if not self._wait_for_parent(process, self._kill_grace_seconds):
            raise ProcessSupervisionError("dbt process did not terminate")

    def _wait_for_parent(self, process: ManagedProcess, timeout_seconds: float) -> bool:
        try:
            process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            return False
        except (OSError, ValueError) as exc:
            raise ProcessSupervisionError("dbt process state could not be checked") from exc
        return True

    def _wait_for_process_group(
        self,
        process: ManagedProcess,
        process_group_id: int,
        timeout_seconds: float,
    ) -> bool:
        deadline = self._monotonic() + timeout_seconds
        parent_stopped = self._wait_for_parent(process, timeout_seconds)
        remaining = max(0.0, deadline - self._monotonic())
        group_stopped = self._wait_for_group_exit(process_group_id, remaining)
        return parent_stopped and group_stopped

    def _signal_group(self, process_group_id: int, signal_number: int) -> None:
        try:
            self._kill_process_group(process_group_id, signal_number)
        except ProcessLookupError:
            return
        except (OSError, PermissionError) as exc:
            raise ProcessSupervisionError("dbt process group could not be signalled") from exc

    def _wait_for_group_exit(self, process_group_id: int, timeout_seconds: float) -> bool:
        deadline = self._monotonic() + timeout_seconds
        while self._group_exists(process_group_id):
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                return False
            self._sleep(min(_POLL_INTERVAL_SECONDS, remaining))
        return True

    def _group_exists(self, process_group_id: int) -> bool:
        try:
            self._kill_process_group(process_group_id, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError as exc:
            raise ProcessSupervisionError("dbt process group state could not be checked") from exc
        return True


def _require_bool(value: bool, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be boolean")
    return value


def _require_positive_duration(value: float, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{field_name} must be a positive number")
    return float(value)


def _kill_process_group(process_group_id: int, signal_number: int) -> None:
    killpg = getattr(os, "killpg", None)
    if killpg is None:
        raise ProcessSupervisionError("POSIX process-group signalling is unavailable")
    killpg(process_group_id, signal_number)


__all__ = [
    "DEFAULT_KILL_GRACE_SECONDS",
    "DEFAULT_TERMINATE_GRACE_SECONDS",
    "DbtProcessSupervisor",
    "ProcessSupervisionError",
]

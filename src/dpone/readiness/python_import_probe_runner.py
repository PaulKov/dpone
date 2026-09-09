"""Bounded platform process runners for Python import probes."""

from __future__ import annotations

import errno
import os
import queue
import signal
import subprocess
import threading
import time
from collections.abc import Mapping, Sequence
from typing import BinaryIO

_QUIESCENCE_TIMEOUT_SECONDS = 2.0


class ProbeProcessCleanupError(OSError):
    """The probe child returned but its owned process scope was not quiescent."""


class ContainedProcessResult(subprocess.CompletedProcess[bytes]):
    """Completed child result with trusted process-communication accounting."""

    def __init__(
        self,
        args: Sequence[str],
        returncode: int,
        *,
        communication_elapsed_seconds: float,
    ) -> None:
        super().__init__(tuple(args), returncode)
        self.communication_elapsed_seconds = communication_elapsed_seconds


def run_contained_import_process(
    command: Sequence[str],
    *,
    check: bool,
    input: bytes,
    stdout: int,
    stderr: int,
    timeout: float,
    env: Mapping[str, str],
    cwd: str,
) -> ContainedProcessResult:
    """Run one probe with the strongest bounded containment for this platform."""

    if check:
        raise OSError
    if os.name == "posix":
        return _run_posix_process(
            command,
            input=input,
            stdout=stdout,
            stderr=stderr,
            timeout=timeout,
            env=env,
            cwd=cwd,
        )
    if os.name == "nt":
        return _run_windows_direct_process(
            command,
            input=input,
            stdout=stdout,
            stderr=stderr,
            timeout=timeout,
            env=env,
            cwd=cwd,
        )
    raise OSError


def _run_posix_process(
    command: Sequence[str],
    *,
    input: bytes,
    stdout: int,
    stderr: int,
    timeout: float,
    env: Mapping[str, str],
    cwd: str,
) -> ContainedProcessResult:
    if not hasattr(os, "killpg"):
        raise OSError
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        stdin=subprocess.PIPE,
        stdout=stdout,
        stderr=stderr,
        start_new_session=True,
    )
    communication_error: BaseException | None = None
    communication_started = time.monotonic()
    try:
        process.communicate(input=input, timeout=timeout)
    except BaseException as exc:
        communication_error = exc
    communication_elapsed = max(0.0, time.monotonic() - communication_started)
    try:
        _quiesce_posix_process_group(process)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ProbeProcessCleanupError from exc
    if communication_error is not None:
        raise communication_error
    return ContainedProcessResult(
        command,
        process.returncode,
        communication_elapsed_seconds=communication_elapsed,
    )


def _quiesce_posix_process_group(process: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + _QUIESCENCE_TIMEOUT_SECONDS
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except OSError:
        process.kill()
        process.wait(timeout=_remaining_cleanup_time(deadline))
        raise
    process.wait(timeout=_remaining_cleanup_time(deadline))
    while True:
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            return
        remaining = _remaining_cleanup_time(deadline)
        time.sleep(min(0.01, remaining))


def _remaining_cleanup_time(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise subprocess.TimeoutExpired(("python-import-probe",), _QUIESCENCE_TIMEOUT_SECONDS)
    return remaining


def _run_windows_direct_process(
    command: Sequence[str],
    *,
    input: bytes,
    stdout: int,
    stderr: int,
    timeout: float,
    env: Mapping[str, str],
    cwd: str,
) -> ContainedProcessResult:
    """Bound one Windows child without claiming descendant containment."""

    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        stdin=subprocess.PIPE,
        stdout=stdout,
        stderr=stderr,
    )
    input_errors: queue.SimpleQueue[BaseException] = queue.SimpleQueue()
    input_writer: threading.Thread | None = None
    communication_error: BaseException | None = None
    communication_started = time.monotonic()
    communication_deadline = communication_started + timeout
    try:
        input_writer = threading.Thread(
            target=_write_process_input,
            args=(process.stdin, input, input_errors),
            name="dpone-python-import-probe-stdin",
            daemon=True,
        )
        input_writer.start()
        remaining = communication_deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(tuple(command), timeout)
        process.wait(timeout=remaining)
        if input_writer.is_alive():
            input_writer.join(timeout=max(0.0, communication_deadline - time.monotonic()))
        if input_writer.is_alive():
            raise subprocess.TimeoutExpired(tuple(command), timeout)
        if not input_errors.empty():
            raise input_errors.get_nowait()
    except BaseException as exc:
        communication_error = exc
    communication_elapsed = max(0.0, time.monotonic() - communication_started)
    if communication_error is not None:
        _quiesce_windows_transport(process, input_writer)
        raise communication_error
    if process.returncode is None:
        _quiesce_windows_transport(process, input_writer)
        raise ProbeProcessCleanupError
    return ContainedProcessResult(
        command,
        process.returncode,
        communication_elapsed_seconds=communication_elapsed,
    )


def _write_process_input(
    stream: BinaryIO | None,
    payload: bytes,
    errors: queue.SimpleQueue[BaseException],
) -> None:
    """Write stdin off-thread so Windows pipe backpressure remains bounded."""

    if stream is None:
        errors.put(OSError("probe stdin pipe is unavailable"))
        return
    try:
        stream.write(payload)
    except BrokenPipeError:
        pass
    except OSError as exc:
        if exc.errno != errno.EINVAL:
            errors.put(exc)
    except BaseException as exc:
        errors.put(exc)
    finally:
        try:
            stream.close()
        except BrokenPipeError:
            pass
        except OSError as exc:
            if exc.errno != errno.EINVAL:
                errors.put(exc)
        except BaseException as exc:
            errors.put(exc)


def _quiesce_windows_transport(
    process: subprocess.Popen[bytes],
    input_writer: threading.Thread | None,
) -> None:
    """Bound direct-child termination and stdin-writer shutdown together."""

    deadline = time.monotonic() + _QUIESCENCE_TIMEOUT_SECONDS
    cleanup_error: BaseException | None = None
    if process.returncode is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass
        except OSError as exc:
            cleanup_error = exc
        try:
            process.wait(timeout=_remaining_cleanup_time(deadline))
        except (OSError, subprocess.TimeoutExpired) as exc:
            cleanup_error = cleanup_error or exc
    if input_writer is not None and input_writer.is_alive():
        try:
            input_writer.join(timeout=_remaining_cleanup_time(deadline))
        except subprocess.TimeoutExpired as exc:
            cleanup_error = cleanup_error or exc
    if input_writer is not None and input_writer.is_alive() and cleanup_error is None:
        cleanup_error = subprocess.TimeoutExpired(
            ("python-import-probe-stdin",),
            _QUIESCENCE_TIMEOUT_SECONDS,
        )
    if input_writer is None or not input_writer.is_alive():
        try:
            if process.stdin is not None:
                process.stdin.close()
        except BrokenPipeError:
            pass
        except OSError as exc:
            if exc.errno != errno.EINVAL:
                cleanup_error = cleanup_error or exc
    if cleanup_error is not None:
        raise ProbeProcessCleanupError from cleanup_error


__all__ = [
    "ContainedProcessResult",
    "ProbeProcessCleanupError",
    "run_contained_import_process",
]

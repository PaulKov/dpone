"""Bounded POSIX process supervision for the SqlClient companion."""

from __future__ import annotations

import os
import re
import signal
import subprocess
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from threading import Thread
from typing import IO

from dpone.ports.mssql_native import RESULT_MAX_BYTES, NativeStageWriteRequest, OperationDeadline, encode_request_frame

_READ_SIZE = 64 * 1024
_MIN_WORK_SECONDS = 0.001
_DIAGNOSTIC = re.compile(rb"mssql_sqlclient\.[a-z0-9_]+\Z")


@dataclass(frozen=True, slots=True)
class CompanionProcessResult:
    """Closed, non-sensitive observation from one supervised child process."""

    classification: str
    stdout: bytes = b""
    exit_code: int | None = None
    diagnostic_code: str | None = None


class _BoundedReader:
    def __init__(self, stream: IO[bytes], limit: int) -> None:
        self._stream = stream
        self._limit = limit
        self._chunks: list[bytes] = []
        self._size = 0
        self.overflow = False
        self.failed = False
        self.thread = Thread(target=self._run, daemon=True)

    @property
    def value(self) -> bytes:
        return b"".join(self._chunks)

    def _run(self) -> None:
        try:
            while block := self._stream.read(_READ_SIZE):
                room = self._limit - self._size
                if room > 0:
                    kept = block[:room]
                    self._chunks.append(kept)
                    self._size += len(kept)
                if len(block) > room:
                    self.overflow = True
        except Exception:
            self.failed = True
        finally:
            try:
                self._stream.close()
            except Exception:
                self.failed = True


class _FrameWriter:
    def __init__(self, stream: IO[bytes], frame: bytes | bytearray) -> None:
        self._stream = stream
        self._frame = frame
        self.failed = False
        self.thread = Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        try:
            self._stream.write(self._frame)
            self._stream.flush()
        except Exception:
            self.failed = True
        finally:
            try:
                self._stream.close()
            except Exception:
                self.failed = True


def run_sqlclient_companion(
    command: Sequence[str],
    *,
    request: NativeStageWriteRequest,
    credential_frame: bytearray,
    deadline: OperationDeadline,
    cleanup_reserve_seconds: float,
    clock: Callable[[], float] = time.monotonic,
    process_started: Callable[[int, NativeStageWriteRequest], None] | None = None,
) -> CompanionProcessResult:
    """Run one child with bounded pipes and retain time to kill its process group."""
    if (
        os.name != "posix"
        or not command
        or any(type(part) is not str or not part or "\x00" in part for part in command)
    ):
        _clear(credential_frame)
        return CompanionProcessResult("custody_lost")
    read_fd = -1
    write_fd = -1
    process: subprocess.Popen[bytes] | None = None
    readers: tuple[_BoundedReader, ...] = ()
    writers: tuple[_FrameWriter, ...] = ()
    try:
        remaining = deadline.remaining_seconds()
        work_seconds = remaining - cleanup_reserve_seconds
        if work_seconds < _MIN_WORK_SECONDS:
            return CompanionProcessResult("timeout")
        budget_ms = max(1, min(2_147_483_647, int(work_seconds * 1000)))
        request_frame = encode_request_frame(request, deadline_budget_ms=budget_ms)
        work_deadline = clock() + budget_ms / 1000.0
        read_fd, write_fd = os.pipe()
        command_line = (*command, "--secret-fd", str(read_fd))
        process = subprocess.Popen(  # noqa: S603 - exact injected argv, no shell.
            command_line,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
            pass_fds=(read_fd,),
            start_new_session=True,
            env=_minimal_environment(),
        )
        if process_started is not None:
            process_started(process.pid, request)
        os.close(read_fd)
        read_fd = -1
        assert process.stdin is not None and process.stdout is not None and process.stderr is not None
        secret_stream = os.fdopen(write_fd, "wb", closefd=True)
        write_fd = -1
        stdout_reader = _BoundedReader(process.stdout, RESULT_MAX_BYTES + 4)
        stderr_reader = _BoundedReader(process.stderr, RESULT_MAX_BYTES)
        request_writer = _FrameWriter(process.stdin, request_frame)
        secret_writer = _FrameWriter(secret_stream, credential_frame)
        readers = (stdout_reader, stderr_reader)
        writers = (request_writer, secret_writer)
        for worker in (*readers, *writers):
            worker.thread.start()
        try:
            exit_code = process.wait(timeout=_positive_wait(work_deadline, clock))
        except (subprocess.TimeoutExpired, TimeoutError):
            if not _terminate_group(process, deadline=deadline, clock=clock):
                return CompanionProcessResult("cleanup_failed")
            if not _join_workers((*readers, *writers), deadline=deadline, clock=clock):
                return CompanionProcessResult("cleanup_failed")
            return CompanionProcessResult("timeout")
        if not _join_workers((*readers, *writers), deadline=deadline, clock=clock):
            if not _terminate_group(process, deadline=deadline, clock=clock):
                return CompanionProcessResult("cleanup_failed")
            return CompanionProcessResult("cleanup_failed")
        if any(worker.failed for worker in (*readers, *writers)):
            return CompanionProcessResult("lost_ack", exit_code=exit_code)
        if any(reader.overflow for reader in readers):
            return CompanionProcessResult("lost_ack", exit_code=exit_code)
        diagnostic_code = _safe_diagnostic(stderr_reader.value)
        if exit_code != 0:
            return CompanionProcessResult("lost_ack", exit_code=exit_code, diagnostic_code=diagnostic_code)
        return CompanionProcessResult(
            "completed",
            stdout=stdout_reader.value,
            exit_code=exit_code,
            diagnostic_code=diagnostic_code,
        )
    except TimeoutError:
        if process is not None and not _terminate_group(process, deadline=deadline, clock=clock):
            return CompanionProcessResult("cleanup_failed")
        return CompanionProcessResult("timeout")
    except Exception:
        if process is not None and not _terminate_group(process, deadline=deadline, clock=clock):
            return CompanionProcessResult("cleanup_failed")
        return CompanionProcessResult("custody_lost")
    finally:
        _close_descriptor(read_fd)
        _close_descriptor(write_fd)
        _clear(credential_frame)


def _minimal_environment() -> dict[str, str]:
    return {
        "PATH": os.defpath,
        "DOTNET_NOLOGO": "1",
        "DOTNET_CLI_TELEMETRY_OPTOUT": "1",
    }


def _positive_wait(expires_at: float, clock: Callable[[], float]) -> float:
    remaining = expires_at - clock()
    if remaining <= 0:
        raise TimeoutError
    return remaining


def _join_workers(
    workers: Sequence[_BoundedReader | _FrameWriter],
    *,
    deadline: OperationDeadline,
    clock: Callable[[], float],
) -> bool:
    for worker in workers:
        try:
            worker.thread.join(timeout=min(0.1, deadline.remaining_seconds()))
        except TimeoutError:
            return False
        while worker.thread.is_alive():
            try:
                worker.thread.join(timeout=min(0.1, deadline.remaining_seconds()))
            except TimeoutError:
                return False
    return True


def _terminate_group(
    process: subprocess.Popen[bytes],
    *,
    deadline: OperationDeadline,
    clock: Callable[[], float],
) -> bool:
    if process.poll() is not None:
        return True
    for signal_number in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, signal_number)
        except ProcessLookupError:
            pass
        except OSError:
            return False
        try:
            process.wait(timeout=min(0.1, deadline.remaining_seconds()))
            return True
        except subprocess.TimeoutExpired:
            continue
        except TimeoutError:
            return False
    try:
        process.wait(timeout=_positive_wait(deadline.expires_at_monotonic, clock))
    except (subprocess.TimeoutExpired, TimeoutError):
        return False
    return True


def _close_descriptor(descriptor: int) -> None:
    if descriptor < 0:
        return
    try:
        os.close(descriptor)
    except OSError:
        pass


def _clear(value: bytearray) -> None:
    value[:] = b"\x00" * len(value)


def _safe_diagnostic(value: bytes) -> str | None:
    """Project only the companion's bounded stable token, never arbitrary stderr."""
    if _DIAGNOSTIC.fullmatch(value) is None:
        return None
    return value.decode("ascii")


__all__ = ["CompanionProcessResult", "run_sqlclient_companion"]

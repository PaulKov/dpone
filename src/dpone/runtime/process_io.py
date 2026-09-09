"""Small process pipe drainers for long-running streaming subprocesses."""

from __future__ import annotations

import os
import subprocess
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from threading import Thread
from typing import IO, Any

LineCallback = Callable[[str], None]
DEFAULT_PROCESS_ABORT_TIMEOUT_SECONDS = 5.0
DEFAULT_FIFO_POLL_INTERVAL_SECONDS = 0.01


class ProcessAbortError(RuntimeError):
    """Raised when a subprocess remains unreaped after terminate and kill."""

    code = "process_abort_unreaped"

    def __init__(self, *, timeout_seconds: float) -> None:
        self.timeout_seconds = timeout_seconds
        super().__init__(f"{self.code}: timeout_seconds={timeout_seconds}")


class ProcessCleanupError(RuntimeError):
    """Redacted process-cleanup failure safe to propagate across artifact boundaries."""

    code = "process_cleanup_failed"

    def __init__(self, *, operation: str, cause_type: str) -> None:
        self.operation = operation
        self.cause_type = cause_type
        super().__init__(f"{self.code}: operation={operation}; cause_type={cause_type}")


class ProcessOutputDrainTimeout(RuntimeError):
    """Raised when process output readers remain alive after a bounded join."""

    code = "process_output_drain_timeout"

    def __init__(self, *, timeout_seconds: float | None, active_readers: int) -> None:
        self.timeout_seconds = timeout_seconds
        self.active_readers = active_readers
        super().__init__(f"{self.code}: timeout_seconds={timeout_seconds}; active_readers={active_readers}")


@dataclass
class ProcessOutputCapture:
    """Captured text output drained from subprocess pipes."""

    stdout: str = ""
    stderr: str = ""


class ProcessOutputDrainer:
    """Drain stdout/stderr concurrently so producer processes cannot deadlock."""

    def __init__(
        self,
        process: Any,
        *,
        stdout_callback: LineCallback | None = None,
        stderr_callback: LineCallback | None = None,
    ) -> None:
        self._stdout: list[str] = []
        self._stderr: list[str] = []
        self._threads: list[Thread] = []
        self._add_reader(getattr(process, "stdout", None), self._stdout, stdout_callback)
        self._add_reader(getattr(process, "stderr", None), self._stderr, stderr_callback)

    def start(self) -> None:
        for thread in self._threads:
            thread.start()

    def join(self, timeout: float | None = None) -> ProcessOutputCapture:
        for thread in self._threads:
            thread.join(timeout=timeout)
        active_readers = sum(thread.is_alive() for thread in self._threads)
        if active_readers:
            raise ProcessOutputDrainTimeout(
                timeout_seconds=timeout,
                active_readers=active_readers,
            )
        return ProcessOutputCapture(stdout="".join(self._stdout), stderr="".join(self._stderr))

    def _add_reader(
        self,
        stream: IO[bytes] | IO[str] | None,
        sink: list[str],
        callback: LineCallback | None,
    ) -> None:
        if stream is None:
            return
        self._threads.append(Thread(target=self._read_stream, args=(stream, sink, callback), daemon=True))

    @staticmethod
    def _read_stream(stream: IO[bytes] | IO[str], sink: list[str], callback: LineCallback | None) -> None:
        try:
            for raw_line in stream:
                line = ProcessOutputDrainer._decode_line(raw_line)
                sink.append(line)
                if callback is not None:
                    callback(line.rstrip("\r\n"))
        finally:
            close = getattr(stream, "close", None)
            if callable(close):
                close()

    @staticmethod
    def _decode_line(line: bytes | str) -> str:
        if isinstance(line, bytes):
            return line.decode("utf-8", errors="replace")
        return line


def iter_fifo_bytes(
    fifo_path: Path,
    process: Any,
    *,
    read_buffer_bytes: int,
    poll_interval_seconds: float = DEFAULT_FIFO_POLL_INTERVAL_SECONDS,
) -> Iterator[bytes]:
    """Read a FIFO without blocking forever when its producer exits before opening it."""

    descriptor = os.open(fifo_path, os.O_RDONLY | os.O_NONBLOCK)
    try:
        while True:
            try:
                chunk = os.read(descriptor, read_buffer_bytes)
            except BlockingIOError:
                chunk = None
            if chunk:
                yield chunk
                continue
            if process.poll() is not None:
                try:
                    final_chunk = os.read(descriptor, read_buffer_bytes)
                except BlockingIOError:
                    final_chunk = None
                if final_chunk:
                    yield final_chunk
                    continue
                return
            time.sleep(poll_interval_seconds)
    finally:
        os.close(descriptor)


def add_exception_note(error: BaseException, note: str) -> None:
    """Attach cleanup context without coupling type checking to Python 3.11 stubs."""

    add_note = getattr(error, "add_note", None)
    if callable(add_note):
        add_note(note)


def add_cleanup_failure_note(error: BaseException, *, context: str, cleanup_error: Exception) -> None:
    """Attach stable cleanup context without exposing an exception message."""

    code = cleanup_error.code if isinstance(cleanup_error, ProcessCleanupError) else type(cleanup_error).__name__
    add_exception_note(error, f"{context} failed: {code}")


def abort_after_failure(process: Any, parent_error: BaseException, *, operation: str) -> None:
    """Abort a process capability without replacing its primary failure."""

    cleanup_error: ProcessCleanupError | None = None
    try:
        process.abort()
    except Exception as abort_error:
        cleanup_error = ProcessCleanupError(
            operation=operation,
            cause_type=type(abort_error).__name__,
        )
    if cleanup_error is None:
        return
    if isinstance(parent_error, GeneratorExit):
        raise cleanup_error from None
    add_cleanup_failure_note(parent_error, context="process abort", cleanup_error=cleanup_error)


def abort_process(
    process: Any,
    *,
    output_drainer: ProcessOutputDrainer | None = None,
    timeout_seconds: float = DEFAULT_PROCESS_ABORT_TIMEOUT_SECONDS,
) -> None:
    """Terminate, reap, and drain a subprocess within bounded wait intervals."""

    try:
        _terminate_and_reap(process, timeout_seconds=timeout_seconds)
    except BaseException as error:
        _join_output_drainer(output_drainer, timeout_seconds=timeout_seconds, parent_error=error)
        raise
    _join_output_drainer(output_drainer, timeout_seconds=timeout_seconds)


def _terminate_and_reap(process: Any, *, timeout_seconds: float) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=timeout_seconds)
        return
    except subprocess.TimeoutExpired:
        process.kill()
    try:
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        raise ProcessAbortError(timeout_seconds=timeout_seconds) from exc


def _join_output_drainer(
    output_drainer: ProcessOutputDrainer | None,
    *,
    timeout_seconds: float,
    parent_error: BaseException | None = None,
) -> None:
    if output_drainer is None:
        return
    try:
        output_drainer.join(timeout=timeout_seconds)
    except Exception as error:
        if parent_error is None:
            raise
        add_exception_note(parent_error, f"process output drain failed: {type(error).__name__}")


__all__ = [
    "DEFAULT_PROCESS_ABORT_TIMEOUT_SECONDS",
    "DEFAULT_FIFO_POLL_INTERVAL_SECONDS",
    "LineCallback",
    "ProcessAbortError",
    "ProcessCleanupError",
    "ProcessOutputCapture",
    "ProcessOutputDrainTimeout",
    "ProcessOutputDrainer",
    "abort_after_failure",
    "add_cleanup_failure_note",
    "add_exception_note",
    "abort_process",
    "iter_fifo_bytes",
]

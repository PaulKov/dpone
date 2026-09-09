"""Bounded, shell-free adapters for the locked dbt runtime."""

from __future__ import annotations

import io
import math
import os
import selectors
import subprocess
import time
from collections.abc import Callable
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from threading import Event, Thread
from typing import Any, BinaryIO

from dpone.adapters.dbt_executable import current_environment_dbt_executable
from dpone.adapters.dbt_process_supervisor import (
    DbtProcessSupervisor,
    ManagedProcess,
    ProcessSupervisionError,
)
from dpone.contracts.dbt_invocation import DbtInvocationContext
from dpone.contracts.dbt_publishing import DbtPublishingError
from dpone.contracts.dbt_toolchain import certified_adapter_distribution
from dpone.ports.dbt_publishing import DbtCommandResult, DbtInstalledToolchain

DEFAULT_DBT_OUTPUT_LIMIT_BYTES = 1024 * 1024
DEFAULT_DBT_COLLECTOR_JOIN_TIMEOUT_SECONDS = 5.0


class DistributionDbtToolchainInspector:
    """Read installed package metadata without importing dbt or an adapter."""

    def inspect(self, adapter_name: str) -> DbtInstalledToolchain:
        try:
            distribution = certified_adapter_distribution(adapter_name)
            return DbtInstalledToolchain(
                dbt_core_version=version("dbt-core"),
                adapter_name=adapter_name,
                adapter_version=version(distribution),
            )
        except (PackageNotFoundError, ValueError) as exc:
            raise _execution_error("pinned dbt runtime packages are unavailable") from exc


class SubprocessDbtCommandRunner:
    """Drain bounded output, redact secrets and preserve the process exit code."""

    def __init__(
        self,
        *,
        max_output_bytes: int = DEFAULT_DBT_OUTPUT_LIMIT_BYTES,
        popen_factory: Callable[..., Any] = subprocess.Popen,
        process_supervisor: DbtProcessSupervisor | None = None,
        collector_join_timeout_seconds: float = DEFAULT_DBT_COLLECTOR_JOIN_TIMEOUT_SECONDS,
        dbt_executable: str | None = None,
    ) -> None:
        if isinstance(max_output_bytes, bool) or not isinstance(max_output_bytes, int):
            raise ValueError("max_output_bytes must be a positive integer")
        if max_output_bytes <= 0:
            raise ValueError("max_output_bytes must be a positive integer")
        if (
            isinstance(collector_join_timeout_seconds, bool)
            or not isinstance(collector_join_timeout_seconds, int | float)
            or not math.isfinite(collector_join_timeout_seconds)
            or collector_join_timeout_seconds <= 0
        ):
            raise ValueError("collector_join_timeout_seconds must be a positive number")
        self._max_output_bytes = max_output_bytes
        self._popen = popen_factory
        self._process_supervisor = process_supervisor or DbtProcessSupervisor()
        self._collector_join_timeout_seconds = float(collector_join_timeout_seconds)
        self._dbt_executable = dbt_executable or current_environment_dbt_executable()

    def run(
        self,
        args: tuple[str, ...],
        *,
        cwd: Path,
        timeout_seconds: int,
        redactions: tuple[str, ...],
    ) -> DbtCommandResult:
        if not args or args[0] != "dbt" or any(not isinstance(item, str) or "\x00" in item for item in args):
            raise _execution_error("dbt command argv is invalid")
        secrets = _redactions(redactions)
        retain_bytes = self._max_output_bytes + max(
            (len(item.encode("utf-8")) for item in secrets),
            default=0,
        )
        process: ManagedProcess | None = None
        collectors: tuple[_BoundedCollector, ...] = ()
        cleanup_started = False
        try:
            invocation_home = _invocation_home(args)
            invocation_home.mkdir(parents=True, exist_ok=True, mode=0o700)
            process = self._popen(
                (self._dbt_executable, *args[1:]),
                cwd=cwd,
                shell=False,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=_environment(invocation_home),
                close_fds=True,
                start_new_session=self._process_supervisor.start_new_session,
            )
            stdout = _BoundedCollector(process.stdout, retain_bytes)
            collectors = (stdout,)
            stderr = _BoundedCollector(process.stderr, retain_bytes)
            collectors = (stdout, stderr)
            stdout.start()
            stderr.start()
            try:
                exit_code = process.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired as exc:
                cleanup_started = True
                self._cleanup_process(process, collectors)
                raise _execution_error("dbt execution timed out") from exc
            if not _finish_collectors(
                collectors,
                timeout_seconds=self._collector_join_timeout_seconds,
            ):
                cleanup_started = True
                self._cleanup_process(process, collectors)
                raise _execution_error("dbt output could not be captured safely")
        except DbtPublishingError:
            if process is not None and not cleanup_started:
                self._cleanup_process(process, collectors)
            raise
        except (
            FileNotFoundError,
            PermissionError,
            OSError,
            RuntimeError,
            ValueError,
        ) as exc:
            if process is not None and not cleanup_started:
                self._cleanup_process(process, collectors)
            raise _execution_error("dbt executable is unavailable or unsafe") from exc
        except BaseException:
            if process is not None and not cleanup_started:
                self._cleanup_process(process, collectors)
            raise
        stdout_text, stdout_truncated = _sanitize(
            stdout.content,
            total_bytes=stdout.total_bytes,
            limit_bytes=self._max_output_bytes,
            secrets=secrets,
        )
        stderr_text, stderr_truncated = _sanitize(
            stderr.content,
            total_bytes=stderr.total_bytes,
            limit_bytes=self._max_output_bytes,
            secrets=secrets,
        )
        return DbtCommandResult(
            exit_code=exit_code,
            stdout=stdout_text,
            stderr=stderr_text,
            stdout_truncated=stdout_truncated,
            stderr_truncated=stderr_truncated,
        )

    def _cleanup_process(
        self,
        process: ManagedProcess,
        collectors: tuple[_BoundedCollector, ...],
    ) -> None:
        supervision_error: ProcessSupervisionError | None = None
        try:
            self._process_supervisor.terminate(process)
        except ProcessSupervisionError as exc:
            supervision_error = exc
        _stop_collectors(collectors)
        _close_process_pipes(process)
        collectors_finished = _finish_collectors(
            collectors,
            timeout_seconds=self._collector_join_timeout_seconds,
        )
        if supervision_error is not None:
            raise _execution_error("dbt process cleanup did not complete safely") from supervision_error
        if not collectors_finished:
            raise _execution_error("dbt process cleanup did not complete safely")


class _BoundedCollector:
    def __init__(self, stream: BinaryIO | None, retain_bytes: int) -> None:
        if stream is None:
            raise _execution_error("dbt output pipe is unavailable")
        self._stream = stream
        self._retain_bytes = retain_bytes
        self._content = bytearray()
        self._total_bytes = 0
        self._error: OSError | ValueError | None = None
        self._descriptor = _file_descriptor(stream)
        self._memory_buffer = type(stream) is io.BytesIO
        if self._descriptor is None and not self._memory_buffer:
            _close_stream(stream)
            raise _execution_error("dbt output pipe is unavailable or unsafe")
        self._thread = Thread(target=self._drain, daemon=True)
        self._started = False
        self._stop_requested = Event()

    @property
    def content(self) -> bytes:
        return bytes(self._content)

    @property
    def total_bytes(self) -> int:
        return self._total_bytes

    def start(self) -> None:
        self._thread.start()
        self._started = True

    def finish(self, timeout_seconds: float) -> bool:
        if not self._started:
            return True
        self._thread.join(timeout=timeout_seconds)
        if self._thread.is_alive():
            return False
        if self._error is not None:
            raise _execution_error("dbt output could not be captured safely") from self._error
        return True

    def request_stop(self) -> None:
        self._stop_requested.set()
        _close_stream(self._stream)

    def _drain(self) -> None:
        try:
            if self._descriptor is not None:
                self._drain_descriptor()
            else:
                self._drain_memory_buffer()
        except (OSError, ValueError) as exc:
            if not self._stop_requested.is_set():
                self._error = exc
        finally:
            _close_stream(self._stream)

    def _drain_memory_buffer(self) -> None:
        while chunk := self._stream.read(64 * 1024):
            self._retain(chunk)

    def _drain_descriptor(self) -> None:
        descriptor = self._descriptor
        if descriptor is None:
            raise OSError("dbt output descriptor is unavailable")
        os.set_blocking(descriptor, False)
        if os.name != "posix":
            self._drain_nonblocking_descriptor(descriptor)
            return
        with selectors.DefaultSelector() as selector:
            selector.register(descriptor, selectors.EVENT_READ)
            while not self._stop_requested.is_set():
                if not selector.select(timeout=0.05):
                    continue
                try:
                    chunk = os.read(descriptor, 64 * 1024)
                except BlockingIOError:
                    continue
                if not chunk:
                    return
                self._retain(chunk)

    def _drain_nonblocking_descriptor(self, descriptor: int) -> None:
        while not self._stop_requested.is_set():
            try:
                chunk = os.read(descriptor, 64 * 1024)
            except BlockingIOError:
                self._stop_requested.wait(0.05)
                continue
            if not chunk:
                return
            self._retain(chunk)

    def _retain(self, chunk: bytes) -> None:
        self._total_bytes += len(chunk)
        remaining = self._retain_bytes - len(self._content)
        if remaining > 0:
            self._content.extend(chunk[:remaining])


def _file_descriptor(stream: BinaryIO) -> int | None:
    try:
        descriptor = stream.fileno()
    except (AttributeError, OSError, ValueError):
        return None
    return descriptor if isinstance(descriptor, int) and descriptor >= 0 else None


def _finish_collectors(
    collectors: tuple[_BoundedCollector, ...],
    *,
    timeout_seconds: float,
) -> bool:
    deadline = time.monotonic() + timeout_seconds
    for collector in collectors:
        remaining = max(0.0, deadline - time.monotonic())
        if not collector.finish(remaining):
            return False
    return True


def _stop_collectors(collectors: tuple[_BoundedCollector, ...]) -> None:
    for collector in collectors:
        collector.request_stop()


def _close_process_pipes(process: ManagedProcess) -> None:
    for stream in (process.stdout, process.stderr):
        if stream is not None:
            _close_stream(stream)


def _close_stream(stream: BinaryIO) -> None:
    try:
        stream.close()
    except (OSError, ValueError):
        return


def _redactions(values: tuple[str, ...]) -> tuple[str, ...]:
    if any(not isinstance(item, str) or not item or len(item.encode("utf-8")) > 4096 for item in values):
        raise _execution_error("dbt output redaction values are invalid")
    return tuple(sorted(set(values), key=len, reverse=True))


def _sanitize(
    value: bytes,
    *,
    total_bytes: int,
    limit_bytes: int,
    secrets: tuple[str, ...],
) -> tuple[str, bool]:
    text = value.decode("utf-8", errors="replace")
    for secret in secrets:
        text = text.replace(secret, "[REDACTED]")
    encoded = text.encode("utf-8")
    truncated = total_bytes > limit_bytes or len(encoded) > limit_bytes
    if len(encoded) > limit_bytes:
        text = encoded[:limit_bytes].decode("utf-8", errors="ignore")
    return text, truncated


def _environment(cwd: Path) -> dict[str, str]:
    return DbtInvocationContext.canonical().environment(home=str(cwd))


def _invocation_home(args: tuple[str, ...]) -> Path:
    try:
        target = Path(args[args.index("--target-path") + 1])
    except (ValueError, IndexError) as exc:
        raise _execution_error("dbt command target path is missing") from exc
    if not target.is_absolute():
        raise _execution_error("dbt command target path must be absolute")
    parent = target.parent
    attempt = parent.parent if parent.name == "preflight" else parent
    return attempt / "home"


def _execution_error(message: str) -> DbtPublishingError:
    return DbtPublishingError("DPONE_DBT_EXECUTION_FAILED", message)


__all__ = [
    "DEFAULT_DBT_COLLECTOR_JOIN_TIMEOUT_SECONDS",
    "DEFAULT_DBT_OUTPUT_LIMIT_BYTES",
    "DistributionDbtToolchainInspector",
    "SubprocessDbtCommandRunner",
]

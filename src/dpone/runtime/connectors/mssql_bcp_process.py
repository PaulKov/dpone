"""Running Microsoft BCP process lifecycle and result contracts."""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from dpone.runtime.connectors.bulk_text_codec import DEFAULT_EMPTY_STRING_MARKER
from dpone.runtime.connectors.mssql_bcp_dsn import BcpDsnOptions
from dpone.runtime.process_io import (
    DEFAULT_PROCESS_ABORT_TIMEOUT_SECONDS,
    ProcessOutputDrainer,
    abort_after_failure,
    abort_process,
    add_exception_note,
)


class BcpInputFileAuthority(Protocol):
    """Runtime-only authority for an immutable external-process input."""

    def prepare_process_input(self, source_path: str) -> tuple[str, tuple[int, ...]]:
        """Return the pinned process path and descriptors that it requires."""


@dataclass(frozen=True, slots=True)
class BcpProcessInput:
    """Validated process path, inherited descriptors, and redaction policy."""

    path: str
    inherited_file_descriptors: tuple[int, ...] = ()
    private_values: tuple[str, ...] = ()

    @classmethod
    def resolve(
        cls,
        source_path: str,
        authority: BcpInputFileAuthority | None,
    ) -> BcpProcessInput:
        if authority is None:
            return cls(source_path)
        path, descriptors = authority.prepare_process_input(source_path)
        return cls(
            path=path,
            inherited_file_descriptors=_validated_descriptors(descriptors),
            private_values=(path,),
        )

    def subprocess_options(self) -> dict[str, tuple[int, ...]]:
        if not self.inherited_file_descriptors:
            return {}
        return {"pass_fds": self.inherited_file_descriptors}

    def redact(self, text: str) -> str:
        redacted = text
        for value in sorted(set(self.private_values), key=len, reverse=True):
            if value:
                redacted = redacted.replace(value, "<pinned-input>")
        return redacted


@dataclass(frozen=True)
class BcpCredentials:
    """Connection data required by the ``bcp`` utility."""

    host: str
    port: int
    database: str
    user: str | None = None
    password: str | None = None
    trusted_connection: bool = False

    @property
    def server(self) -> str:
        return f"{self.host},{self.port}" if self.port else self.host


@dataclass(frozen=True)
class BcpOptions:
    """Runtime options for bcp import/export."""

    bcp_path: str = "bcp"
    file_format: str = "character"
    code_page: str = "65001"
    field_terminator: str = "\t"
    row_terminator: str = "\n"
    batch_size: int = 100_000
    packet_size: int = 16_384
    timeout_seconds: int | None = None
    error_file: str | None = None
    trust_server_certificate: bool = False
    table_lock: bool = True
    keep_nulls: bool = True
    empty_string_marker: str = DEFAULT_EMPTY_STRING_MARKER
    encode_text: bool = True
    password_transport: str = "stdin"
    connection_dsn: BcpDsnOptions | None = None
    input_file_authority: BcpInputFileAuthority | None = field(
        default=None,
        repr=False,
        compare=False,
    )


@dataclass(frozen=True)
class BcpResult:
    """Structured result returned by a BCP command."""

    command: tuple[str, ...]
    redacted_command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    rows_copied: int | None


class BcpTimeoutError(subprocess.TimeoutExpired):
    """Redacted timeout raised after a supervised BCP process is reaped."""

    code = "mssql_bcp.process_timeout"

    def __init__(self, *, redacted_command: tuple[str, ...], timeout_seconds: int) -> None:
        self.redacted_command = redacted_command
        super().__init__(redacted_command, timeout_seconds)


@dataclass
class BcpProcess:
    """Running BCP process with redacted wait and deterministic abort handling."""

    process: subprocess.Popen
    command: tuple[str, ...]
    redacted_command: tuple[str, ...]
    timeout_seconds: int | None = None
    stdin_text: str | None = None
    output_drainer: ProcessOutputDrainer | None = None
    cleanup_callback: Callable[[], None] | None = None
    redact_output: Callable[[str], str] = str

    def wait(self) -> BcpResult:
        timeout_error: BcpTimeoutError | None = None
        try:
            stdout, stderr = self._wait_for_output()
        except subprocess.TimeoutExpired:
            timeout_error = BcpTimeoutError(
                redacted_command=self.redacted_command,
                timeout_seconds=int(self.timeout_seconds or 0),
            )
            self._abort_and_cleanup(
                timeout_error,
                timeout_seconds=DEFAULT_PROCESS_ABORT_TIMEOUT_SECONDS,
                context="bcp timeout",
            )
        except BaseException as error:
            self._abort_and_cleanup(
                error,
                timeout_seconds=DEFAULT_PROCESS_ABORT_TIMEOUT_SECONDS,
                context="bcp wait",
            )
            raise
        if timeout_error is not None:
            raise timeout_error from None
        self._cleanup()
        stdout = self.redact_output(stdout or "")
        stderr = self.redact_output(stderr or "")
        return _checked_bcp_result(
            command=self.command,
            redacted_command=self.redacted_command,
            returncode=int(self.process.returncode or 0),
            stdout=stdout,
            stderr=stderr,
        )

    def _wait_for_output(self) -> tuple[str, str]:
        if self.output_drainer is None:
            stdout, stderr = self.process.communicate(input=self.stdin_text, timeout=self.timeout_seconds)
            return stdout or "", stderr or ""
        self.process.wait(timeout=self.timeout_seconds)
        captured = self.output_drainer.join(timeout=DEFAULT_PROCESS_ABORT_TIMEOUT_SECONDS)
        return captured.stdout, captured.stderr

    def _abort_and_cleanup(self, error: BaseException, *, timeout_seconds: float, context: str) -> None:
        try:
            abort_process(
                self.process,
                output_drainer=self.output_drainer,
                timeout_seconds=timeout_seconds,
            )
        except BaseException as cleanup_error:
            add_exception_note(error, f"{context} cleanup failed: {type(cleanup_error).__name__}")
        if self.output_drainer is None:
            try:
                self.process.communicate(timeout=timeout_seconds)
            except BaseException as cleanup_error:
                add_exception_note(error, f"bcp output cleanup failed: {type(cleanup_error).__name__}")
        try:
            self._cleanup()
        except BaseException as cleanup_error:
            add_exception_note(error, f"bcp resource cleanup failed: {type(cleanup_error).__name__}")

    def poll(self) -> int | None:
        returncode = self.process.poll()
        if returncode is not None:
            self._cleanup()
        return returncode

    def abort(self, *, timeout_seconds: float = DEFAULT_PROCESS_ABORT_TIMEOUT_SECONDS) -> None:
        try:
            abort_process(self.process, output_drainer=self.output_drainer, timeout_seconds=timeout_seconds)
            if self.output_drainer is None:
                self.process.communicate(timeout=timeout_seconds)
        except BaseException as error:
            try:
                self._cleanup()
            except BaseException as cleanup_error:
                add_exception_note(error, f"bcp resource cleanup failed: {type(cleanup_error).__name__}")
            raise
        else:
            self._cleanup()

    def _cleanup(self) -> None:
        callback = self.cleanup_callback
        if callback is None:
            return
        self.cleanup_callback = None
        callback()

    terminate = abort


def start_bcp_process(
    command: Sequence[str],
    *,
    redacted_command: Sequence[str],
    stdin_text: str | None,
    timeout_seconds: int,
    environment: dict[str, str] | None,
    cleanup_callback: Callable[[], None] | None,
    progress_callback: Callable[[str], None] | None,
    process_input: BcpProcessInput | None = None,
    drain_output: bool = True,
    popen: Callable[..., Any] = subprocess.Popen,
) -> BcpProcess:
    """Start BCP with the pipe policy required by its execution shape."""

    kwargs: dict[str, Any] = {
        "stdin": subprocess.PIPE if stdin_text is not None else None,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
    }
    if environment is not None:
        kwargs["env"] = environment
    if process_input is not None:
        kwargs.update(process_input.subprocess_options())
    try:
        process = popen(list(command), **kwargs)
    except BaseException:
        if cleanup_callback is not None:
            cleanup_callback()
        raise
    output_drainer = (
        ProcessOutputDrainer(
            process,
            stdout_callback=progress_callback,
            stderr_callback=progress_callback,
        )
        if drain_output
        else None
    )
    handle = BcpProcess(
        process=process,
        command=tuple(command),
        redacted_command=tuple(redacted_command),
        timeout_seconds=timeout_seconds,
        stdin_text=stdin_text,
        output_drainer=output_drainer,
        cleanup_callback=cleanup_callback,
        redact_output=process_input.redact if process_input is not None else str,
    )
    try:
        if output_drainer is not None:
            output_drainer.start()
        if output_drainer is not None and stdin_text is not None and process.stdin is not None:
            process.stdin.write(stdin_text)
            process.stdin.flush()
            process.stdin.close()
            process.stdin = None
            handle.stdin_text = None
    except BaseException as error:
        abort_after_failure(handle, error, operation="bcp_start")
        raise
    return handle


def _checked_bcp_result(
    *,
    command: tuple[str, ...],
    redacted_command: tuple[str, ...],
    returncode: int,
    stdout: str,
    stderr: str,
) -> BcpResult:
    """Build a result and expose only redacted output on a BCP failure."""

    result = BcpResult(
        command=command,
        redacted_command=redacted_command,
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
        rows_copied=parse_bcp_rows_copied(stdout + "\n" + stderr),
    )
    if result.returncode != 0:
        redacted = " ".join(result.redacted_command)
        raise RuntimeError(f"bcp failed with exit code {result.returncode}: {redacted}\n{stderr or stdout}")
    return result


def parse_bcp_rows_copied(output: str) -> int | None:
    """Return the final BCP row count from process output."""

    matches = re.findall(r"(\d+)\s+rows?\s+copied", output, flags=re.IGNORECASE)
    return int(matches[-1]) if matches else None


def _validated_descriptors(values: tuple[int, ...]) -> tuple[int, ...]:
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in values):
        raise ValueError("bcp inherited file descriptors must be non-negative integers")
    inherited = tuple(dict.fromkeys(values))
    if inherited and os.name == "nt":
        raise OSError("bcp_inherited_file_descriptors_unsupported")
    for descriptor in inherited:
        try:
            os.fstat(descriptor)
        except OSError:
            raise OSError("bcp_inherited_file_descriptor_invalid") from None
    return inherited


__all__ = [
    "BcpCredentials",
    "BcpInputFileAuthority",
    "BcpOptions",
    "BcpProcess",
    "BcpProcessInput",
    "BcpResult",
    "BcpTimeoutError",
    "parse_bcp_rows_copied",
    "start_bcp_process",
]

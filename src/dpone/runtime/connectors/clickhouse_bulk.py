"""Native ClickHouse bulk insert helpers.

This module is independent from ``clickhouse-driver`` so command construction
can be tested without a running ClickHouse server or local client binary.
"""

from __future__ import annotations

import subprocess
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.runtime.connectors.clickhouse_client_request import (
    ClickHouseClientCredentials as ClickHouseClientCredentials,
)
from dpone.runtime.connectors.clickhouse_client_request import (
    ClickHouseClientOptions as ClickHouseClientOptions,
)
from dpone.runtime.connectors.clickhouse_client_request import (
    build_base_command,
    build_insert_query,
    redact_command,
)
from dpone.runtime.process_io import ProcessOutputCapture, ProcessOutputDrainer


@dataclass(frozen=True)
class ClickHouseClientResult:
    """Structured result returned by ``ClickHouseClientRunner``."""

    command: tuple[str, ...]
    redacted_command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


class ClickHouseClientRunner:
    """Injectable wrapper around ``clickhouse-client`` bulk inserts."""

    def __init__(
        self,
        credentials: ClickHouseClientCredentials,
        options: ClickHouseClientOptions | None = None,
        run=subprocess.run,
        popen=subprocess.Popen,
    ) -> None:
        self.credentials = credentials
        self.options = options or ClickHouseClientOptions()
        self._run = run
        self._popen = popen

    def insert_file(self, table: str, columns: Sequence[str], input_path: str) -> ClickHouseClientResult:
        command = self.build_insert_command(table, columns)
        with open(input_path, "rb") as handle:
            completed = self._run(
                command,
                stdin=handle,
                check=False,
                capture_output=True,
                text=False,
                timeout=self.options.timeout_seconds,
            )
        stdout = self._decode(completed.stdout)
        stderr = self._decode(completed.stderr)
        result = ClickHouseClientResult(
            command=tuple(command),
            redacted_command=tuple(self.redact_command(command)),
            returncode=int(completed.returncode),
            stdout=stdout,
            stderr=stderr,
        )
        if result.returncode != 0:
            redacted = " ".join(result.redacted_command)
            raise RuntimeError(f"clickhouse-client failed with exit code {result.returncode}: {redacted}\n{stderr}")
        return result

    def insert_stream(self, table: str, columns: Sequence[str], chunks: Iterable[bytes]) -> ClickHouseClientResult:
        command = self.build_insert_command(table, columns)
        chunk_iter = iter(chunks)
        first_chunk = next(chunk_iter, None)
        if first_chunk is None:
            return ClickHouseClientResult(
                command=tuple(command),
                redacted_command=tuple(self.redact_command(command)),
                returncode=0,
                stdout="",
                stderr="",
            )
        process = self._popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if process.stdin is None:
            raise RuntimeError("clickhouse-client stdin pipe was not created")
        stdin = process.stdin
        output_drainer = ProcessOutputDrainer(process)
        output_drainer.start()
        try:
            stdin.write(first_chunk)
            for chunk in chunk_iter:
                stdin.write(chunk)
            stdin.close()
            process.stdin = None
        except Exception as exc:
            try:
                stdin.close()
            except Exception:
                pass
            capture = self._wait_for_stream_process(process, output_drainer)
            redacted = " ".join(self.redact_command(command))
            detail = capture.stderr or capture.stdout
            raise RuntimeError(
                f"clickhouse-client stream write failed: {redacted}\n{type(exc).__name__}: {exc}\n{detail}"
            ) from exc
        capture = self._wait_for_stream_process(process, output_drainer)
        stdout = capture.stdout
        stderr = capture.stderr
        result = ClickHouseClientResult(
            command=tuple(command),
            redacted_command=tuple(self.redact_command(command)),
            returncode=int(process.returncode),
            stdout=stdout,
            stderr=stderr,
        )
        if result.returncode != 0:
            redacted = " ".join(result.redacted_command)
            raise RuntimeError(f"clickhouse-client failed with exit code {result.returncode}: {redacted}\n{stderr}")
        return result

    def _wait_for_stream_process(self, process: Any, drainer: ProcessOutputDrainer) -> ProcessOutputCapture:
        if getattr(process, "stdout", None) is None and getattr(process, "stderr", None) is None:
            communicate = getattr(process, "communicate", None)
            if callable(communicate):
                stdout_raw, stderr_raw = communicate(timeout=self.options.timeout_seconds)
                return ProcessOutputCapture(stdout=self._decode(stdout_raw), stderr=self._decode(stderr_raw))
        try:
            process.wait(timeout=self.options.timeout_seconds)
        except Exception:
            if getattr(process, "poll", lambda: None)() is None:
                terminate = getattr(process, "terminate", None)
                if callable(terminate):
                    terminate()
            raise
        return drainer.join()

    def execute_query(self, query: str) -> ClickHouseClientResult:
        command = self._base_command() + ["--query", query]
        completed = self._run(
            command,
            check=False,
            capture_output=True,
            text=False,
            timeout=self.options.timeout_seconds,
        )
        stdout = self._decode(completed.stdout)
        stderr = self._decode(completed.stderr)
        result = ClickHouseClientResult(
            command=tuple(command),
            redacted_command=tuple(self.redact_command(command)),
            returncode=int(completed.returncode),
            stdout=stdout,
            stderr=stderr,
        )
        if result.returncode != 0:
            redacted = " ".join(result.redacted_command)
            raise RuntimeError(f"clickhouse-client failed with exit code {result.returncode}: {redacted}\n{stderr}")
        return result

    def build_insert_command(self, table: str, columns: Sequence[str]) -> list[str]:
        query = build_insert_query(table, columns, self.options.input_format)
        return self._base_command() + ["--query", query]

    def build_query_command(self, sql: str) -> list[str]:
        """Build one identified query without executing or changing runner policy."""
        return self._base_command() + ["--query", sql]

    def _base_command(self) -> list[str]:
        return build_base_command(self.credentials, self.options)

    @staticmethod
    def redact_command(command: Sequence[str]) -> list[str]:
        return redact_command(command)

    @staticmethod
    def _decode(value: bytes | str | None) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        return value.decode("utf-8", errors="replace")

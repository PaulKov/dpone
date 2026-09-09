"""Native ClickHouse bulk insert helpers.

This module is independent from ``clickhouse-driver`` so command construction
can be tested without a running ClickHouse server or local client binary.
"""

from __future__ import annotations

import shlex
import subprocess
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from dpone.runtime.process_io import ProcessOutputCapture, ProcessOutputDrainer


@dataclass(frozen=True)
class ClickHouseClientCredentials:
    """Connection settings used by ``clickhouse-client``."""

    host: str
    port: int
    database: str
    user: str
    password: str = ""
    secure: bool = False


@dataclass(frozen=True)
class ClickHouseClientOptions:
    """Runtime options for native ClickHouse file ingest."""

    client_command: str = "clickhouse-client"
    input_format: str = "TabSeparated"
    timeout_seconds: int | None = None
    max_insert_block_size: int | None = None
    settings: dict[str, Any] = field(default_factory=dict)
    query_id: str | None = None
    insert_deduplication_token: str | None = None

    @classmethod
    def from_bulk_wire_contract(
        cls,
        contract: Any,
        *,
        base: ClickHouseClientOptions | None = None,
    ) -> ClickHouseClientOptions:
        """Return options with input format/settings required by a typed wire contract."""

        base_options = base or cls()
        return cls(
            client_command=base_options.client_command,
            input_format=str(getattr(contract, "input_format", base_options.input_format)),
            timeout_seconds=base_options.timeout_seconds,
            max_insert_block_size=base_options.max_insert_block_size,
            settings={**base_options.settings, **dict(contract.delimiter_profile.clickhouse_settings)},
            query_id=base_options.query_id,
            insert_deduplication_token=base_options.insert_deduplication_token,
        )


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
        column_sql = ", ".join(f"`{column}`" for column in columns)
        query = f"INSERT INTO {table} ({column_sql}) FORMAT {self.options.input_format}"
        return self._base_command() + ["--query", query]

    def _base_command(self) -> list[str]:
        command = shlex.split(self.options.client_command)
        command += [
            "--host",
            self.credentials.host,
            "--port",
            str(self.credentials.port),
            "--database",
            self.credentials.database,
            "--user",
            self.credentials.user,
            "--password",
            self.credentials.password,
        ]
        if self.credentials.secure:
            command.append("--secure")
        if self.options.max_insert_block_size:
            command += ["--max_insert_block_size", str(self.options.max_insert_block_size)]
        for key, value in self.options.settings.items():
            command += [f"--{key}", str(value)]
        if self.options.query_id:
            command += ["--query_id", self.options.query_id]
        if self.options.insert_deduplication_token:
            command += ["--insert_deduplication_token", self.options.insert_deduplication_token]
        return command

    @staticmethod
    def redact_command(command: Sequence[str]) -> list[str]:
        redacted: list[str] = []
        hide_next = False
        for token in command:
            if hide_next:
                redacted.append("***")
                hide_next = False
                continue
            redacted.append(token)
            if token == "--password":
                hide_next = True
        return redacted

    @staticmethod
    def _decode(value: bytes | str | None) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        return value.decode("utf-8", errors="replace")

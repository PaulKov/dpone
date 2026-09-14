"""Controlled ClickHouse client adapter with bounded raw pipes and exact identity."""

from __future__ import annotations

import hashlib
import os
import selectors
import subprocess
from collections.abc import Callable, Iterable
from dataclasses import replace

from dpone.runtime.clickhouse_file_stage_contract import (
    ABORT_PHASE_SECONDS,
    CHUNK_BYTES,
    MAX_RESPONSE_BYTES,
    QUERY_SETTINGS,
    IdentifiedFileRunner,
    IdentifiedStageQuery,
    QueryIdentity,
    QueryObservation,
    StageQueryResult,
)
from dpone.runtime.connectors.clickhouse_client_request import (
    ClickHouseClientCredentials,
    ClickHouseClientOptions,
    build_query_command,
)
from dpone.runtime.process_io import abort_process, add_exception_note


class ClickHouseFileClientRunner(IdentifiedFileRunner):
    """Send once, bound both output pipes, and reap on every BaseException.

    No line-oriented accumulator or retrying connector path is used. A stopped
    process alone never proves the ClickHouse query stopped.
    """

    def __init__(
        self,
        credentials: ClickHouseClientCredentials,
        options: ClickHouseClientOptions,
        *,
        clock: Callable[[], float],
        popen: Callable[..., subprocess.Popen[bytes]] = subprocess.Popen,
    ) -> None:
        super().__init__(host=credentials.host, port=credentials.port, secure=credentials.secure, clock=clock)
        self.credentials = credentials
        self.options = replace(options, settings=dict(options.settings))
        self._popen = popen
        self._process: subprocess.Popen[bytes] | None = None
        self._pending_chunks: Iterable[bytes] | None = None

    def execute(
        self, request: IdentifiedStageQuery, *, chunks: Iterable[bytes] | None, deadline_monotonic: float
    ) -> StageQueryResult:
        self.require_request(request)
        if not self.local_stopped:
            raise RuntimeError("clickhouse_file_sender_still_active")
        if self.clock() >= deadline_monotonic:
            raise TimeoutError("clickhouse file query deadline exceeded")
        options = replace(
            self.options,
            input_format="RowBinary",
            settings=dict(QUERY_SETTINGS),
            query_id=request.identity.query_id,
            insert_deduplication_token=None,
        )
        command = build_query_command(self.credentials, options, request.sql)
        process = self._popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
        self._process, self._pending_chunks = process, chunks
        self.local_stopped = False
        try:
            body, size, digest = self._pump(process, chunks or (), deadline_monotonic)
            remaining = deadline_monotonic - self.clock()
            if remaining <= 0:
                raise TimeoutError("clickhouse file query deadline exceeded")
            returncode = process.wait(timeout=remaining)
            self.local_stopped = True
            if self.clock() >= deadline_monotonic:
                raise TimeoutError("clickhouse file query deadline exceeded")
            if returncode != 0:
                raise RuntimeError(f"clickhouse_file_client_exit:{returncode}")
            return self.completed(request, body, size, digest)
        except BaseException as error:
            try:
                abort_process(process, output_drainer=None, timeout_seconds=ABORT_PHASE_SECONDS)
            except BaseException as cleanup_error:
                add_exception_note(error, f"client reap failed:{type(cleanup_error).__name__}")
            self.local_stopped = process.poll() is not None
            raise
        finally:
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    stream.close()
            if self.local_stopped:
                self._pending_chunks = None

    def _pump(
        self, process: subprocess.Popen[bytes], chunks: Iterable[bytes], deadline: float
    ) -> tuple[bytes, int, str]:
        assert process.stdin is not None and process.stdout is not None and process.stderr is not None
        streams = {"stdin": process.stdin, "stdout": process.stdout, "stderr": process.stderr}
        output: dict[str, bytearray] = {"stdout": bytearray(), "stderr": bytearray()}
        iterator, pending = iter(chunks), memoryview(b"")
        digest, size = hashlib.sha256(), 0
        with selectors.DefaultSelector() as selector:
            for name, stream in streams.items():
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_WRITE if name == "stdin" else selectors.EVENT_READ, name)
            while selector.get_map():
                remaining = deadline - self.clock()
                if remaining <= 0:
                    raise TimeoutError("clickhouse file query deadline exceeded")
                for key, _events in selector.select(min(remaining, 0.1)):
                    name = key.data
                    if name == "stdin":
                        if not pending:
                            try:
                                chunk = next(iterator)
                            except StopIteration:
                                selector.unregister(process.stdin)
                                process.stdin.close()
                                continue
                            if not isinstance(chunk, bytes) or len(chunk) > CHUNK_BYTES:
                                raise RuntimeError("clickhouse_file_chunk_limit")
                            pending = memoryview(chunk)
                            if not pending:
                                continue
                        try:
                            written = os.write(key.fd, pending)
                        except BlockingIOError:
                            continue
                        digest.update(pending[:written])
                        size += written
                        pending = pending[written:]
                    else:
                        try:
                            data = os.read(key.fd, 16_384)
                        except BlockingIOError:
                            continue
                        if not data:
                            selector.unregister(streams[name])
                            streams[name].close()
                            continue
                        if sum(map(len, output.values())) + len(data) > MAX_RESPONSE_BYTES:
                            raise RuntimeError("clickhouse_file_response_limit")
                        output[name].extend(data)
        return bytes(output["stdout"]), size, digest.hexdigest()

    def cancel_and_observe(self, query: QueryIdentity, *, deadline_monotonic: float) -> QueryObservation:
        if not self.local_stopped and self._process is not None:
            try:
                abort_process(self._process, output_drainer=None, timeout_seconds=ABORT_PHASE_SECONDS)
            except BaseException:
                pass
            self.local_stopped = self._process.poll() is not None
            if self.local_stopped:
                self._pending_chunks = None
        return super().cancel_and_observe(query, deadline_monotonic=deadline_monotonic)


def build_file_client_runner(
    *,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    secure: bool,
    timeout: int,
    command: str,
    clock: Callable[[], float],
) -> ClickHouseFileClientRunner:
    """Compose one controlled adapter from already resolved scalar settings."""
    return ClickHouseFileClientRunner(
        ClickHouseClientCredentials(host, port, database, user, password, secure),
        ClickHouseClientOptions(client_command=command, input_format="RowBinary", timeout_seconds=timeout),
        clock=clock,
    )

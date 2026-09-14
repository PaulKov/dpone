"""Controlled synchronous HTTP RowBinary with bounded framing and response I/O."""

from __future__ import annotations

import hashlib
import http.client
import sys
from collections.abc import Callable, Iterable
from dataclasses import replace
from functools import partial

from dpone.runtime.clickhouse_file_stage_contract import (
    CHUNK_BYTES,
    MAX_RESPONSE_BYTES,
    QUERY_SETTINGS,
    IdentifiedFileRunner,
    IdentifiedStageQuery,
    StageQueryResult,
)
from dpone.runtime.connectors.clickhouse_file_response import _BoundedResponse
from dpone.runtime.connectors.clickhouse_http_request import (
    ClickHouseHttpCredentials,
    ClickHouseHttpOptions,
    build_insert_url,
)


class ClickHouseFileHttpRunner(IdentifiedFileRunner):
    """Close local synchronous I/O on every exit, without inferring remote stop."""

    def __init__(
        self,
        credentials: ClickHouseHttpCredentials,
        options: ClickHouseHttpOptions,
        *,
        clock: Callable[[], float],
        connection_factory: Callable[..., http.client.HTTPConnection] | None = None,
    ) -> None:
        super().__init__(host=credentials.host, port=credentials.port, secure=credentials.secure, clock=clock)
        self.credentials = credentials
        self.options = replace(options, settings=dict(options.settings))
        self._connection_factory = connection_factory or (
            http.client.HTTPSConnection if credentials.secure else http.client.HTTPConnection
        )
        self._peer_address: str | None = None
        self._unclosed_resources: list[http.client.HTTPResponse | http.client.HTTPConnection] = []

    def execute(
        self, request: IdentifiedStageQuery, *, chunks: Iterable[bytes] | None, deadline_monotonic: float
    ) -> StageQueryResult:
        self.require_request(request)
        if not self.local_stopped:
            raise RuntimeError("clickhouse_file_sender_still_active")
        options = replace(
            self.options,
            input_format="RowBinary",
            settings=dict(QUERY_SETTINGS),
            query_id=request.identity.query_id,
            insert_deduplication_token=None,
        )
        url = build_insert_url(self.credentials, options, "", (), query=request.sql)
        connection = self._connection_factory(self.host, self.port, timeout=self._remaining(deadline_monotonic))
        connection.response_class = partial(  # type: ignore[assignment]
            _BoundedResponse,
            remaining=partial(self._remaining, deadline_monotonic),
            max_metadata_bytes=MAX_RESPONSE_BYTES,
            max_body_bytes=MAX_RESPONSE_BYTES,
        )
        response: http.client.HTTPResponse | None = None
        self.local_stopped = False
        try:
            connection.connect()
            assert connection.sock is not None
            peer = str(connection.sock.getpeername()[0])
            if self._peer_address is not None and peer != self._peer_address:
                raise RuntimeError("clickhouse_file_endpoint_changed")
            self._peer_address = peer
            self._timeout(connection, deadline_monotonic)
            connection.putrequest("POST", url)
            connection.putheader("Transfer-Encoding", "chunked")
            connection.endheaders()
            digest, size = hashlib.sha256(), 0
            for chunk in chunks or ():
                if not isinstance(chunk, bytes) or len(chunk) > CHUNK_BYTES:
                    raise RuntimeError("clickhouse_file_chunk_limit")
                if not chunk:
                    continue
                for frame in (f"{len(chunk):X}\r\n".encode("ascii"), chunk, b"\r\n"):
                    self._timeout(connection, deadline_monotonic)
                    connection.send(frame)
                digest.update(chunk)
                size += len(chunk)
            self._timeout(connection, deadline_monotonic)
            connection.send(b"0\r\n\r\n")
            self._timeout(connection, deadline_monotonic)
            response = connection.getresponse()
            body = bytearray()
            while True:
                self._timeout(connection, deadline_monotonic)
                piece = response.read(min(16_384, MAX_RESPONSE_BYTES + 1 - len(body)))
                body.extend(piece)
                if len(body) > MAX_RESPONSE_BYTES:
                    raise RuntimeError("clickhouse_file_response_limit")
                if not piece:
                    break
            self._remaining(deadline_monotonic)
            if response.status != 200:
                raise RuntimeError(f"clickhouse_file_http_status:{response.status}")
        finally:
            self._settle_local(response, connection, primary=sys.exception())
        self._remaining(deadline_monotonic)
        return self.completed(request, bytes(body), size, digest.hexdigest())

    def _settle_local(
        self,
        response: http.client.HTTPResponse | None,
        connection: http.client.HTTPConnection,
        *,
        primary: BaseException | None,
    ) -> None:
        failures: list[BaseException] = []
        for resource in (response, connection):
            if resource is not None:
                try:
                    resource.close()
                except BaseException as error:
                    failures.append(error)
                    self._unclosed_resources.append(resource)
        self.local_stopped = not failures
        if failures:
            if primary is not None:
                primary.add_note("clickhouse_file_http_local_close_failed")
            else:
                raise failures[0]

    def _remaining(self, deadline: float) -> float:
        remaining = deadline - self.clock()
        if remaining <= 0:
            raise TimeoutError("clickhouse file query deadline exceeded")
        return remaining

    def _timeout(self, connection: http.client.HTTPConnection, deadline: float) -> None:
        remaining = self._remaining(deadline)
        connection.timeout = remaining
        if connection.sock is not None:
            connection.sock.settimeout(remaining)


def build_file_http_runner(
    *,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    secure: bool,
    timeout: int,
    clock: Callable[[], float],
) -> ClickHouseFileHttpRunner:
    """Compose one controlled adapter from already resolved scalar settings."""
    return ClickHouseFileHttpRunner(
        ClickHouseHttpCredentials(host, port, database, user, password, secure),
        ClickHouseHttpOptions(input_format="RowBinary", timeout_seconds=timeout),
        clock=clock,
    )

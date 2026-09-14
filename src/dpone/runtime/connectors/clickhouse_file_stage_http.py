"""Controlled synchronous HTTP RowBinary with bounded framing and response I/O."""

from __future__ import annotations

import hashlib
import http.client
from collections.abc import Callable, Iterable
from dataclasses import replace

from dpone.runtime.clickhouse_file_stage_contract import (
    CHUNK_BYTES,
    MAX_RESPONSE_BYTES,
    QUERY_SETTINGS,
    IdentifiedFileRunner,
    IdentifiedStageQuery,
    StageQueryResult,
)
from dpone.runtime.connectors.clickhouse_http_bulk import (
    ClickHouseHttpBulkRunner,
    ClickHouseHttpCredentials,
    ClickHouseHttpOptions,
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
        builder = ClickHouseHttpBulkRunner(self.credentials, options)
        url = builder.build_insert_url("", (), query=request.sql)
        connection = self._connection_factory(self.host, self.port, timeout=self._remaining(deadline_monotonic))
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
            return self.completed(request, bytes(body), size, digest.hexdigest())
        finally:
            connection.close()
            self.local_stopped = True

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

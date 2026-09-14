"""Controlled synchronous HTTP RowBinary with bounded framing and response I/O."""

from __future__ import annotations

import hashlib
import http.client
import io
import re
import socket
import sys
from collections.abc import Callable, Iterable
from dataclasses import replace
from functools import partial
from typing import BinaryIO, cast

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


class _DeadlineRaw(io.RawIOBase):
    """Bound each actual receive, retaining SocketIO's descriptor ownership."""

    def __init__(self, sock: socket.socket, remaining: Callable[[], float]) -> None:
        self._socket, self._remaining = sock, remaining
        self._raw = cast(BinaryIO, sock.makefile("rb", buffering=0))

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: object) -> int | None:
        self._socket.settimeout(self._remaining())
        count = self._raw.readinto(buffer)  # type: ignore[arg-type]
        self._remaining()
        return count

    def close(self) -> None:
        try:
            self._raw.close()
        finally:
            super().close()


class _FramingReader(io.BufferedReader):
    """One metadata cap across status, headers, chunk framing and trailers."""

    def __init__(self, raw: _DeadlineRaw) -> None:
        super().__init__(raw)
        self._metadata_bytes = 0

    def account_metadata(self, size: int) -> None:
        self._metadata_bytes += size
        if self._metadata_bytes > MAX_RESPONSE_BYTES:
            raise RuntimeError("clickhouse_file_response_metadata_limit")

    def readline(self, size: int = -1, /) -> bytes:
        available = MAX_RESPONSE_BYTES + 1 - self._metadata_bytes
        line = super().readline(min(size, available) if size >= 0 else available)
        self.account_metadata(len(line))
        if not line.endswith(b"\r\n"):
            raise http.client.IncompleteRead(b"")
        return line


class _ResponseSocket:
    """Expose only the response constructor's makefile dependency."""

    def __init__(self, reader: _FramingReader) -> None:
        self.reader = reader

    def makefile(self, _mode: str) -> _FramingReader:
        return self.reader


class _BoundedResponse(http.client.HTTPResponse):
    """CPython response factory with strict, bounded HTTP body framing.

    The supported 3.11/3.12 connection response_class hook preserves the supplied
    connection factory. No socket/SDK instance methods are replaced. The owned
    chunk parser validates terminators that HTTPResponse normally discards.
    """

    _token = rb"[!#$%&'*+\-.^_`|~0-9A-Za-z]+"
    _quoted = rb'"(?:[\t !#-\[\]-~\x80-\xff]|\\[\t -~\x80-\xff])*"'
    _chunk_line = re.compile(
        rb"([0-9a-fA-F]+)(?:[ \t]*;[ \t]*" + _token + rb"(?:[ \t]*=[ \t]*(?:" + _token + rb"|" + _quoted + rb"))?)*\r\n"
    )
    _trailer_line = re.compile(_token + rb":[\t\x20-\x7e\x80-\xff]*\r\n")

    def __init__(
        self,
        sock: socket.socket,
        debuglevel: int = 0,
        *,
        method: str | None = None,
        remaining: Callable[[], float],
    ) -> None:
        self._reader = _FramingReader(_DeadlineRaw(sock, remaining))
        self._chunk_remaining = 0
        self._chunk_terminator = False
        self._body_bytes = 0
        try:
            super().__init__(cast(socket.socket, _ResponseSocket(self._reader)), debuglevel, method=method)
        except BaseException:
            self._reader.close()
            raise

    def begin(self) -> None:
        super().begin()
        assert self.headers is not None
        if self.headers.defects or any(
            not self._trailer_line.fullmatch(f"{name}:{value}\r\n".encode("iso-8859-1"))
            for name, value in self.headers.raw_items()
        ):
            raise http.client.HTTPException("clickhouse_file_response_framing")
        transfers = self.headers.get_all("Transfer-Encoding", [])
        lengths = self.headers.get_all("Content-Length", [])
        if transfers and ([value.lower() for value in transfers] != ["chunked"] or lengths):
            raise http.client.HTTPException("clickhouse_file_response_framing")
        if lengths and (self.length is None or len(set(lengths)) != 1 or not re.fullmatch(r"[0-9]+", lengths[0])):
            raise http.client.HTTPException("clickhouse_file_response_framing")

    def read(self, amt: int | None = None) -> bytes:
        if amt == 0:
            return b""
        if amt is None or amt < 1:
            raise ValueError("file-stage response reads require a positive bound")
        if not self.chunked:
            data = super().read(amt)
            if not data and self.length not in (None, 0):
                raise http.client.IncompleteRead(b"", self.length)
            return data
        if self.fp is None:
            return b""
        if not self._chunk_remaining:
            if self._chunk_terminator:
                self._reader.account_metadata(2)
                if self._reader.read(2) != b"\r\n":
                    raise http.client.IncompleteRead(b"")
            size = self._chunk_line.fullmatch(self._reader.readline())
            if size is None:
                raise http.client.HTTPException("clickhouse_file_response_framing")
            self._chunk_remaining = int(size[1], 16)
            if self._chunk_remaining > MAX_RESPONSE_BYTES - self._body_bytes:
                raise RuntimeError("clickhouse_file_response_limit")
            if not self._chunk_remaining:
                while (trailer := self._reader.readline()) != b"\r\n":
                    if not self._trailer_line.fullmatch(trailer):
                        raise http.client.HTTPException("clickhouse_file_response_framing")
                self.close()
                return b""
        expected = min(amt, self._chunk_remaining)
        data = self._reader.read(expected)
        if len(data) != expected:
            raise http.client.IncompleteRead(b"", expected - len(data))
        self._chunk_remaining -= len(data)
        self._body_bytes += len(data)
        self._chunk_terminator = True
        return data


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
        builder = ClickHouseHttpBulkRunner(self.credentials, options)
        url = builder.build_insert_url("", (), query=request.sql)
        connection = self._connection_factory(self.host, self.port, timeout=self._remaining(deadline_monotonic))
        connection.response_class = partial(  # type: ignore[assignment]
            _BoundedResponse, remaining=partial(self._remaining, deadline_monotonic)
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

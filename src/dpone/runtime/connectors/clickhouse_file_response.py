"""Bounded HTTP response framing with explicit deadline and byte-limit inputs.

Only standard-library I/O lives here; query identity and staging policy stay in
its caller. Metadata and chunk-body accounting use independent injected caps.
"""

from __future__ import annotations

import http.client
import io
import re
import socket
from collections.abc import Callable
from typing import BinaryIO, cast


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

    def __init__(self, raw: _DeadlineRaw, *, max_metadata_bytes: int) -> None:
        super().__init__(raw)
        self._metadata_bytes = 0
        self._metadata_limit = max_metadata_bytes

    def account_metadata(self, size: int) -> None:
        self._metadata_bytes += size
        if self._metadata_bytes > self._metadata_limit:
            raise RuntimeError("clickhouse_file_response_metadata_limit")

    def readline(self, size: int = -1, /) -> bytes:
        available = self._metadata_limit + 1 - self._metadata_bytes
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
        max_metadata_bytes: int,
        max_body_bytes: int,
    ) -> None:
        self._reader = _FramingReader(_DeadlineRaw(sock, remaining), max_metadata_bytes=max_metadata_bytes)
        self._body_limit = max_body_bytes
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
            if self._chunk_remaining > self._body_limit - self._body_bytes:
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

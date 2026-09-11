"""Bounded, synchronous ClickHouse HTTP framing shared by closed clients.

Literal IP endpoints only; localhost maps explicitly to 127.0.0.1. No DNS,
redirects, proxy environment, retries, session reuse or asynchronous execution.
The TLS peer remains certificate verified. A response is evidence of HTTP
completion only, never database outcome or writer quiescence.
"""

from __future__ import annotations

import base64
import http.client
import math
import re
import socket
import ssl
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from ipaddress import ip_address
from threading import Timer
from typing import IO, Any
from urllib.parse import urlencode, urlsplit

from dpone.contracts.composition_clickhouse_dispatch import MAX_DISPATCH_PAYLOAD_BYTES
from dpone.contracts.composition_identity import CompositionAdmissionError


@dataclass(frozen=True, slots=True)
class ClickHouseTransportCredentials:
    """Protected gateway's sole-writer material; never put in URLs or evidence."""

    username: str = field(repr=False)
    password: str = field(repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.username) is not str
            or not 1 <= len(self.username) <= 128
            or any(value in self.username for value in (":", "\r", "\n", "\x00"))
            or type(self.password) is not str
            or not 1 <= len(self.password) <= 4096
        ):
            raise CompositionAdmissionError("clickhouse_transport_credentials")


@dataclass(frozen=True, slots=True)
class ClickHouseHttpObservation:
    request_body_bytes: int
    body: bytes = field(repr=False)
    framing: str


class ClickHouseHttpError(RuntimeError):
    """No dynamic server, driver, request or authentication text escapes."""

    def __init__(self, request_body_bytes: int) -> None:
        super().__init__("ClickHouse request is unacknowledged; protected reconciliation is required")
        self.request_body_bytes = request_body_bytes


def clickhouse_http_path(
    *,
    query_id: str,
    statement: str | None = None,
    parameters: Mapping[str, str] | None = None,
    result_rows: int | None = None,
) -> str:
    """Build the common fixed synchronous profile; callers validate closed SQL."""
    settings = {
        "query_id": query_id,
        "wait_end_of_query": "1",
        "async_insert": "0",
        "wait_for_async_insert": "1",
        "send_progress_in_http_headers": "0",
        "enable_http_compression": "0",
    }
    if result_rows is not None:
        if type(result_rows) is not int or not 1 <= result_rows <= 65:
            raise CompositionAdmissionError("clickhouse_transport_result_limit")
        settings.update(
            max_result_rows=str(result_rows),
            result_overflow_mode="throw",
            read_overflow_mode="throw",
            timeout_overflow_mode="throw",
        )
    if statement is not None:
        settings["query"] = statement
    if parameters:
        settings.update({"param_" + key: value for key, value in parameters.items()})
    path = "/?" + urlencode(settings)
    if len(path) > 1024 * 1024:
        raise CompositionAdmissionError("clickhouse_dispatch_uri_budget")
    return path


class _CompleteHeaderReader:
    """The standard parser must consume a real complete CRLF header block."""

    def __init__(self, stream: IO[bytes]) -> None:
        self.stream, self.total = stream, 0
        self.status_line = True

    def readline(self, limit: int = -1) -> bytes:
        line = self.stream.readline(min(8193, limit) if limit >= 0 else 8193)
        self.total += len(line)
        if not line.endswith(b"\r\n") or self.total > 65536 or line.startswith((b" ", b"\t")):
            raise ValueError
        if self.status_line:
            self.status_line = False
        elif (
            line != b"\r\n" and re.fullmatch(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+:[\t\x20-\x7e\x80-\xff]*\r\n", line) is None
        ):
            raise ValueError
        return line


class _CompleteHttpResponse(http.client.HTTPResponse):
    """Require complete CRLF chunk framing and reject uninspected trailers."""

    # CPython sets fp to None at complete chunk EOF; its stub omits that
    # lifecycle and the temporary readline-only header reader used by begin.
    fp: Any

    def begin(self) -> None:
        stream = self.fp
        assert stream is not None
        self.fp = _CompleteHeaderReader(stream)
        try:
            super().begin()
            if self.headers.defects:
                raise ValueError
        finally:
            self.fp = stream

    def _read_next_chunk_size(self) -> int:
        assert self.fp is not None
        line = self.fp.readline(128)
        if re.fullmatch(rb"[0-9a-fA-F]+\r\n", line) is None:
            raise ValueError
        return int(line[:-2], 16)

    def _get_chunk_left(self) -> int | None:
        chunk_left = self.chunk_left
        if not chunk_left:
            assert self.fp is not None
            if chunk_left is not None and self.fp.read(2) != b"\r\n":
                raise ValueError
            chunk_left = self._read_next_chunk_size()
            if chunk_left == 0:
                self._read_and_discard_trailer()
                self.fp.close()
                self.fp = None
                chunk_left = None
            self.chunk_left = chunk_left
        return chunk_left

    def _read_and_discard_trailer(self) -> None:
        assert self.fp is not None
        if self.fp.readline(8193) != b"\r\n":
            raise ValueError


class _LiteralHttpConnection(http.client.HTTPConnection):
    """Connect directly to an already validated numeric address, without DNS."""

    def __init__(self, host: str, port: int, timeout: float, context: ssl.SSLContext | None) -> None:
        self.default_port = 443 if context is not None else 80
        super().__init__(host, port, timeout=timeout)
        self._tls_context = context

    def connect(self) -> None:
        assert self.timeout is not None
        deadline = time.monotonic() + self.timeout
        self.sock = socket.socket(socket.AF_INET6 if ":" in self.host else socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect((self.host, self.port))
        if self._tls_context is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            self.sock.settimeout(remaining)
            self.sock = self._tls_context.wrap_socket(self.sock, server_hostname=self.host)


class BoundedClickHouseHttp:
    """One connection per call; owners decide admission before calling request."""

    def __init__(
        self,
        *,
        endpoint: str,
        credentials: ClickHouseTransportCredentials,
        timeout_seconds: float,
        max_payload_bytes: int = MAX_DISPATCH_PAYLOAD_BYTES,
        max_response_bytes: int = 64 * 1024,
        ca_file: str | None = None,
    ) -> None:
        try:
            parsed = urlsplit(endpoint)
            valid = (
                type(endpoint) is str
                and parsed.scheme in {"https", "http"}
                and bool(parsed.hostname)
                and parsed.path in {"", "/"}
                and not parsed.query
                and not parsed.fragment
                and parsed.username is None
                and parsed.password is None
                and (parsed.scheme == "https" or parsed.hostname in {"localhost", "127.0.0.1", "::1"})
                and not any(character.isspace() or ord(character) < 32 for character in endpoint)
            )
            port = parsed.port if parsed.port is not None else (443 if parsed.scheme == "https" else 80)
        except (ValueError, TypeError, AttributeError):
            valid, port = False, 0
        if not valid or not 1 <= port <= 65535:
            raise CompositionAdmissionError("clickhouse_transport_endpoint")
        if (
            type(timeout_seconds) not in {int, float}
            or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 3600
            or type(max_payload_bytes) is not int
            or not 1 <= max_payload_bytes <= MAX_DISPATCH_PAYLOAD_BYTES
            or type(max_response_bytes) is not int
            or not 1 <= max_response_bytes <= 1024 * 1024
            or (parsed.scheme == "http" and ca_file is not None)
        ):
            raise CompositionAdmissionError("clickhouse_transport_limits")
        if type(credentials) is not ClickHouseTransportCredentials:
            raise CompositionAdmissionError("clickhouse_transport_credentials")
        credentials.__post_init__()
        host = parsed.hostname or ""
        if host == "localhost":
            host = "127.0.0.1"
        try:
            if "%" in host:
                raise ValueError
            ip_address(host)
        except ValueError:
            raise CompositionAdmissionError("clickhouse_transport_endpoint") from None
        self._host, self._port, self._scheme = host, port, parsed.scheme
        self._context = ssl.create_default_context(cafile=ca_file) if parsed.scheme == "https" else None
        self._authorization = (
            "Basic " + base64.b64encode(f"{credentials.username}:{credentials.password}".encode()).decode()
        )
        self._timeout = float(timeout_seconds)
        self._max_payload = max_payload_bytes
        self._max_response = max_response_bytes

    def request(self, *, path: str, payload: bytes, query_id: str) -> ClickHouseHttpObservation:
        if type(payload) is not bytes or len(payload) > self._max_payload:
            raise CompositionAdmissionError("clickhouse_transport_payload_budget")
        if type(path) is not str or not path.startswith("/?") or len(path) > 1024 * 1024:
            raise CompositionAdmissionError("clickhouse_transport_path")
        deadline = time.monotonic() + self._timeout
        connection = None
        sent = 0
        timer = None
        try:
            connection = self._connection()
            connection.timeout = max(0, deadline - time.monotonic())
            connection.response_class = _CompleteHttpResponse
            connection.connect()
            assert connection.sock is not None
            transport_socket = connection.sock
            timer = Timer(max(0, deadline - time.monotonic()), _expire, args=(transport_socket,))
            timer.daemon = True
            timer.start()
            self._remaining(connection, deadline)
            connection.putrequest("POST", path, skip_accept_encoding=True)
            for key, value in {
                "Authorization": self._authorization,
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(payload)),
                "Connection": "close",
                "Accept-Encoding": "identity",
            }.items():
                connection.putheader(key, value)
            connection.endheaders()
            while sent < len(payload):
                self._remaining(connection, deadline)
                assert connection.sock is not None
                count = connection.sock.send(memoryview(payload)[sent : sent + 64 * 1024])
                if count <= 0:
                    raise OSError
                sent += count
            self._remaining(connection, deadline)
            response = connection.getresponse()
            try:
                body, framing = self._response(response, transport_socket, deadline)
                if response.headers.get_all("X-ClickHouse-Query-Id", []) != [query_id]:
                    raise ValueError
            finally:
                response.close()
            return ClickHouseHttpObservation(sent, body, framing)
        except Exception:
            raise ClickHouseHttpError(sent) from None
        finally:
            if timer is not None:
                timer.cancel()
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass

    def _connection(self) -> http.client.HTTPConnection:
        return _LiteralHttpConnection(self._host, self._port, self._timeout, self._context)

    @staticmethod
    def _remaining(connection: http.client.HTTPConnection, deadline: float) -> None:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or connection.sock is None:
            raise TimeoutError
        connection.sock.settimeout(remaining)

    def _response(
        self, response: http.client.HTTPResponse, transport_socket: socket.socket, deadline: float
    ) -> tuple[bytes, str]:
        lengths = response.headers.get_all("Content-Length", [])
        transfers = response.headers.get_all("Transfer-Encoding", [])
        if response.headers.get_all("Content-Encoding", []) not in ([], ["identity"]):
            raise ValueError
        if transfers == ["chunked"] and not lengths:
            framing = "chunked"
        elif len(lengths) == 1 and not transfers and lengths[0].isascii() and lengths[0].isdigit():
            framing = "content-length"
            if int(lengths[0]) > self._max_response:
                raise ValueError
        else:
            raise ValueError
        body = bytearray()
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            # Keep the actual socket after HTTPConnection detaches it for a
            # Connection:close response; the watchdog also bounds slow headers.
            if not response.isclosed():
                transport_socket.settimeout(remaining)
            chunk = response.read1(min(64 * 1024, self._max_response + 1 - len(body)))
            if not chunk:
                break
            body.extend(chunk)
            if len(body) > self._max_response:
                raise ValueError
        if framing == "content-length" and len(body) != int(lengths[0]):
            raise ValueError
        errors = response.headers.get_all("X-ClickHouse-Exception-Code", [])
        if response.status != 200 or errors not in ([], ["0"]) or time.monotonic() >= deadline:
            raise ValueError
        return bytes(body), framing


def _expire(transport_socket: socket.socket) -> None:
    """Interrupt a slow partial header/chunk/body at the absolute deadline."""
    try:
        transport_socket.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass

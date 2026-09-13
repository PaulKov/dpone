"""Direct numeric-IP TLS dispatcher client with no proxy, redirect or retry.

Public socket helpers provide the same absolute I/O deadline and strict HTTP
framing to the matching server. No business admission decisions live here.
"""

from __future__ import annotations

import ipaddress
import math
import re
import socket
import ssl
import time
from urllib.parse import urlsplit

from dpone.contracts.composition_dispatch_rpc import (
    DEFAULT_PAYLOAD_BYTES,
    MAX_RESPONSE_BYTES,
    RPC_CONTENT_TYPE,
    RPC_ROUTE,
    DispatchRpcRequest,
    DispatchRpcResponse,
    decode_response,
    require_bearer_token,
    require_payload_limit,
    validate_payload,
)

MAX_HTTP_HEADERS_BYTES = 16 * 1024


class DispatchHttpError(RuntimeError):
    """Transport uncertainty; callers must not retry a mutation automatically."""


def require_timeout(value: float) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise DispatchHttpError("rpc_timeout_configuration")


def remaining(deadline: float) -> float:
    """Recompute before every I/O operation, preventing slow-drip extensions."""
    value = deadline - time.monotonic()
    if value <= 0:
        raise DispatchHttpError("rpc_deadline")
    return value


def receive_exact(connection: ssl.SSLSocket, length: int, deadline: float) -> bytes:
    chunks = []
    while length:
        connection.settimeout(remaining(deadline))
        chunk = connection.recv(min(length, 64 * 1024))
        if not chunk:
            raise DispatchHttpError("rpc_body_truncated")
        chunks.append(chunk)
        length -= len(chunk)
    return b"".join(chunks)


def send_bytes(connection: ssl.SSLSocket, document: bytes, deadline: float) -> None:
    view = memoryview(document)
    while view:
        connection.settimeout(remaining(deadline))
        sent = connection.send(view[: 64 * 1024])
        if not sent:
            raise DispatchHttpError("rpc_connection_closed")
        view = view[sent:]


def receive_headers(connection: ssl.SSLSocket, deadline: float) -> tuple[str, dict[str, str]]:
    """Never prefetch body bytes: authentication happens after this returns."""
    document = bytearray()
    while not document.endswith(b"\r\n\r\n"):
        if len(document) >= MAX_HTTP_HEADERS_BYTES:
            raise DispatchHttpError("rpc_headers_size")
        document.extend(receive_exact(connection, 1, deadline))
    try:
        lines = document.decode("ascii").split("\r\n")
        headers: dict[str, str] = {}
        for line in lines[1:-2]:
            name, separator, value = line.partition(":")
            if not separator or re.fullmatch(r"[A-Za-z-]+", name) is None or name.lower() in headers:
                raise DispatchHttpError("rpc_headers_invalid")
            if not value.startswith(" ") or any(ord(char) < 32 or ord(char) > 126 for char in value):
                raise DispatchHttpError("rpc_headers_invalid")
            headers[name.lower()] = value[1:]
        return lines[0], headers
    except UnicodeError:
        raise DispatchHttpError("rpc_headers_invalid") from None


def content_length(headers: dict[str, str], maximum: int) -> int:
    """One canonical Content-Length, no transfer coding or content encoding."""
    value = headers.get("content-length", "")
    if re.fullmatch(r"[1-9][0-9]{0,10}", value) is None or int(value) > maximum:
        raise DispatchHttpError("rpc_content_length")
    if headers.get("content-type") != RPC_CONTENT_TYPE or headers.get("connection") != "close":
        raise DispatchHttpError("rpc_content_type")
    return int(value)


def _endpoint(value: str) -> tuple[str, int]:
    try:
        parsed = urlsplit(value)
        host = parsed.hostname or ""
        ipaddress.ip_address(host)
        if (
            parsed.scheme != "https"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or "%" in host
            or "?" in value
            or "#" in value
        ):
            raise ValueError
        port = 443 if parsed.port is None else parsed.port
        if not 0 < port < 65536:
            raise ValueError
        return host, port
    except (ValueError, TypeError, AttributeError):
        raise DispatchHttpError("rpc_endpoint_configuration") from None


class DispatchHttpClient:
    """Each call opens one verified TLS connection and sends exactly once.

    A non-ACK, timeout, lost response, malformed evidence or identity mismatch
    raises a fixed error. Recovery must consult protected journals explicitly.
    The bearer must be independently provisioned for this dispatcher endpoint.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        ssl_context: ssl.SSLContext,
        bearer_token: str,
        timeout_seconds: float = 30,
        max_payload_bytes: int = DEFAULT_PAYLOAD_BYTES,
    ) -> None:
        self._host, self._port = _endpoint(endpoint)
        if (
            not isinstance(ssl_context, ssl.SSLContext)
            or not ssl_context.check_hostname
            or ssl_context.verify_mode != ssl.CERT_REQUIRED
            or ssl_context.minimum_version < ssl.TLSVersion.TLSv1_2
        ):
            raise DispatchHttpError("rpc_tls_configuration")
        require_bearer_token(bearer_token)
        require_payload_limit(max_payload_bytes)
        require_timeout(timeout_seconds)
        self._context = ssl_context
        self._token = bearer_token
        self._timeout = timeout_seconds
        self._max_payload = max_payload_bytes

    def call(self, request: DispatchRpcRequest, payload: bytes = b"") -> DispatchRpcResponse:
        validate_payload(request, payload, self._max_payload)
        metadata = request.to_bytes()
        deadline = time.monotonic() + self._timeout
        family = socket.AF_INET6 if ":" in self._host else socket.AF_INET
        host = f"[{self._host}]" if family == socket.AF_INET6 else self._host
        header = (
            f"POST {RPC_ROUTE} HTTP/1.1\r\nHost: {host}:{self._port}\r\n"
            f"Authorization: Bearer {self._token}\r\nContent-Type: {RPC_CONTENT_TYPE}\r\n"
            f"Content-Length: {4 + len(metadata) + len(payload)}\r\nConnection: close\r\n\r\n"
        ).encode("ascii")
        try:
            with socket.socket(family, socket.SOCK_STREAM) as raw:
                raw.settimeout(remaining(deadline))
                raw.connect((self._host, self._port))
                with self._context.wrap_socket(
                    raw, server_hostname=self._host, do_handshake_on_connect=False
                ) as connection:
                    connection.settimeout(remaining(deadline))
                    connection.do_handshake()
                    for part in (header, len(metadata).to_bytes(4, "big"), metadata, payload):
                        send_bytes(connection, part, deadline)
                    status, headers = receive_headers(connection, deadline)
                    if status != "HTTP/1.1 200 OK" or set(headers) != {"content-type", "content-length", "connection"}:
                        raise DispatchHttpError("rpc_response_http")
                    document = receive_exact(connection, content_length(headers, MAX_RESPONSE_BYTES), deadline)
                    connection.settimeout(remaining(deadline))
                    if connection.recv(1):
                        raise DispatchHttpError("rpc_response_overflow")
                    response = decode_response(document, request)
                    if response.status != "ACKNOWLEDGED":
                        raise DispatchHttpError("rpc_response_not_acknowledged")
                    return response
        except (OSError, ValueError, DispatchHttpError):
            raise DispatchHttpError("rpc_transport_unknown") from None

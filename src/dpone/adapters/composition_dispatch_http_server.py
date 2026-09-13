"""Bounded TLS frontend for the closed dispatcher application service.

Authentication is mandatory before reading any body and again with the decoded
subject before reading Native bytes. The injected application handler verifies
protected originals, owns journals and must honor the supplied absolute deadline.
Transport acknowledgement alone never establishes mutation success.
"""

from __future__ import annotations

import hmac
import ipaddress
import select
import socket
import socketserver
import ssl
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeAlias

from dpone.adapters.composition_dispatch_http_client import (
    DispatchHttpError,
    content_length,
    receive_exact,
    receive_headers,
    remaining,
    require_timeout,
    send_bytes,
)
from dpone.contracts.composition_dispatch_rpc import (
    DEFAULT_PAYLOAD_BYTES,
    MAX_METADATA_BYTES,
    RPC_CONTENT_TYPE,
    RPC_ROUTE,
    DispatchRpcRequest,
    DispatchRpcResponse,
    decode_metadata,
    decode_response,
    require_bearer_token,
    require_payload_limit,
    validate_payload,
)


@dataclass(frozen=True, slots=True)
class DispatchRequestContext:
    """Peer and optional authenticated metadata; never contains a credential."""

    peer_address: tuple[str, int]
    request: DispatchRpcRequest | None = None


Authenticator: TypeAlias = Callable[[DispatchRequestContext, str], object]
DispatchHandler: TypeAlias = Callable[[DispatchRpcRequest, bytes, float], DispatchRpcResponse]


def make_bearer_authenticator(
    bearer_token: str, authorize_context: Callable[[DispatchRequestContext], object]
) -> Authenticator:
    """Constant-time dedicated bearer comparison plus mandatory context policy.

    The policy is invoked twice: peer-only before metadata, then with the closed
    request. It must raise on denial and return None on acceptance. A credential
    must contain independently generated cryptographic randomness; the factory
    validates its bounded encoding, not its entropy or provisioning provenance.
    """
    require_bearer_token(bearer_token)
    if not callable(authorize_context):
        raise DispatchHttpError("rpc_authenticator_configuration")
    expected = ("Bearer " + bearer_token).encode("ascii")

    def authenticate(context: DispatchRequestContext, authorization: str) -> None:
        try:
            if not hmac.compare_digest(authorization.encode("ascii"), expected):
                raise DispatchHttpError("rpc_unauthorized")
            if authorize_context(context) is not None:
                raise DispatchHttpError("rpc_unauthorized")
        except Exception:
            raise DispatchHttpError("rpc_unauthorized") from None

    return authenticate


class _RequestHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        server = self.server
        assert isinstance(server, DispatchHttpServer)
        deadline = time.monotonic() + server.timeout_seconds
        try:
            with server.ssl_context.wrap_socket(
                self.request, server_side=True, do_handshake_on_connect=False
            ) as connection:
                connection.settimeout(remaining(deadline))
                connection.do_handshake()
                self._serve(connection, server, deadline)
        except Exception:
            # Peer disconnects and TLS failures must never print request/secret
            # context through socketserver's default traceback logger.
            return

    def _serve(self, connection: ssl.SSLSocket, server: DispatchHttpServer, deadline: float) -> None:
        error_status = "400 Bad Request"
        try:
            line, headers = receive_headers(connection, deadline)
            if line != f"POST {RPC_ROUTE} HTTP/1.1" or set(headers) != {
                "host",
                "authorization",
                "content-type",
                "content-length",
                "connection",
            }:
                raise DispatchHttpError("rpc_request_http")
            total = content_length(headers, 4 + MAX_METADATA_BYTES + server.max_payload_bytes)
            if total < 5:
                raise DispatchHttpError("rpc_content_length")
            error_status = "401 Unauthorized"
            peer = (str(self.client_address[0]), int(self.client_address[1]))
            self._authenticate(server, DispatchRequestContext(peer), headers["authorization"])
            error_status = "400 Bad Request"
            size = int.from_bytes(receive_exact(connection, 4, deadline), "big")
            if not 0 < size <= MAX_METADATA_BYTES or 4 + size > total:
                raise DispatchHttpError("rpc_metadata_size")
            request = decode_metadata(receive_exact(connection, size, deadline))
            payload_length, _ = request.payload_spec
            if payload_length > server.max_payload_bytes or total != 4 + size + payload_length:
                raise DispatchHttpError("rpc_frame_size")
            error_status = "401 Unauthorized"
            self._authenticate(server, DispatchRequestContext(peer, request), headers["authorization"])
            error_status = "400 Bad Request"
            payload = receive_exact(connection, payload_length, deadline)
            validate_payload(request, payload, server.max_payload_bytes)
            # Only one request is accepted per connection. Reject bytes already
            # queued beyond the declared frame; later pipelined bytes are never
            # parsed or executed because the connection always closes.
            if connection.pending() or select.select([connection], [], [], 0)[0]:
                connection.settimeout(remaining(deadline))
                if connection.recv(1):
                    raise DispatchHttpError("rpc_body_overflow")
            error_status = "500 Internal Server Error"
            remaining(deadline)
            response = server.handler(request, payload, deadline)
            remaining(deadline)
            document = response.to_bytes()
            decode_response(document, request)
            self._respond(connection, "200 OK", document, deadline)
        except Exception:
            self._respond(connection, error_status, b'{"error":"rpc_request_rejected"}', deadline)

    @staticmethod
    def _authenticate(server: DispatchHttpServer, context: DispatchRequestContext, authorization: str) -> None:
        if server.authenticator(context, authorization) is not None:
            raise DispatchHttpError("rpc_unauthorized")

    @staticmethod
    def _respond(connection: ssl.SSLSocket, status: str, document: bytes, deadline: float) -> None:
        header = (
            f"HTTP/1.1 {status}\r\nContent-Type: {RPC_CONTENT_TYPE}\r\n"
            f"Content-Length: {len(document)}\r\nConnection: close\r\n\r\n"
        ).encode("ascii")
        send_bytes(connection, header, deadline)
        send_bytes(connection, document, deadline)


class DispatchHttpServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Bounded concurrent requests, including TLS handshakes and slow bodies.

    Construct through create_dispatch_http_server, then call serve_forever.
    shutdown must be called from another thread; server_close releases the
    listener. Pending handlers retain their slots until they return, even when
    their deadline expires: forcibly interrupting a mutation would be unsafe.
    Application handlers must enforce the supplied deadline on their own I/O.
    Saturation closes an unaccepted connection without reading its body.
    """

    daemon_threads = False
    allow_reuse_address = False

    def __init__(
        self,
        address: tuple[str, int],
        *,
        ssl_context: ssl.SSLContext,
        authenticator: Authenticator,
        handler: DispatchHandler,
        timeout_seconds: float,
        max_payload_bytes: int,
        max_concurrency: int,
    ) -> None:
        if (
            not isinstance(ssl_context, ssl.SSLContext)
            or ssl_context.protocol != ssl.PROTOCOL_TLS_SERVER
            or ssl_context.minimum_version < ssl.TLSVersion.TLSv1_2
        ):
            raise DispatchHttpError("rpc_tls_configuration")
        if not callable(authenticator) or not callable(handler):
            raise DispatchHttpError("rpc_handler_configuration")
        require_timeout(timeout_seconds)
        require_payload_limit(max_payload_bytes)
        if type(max_concurrency) is not int or not 0 < max_concurrency <= 256:
            raise DispatchHttpError("rpc_concurrency_configuration")
        try:
            ip = ipaddress.ip_address(address[0])
            if "%" in address[0] or type(address[1]) is not int or not 0 <= address[1] <= 65535:
                raise ValueError
        except (TypeError, ValueError, IndexError):
            raise DispatchHttpError("rpc_listener_configuration") from None
        self.address_family = socket.AF_INET6 if ip.version == 6 else socket.AF_INET
        self.ssl_context = ssl_context
        self.authenticator = authenticator
        self.handler = handler
        self.timeout_seconds = timeout_seconds
        self.max_payload_bytes = max_payload_bytes
        self.request_queue_size = max_concurrency
        self._slots = threading.BoundedSemaphore(max_concurrency)
        super().__init__(address, _RequestHandler)

    def process_request(
        self, request: socket.socket | tuple[bytes, socket.socket], client_address: tuple[str, int]
    ) -> None:
        if not self._slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self._slots.release()
            raise

    def process_request_thread(
        self, request: socket.socket | tuple[bytes, socket.socket], client_address: tuple[str, int]
    ) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._slots.release()

    def handle_error(
        self, request: socket.socket | tuple[bytes, socket.socket], client_address: tuple[str, int]
    ) -> None:
        """Do not expose exception locals or request headers in default logging."""


def create_dispatch_http_server(
    address: tuple[str, int],
    *,
    ssl_context: ssl.SSLContext,
    authenticator: Authenticator,
    handler: DispatchHandler,
    timeout_seconds: float = 30,
    max_payload_bytes: int = DEFAULT_PAYLOAD_BYTES,
    max_concurrency: int = 8,
) -> DispatchHttpServer:
    """Create a listener with required TLS, authentication and business handler.

    This factory performs no background startup. The owner calls serve_forever
    and owns shutdown. Port zero is supported for local testing. Production
    provisioning must pin the listener, certificate and independent bearer.
    """
    return DispatchHttpServer(
        address,
        ssl_context=ssl_context,
        authenticator=authenticator,
        handler=handler,
        timeout_seconds=timeout_seconds,
        max_payload_bytes=max_payload_bytes,
        max_concurrency=max_concurrency,
    )

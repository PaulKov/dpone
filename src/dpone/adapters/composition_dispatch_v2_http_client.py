"""Metadata-only whole-cell TLS client; uncertain transport never invents a receipt.

The short acceptance deadline bounds connection, TLS and request submission.
One execution deadline and its bounded cleanup grace then bound the complete
response, including EOF. Neither response chunks nor retries extend a budget.
"""

from __future__ import annotations

import socket
import ssl
import time

from dpone.adapters.composition_dispatch_http_client import (
    DispatchHttpError,
    content_length,
    receive_exact,
    receive_headers,
    remaining,
    require_endpoint,
    require_timeout,
    require_tls_context,
    send_bytes,
)
from dpone.contracts.composition_dispatch_rpc import RPC_CONTENT_TYPE, require_bearer_token
from dpone.contracts.composition_dispatch_v2 import (
    MAX_RESPONSE_BYTES,
    RPC_PATH,
    DispatchV2Request,
    DispatchV2Response,
    decode_response,
    encode_request,
)


class DispatchV2HttpClient:
    """One numeric-IP verified connection and one request; no automatic replay.

    IN_PROGRESS/UNKNOWN retain their validated RUNNING/COMMIT_UNKNOWN evidence.
    Late terminal success or failure is transport uncertainty, not a new receipt.
    Endpoint bearer provisioning remains the protected composition root's duty.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        ssl_context: ssl.SSLContext,
        bearer_token: str,
        accept_timeout_seconds: float = 30,
        execution_timeout_seconds: int = 900,
        cleanup_timeout_seconds: float = 60,
    ) -> None:
        self._host, self._port = require_endpoint(endpoint)
        require_tls_context(ssl_context)
        require_bearer_token(bearer_token)
        for value in (accept_timeout_seconds, execution_timeout_seconds, cleanup_timeout_seconds):
            require_timeout(value)
        if (
            accept_timeout_seconds > 30
            or type(execution_timeout_seconds) is not int
            or not 1 <= execution_timeout_seconds <= 900
            or cleanup_timeout_seconds > 60
        ):
            raise DispatchHttpError("rpc_v2_budget_configuration")
        self._context, self._token = ssl_context, bearer_token
        self._accept, self._execution, self._cleanup = (
            accept_timeout_seconds,
            execution_timeout_seconds,
            cleanup_timeout_seconds,
        )

    def call(self, request: DispatchV2Request) -> DispatchV2Response:
        """Send no source payload; return only complete correlated canonical evidence."""
        try:
            if type(request) is not DispatchV2Request:
                raise ValueError
            request.__post_init__()
            frame = encode_request(request)
            accept_deadline = time.monotonic() + self._accept
            family = socket.AF_INET6 if ":" in self._host else socket.AF_INET
            host = f"[{self._host}]" if family == socket.AF_INET6 else self._host
            header = (
                f"POST {RPC_PATH} HTTP/1.1\r\nHost: {host}:{self._port}\r\n"
                f"Authorization: Bearer {self._token}\r\nContent-Type: {RPC_CONTENT_TYPE}\r\n"
                f"Content-Length: {len(frame)}\r\nConnection: close\r\n\r\n"
            ).encode("ascii")
            with socket.socket(family, socket.SOCK_STREAM) as raw:
                raw.settimeout(remaining(accept_deadline))
                raw.connect((self._host, self._port))
                with self._context.wrap_socket(
                    raw, server_hostname=self._host, do_handshake_on_connect=False
                ) as connection:
                    connection.settimeout(remaining(accept_deadline))
                    connection.do_handshake()
                    # Start before submission: the server may start as the final bytes arrive.
                    execution_deadline = time.monotonic() + self._execution
                    response_deadline = execution_deadline + self._cleanup
                    send_bytes(connection, header, accept_deadline)
                    send_bytes(connection, frame, accept_deadline)
                    status, headers = receive_headers(connection, response_deadline)
                    if status != "HTTP/1.1 200 OK" or set(headers) != {"content-type", "content-length", "connection"}:
                        raise DispatchHttpError("rpc_v2_response_http")
                    document = receive_exact(connection, content_length(headers, MAX_RESPONSE_BYTES), response_deadline)
                    connection.settimeout(remaining(response_deadline))
                    if connection.recv(1):
                        raise DispatchHttpError("rpc_v2_response_overflow")
                    response = decode_response(document, request)
                    remaining(response_deadline)
                    if response.status in {"SUCCEEDED", "FAILED"}:
                        remaining(execution_deadline)
                    return response
        except Exception:
            raise DispatchHttpError("rpc_v2_transport_unknown") from None

"""One closed synchronous ClickHouse HTTP request after a durable gateway claim.

No proxies, redirects, retries, sessions or asynchronous inserts. HTTP 200 alone
is insufficient (https://clickhouse.com/docs/interfaces/http): only a complete,
framed, bounded empty response with no error header acknowledges these commands.
Acknowledgement is transport evidence, not catalog outcome or writer quiescence.
"""

from __future__ import annotations

import base64
import http.client
import math
import socket
import ssl
import time
from dataclasses import dataclass, field
from hashlib import sha256
from threading import Timer
from typing import Protocol
from urllib.parse import urlencode, urlsplit

from dpone.contracts.composition_clickhouse_dispatch import (
    MAX_DISPATCH_PAYLOAD_BYTES,
    ClickHouseDispatch,
    CreateGenerationDispatch,
    ExchangeSnapshotDispatch,
    InsertGenerationDispatch,
)
from dpone.contracts.composition_identity import CompositionAdmissionError


class ClickHouseDispatchJournal(Protocol):
    """Commit the unique global-SQL claim before socket creation.

    The trusted gateway reopens exact ACTIVE/RUNNING ownership, epochs, physical
    enrollment, scoped credentials and immutable operation originals here.
    Existing claims or lost commit acknowledgements MUST raise; durable readback
    is not a new executor permit. Closure retains every pending claim until its
    complete response and outcome are durably recorded by the gateway.
    """

    def claim_once(self, dispatch: ClickHouseDispatch) -> None: ...


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
class ClickHouseDispatchObservation:
    """Complete transport observation; root persists it before worker ACK.

    request_body_bytes counts payload octets actually accepted by socket.send,
    excluding HTTP headers, URI SQL, TLS records and TCP framing. It is not
    source-export bytes. No failed/partial observation is a terminal receipt.
    """

    dispatch_sha256: str
    claim_key: str
    query_id: str
    request_body_bytes: int
    response_body_bytes: int
    response_body_sha256: str
    response_framing: str


class ClickHouseDispatchTransportError(RuntimeError):
    """Unacknowledged request; retain its claim and never resend automatically.

    The byte count is a lower bound on failures: a failed send may have accepted
    additional bytes without returning a count. Do not refund its reservation.
    Response/server/driver text and credential values are deliberately omitted.
    """

    def __init__(self, *, dispatch_sha256: str, request_body_bytes: int) -> None:
        super().__init__("ClickHouse dispatch is unacknowledged; protected reconciliation is required")
        self.dispatch_sha256 = dispatch_sha256
        self.request_body_bytes = request_body_bytes


def _statement(dispatch: ClickHouseDispatch) -> str:
    target = dispatch.target
    generation = f"`{target.database}`.`{target.generation_table}`"
    if type(dispatch) is CreateGenerationDispatch:
        fields = ", ".join(f"`{column.name}` {column.type_name}" for column in dispatch.columns)
        return f"CREATE TABLE {generation} UUID '{dispatch.generation_uuid}' ({fields}) ENGINE = MergeTree ORDER BY tuple()"
    if type(dispatch) is InsertGenerationDispatch:
        fields = ", ".join(f"`{column.name}`" for column in dispatch.columns)
        return f"INSERT INTO {generation} ({fields}) FORMAT Native"
    if type(dispatch) is ExchangeSnapshotDispatch:
        return f"EXCHANGE TABLES `{target.database}`.`{target.target_table}` AND {generation}"
    raise CompositionAdmissionError("clickhouse_dispatch_operation")


class ClickHouseDispatchTransport:
    """Use only inside the trusted gateway; DTO possession grants no authority."""

    def __init__(
        self,
        *,
        endpoint: str,
        credentials: ClickHouseTransportCredentials,
        journal: ClickHouseDispatchJournal,
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
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
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
        self._host, self._port, self._scheme = parsed.hostname or "", port, parsed.scheme
        self._context = ssl.create_default_context(cafile=ca_file) if parsed.scheme == "https" else None
        self._authorization = (
            "Basic " + base64.b64encode(f"{credentials.username}:{credentials.password}".encode()).decode()
        )
        self._journal = journal
        self._timeout = float(timeout_seconds)
        self._max_payload = max_payload_bytes
        self._max_response = max_response_bytes

    def execute(self, dispatch: ClickHouseDispatch, *, payload: bytes = b"") -> ClickHouseDispatchObservation:
        """Validate originals, claim once, send once, drain a complete response.

        Callers must persist the returned observation before acknowledging work.
        A crash between response and persistence leaves the durable claim pending.
        This method never reconciles/reopens a claim and never changes ownership.
        """
        if type(dispatch) not in {CreateGenerationDispatch, InsertGenerationDispatch, ExchangeSnapshotDispatch}:
            raise CompositionAdmissionError("clickhouse_dispatch_operation")
        dispatch.__post_init__()
        dispatch_hash = dispatch.dispatch_sha256
        if type(payload) is not bytes or len(payload) > self._max_payload:
            raise CompositionAdmissionError("clickhouse_dispatch_payload_budget")
        if isinstance(dispatch, InsertGenerationDispatch):
            if (
                len(payload) != dispatch.payload_bytes
                or "sha256:" + sha256(payload).hexdigest() != dispatch.payload_sha256
            ):
                raise CompositionAdmissionError("clickhouse_dispatch_payload_identity")
        elif payload:
            raise CompositionAdmissionError("clickhouse_dispatch_unexpected_payload")
        path = "/?" + urlencode(
            {
                "query": _statement(dispatch),
                "query_id": dispatch.query_id,
                "wait_end_of_query": "1",
                "async_insert": "0",
                "wait_for_async_insert": "1",
                "send_progress_in_http_headers": "0",
                "enable_http_compression": "0",
            }
        )
        if len(path) > 1024 * 1024:
            raise CompositionAdmissionError("clickhouse_dispatch_uri_budget")
        self._journal.claim_once(dispatch)
        deadline = time.monotonic() + self._timeout
        connection = self._connection()
        sent = 0
        timer: Timer | None = None
        try:
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
            assert connection.sock is not None
            transport_socket = connection.sock
            timer = Timer(max(0, deadline - time.monotonic()), _expire, args=(transport_socket,))
            timer.daemon = True
            timer.start()
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
                if response.headers.get_all("X-ClickHouse-Query-Id", []) != [dispatch.query_id]:
                    raise ValueError
            finally:
                response.close()
            return ClickHouseDispatchObservation(
                dispatch_hash,
                dispatch.claim_key,
                dispatch.query_id,
                sent,
                len(body),
                "sha256:" + sha256(body).hexdigest(),
                framing,
            )
        except Exception:
            raise ClickHouseDispatchTransportError(dispatch_sha256=dispatch_hash, request_body_bytes=sent) from None
        finally:
            if timer is not None:
                timer.cancel()
            connection.close()

    def _connection(self) -> http.client.HTTPConnection:
        if self._scheme == "https":
            return http.client.HTTPSConnection(self._host, self._port, timeout=self._timeout, context=self._context)
        return http.client.HTTPConnection(self._host, self._port, timeout=self._timeout)

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
        if response.headers.get("Content-Encoding", "identity").lower() != "identity":
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
        if response.status != 200 or body or any(value != "0" for value in errors) or time.monotonic() >= deadline:
            raise ValueError
        return bytes(body), framing


def _expire(transport_socket: socket.socket) -> None:
    """Interrupt a slow partial header/chunk/body at the absolute deadline."""
    try:
        transport_socket.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass

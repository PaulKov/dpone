"""Pinned direct native DDL transport: one owned connection, one query, no retry.

Use only through the authority publisher. This low-level transport cannot prove
exclusive credentials, candidate sealing or outcome correctness. Socket timeout
is not a total deadline; uncertain outcomes retain the original durable owner.
"""

from __future__ import annotations

import hashlib
import importlib
import ipaddress
import math
from dataclasses import dataclass, field
from typing import Any

from dpone.contracts.clickhouse_native_publication import (
    NativePublicationCompletion,
    NativePublicationError,
    NativePublicationRequest,
)


@dataclass(frozen=True)
class NativePublicationEndpoint:
    """Explicit registered endpoint. Plaintext is for isolated local tests only."""

    server_id: str
    host: str
    port: int
    user: str
    password: str = field(repr=False)
    connect_timeout: float = 10.0
    send_receive_timeout: float = 30.0
    secure: bool = True
    ca_certs: str | None = None
    server_hostname: str | None = None

    def __post_init__(self) -> None:
        if type(self.host) is not str or str(ipaddress.IPv4Address(self.host)) != self.host:
            raise ValueError("Native publication requires one literal IPv4 address")
        if type(self.port) is not int or not 0 < self.port < 65536 or type(self.secure) is not bool:
            raise ValueError("Invalid native port or TLS mode")
        for timeout in (self.connect_timeout, self.send_receive_timeout):
            if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
                raise ValueError("Native socket timeouts must be finite and positive")
        for value in (self.server_id, self.user):
            if type(value) is not str or not value or value != value.strip() or "\x00" in value:
                raise ValueError("Invalid registered native identity")
        if type(self.password) is not str:
            raise ValueError("Native credentials must be explicit strings")
        for tls_value in (self.ca_certs, self.server_hostname):
            if tls_value is not None and (type(tls_value) is not str or not tls_value or "\x00" in tls_value):
                raise ValueError("Invalid native TLS configuration")
        if not self.secure and (self.ca_certs is not None or self.server_hostname is not None):
            raise ValueError("TLS configuration cannot be ignored on a plaintext connection")


def _drain(connection: Any, packets: Any) -> None:
    auxiliary = {packets.PROGRESS, packets.PROFILE_INFO, packets.LOG, packets.PROFILE_EVENTS, packets.TIMEZONE_UPDATE}
    while True:
        packet = connection.receive_packet()
        if packet.type == packets.END_OF_STREAM:
            return
        if packet.type in auxiliary:
            continue
        if packet.type == packets.DATA and packet.block.num_rows == 0 and packet.block.num_columns == 0:
            continue
        raise NativePublicationError("Native publication did not return an allowed successful response")


class DirectNativePublicationTransport:
    """Synchronous single-use connection per call; never reuse a generic client."""

    def __init__(self, endpoint: NativePublicationEndpoint) -> None:
        self._endpoint = endpoint

    def execute(self, request: NativePublicationRequest) -> NativePublicationCompletion:
        """Return only after successful EOS and synchronous connection teardown."""
        endpoint = self._endpoint
        statement = request.statement
        if statement is None or request.binding.subject.server_id != endpoint.server_id:
            raise NativePublicationError("No mutation or registered endpoint mismatch") from None
        try:
            driver = importlib.import_module("clickhouse_driver")
            packets = importlib.import_module("clickhouse_driver.protocol").ServerPacketTypes
            if driver.__version__ != "0.2.10":
                raise NativePublicationError("Native publication requires clickhouse-driver 0.2.10")
            client = driver.Client(
                host=endpoint.host,
                port=endpoint.port,
                database=request.binding.subject.database,
                user=endpoint.user,
                password=endpoint.password,
                compression=False,
                disable_reconnect=True,
                connect_timeout=endpoint.connect_timeout,
                send_receive_timeout=endpoint.send_receive_timeout,
                secure=endpoint.secure,
                verify=True,
                ca_certs=endpoint.ca_certs,
                server_hostname=endpoint.server_hostname,
            )
            try:
                connection = client.connection
                connection.connect()
                info = connection.server_info
                version = (info.version_major, info.version_minor, info.version_patch)
                if (
                    connection.socket.getpeername() != (endpoint.host, endpoint.port)
                    or version != (24, 8, 14)
                    or info.name != "ClickHouse"
                    or not connection.connected
                ):
                    raise NativePublicationError("Native peer or server version outside the pinned profile")
                connection.send_query(statement, query_id=request.intent.query_id, params=None)
                connection.send_external_tables(None)
                _drain(connection, packets)
                completion = NativePublicationCompletion(
                    request.intent.operation_id,
                    request.intent.query_id,
                    endpoint.server_id,
                    hashlib.sha256(statement.encode("utf-8")).hexdigest(),
                    version,
                    info.revision,
                    driver.__version__,
                )
            finally:
                client.disconnect()
            return completion
        except Exception:
            # Driver errors may carry credentials, queries or server-provided
            # payloads. Do not expose their text or traceback as public output.
            raise NativePublicationError(
                "Native publication lacks confirmed completion; inspect original authority"
            ) from None

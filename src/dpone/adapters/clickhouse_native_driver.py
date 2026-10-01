"""Shared pinned-native peer and private-logging integration boundary.

The SDK logs raw packet/error/query text through module globals. Rebind only
the owned connection's logging methods to private globals; preserve their exact
code instead of reimplementing protocol decoding. No SDK class, module or shared
logger is changed. This is a pinned integration seam, not a general patch API.
"""

import logging
from types import FunctionType, MethodType
from typing import Any, Protocol


class _NativeEndpoint(Protocol):
    """Read-only transport input shape, not endpoint validation or authority.

    Composition supplies an already-validated endpoint. Structural typing keeps
    candidate I/O independent of the publication adapter without relocating its
    existing public endpoint dataclass. No SDK import or connection occurs here.
    """

    @property
    def server_id(self) -> str: ...

    @property
    def host(self) -> str: ...

    @property
    def port(self) -> int: ...

    @property
    def user(self) -> str: ...

    @property
    def password(self) -> str: ...

    @property
    def connect_timeout(self) -> float: ...

    @property
    def send_receive_timeout(self) -> float: ...

    @property
    def secure(self) -> bool: ...

    @property
    def ca_certs(self) -> str | None: ...

    @property
    def server_hostname(self) -> str | None: ...


def matches_native_peer(connection: Any, endpoint: _NativeEndpoint, version: tuple[int, int, int], info: Any) -> bool:
    """Inspect the owned socket and captured handshake facts without sending.

    Callers retain the captured ``info`` and ``version`` for their receipts.
    Preserve short-circuit ordering and propagate lookup errors to each
    transport's distinct sanitization boundary. This is not completion proof.
    """
    return not (
        connection.socket.getpeername() != (endpoint.host, endpoint.port)
        or version != (24, 8, 14)
        or info.name != "ClickHouse"
        or not connection.connected
    )


LOGGING_METHODS = (
    "force_connect",
    "connect",
    "disconnect",
    "receive_hello",
    "ping",
    "receive_packet",
    "send_data",
    "send_query",
)


def _discard_log_block(block: Any) -> None:
    """Consume already-decoded server diagnostics without emitting their rows."""


def isolate_native_logging(connection: Any) -> None:
    """Fail closed on drift, then bind all methods before any socket activity."""
    originals = []
    for name in LOGGING_METHODS:
        method = getattr(connection, name, None)
        function = getattr(method, "__func__", None)
        if (
            not isinstance(function, FunctionType)
            or function.__module__ != "clickhouse_driver.connection"
            or "logger" not in function.__code__.co_names
            or "logger" not in function.__globals__
            or not callable(function.__globals__.get("log_block"))
        ):
            raise RuntimeError("Native driver logging profile differs from the audited methods")
        originals.append((name, function))
    logger = logging.Logger("dpone.native.private")  # Not registered with logging's global manager.
    logger.disabled = True
    for name, function in originals:
        private_globals = function.__globals__ | {"logger": logger, "log_block": _discard_log_block}
        private = FunctionType(
            function.__code__, private_globals, function.__name__, function.__defaults__, function.__closure__
        )
        private.__kwdefaults__ = function.__kwdefaults__
        setattr(connection, name, MethodType(private, connection))

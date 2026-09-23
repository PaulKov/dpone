"""Outbound public and credential frames for permission-grant custody."""

import struct
from collections.abc import Callable
from dataclasses import replace
from typing import Any, Never, Protocol

from dpone.adapters.mssql_sqlclient_permission_grant_process_state import (
    PermissionGrantCredentialAttempt,
    PermissionGrantPublicAttempt,
)

__all__ = ["PermissionGrantCredentialAttempt", "PermissionGrantPublicAttempt", "send_credentials", "send_public"]


class _PermissionGrantSendOwner(Protocol):
    _operation_deadline: float
    _binding: Any
    _wire: Any
    _credential: Any | None
    _channel: Any
    _busy: bool

    def _enter(self) -> None: ...

    def _attempt(self, direction: str, kind: Any, ordinal: int) -> int: ...

    def _deadline(self, ceiling: float, supplied: float | None) -> float: ...

    def _replace_attempt(self, index: int, **changes: object) -> None: ...

    def _send(self, index: int, data: bytes, deadline: float) -> None: ...

    def _fail(self, error: BaseException) -> Never: ...


def send_public(
    owner: _PermissionGrantSendOwner,
    kind: Any,
    ordinal: int,
    body: dict,
    *,
    deadline: float | None = None,
    dependencies: tuple[Any, str],
) -> Any:
    """Encode and write one admitted public frame, retaining exact progress."""
    wire, error_code = dependencies
    owner._enter()
    index = owner._attempt("PARENT_TO_CHILD", kind, ordinal)
    try:
        effective = owner._deadline(owner._operation_deadline, deadline)
        outbound_kinds = (
            wire.PermissionWireKind.REQUEST,
            wire.PermissionWireKind.EXECUTE,
            wire.PermissionWireKind.CHECK_HELD,
            wire.PermissionWireKind.RELEASE,
        )
        if type(kind) is not wire.PermissionWireKind or kind not in outbound_kinds:
            raise ValueError(error_code)
        payload = wire.encode_permission_message(owner._binding, kind, ordinal, body)
        prefix = struct.pack("!I", len(payload))
        owner._replace_attempt(index, prefix=prefix, payload=payload, encoded=True)
        message = owner._wire.accept(payload, direction="PARENT_TO_CHILD")
        framed = wire.frame_permission_payload(payload)
        if framed != prefix + payload:
            raise ValueError(error_code)
        owner._send(index, framed, effective)
        owner._replace_attempt(index, complete=True)
        owner._busy = False
        return message
    except BaseException as error:
        owner._fail(error)


def send_credentials(
    owner: _PermissionGrantSendOwner,
    payload: bytes,
    *,
    deadline: float | None = None,
    dependencies: tuple[Any, str, Callable[..., bool], Callable[..., Any]],
) -> None:
    """Write the one credential frame while retaining counts rather than bytes."""
    wire, error_code, socket_ready, credential_attempt = dependencies
    owner._enter()
    try:
        effective = owner._deadline(owner._operation_deadline, deadline)
        if type(payload) is not bytes or not 0 < len(payload) <= wire.CREDENTIAL_LIMIT or owner._credential is not None:
            raise ValueError(error_code)
        size = len(payload)
        owner._credential = credential_attempt(size, size + 4, 0, False)
        owner._wire.consume_credentials(payload_size=size)
        for part in (memoryview(struct.pack("!I", size)), memoryview(payload)):
            while part:
                if not socket_ready(owner._channel, effective, writing=True):
                    raise TimeoutError(error_code)
                try:
                    written = owner._channel.send(part)
                except BlockingIOError:
                    continue
                if written <= 0:
                    raise ValueError(error_code)
                part = part[written:]
                current = owner._credential
                assert current is not None
                owner._credential = replace(current, transferred=current.transferred + written)
        current = owner._credential
        assert current is not None
        owner._credential = replace(current, complete=True)
        owner._busy = False
    except BaseException as error:
        owner._fail(error)

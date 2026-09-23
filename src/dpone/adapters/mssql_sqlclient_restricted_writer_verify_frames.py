"""Bounded deadline-aware socket frames for the private VERIFY transcript."""

import select
import socket
import struct
import time
from collections.abc import Callable
from dataclasses import dataclass

LIMIT = 1048576
ERROR = "mssql_native.sqlclient_restricted_writer_verify_invalid"


def _wait(channel: socket.socket, deadline: float, *, write: bool) -> None:
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(ERROR)
        ready = (
            select.select((), (channel,), (channel,), min(remaining, 0.05))
            if write
            else select.select((channel,), (), (channel,), min(remaining, 0.05))
        )
        if ready[2]:
            raise ValueError(ERROR)
        if ready[1 if write else 0]:
            return


def read_frame(channel: socket.socket, deadline: float) -> bytes:
    """Read one non-empty bounded frame before the absolute deadline."""

    def exact(size: int) -> bytes:
        retained = bytearray()
        while len(retained) < size:
            _wait(channel, deadline, write=False)
            part = channel.recv(size - len(retained))
            if not part:
                raise ValueError(ERROR)
            retained.extend(part)
        return bytes(retained)

    size = struct.unpack("!I", exact(4))[0]
    if not 0 < size <= LIMIT:
        raise ValueError(ERROR)
    return exact(size)


def write_frame(channel: socket.socket, payload: bytes | bytearray, deadline: float) -> None:
    """Write one non-empty bounded frame before the absolute deadline."""
    if type(payload) not in (bytes, bytearray) or not 0 < len(payload) <= LIMIT:
        raise ValueError(ERROR)
    for frame in (memoryview(struct.pack("!I", len(payload))), memoryview(payload)):
        remaining = frame
        while remaining:
            _wait(channel, deadline, write=True)
            sent = channel.send(remaining)
            if sent <= 0:
                raise ValueError(ERROR)
            remaining = remaining[sent:]


@dataclass(frozen=True)
class RestrictedWriterVerifyFrames:
    """Canonical immutable frame operations injected into a VERIFY process."""

    wait: Callable[[socket.socket, float], None]
    read: Callable[[socket.socket, float], bytes]
    write: Callable[[socket.socket, bytes | bytearray, float], None]


FRAMES = RestrictedWriterVerifyFrames(
    wait=lambda channel, deadline: _wait(channel, deadline, write=False),
    read=read_frame,
    write=write_frame,
)

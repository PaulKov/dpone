"""Pure launch-time descriptor and deadline checks for the departure worker."""

from __future__ import annotations

import math
import os
from collections.abc import Callable
from typing import Protocol


class _WriteControl(Protocol):
    def __call__(self, fd: int, payload: bytes, *, deadline: float, max_bytes: int) -> None: ...


class _EncodeMessage(Protocol):
    def __call__(self, payload: bytes, *, max_payload: int) -> bytes: ...


class _Closeable(Protocol):
    def close(self) -> None: ...


def valid_departure_descriptors(descriptors: tuple[int, int, int]) -> bool:
    """Return whether inherited descriptors are valid and pairwise distinct."""
    return not any(type(fd) is not int or fd < 0 for fd in descriptors) and len(set(descriptors)) == 3


def before_departure_deadline(deadline: float, clock: Callable[[], float]) -> None:
    """Fail closed when a bounded worker operation reaches its deadline."""
    now = clock()
    if type(deadline) is not float or not math.isfinite(deadline) or not math.isfinite(now) or now >= deadline:
        raise ValueError("mssql_native.sqlclient_departure_deadline")


def send_departure_frame(
    fd: int,
    body: bytes,
    limit: int,
    deadline: float,
    *,
    owned: set[int],
    clock: Callable[[], float],
    write_control: _WriteControl,
    encode_message: _EncodeMessage,
) -> None:
    """Transfer one descriptor and close it exactly once after the write attempt."""
    owned.remove(fd)
    try:
        before_departure_deadline(deadline, clock)
        write_control(fd, encode_message(body, max_payload=limit), deadline=deadline, max_bytes=limit + 4)
    finally:
        os.close(fd)
    before_departure_deadline(deadline, clock)


def close_departure_resources(connection: _Closeable | None, owned: set[int]) -> bool:
    """Detach and attempt every remaining worker-owned resource exactly once."""
    succeeded = True
    if connection is not None:
        try:
            connection.close()
        except BaseException:
            succeeded = False
    for fd in tuple(owned):
        owned.remove(fd)
        try:
            os.close(fd)
        except BaseException:
            succeeded = False
    return succeeded

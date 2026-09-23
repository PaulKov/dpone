"""Private mechanical readiness probes for retained nonblocking sockets."""

import select
import socket
import time


def _socket_ready(channel: socket.socket, deadline: float, *, writing: bool = False) -> bool:
    """Return whether the requested direction is ready before the absolute deadline."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return False
    ready = select.select([] if writing else [channel], [channel] if writing else [], [], remaining)
    if not any(ready):
        return False
    return time.monotonic() < deadline


def _has_queued_input(channel: socket.socket) -> bool:
    """Return whether data or EOF is immediately readable without consuming it."""
    return bool(select.select([channel], [], [], 0)[0])


def _has_unexpected_queued_input(channel: socket.socket, *, allow_eof: bool) -> bool:
    """Distinguish an allowed peer half-close from queued protocol bytes."""
    if not _has_queued_input(channel):
        return False
    try:
        queued = channel.recv(1, socket.MSG_PEEK)
    except BlockingIOError:
        return False
    return bool(queued) or not allow_eof

"""Finite nonblocking IPC for the single owning TDS supervisor thread.

The caller exclusively owns these descriptors for the duration of each call;
other threads must not close, reuse, read or change their blocking mode. This
module neither owns their lifetime nor establishes process/SQL settlement.
Control bytes stay in memory and never become diagnostics or artifacts.
"""

from __future__ import annotations

import math
import os
import select
import stat
import time
from collections.abc import Callable
from typing import Literal

from dpone.contracts.mssql_tds_frames import TdsMessageFrame, encode_message


def _remaining(deadline: float) -> float:
    if type(deadline) not in (int, float) or not math.isfinite(deadline):
        raise ValueError("mssql_native.tds_channel_deadline")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ValueError("mssql_native.tds_channel_deadline")
    return remaining


def _admit(fd: int, deadline: float) -> None:
    _remaining(deadline)
    if type(fd) is not int or fd < 0:
        raise ValueError("mssql_native.tds_channel_io")
    try:
        if not stat.S_ISFIFO(os.fstat(fd).st_mode):
            raise ValueError("mssql_native.tds_channel_requires_pipe")
        blocking = os.get_blocking(fd)
    except OSError:
        raise ValueError("mssql_native.tds_channel_io") from None
    if blocking:
        raise ValueError("mssql_native.tds_channel_requires_nonblocking")


def _wait(fd: int, *, writing: bool, deadline: float) -> None:
    poller = select.poll()
    poller.register(fd, select.POLLOUT if writing else select.POLLIN)
    while True:
        remaining = _remaining(deadline)
        try:
            events = poller.poll(min(50, math.ceil(remaining * 1000)))
        except InterruptedError:
            continue
        _remaining(deadline)
        for observed, flags in events:
            if observed != fd or flags & (select.POLLERR | select.POLLNVAL):
                raise ValueError("mssql_native.tds_channel_io")
            if writing:
                if flags & select.POLLHUP:
                    raise ValueError("mssql_native.tds_channel_io")
                if flags & select.POLLOUT:
                    return
            elif flags & (select.POLLIN | select.POLLHUP):
                return  # HUP still requires draining data and an actual read EOF.


def write_worker_control(fd: int, payload: bytes, *, deadline: float, max_bytes: int) -> None:
    """Write all private bytes within the inherited absolute attempt deadline.

    A partial write followed by failure is unresolved delivery, never a retry
    instruction. The supervisor must contain the worker before retiring SQL.
    """
    if type(max_bytes) is not int or not 0 < max_bytes <= 1024 * 1024 + 4:
        raise ValueError("mssql_native.tds_channel_payload")
    if type(payload) is not bytes or not 0 < len(payload) <= max_bytes:
        raise ValueError("mssql_native.tds_channel_payload")
    _admit(fd, deadline)
    offset = 0
    view = memoryview(payload)
    try:
        while offset < len(payload):
            _wait(fd, writing=True, deadline=deadline)
            try:
                written = os.write(fd, view[offset:])
            except (BlockingIOError, InterruptedError):
                continue
            if written <= 0:
                raise ValueError("mssql_native.tds_channel_io")
            offset += written
        _remaining(deadline)
    except OSError:
        raise ValueError("mssql_native.tds_channel_io") from None


def write_worker_message(
    fd: int,
    body: bytes,
    *,
    deadline: float,
    max_payload: int,
    writer: Callable[..., None],
) -> None:
    """Frame one private message, preserving the caller's raw writer injection.

    Validate before invoking the writer; its wire allowance includes the fixed
    four-byte header. The writer retains deadline and borrowed-descriptor rules.
    """
    writer(fd, encode_message(body, max_payload=max_payload), deadline=deadline, max_bytes=max_payload + 4)


def read_worker_message(
    fd: int, *, deadline: float, max_payload: int, on_complete: Callable[[bytes], None] | None = None
) -> bytes:
    """Read one bounded message through actual pipe EOF, without interpretation.

    A complete frame with its writer still open is not completion. The caller
    validates the message domain and its deadline before accepting a result;
    process reaping and SQL settlement remain separate requirements.
    Optional observation retains EOF-confirmed bytes before post-read deadline
    rejection. It grants no acceptance or persistence authority. Callback errors
    propagate; no complete observation is emitted for invalid framing or no EOF.
    """
    _admit(fd, deadline)
    frame = TdsMessageFrame(max_payload=max_payload)
    try:
        while True:
            _wait(fd, writing=False, deadline=deadline)
            try:
                chunk = os.read(fd, 4096)
            except (BlockingIOError, InterruptedError):
                continue
            if not chunk:
                if on_complete is None:
                    _remaining(deadline)
                result = frame.finish()
                if on_complete is not None:
                    on_complete(result)
                _remaining(deadline)
                return result
            _remaining(deadline)
            frame.feed(chunk)
    except OSError:
        raise ValueError("mssql_native.tds_channel_io") from None


def read_session_or_result(
    session_fd: int, result_fd: int, *, deadline: float, on_result: Callable[[bytes], None] | None = None
) -> tuple[Literal["session", "result"], bytes]:
    """Prefer a complete result; optionally retain it before deadline rejection.

    The callback observes only a valid frame at actual result EOF, never a
    session, partial body or timely-acceptance/SQL authority. Its errors propagate.
    """
    if session_fd == result_fd:
        raise ValueError("mssql_native.sqlclient_channel_alias")
    for fd in (result_fd, session_fd):
        _admit(fd, deadline)
    poller = select.poll()
    frames = {fd: TdsMessageFrame(max_payload=16384) for fd in (result_fd, session_fd)}
    seen = {fd: False for fd in frames}
    session_active = True
    session_candidate: bytes | None = None
    for fd in frames:
        poller.register(fd, select.POLLIN)
    try:
        while True:
            try:
                timeout = 0 if session_candidate is not None else min(50, math.ceil(_remaining(deadline) * 1000))
                events = dict(poller.poll(timeout))
            except InterruptedError:
                continue
            _remaining(deadline)
            if session_candidate is not None and result_fd not in events:
                return "session", session_candidate
            for fd in (result_fd, session_fd):
                if fd not in events or (fd == session_fd and not session_active):
                    continue
                if events[fd] & (select.POLLERR | select.POLLNVAL):
                    raise ValueError("mssql_native.tds_channel_io")
                while True:
                    _remaining(deadline)
                    try:
                        chunk = os.read(fd, 4096)
                    except (BlockingIOError, InterruptedError):
                        break
                    if not chunk:
                        completed = on_result if fd == result_fd else None
                        if completed is None:
                            _remaining(deadline)
                        if fd == session_fd and not seen[fd]:
                            poller.unregister(session_fd)
                            session_active = False
                            break
                        body = frames[fd].finish()
                        if completed is not None:
                            completed(body)
                        _remaining(deadline)
                        if fd == result_fd:
                            return "result", body
                        session_candidate = body
                        poller.unregister(session_fd)
                        session_active = False
                        break  # Recheck result readiness after draining the session.
                    _remaining(deadline)
                    seen[fd] = True
                    if fd == result_fd:
                        session_candidate = None
                    if fd == result_fd and session_active:
                        poller.unregister(session_fd)
                        session_active = False
                    frames[fd].feed(chunk)
    except OSError:
        raise ValueError("mssql_native.tds_channel_io") from None

"""Private bounded pipes and one absolute acceptance deadline for POSIX workers."""

from __future__ import annotations

import os
import selectors
import signal
import subprocess
import time
from collections.abc import Sequence
from typing import Any

from dpone.contracts.strict_json import strict_json_object
from dpone.contracts.target_acceptance import MAX_FRAME_BYTES, TargetAcceptanceError


def _stop(process: subprocess.Popen[bytes]) -> bool:
    """Revoke transport first, then kill the whole session and reap within five seconds."""
    for stream in (process.stdin, process.stdout):
        if stream is not None:
            stream.close()
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except OSError:
        return False
    try:
        process.wait(timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        return False
    return True


def supervise(command: Sequence[str], payload: bytes, *, deadline: float) -> dict[str, Any]:
    """Accept exactly one newline frame only after clean EOF and process exit.

    ``command`` is supplied by the fixed reader adapter; it is never user input.
    The independently useful supervisor admits synthetic commands only in tests.
    No worker stderr, SQL, exception text or credential is surfaced to callers.
    """
    if os.name != "posix" or len(payload) > MAX_FRAME_BYTES:
        raise TargetAcceptanceError("UNSUPPORTED")
    if time.monotonic() >= deadline:
        raise TargetAcceptanceError("INCOMPLETE")
    process: subprocess.Popen[bytes] | None = None
    try:
        process = subprocess.Popen(
            list(command),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        result = _exchange(process, payload, deadline)
        if time.monotonic() >= deadline:
            raise TargetAcceptanceError("INCOMPLETE")
        return result
    except BaseException as error:
        quiescent = process is not None and _stop(process)
        if not isinstance(error, Exception):
            # Propagate cancellation, but give the owner verified lifecycle facts.
            try:
                error.quiescent = quiescent  # type: ignore[attr-defined]
                error.output_revoked = True  # type: ignore[attr-defined]
            finally:
                raise
        if isinstance(error, TargetAcceptanceError):
            error.quiescent = quiescent
            error.output_revoked = True
            raise
        raise TargetAcceptanceError("INCOMPLETE", quiescent=quiescent) from None
    finally:
        if process is not None:
            for stream in (process.stdin, process.stdout):
                if stream is not None:
                    stream.close()


def _exchange(process: subprocess.Popen[bytes], payload: bytes, deadline: float) -> dict[str, Any]:
    assert process.stdin is not None and process.stdout is not None
    outgoing = memoryview(payload)
    incoming = bytearray()
    with selectors.DefaultSelector() as selector:
        os.set_blocking(process.stdin.fileno(), False)
        os.set_blocking(process.stdout.fileno(), False)
        selector.register(process.stdin, selectors.EVENT_WRITE)
        selector.register(process.stdout, selectors.EVENT_READ)
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TargetAcceptanceError("INCOMPLETE")
            for key, _ in selector.select(remaining):
                if time.monotonic() >= deadline:
                    raise TargetAcceptanceError("INCOMPLETE")
                if key.fileobj is process.stdin:
                    if outgoing:
                        outgoing = outgoing[os.write(key.fd, outgoing[:65536]) :]
                    if not outgoing:
                        selector.unregister(process.stdin)
                        process.stdin.close()
                else:
                    chunk = os.read(key.fd, min(65536, MAX_FRAME_BYTES + 1 - len(incoming)))
                    if not chunk:
                        selector.unregister(process.stdout)
                        process.stdout.close()
                    else:
                        incoming.extend(chunk)
                        if len(incoming) > MAX_FRAME_BYTES:
                            raise TargetAcceptanceError()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TargetAcceptanceError("INCOMPLETE")
        try:
            code = process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            raise TargetAcceptanceError("INCOMPLETE") from None
    if code != 0 or not incoming.endswith(b"\n") or incoming.count(b"\n") != 1:
        raise TargetAcceptanceError()
    try:
        return strict_json_object(bytes(incoming[:-1]))
    except (ValueError, TypeError):
        raise TargetAcceptanceError() from None

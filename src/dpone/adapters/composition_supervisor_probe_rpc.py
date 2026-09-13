"""One nonce-bound, read-only host observation over a protected Unix socket.

No caller policy, paths, commands or SQL enter this protocol. The root host
preconfigures one enrollment and accepts only its reserved dispatcher UID.
Complete canonical framing and one absolute deadline apply in both directions;
there is no reconnect, retry, cached observation or acknowledgement recovery.
"""

from __future__ import annotations

import math
import os
import re
import secrets
import socket
import stat
import struct
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

REQUEST_SCHEMA = "dpone.composition-host-probe-request.v1"
RESPONSE_SCHEMA = "dpone.composition-host-probe-response.v1"
MAX_REQUEST_BYTES = 1024
MAX_RESPONSE_BYTES = 8 * 1024 * 1024


class SupervisorProbeError(RuntimeError):
    """Closed diagnostic reason; never includes raw OS errors or host facts."""


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise SupervisorProbeError("supervisor_probe_" + reason)


def _identifier(value: object) -> None:
    _require(type(value) is int and 0 < value < 2**31, "dispatcher_identity")


def _digest(value: object) -> None:
    _require(type(value) is str and re.fullmatch(r"sha256:[0-9a-f]{64}", value) is not None, "enrollment")


def _timeout(value: float) -> float:
    _require(type(value) in (float, int) and math.isfinite(value) and 0 < value <= 60, "timeout")
    return float(value)


def _path(value: Path) -> Path:
    path = Path(value)
    _require(path.is_absolute() and ".." not in path.parts and 0 < len(os.fsencode(path)) <= 103, "path")
    return path


def _protected_parent(path: Path) -> None:
    for parent in reversed(path.parents):
        info = parent.lstat()
        _require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022, "parent")


def _require_endpoint(path: Path, gid: int) -> None:
    _protected_parent(path)
    info = path.lstat()
    _require(
        stat.S_ISSOCK(info.st_mode) and info.st_uid == 0 and info.st_gid == gid and stat.S_IMODE(info.st_mode) == 0o660,
        "endpoint",
    )


def _require_host() -> None:
    _require(sys.platform == "linux" and os.geteuid() == 0, "host")


def _peer_uid(connection: socket.socket) -> int:
    option = getattr(socket, "SO_PEERCRED", None)
    _require(sys.platform == "linux" and type(option) is int, "peer_platform")
    assert isinstance(option, int)
    original = connection.getsockopt(socket.SOL_SOCKET, option, struct.calcsize("3i"))
    pid, uid, gid = struct.unpack("3i", original)
    # The host peer can be outside the client's PID namespace (reported PID 0).
    # Authorization uses the kernel-mapped UID, never process-namespace visibility.
    _require(pid >= 0 and uid >= 0 and gid >= 0, "peer_credentials")
    return uid


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    _require(remaining > 0, "deadline")
    return remaining


def _read_exact(connection: socket.socket, size: int, deadline: float) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        connection.settimeout(_remaining(deadline))
        part = connection.recv(min(65536, size - len(chunks)))
        _require(bool(part), "truncated")
        chunks.extend(part)
    return bytes(chunks)


def _read_frame(connection: socket.socket, maximum: int, deadline: float) -> dict[str, Any]:
    size = struct.unpack("!I", _read_exact(connection, 4, deadline))[0]
    _require(0 < size <= maximum, "frame_budget")
    document = _read_exact(connection, size, deadline)
    connection.settimeout(_remaining(deadline))
    _require(connection.recv(1) == b"", "trailing_bytes")
    body = strict_json_object(document)
    _require(canonical_json_bytes(body) == document, "canonical")
    return body


def _write_frame(connection: socket.socket, body: dict[str, Any], maximum: int, deadline: float) -> None:
    document = canonical_json_bytes(body)
    _require(0 < len(document) <= maximum, "frame_budget")
    connection.settimeout(_remaining(deadline))
    connection.sendall(struct.pack("!I", len(document)) + document)
    connection.shutdown(socket.SHUT_WR)


def _request(body: dict[str, Any], enrollment: str) -> str:
    _require(
        set(body) == {"schema", "enrollment_sha256", "nonce"}
        and body["schema"] == REQUEST_SCHEMA
        and body["enrollment_sha256"] == enrollment,
        "request_subject",
    )
    nonce = body["nonce"]
    _require(type(nonce) is str and re.fullmatch(r"[0-9a-f]{64}", nonce) is not None, "nonce")
    return nonce


def _facts(value: object) -> None:
    _require(
        type(value) is dict
        and set(value) == {"docker", "linux"}
        and type(value["docker"]) is dict
        and type(value["linux"]) is dict,
        "facts",
    )


class CaptureSupervisorFactsClient:
    """One root-authenticated observation; caller still compares protected SQL originals."""

    def __init__(self, path: Path, *, dispatcher_gid: int, timeout_seconds: float = 5.0) -> None:
        _identifier(dispatcher_gid)
        self.path, self.gid, self.timeout = _path(path), dispatcher_gid, _timeout(timeout_seconds)

    def capture(self, enrollment_sha256: str) -> dict[str, Any]:
        """Verify exact nonce/enrollment echoes and complete response before returning facts."""
        _digest(enrollment_sha256)
        nonce = secrets.token_hex(32)
        deadline = time.monotonic() + self.timeout
        try:
            _require_endpoint(self.path, self.gid)
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.settimeout(_remaining(deadline))
                connection.connect(str(self.path))
                _require(_peer_uid(connection) == 0, "server_identity")
                _write_frame(
                    connection,
                    {"schema": REQUEST_SCHEMA, "enrollment_sha256": enrollment_sha256, "nonce": nonce},
                    MAX_REQUEST_BYTES,
                    deadline,
                )
                body = _read_frame(connection, MAX_RESPONSE_BYTES, deadline)
            _require(
                set(body) == {"schema", "enrollment_sha256", "nonce", "facts"}
                and body["schema"] == RESPONSE_SCHEMA
                and body["enrollment_sha256"] == enrollment_sha256
                and body["nonce"] == nonce,
                "response_subject",
            )
            _facts(body["facts"])
            _remaining(deadline)
            return body["facts"]
        except SupervisorProbeError:
            raise
        except Exception:
            raise SupervisorProbeError("supervisor_probe_unavailable") from None


class SupervisorFactsServer:
    """Root-owned single-enrollment server, with serial bounded requests.

    The caller owns the serve loop and shutdown. ``serve_once`` accepts at most
    one connection and waits at most the configured socket deadline. Close never
    removes a replaced endpoint. It does not start threads or retry observations.
    """

    def __init__(
        self,
        path: Path,
        *,
        enrollment_sha256: str,
        dispatcher_uid: int,
        dispatcher_gid: int,
        capture: Callable[[float], dict[str, Any]],
        timeout_seconds: float = 5.0,
    ) -> None:
        _digest(enrollment_sha256)
        _identifier(dispatcher_uid)
        _identifier(dispatcher_gid)
        self.path, self.enrollment = _path(path), enrollment_sha256
        self.uid, self.gid, self.capture = dispatcher_uid, dispatcher_gid, capture
        self.timeout = _timeout(timeout_seconds)
        self._socket: socket.socket | None = None
        self._identity: tuple[int, int] | None = None

    def __enter__(self) -> SupervisorFactsServer:
        _require(self._socket is None, "already_bound")
        try:
            _require_host()
            _protected_parent(self.path)
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._socket = listener
            # No unlink or reuse option: an existing file/socket always rejects.
            listener.bind(str(self.path))
            info = self.path.lstat()
            self._identity = info.st_dev, info.st_ino
            os.chown(self.path, 0, self.gid)
            os.chmod(self.path, 0o660)
            _require_endpoint(self.path, self.gid)
            listener.listen(1)
            return self
        except Exception:
            self.close()
            raise SupervisorProbeError("supervisor_probe_bind") from None

    def serve_once(self) -> bool:
        """Return False only for idle accept timeout; accepted-request errors raise.

        True acknowledges one complete response. Neither result is SQL outcome
        evidence. The host service may continue idle polling without retrying a
        failed capture or treating an accepted request's timeout as idle.
        """
        _require(self._socket is not None, "not_bound")
        assert self._socket is not None
        deadline = time.monotonic() + self.timeout
        try:
            _require_host()
            self._socket.settimeout(_remaining(deadline))
            try:
                connection, _ = self._socket.accept()
            except TimeoutError:
                return False
            with connection:
                _require(_peer_uid(connection) == self.uid, "dispatcher_identity")
                body = _read_frame(connection, MAX_REQUEST_BYTES, deadline)
                nonce = _request(body, self.enrollment)
                facts = self.capture(deadline)
                _facts(facts)
                _remaining(deadline)
                _write_frame(
                    connection,
                    {"schema": RESPONSE_SCHEMA, "enrollment_sha256": self.enrollment, "nonce": nonce, "facts": facts},
                    MAX_RESPONSE_BYTES,
                    deadline,
                )
            return True
        except SupervisorProbeError:
            raise
        except Exception:
            raise SupervisorProbeError("supervisor_probe_unavailable") from None

    def close(self) -> None:
        """Close only our listener and remove only the endpoint inode we created."""
        if self._socket is not None:
            self._socket.close()
            self._socket = None
        if self._identity is not None:
            try:
                info = self.path.lstat()
                if stat.S_ISSOCK(info.st_mode) and (info.st_dev, info.st_ino) == self._identity:
                    self.path.unlink()
            except FileNotFoundError:
                pass
            finally:
                self._identity = None

    def __exit__(self, *args: object) -> None:
        self.close()

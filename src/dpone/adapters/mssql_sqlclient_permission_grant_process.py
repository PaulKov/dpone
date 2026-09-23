"""Internal retained-process custody with sticky UNKNOWN; no SQL success or route readiness claim."""

from __future__ import annotations

import math
import os
import socket
import struct
import threading
import time
from dataclasses import dataclass, replace
from typing import Never, cast

from dpone.adapters._mssql_tds_socket_readiness import _has_unexpected_queued_input, _socket_ready
from dpone.adapters.mssql_sqlclient_permission_custody import await_permission_custody as _await
from dpone.adapters.mssql_sqlclient_permission_grant_process_send import (
    PermissionGrantCredentialAttempt,
    PermissionGrantPublicAttempt,
)
from dpone.adapters.mssql_sqlclient_permission_grant_process_send import (
    send_credentials as _send_credentials,
)
from dpone.adapters.mssql_sqlclient_permission_grant_process_send import send_public as _send_public
from dpone.adapters.mssql_tds_child_process import TdsChildContainmentExecutor, TdsChildExit, TdsChildProcess
from dpone.contracts import mssql_sqlclient_permission_grant_wire as permission_wire
from dpone.contracts.mssql_tds_validation import deadline_nanoseconds

ERROR = "mssql_native.sqlclient_permission_process_unknown"


@dataclass(slots=True, repr=False)
class PermissionGrantProcessUnknown(ValueError):
    process: SqlClientPermissionGrantProcess


_INBOUND_LIMITS = {
    permission_wire.PermissionWireKind.STARTUP: permission_wire.CONTROL_LIMIT,
    permission_wire.PermissionWireKind.REQUEST_ACCEPTED: permission_wire.CONTROL_LIMIT,
    permission_wire.PermissionWireKind.AUTHORITY: permission_wire.CONTROL_LIMIT,
    permission_wire.PermissionWireKind.PERMISSION_HELD: permission_wire.EVIDENCE_LIMIT,
    permission_wire.PermissionWireKind.HELD: permission_wire.CONTROL_LIMIT,
    permission_wire.PermissionWireKind.RELEASED: permission_wire.CONTROL_LIMIT,
}


def _number(value: object) -> bool:
    return type(value) in (int, float) and math.isfinite(cast("int | float", value)) and cast("int | float", value) > 0


class SqlClientPermissionGrantProcess:
    """Own one permission socket and the custody executor until exact settlement."""

    _custody: TdsChildProcess
    _channel: socket.socket  # noqa: E702
    _binding: permission_wire.PermissionWireBinding
    _binding_snapshot: bytes
    _startup_deadline: float
    _operation_deadline: float  # noqa: E702
    _owner: tuple[int, threading.Thread]
    _wire: permission_wire.PermissionWireState  # noqa: E702
    _attempts: list[PermissionGrantPublicAttempt[permission_wire.PermissionWireKind]]
    _trailing: bytes  # noqa: E702
    _credential: PermissionGrantCredentialAttempt | None
    _executor: TdsChildContainmentExecutor | None
    _cause: BaseException | None
    _failed: bool
    _busy: bool  # noqa: E702
    _eof: bool
    _settled: bool  # noqa: E702
    _closed: bool
    _contain_requested: bool  # noqa: E702
    _custody_ready: bool

    @classmethod
    def adopt(
        cls,
        custody: TdsChildProcess,
        channel: socket.socket,
        binding: permission_wire.PermissionWireBinding,
        *,
        startup_deadline: float,
        operation_deadline: float,
        termination_timeout: float,
    ) -> SqlClientPermissionGrantProcess:
        try:
            if type(custody) is not TdsChildProcess or type(channel) is not socket.socket:
                raise ValueError(ERROR)
            if type(binding) is not permission_wire.PermissionWireBinding:
                raise ValueError(ERROR)
            snapshot = binding.snapshot()
            process = custody.process
            if binding.snapshot() != snapshot or process is None or custody.handle is not None:
                raise ValueError(ERROR)
            if getattr(process, "pid", None) != binding.startup.process.pid:
                raise ValueError(ERROR)
            if (
                channel.family != socket.AF_UNIX
                or channel.type != socket.SOCK_STREAM
                or channel.fileno() < 0
                or channel.getblocking()
            ):
                raise ValueError(ERROR)
            if not all(_number(value) for value in (startup_deadline, operation_deadline, termination_timeout)):
                raise ValueError(ERROR)
            if startup_deadline > operation_deadline:
                raise ValueError(ERROR)
            if deadline_nanoseconds(operation_deadline) != binding.operation_deadline_ns:
                raise ValueError(ERROR)
            if any(resource.kind == "socket" and resource.value is channel for resource in custody._resources):
                raise ValueError(ERROR)
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise ValueError(ERROR) from None

        self = cls.__new__(cls)
        self._custody, self._channel, self._binding, self._binding_snapshot = custody, channel, binding, snapshot
        self._startup_deadline, self._operation_deadline = startup_deadline, operation_deadline
        self._owner = (os.getpid(), threading.current_thread())
        self._wire = permission_wire.PermissionWireState(binding)
        self._attempts = []
        self._credential, self._executor, self._cause = None, None, None
        self._trailing = b""
        self._failed = self._busy = self._eof = self._settled = self._closed = False
        self._contain_requested = self._custody_ready = False
        try:
            custody.check_owner()
            custody.retain_socket(channel)
            self._executor = TdsChildContainmentExecutor(
                custody, binding.startup.process, operation_deadline, termination_timeout
            )
        except BaseException as error:
            self._fail(error)
        return self

    def await_custody(self, *, deadline: float) -> None:
        if self._owner != (os.getpid(), threading.current_thread()) or self._busy or self._custody_ready:
            self._fail(ValueError(ERROR))
        self._busy = True
        try:
            _await(
                self._custody,
                self._executor,
                self._binding.startup.process,
                self._deadline(self._startup_deadline, deadline),
            )
            self._custody_ready, self._busy = True, False
        except BaseException as error:
            self._fail(error)

    @property
    def public_attempts(self) -> tuple[PermissionGrantPublicAttempt[permission_wire.PermissionWireKind], ...]:
        return tuple(self._attempts)

    @property
    def credential_attempt(self) -> PermissionGrantCredentialAttempt | None:
        return self._credential

    @property
    def failed(self) -> bool:
        return self._failed

    @property
    def cleanup_deadline(self) -> float | None:
        return None if self._executor is None else self._executor.cleanup_deadline

    @property
    def exit(self) -> TdsChildExit | None:
        return None if self._executor is None else self._executor.exit

    def _fail(self, cause: BaseException) -> Never:
        self._failed = True
        if self._cause is None:
            self._cause = cause
        self._wire.fail()
        self._custody.poison()
        if self._executor is not None and not self._contain_requested:
            self._contain_requested = True
            try:
                self._executor.request()
            except BaseException:
                pass
        raise PermissionGrantProcessUnknown(self) from None

    def _enter(self) -> None:
        if (
            self._owner != (os.getpid(), threading.current_thread())
            or self._busy
            or self._failed
            or self._closed
            or not self._custody_ready
        ):
            self._fail(ValueError(ERROR))
        self._busy = True
        try:
            if self._executor is None or self._executor.failed:
                raise ValueError(ERROR)
            self._custody.check_owner()
        except BaseException as error:
            self._fail(error)

    def _deadline(self, ceiling: float, supplied: float | None) -> float:
        if supplied is None:
            return ceiling
        if not _number(supplied):
            raise ValueError(ERROR)
        return min(ceiling, supplied)

    def _attempt(self, direction: str, kind: permission_wire.PermissionWireKind, ordinal: int) -> int:
        attempt = PermissionGrantPublicAttempt(direction, kind, ordinal, b"", b"", 0, False, False)
        self._attempts.append(attempt)
        return len(self._attempts) - 1

    def _replace_attempt(self, index: int, **changes: object) -> None:
        self._attempts[index] = replace(self._attempts[index], **changes)  # type: ignore[arg-type]

    def _send(self, index: int, data: bytes, deadline: float) -> None:
        view = memoryview(data)
        while view:
            if not _socket_ready(self._channel, deadline, writing=True):
                raise TimeoutError(ERROR)
            try:
                written = self._channel.send(view)
            except BlockingIOError:
                continue
            if written <= 0:
                raise ValueError(ERROR)
            view = view[written:]
            self._replace_attempt(index, transferred=self._attempts[index].transferred + written)

    def send_public(
        self,
        kind: permission_wire.PermissionWireKind,
        ordinal: int,
        body: dict,
        *,
        deadline: float | None = None,
    ) -> permission_wire.PermissionWireMessage:
        return _send_public(self, kind, ordinal, body, deadline=deadline, dependencies=(permission_wire, ERROR))

    def _receive_exact(self, index: int, length: int, deadline: float, *, prefix: bool) -> bytes:
        retained = bytearray()
        while len(retained) < length:
            if not _socket_ready(self._channel, deadline):
                raise TimeoutError(ERROR)
            try:
                part = self._channel.recv(length - len(retained))
            except BlockingIOError:
                continue
            if not part:
                raise ValueError(ERROR)
            retained.extend(part)
            if prefix:
                self._replace_attempt(index, prefix=bytes(retained), transferred=len(retained))
            else:
                attempt = self._attempts[index]
                self._replace_attempt(index, payload=bytes(retained), transferred=len(attempt.prefix) + len(retained))
        return bytes(retained)

    def receive_public(
        self, kind: permission_wire.PermissionWireKind, ordinal: int, *, deadline: float | None = None
    ) -> permission_wire.PermissionWireMessage:
        self._enter()
        index = self._attempt("CHILD_TO_PARENT", kind, ordinal)
        try:
            effective = self._deadline(
                self._startup_deadline
                if kind is permission_wire.PermissionWireKind.STARTUP
                else self._operation_deadline,
                deadline,
            )
            if type(kind) is not permission_wire.PermissionWireKind or kind not in _INBOUND_LIMITS:
                raise ValueError(ERROR)
            prefix = self._receive_exact(index, 4, effective, prefix=True)
            length = struct.unpack("!I", prefix)[0]
            if not 0 < length <= _INBOUND_LIMITS[kind] or self._wire.total + 4 + length > permission_wire.TOTAL_LIMIT:
                raise ValueError(ERROR)
            payload = self._receive_exact(index, length, effective, prefix=False)
            self._replace_attempt(index, encoded=True)
            message = self._wire.accept(payload, direction="CHILD_TO_PARENT")
            allow_eof = kind is permission_wire.PermissionWireKind.RELEASED
            if _has_unexpected_queued_input(self._channel, allow_eof=allow_eof):
                raise ValueError(ERROR)
            self._replace_attempt(index, complete=True)
            self._busy = False
            return message
        except BaseException as error:
            self._fail(error)

    def send_credentials(self, payload: bytes, *, deadline: float | None = None) -> None:
        _send_credentials(
            self,
            payload,
            deadline=deadline,
            dependencies=(permission_wire, ERROR, _socket_ready, PermissionGrantCredentialAttempt),
        )

    def observe_eof(self, *, deadline: float | None = None) -> None:
        self._enter()
        try:
            effective = self._deadline(self._operation_deadline, deadline)
            if self._wire.phase != "RELEASE_ACKNOWLEDGED":
                raise ValueError(ERROR)
            if not _socket_ready(self._channel, effective):
                raise TimeoutError(ERROR)
            trailing = self._channel.recv(1)
            if trailing:
                self._trailing = trailing
                raise ValueError(ERROR)
            self._wire.observe_eof()
            self._eof = True
            self._busy = False
        except BaseException as error:
            self._fail(error)

    def _await_exit(self, deadline: float) -> TdsChildExit:
        executor = self._executor
        if executor is None or not executor.done.wait(max(0.0, deadline - time.monotonic())):
            self._fail(TimeoutError(ERROR))
        exit_value = executor.exit
        if executor.failed or exit_value is None or not exit_value.reaped or not self._custody._closed:
            self._fail(ValueError(ERROR))
        self._settled = True
        return exit_value

    def settle(self, *, natural_deadline: float, containment_deadline: float | None = None) -> TdsChildExit:
        self._enter()
        try:
            if not self._eof or self._executor is None:
                raise ValueError(ERROR)
            natural = self._deadline(self._operation_deadline, natural_deadline)
            cleanup = self._executor.request_settlement(
                natural_deadline=natural, containment_deadline=containment_deadline
            )
            result = self._await_exit(cleanup)
            if result.identity != self._binding.startup.process:
                raise ValueError(ERROR)
            if result.identity is not self._binding.startup.process:
                result = TdsChildExit(self._binding.startup.process, result.exit_code, result.reaped)
            self._busy = False
            return result
        except PermissionGrantProcessUnknown:
            raise
        except BaseException as error:
            self._fail(error)

    def contain(self, *, deadline: float | None = None) -> TdsChildExit:
        current_owner = (os.getpid(), threading.current_thread())
        if self._owner != current_owner or (self._busy and not self._failed) or self._closed:
            self._fail(ValueError(ERROR))
        self._busy = True
        if (executor := self._executor) is None:
            self._fail(ValueError(ERROR))
        try:
            if self._settled:
                exit_value = executor.exit
                if exit_value is None:
                    raise ValueError(ERROR)
                self._busy = False
                return exit_value
            if not self._contain_requested:
                self._contain_requested = True
                cleanup = executor.request(deadline=deadline)
            else:
                captured = executor.cleanup_deadline
                if captured is None:
                    raise ValueError(ERROR)
                cleanup = captured
                if deadline is not None:
                    cleanup = executor.request(deadline=deadline)
            result = self._await_exit(cleanup)
            self._busy = False
            return result
        except PermissionGrantProcessUnknown:
            raise
        except BaseException as error:
            self._fail(error)

    def close(self) -> None:
        if self._closed:
            return
        if self._owner != (os.getpid(), threading.current_thread()) or not self._settled or not self._custody._closed:
            raise PermissionGrantProcessUnknown(self) from None
        self._closed = True

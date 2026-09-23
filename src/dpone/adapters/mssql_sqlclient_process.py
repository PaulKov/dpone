"""One-shot SqlClient pipes over the existing exclusive pidfd resource owner.

Successful transport is not a durable credential/grant barrier or SQL evidence.
The supervisor supplies those decisions. Raw result retention precedes cleanup.
"""

from __future__ import annotations

import math
import time
from tempfile import TemporaryDirectory
from typing import Any, Literal

from dpone.adapters.mssql_tds_channels import (
    read_session_or_result,
    read_worker_message,
    write_worker_control,
    write_worker_message,
)
from dpone.adapters.mssql_tds_child_process import TdsChildProcess, _existing_custody
from dpone.adapters.mssql_tds_process import LinuxTdsProcess
from dpone.contracts.mssql_tds_api import (
    SqlClientInputDescriptor,
    SqlClientLaunch,
    SqlClientReady,
    decode_ready,
    input_descriptor_digest,
    validate_ready,
)
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity


def _deadline(value: float) -> float:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError("mssql_native.sqlclient_deadline")
    return value


class SqlClientChildProcess:
    """Same-thread/process ownership and nonrenewable phase/containment deadlines."""

    def __init__(
        self,
        process: Any,
        handle: LinuxTdsProcess,
        descriptors: tuple[int, ...],
        *,
        launch: SqlClientLaunch,
        cache: TemporaryDirectory[str] | None = None,
        bound_input: SqlClientInputDescriptor | None = None,
    ) -> None:
        if type(launch) is not SqlClientLaunch or (
            bound_input is not None
            and (
                type(bound_input) is not SqlClientInputDescriptor
                or bound_input.fd != launch.descriptors.input
                or input_descriptor_digest(bound_input) != launch.input_binding_sha256
            )
        ):
            raise ValueError("mssql_native.sqlclient_process_binding")
        if len(descriptors) != 5 or len(set(descriptors)) != 5 or handle.identity != launch.process:
            raise ValueError("mssql_native.sqlclient_process_binding")
        self._resources = _existing_custody(process, handle, descriptors) or TdsChildProcess(
            process, handle, descriptors, cache
        )
        self._descriptors, self._launch = descriptors, launch
        self._bound_input = bound_input
        self._phase = 0
        self._startup_receipt: SqlClientReady | None = None
        self._received_result: bytes | None = None

    @property
    def declared_launch(self) -> SqlClientLaunch:
        """Immutable original launch; declarations are not managed startup proof."""
        return self._launch

    @property
    def bound_input(self) -> SqlClientInputDescriptor | None:
        """Child FD metadata for job construction, never a usable parent handle.

        None identifies the legacy opaque-hash constructor path. Parent copies
        of inherited descriptors have already been closed by the launcher.
        """
        return self._bound_input

    @property
    def identity(self) -> TdsProcessIdentity:
        return self._resources.identity

    @property
    def startup_receipt(self) -> SqlClientReady | None:
        return self._startup_receipt

    @property
    def received_result(self) -> bytes | None:
        return self._received_result

    def _retain_result(self, body: bytes) -> None:
        # Called only after valid frame completion at actual EOF, before helper
        # deadline rejection or descriptor close. Retention is not acceptance.
        self._received_result = body

    def _begin(self, phase: int, deadline: float) -> float:
        self._resources.check_cleanup_owner()
        if self._phase != phase:
            if self._phase == -1:
                self._resources.poison()
            raise ValueError("mssql_native.sqlclient_phase")
        self._resources.check_owner()
        self._phase = -1
        original = self._launch.operation_deadline_ns
        if phase == 0:
            original = min(original, self._launch.startup_deadline_ns)
        return min(_deadline(deadline), original / 10**9)

    def _complete(self, phase: int) -> None:
        self._resources.close_descriptor(self._descriptors[phase])
        self._phase = phase + 1

    def startup(self, *, deadline: float) -> None:
        deadline = self._begin(0, deadline)
        body = read_worker_message(self._descriptors[0], deadline=deadline, max_payload=16384)
        ready = decode_ready(body)
        if LinuxTdsProcess.identify(self.identity.pid) != self.identity:
            raise ValueError("mssql_native.sqlclient_process_binding")
        validate_ready(self._launch, ready, now_ns=time.monotonic_ns())
        if time.monotonic() >= deadline:
            raise ValueError("mssql_native.sqlclient_deadline")
        self._startup_receipt = ready
        self._complete(0)

    def _send(self, phase: int, body: bytes, deadline: float, limit: int) -> None:
        deadline = self._begin(phase, deadline)
        write_worker_message(
            self._descriptors[phase], body, deadline=deadline, max_payload=limit, writer=write_worker_control
        )
        self._complete(phase)

    def send_credentials(self, body: bytes, *, deadline: float) -> None:
        self._send(1, body, deadline, 1024**2)

    def observe_session(self, *, deadline: float) -> bytes:
        deadline = self._begin(2, deadline)
        body = read_worker_message(self._descriptors[2], deadline=deadline, max_payload=16384)
        self._complete(2)
        return body

    def receive_session_or_result(self, *, deadline: float) -> tuple[Literal["session", "result"], bytes]:
        deadline = self._begin(2, deadline)
        kind, body = read_session_or_result(
            self._descriptors[2], self._descriptors[4], deadline=deadline, on_result=self._retain_result
        )
        if kind == "result":
            self._received_result = body
            self._complete(4)
        else:
            self._complete(2)
        return kind, body

    def send_grant(self, body: bytes, *, deadline: float) -> None:
        self._send(3, body, deadline, 16384)

    def receive_result(self, *, deadline: float) -> bytes:
        self._resources.check_owner()
        if self._phase not in (2, 3, 4):
            self._resources.poison()
            raise ValueError("mssql_native.sqlclient_phase")
        deadline = self._begin(self._phase, deadline)
        body = read_worker_message(
            self._descriptors[4], deadline=deadline, max_payload=16384, on_complete=self._retain_result
        )
        self._received_result = body
        self._complete(4)
        return body

    def wait(self, *, deadline: float) -> TdsChildExit:
        return self._resources.wait(deadline=min(_deadline(deadline), self._launch.operation_deadline_ns / 10**9))

    def terminate(self, *, deadline: float) -> TdsChildExit:
        return self._resources.terminate(deadline=self._resources.capture_budget(deadline=_deadline(deadline)))

    def close(self) -> None:
        self._resources.close()

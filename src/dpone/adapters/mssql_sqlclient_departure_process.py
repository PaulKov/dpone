"""Fixed three-phase departure transport over the shared exclusive resource owner.

This transport handles bounded bytes only. The owning parent authenticates request,
result, CREATE provenance and durable receipts. No SQL, credential persistence or
remote settlement is performed here; launcher failure retains resource ownership.
"""

import time
from dataclasses import fields
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from dpone.adapters.mssql_tds_channels import read_worker_message, write_worker_control
from dpone.adapters.mssql_tds_child_process import TdsChildProcess, _existing_custody
from dpone.adapters.mssql_tds_process import LinuxTdsProcess
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup, decode_startup, encode_startup
from dpone.contracts.mssql_tds_frames import encode_message
from dpone.contracts.mssql_tds_validation import _hash, deadline_nanoseconds
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity

# P9b's bounded result contains two 128 KiB stage identities plus bounded
# authority observations. Other departure routes retain their 32 KiB default.
_MAX_RESULT_FRAME = 512 * 1024
MAX_REQUEST_FRAME = 1024 * 1024


def _deadline(value: float) -> float:
    deadline_nanoseconds(value)
    return value


def _process(value: TdsProcessIdentity) -> TdsProcessIdentity:
    if type(value) is not TdsProcessIdentity:
        raise ValueError("mssql_native.sqlclient_departure_process_invalid")
    return TdsProcessIdentity(**{field.name: getattr(value, field.name) for field in fields(TdsProcessIdentity)})


def _admit_departure_launch(
    *,
    implementation_sha256: str | None = None,
    startup_deadline: float | None = None,
    operation_deadline: float | None = None,
) -> None:
    """Validate supplied launch inputs without platform or filesystem effects."""
    if implementation_sha256 is not None:
        _hash(implementation_sha256)
    if startup_deadline is None and operation_deadline is None:
        return
    if startup_deadline is None or operation_deadline is None:
        raise ValueError("mssql_native.sqlclient_departure_deadline_invalid")
    deadline_nanoseconds(startup_deadline)
    deadline_nanoseconds(operation_deadline)
    if startup_deadline > operation_deadline:
        raise ValueError("mssql_native.sqlclient_departure_deadline_invalid")


def _canonical_departure_startup(
    actual: TdsProcessIdentity,
    implementation_sha256: str,
    package_root: Path,
    launch_nonce: bytes,
) -> TdsCoordinatorStartup:
    """Build startup expectations from a deeply validated identity copy."""
    return TdsCoordinatorStartup(_process(actual), implementation_sha256, str(package_root), launch_nonce)


class SqlClientDepartureProcess:
    """One owner, one send, original deadlines and EOF-confirmed result retention."""

    def __init__(
        self,
        process: Any,
        handle: LinuxTdsProcess,
        descriptors: tuple[int, ...],
        *,
        expected: TdsCoordinatorStartup,
        startup_deadline: float,
        operation_deadline: float,
        cache: TemporaryDirectory[str] | None = None,
    ) -> None:
        if type(expected) is not TdsCoordinatorStartup:
            raise ValueError("mssql_native.sqlclient_departure_process_invalid")
        expected.__post_init__()
        if _process(expected.process) != _process(handle.identity):
            raise ValueError("mssql_native.sqlclient_departure_process_invalid")
        if (
            type(descriptors) is not tuple
            or len(descriptors) != 3
            or any(type(fd) is not int or fd < 0 for fd in descriptors)
            or len(set(descriptors)) != 3
        ):
            raise ValueError("mssql_native.sqlclient_departure_descriptors_invalid")
        self._startup_deadline, self._operation_deadline = _deadline(startup_deadline), _deadline(operation_deadline)
        if self._startup_deadline > self._operation_deadline:
            raise ValueError("mssql_native.sqlclient_departure_deadline_invalid")
        self._expected = decode_startup(encode_startup(expected))
        self._resources = _existing_custody(process, handle, descriptors) or TdsChildProcess(
            process, handle, descriptors, cache
        )
        self._descriptors = descriptors
        self._phase = 0
        self._faulted = False
        self._startup_receipt: TdsCoordinatorStartup | None = None
        self._received_result: bytes | None = None

    @property
    def identity(self) -> TdsProcessIdentity:
        return self._resources.identity

    @property
    def declared_startup(self) -> TdsCoordinatorStartup:
        """Independent launch expectations, never a claim of received startup."""
        return self._expected

    @property
    def startup_receipt(self) -> TdsCoordinatorStartup | None:
        return self._startup_receipt

    @property
    def received_result(self) -> bytes | None:
        """Volatile EOF-confirmed bytes, not accepted evidence or normal success."""
        return self._received_result

    def _begin(self, phase: int, deadline: float) -> float:
        self._resources.check_owner()
        original, self._phase = self._phase, -1
        if self._faulted or original != phase:
            self._faulted = True
            raise ValueError("mssql_native.sqlclient_departure_phase")
        return min(_deadline(deadline), self._startup_deadline if phase == 0 else self._operation_deadline)

    def _complete(self, phase: int, deadline: float) -> None:
        if self._faulted:
            raise ValueError("mssql_native.sqlclient_departure_phase")
        self._resources.close_descriptor(self._descriptors[phase])
        if self._faulted or time.monotonic() >= deadline:
            raise ValueError("mssql_native.sqlclient_departure_deadline")
        self._phase = phase + 1

    def startup(self, *, deadline: float) -> TdsCoordinatorStartup:
        deadline = self._begin(0, deadline)
        receipt = decode_startup(read_worker_message(self._descriptors[0], deadline=deadline, max_payload=16384))
        if receipt != self._expected:
            raise ValueError("mssql_native.sqlclient_departure_startup_binding")
        if time.monotonic() >= deadline:
            raise ValueError("mssql_native.sqlclient_departure_deadline")
        self._startup_receipt = receipt
        self._complete(0, deadline)
        return receipt

    def send_request(self, body: bytes, *, deadline: float) -> None:
        deadline = self._begin(1, deadline)
        write_worker_control(
            self._descriptors[1],
            encode_message(body, max_payload=MAX_REQUEST_FRAME),
            deadline=deadline,
            max_bytes=MAX_REQUEST_FRAME + 4,
        )
        self._complete(1, deadline)

    def _retain_result(self, body: bytes) -> None:
        self._received_result = body

    def receive_result(self, *, deadline: float) -> bytes:
        """Receive the original bounded departure result contract."""
        return self.receive_result_bounded(deadline=deadline, max_payload=32768)

    def receive_result_bounded(self, *, deadline: float, max_payload: int) -> bytes:
        """Receive a route-specific frame without widening the public legacy signature."""
        if type(max_payload) is not int or not 1 <= max_payload <= _MAX_RESULT_FRAME:
            raise ValueError("mssql_native.sqlclient_departure_result_limit")
        deadline = self._begin(2, deadline)
        body = read_worker_message(
            self._descriptors[2], deadline=deadline, max_payload=max_payload, on_complete=self._retain_result
        )
        self._complete(2, deadline)
        return body

    def wait(self, *, deadline: float) -> TdsChildExit:
        return self._resources.wait(deadline=min(_deadline(deadline), self._operation_deadline))

    def terminate(self, *, deadline: float) -> TdsChildExit:
        return self._resources.terminate(deadline=self._resources.capture_budget(deadline=_deadline(deadline)))

    def close(self) -> None:
        self._resources.close()

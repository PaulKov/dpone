"""Guarded process and one-shot protocol backend for the P10d observer."""

from __future__ import annotations

import os
import secrets
import socket
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from dpone.adapters import _mssql_tds_socket_readiness as readiness
from dpone.adapters import mssql_sqlclient_departure_process as departure_process
from dpone.adapters import mssql_sqlclient_observe_process as observe_process
from dpone.adapters.mssql_sqlclient_observe_transport import read_frame, require_quiet, write_frame
from dpone.adapters.mssql_tds_child_process import TdsChildProcess, UnresolvedPythonTdsLaunch
from dpone.adapters.mssql_tds_coordinator_connection import decode_connection_admission, encode_connection_admission
from dpone.adapters.mssql_tds_installation import worker_installation_digest
from dpone.adapters.mssql_tds_launch_shim import SOURCE_SHIM
from dpone.adapters.mssql_tds_process import LinuxTdsProcess
from dpone.adapters.mssql_tds_python_admission import AdmittedPythonInputs
from dpone.contracts import mssql_sqlclient_writer_observer_wire as wire
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown

ERROR = "mssql_native.sqlclient_writer_observer_process_unknown"
_SHIM = SOURCE_SHIM.replace("'mssql_tds_worker_bootstrap.py'", "'mssql_sqlclient_writer_observer_bootstrap.py'")
WriterObserverRequest = wire.WriterObserverRequest


class _ObserverSession(Protocol):
    @property
    def incarnation(self) -> wire.SqlClientObserverIncarnation: ...

    def observe_once(self, command: wire.WriterObserverCommand) -> wire.SqlClientWriterObservation: ...

    def close(self) -> None: ...


def validate_writer_observer_request(value: object) -> wire.WriterObserverRequest:
    """Return one exact canonical request for the concrete observer protocol."""
    if type(value) is not wire.WriterObserverRequest:
        raise ValueError(ERROR)
    value.__post_init__()
    return value


def validate_writer_observer_deadline(value: float) -> None:
    """Apply the canonical finite int64-nanosecond deadline domain."""
    wire.validate_deadline(value)


class SqlClientWriterObserverParentProtocol:
    """Drive the fixed parent half without exposing codec substitution."""

    def __init__(self, process: observe_process.SqlClientObserveProcess, request: object) -> None:
        self._process = process
        self._request = validate_writer_observer_request(request)
        self._startup: observe_process.TdsCoordinatorStartup | None = None
        self._ready = False

    def acknowledge_request(self) -> None:
        startup = self._process.startup()
        write_frame(
            self._process.channel,
            wire.encode_request(self._request),
            deadline=self._process.operation_deadline,
            limit=wire.MAX_CONTROL_BYTES,
        )
        wire.validate_request_ack(
            read_frame(
                self._process.channel,
                deadline=self._process.operation_deadline,
                limit=wire.MAX_CONTROL_BYTES,
            ),
            self._request,
            startup.launch_nonce,
        )
        self._process.assert_current()
        self._startup = startup

    def send_credentials_and_receive_ready(self, credentials: wire.SqlClientCredentials):
        if self._startup is None or self._ready:
            raise ValueError(ERROR)
        payload = wire.encode_credentials(credentials, self._request, self._startup.launch_nonce)
        try:
            write_frame(
                self._process.channel,
                payload,
                deadline=self._process.operation_deadline,
                limit=wire.MAX_CREDENTIAL_BYTES,
            )
        finally:
            del payload, credentials
        incarnation = wire.decode_ready(
            read_frame(
                self._process.channel,
                deadline=self._process.operation_deadline,
                limit=wire.MAX_CONTROL_BYTES,
            ),
            self._request,
            self._startup.launch_nonce,
        )
        require_quiet(self._process.channel)
        self._process.assert_current()
        self._ready = True
        return incarnation

    def backend(self) -> SqlClientWriterObserverProcessBackend:
        if not self._ready:
            raise ValueError(ERROR)
        return SqlClientWriterObserverProcessBackend(self._process, self._request)


def serve_writer_observer_protocol(
    channel: socket.socket,
    *,
    startup_deadline: float,
    operation_deadline: float,
    launch_nonce: bytes,
    implementation_sha256: str,
    package_root: str,
    process_identity: departure_process.TdsProcessIdentity,
    admission: bytes,
    session_factory: Callable[..., _ObserverSession],
) -> None:
    """Serve the closed child protocol over one already-admitted channel."""
    startup = departure_process.TdsCoordinatorStartup(
        process_identity, implementation_sha256, package_root, launch_nonce
    )
    write_frame(
        channel,
        departure_process.encode_startup(startup),
        deadline=startup_deadline,
        limit=wire.MAX_CONTROL_BYTES,
    )
    request = wire.decode_request(read_frame(channel, deadline=operation_deadline, limit=wire.MAX_CONTROL_BYTES))
    if request.operation_deadline != operation_deadline:
        raise ValueError(ERROR)
    write_frame(
        channel,
        wire.encode_request_ack(request, launch_nonce),
        deadline=operation_deadline,
        limit=wire.MAX_CONTROL_BYTES,
    )
    payload = read_frame(channel, deadline=operation_deadline, limit=wire.MAX_CREDENTIAL_BYTES)
    try:
        credentials = wire.decode_credentials(payload, request, launch_nonce)
    finally:
        del payload
    session = None
    try:
        try:
            session = session_factory(request, credentials, admission, request_sha256=wire.request_digest(request))
        finally:
            del credentials
        write_frame(
            channel,
            wire.encode_ready(request, launch_nonce, session.incarnation),
            deadline=operation_deadline,
            limit=wire.MAX_CONTROL_BYTES,
        )
        command = wire.decode_command(
            read_frame(channel, deadline=operation_deadline, limit=wire.MAX_CONTROL_BYTES), request
        )
        observation = session.observe_once(command)
        write_frame(
            channel,
            wire.encode_observation(request, command, observation),
            deadline=operation_deadline,
            limit=wire.MAX_CONTROL_BYTES,
        )
        write_frame(
            channel,
            wire.terminal_ack(request, command),
            deadline=operation_deadline,
            limit=wire.MAX_CONTROL_BYTES,
        )
    finally:
        if session is not None:
            session.close()


class SqlClientWriterObserverProcessBackend:
    """Expose one fixed command and bounded cleanup over an authenticated child."""

    def __init__(self, process: observe_process.SqlClientObserveProcess, request: wire.WriterObserverRequest) -> None:
        self._process, self._request = process, request
        self._used = self._closed = False

    @property
    def identity(self):
        return self._process.identity

    def observe_once(self, *, session_id: int, nonce: bytes, deadline: float):
        if self._used or self._closed or deadline != self._process.operation_deadline:
            raise ValueError(ERROR)
        self._used = True
        self._process.assert_current()
        require_quiet(self._process.channel)
        command = wire.WriterObserverCommand(
            request_sha256=wire.request_digest(self._request),
            session_id=session_id,
            nonce=nonce.hex(),
        )
        write_frame(
            self._process.channel,
            wire.encode_command(command),
            deadline=deadline,
            limit=wire.MAX_CONTROL_BYTES,
        )
        result = wire.decode_observation(
            read_frame(self._process.channel, deadline=deadline, limit=wire.MAX_CONTROL_BYTES),
            self._request,
            command,
        )
        if read_frame(self._process.channel, deadline=deadline, limit=wire.MAX_CONTROL_BYTES) != wire.terminal_ack(
            self._request, command
        ):
            raise ValueError(ERROR)
        if not readiness._socket_ready(self._process.channel, deadline):
            raise TimeoutError(ERROR)
        if self._process.channel.recv(1) != b"":
            raise ValueError(ERROR)
        return result

    def contain(self, *, deadline: float):
        return self._process.contain(deadline=deadline)

    def close(self, *, deadline: float) -> None:
        observe_process.deadline_nanoseconds(deadline)
        if self._closed:
            return
        self._process.close()
        self._closed = True


class _UnresolvedWriterObserver:
    def __init__(self, process: observe_process.SqlClientObserveProcess) -> None:
        self._process = process

    def contain(self, *, deadline: float) -> None:
        self._process.contain(deadline=deadline)

    def close(self) -> None:
        self._process.close()


class PythonSqlClientWriterObserverLauncher:
    """Launch only the admitted one-shot observer bootstrap."""

    def __init__(
        self,
        *,
        python_executable: Path,
        package_root: Path,
        implementation_sha256: str,
        admission: bytes,
        max_address_space_bytes: int,
        dependency_paths: tuple[Path, ...] = (),
    ) -> None:
        observe_process._hash(implementation_sha256)
        self.admission = encode_connection_admission(*decode_connection_admission(admission))
        inputs = AdmittedPythonInputs(python_executable, package_root, dependency_paths)
        self.python, self.package_root, self.dependency_paths = (
            inputs.python,
            inputs.package_root,
            inputs.dependency_paths,
        )
        if type(max_address_space_bytes) is not int or not 0 < max_address_space_bytes < 2**63:
            raise ValueError(ERROR)
        self.implementation_sha256, self.max_address_space_bytes = implementation_sha256, max_address_space_bytes

    def assert_installation(self) -> None:
        if (
            not (self.package_root / "dpone/app/mssql_sqlclient_writer_observer_bootstrap.py").is_file()
            or worker_installation_digest(self.package_root) != self.implementation_sha256
        ):
            raise ValueError(ERROR)
        AdmittedPythonInputs(self.python, self.package_root, self.dependency_paths)

    def spawn(
        self, *, startup_deadline: float, operation_deadline: float, termination_timeout: float
    ) -> observe_process.SqlClientObserveProcess:
        for deadline_value in (startup_deadline, operation_deadline, termination_timeout):
            observe_process.deadline_nanoseconds(deadline_value)
        if startup_deadline > operation_deadline:
            raise ValueError(ERROR)
        LinuxTdsProcess.admit()
        self.assert_installation()
        parent, nonce = LinuxTdsProcess.identify(os.getpid()), secrets.token_bytes(32)
        custody, local, remote = TdsChildProcess.launch(), *socket.socketpair()
        custody.retain_socket(local)
        custody.retain_socket(remote)
        child = retained = None
        try:
            local.setblocking(False)
            remote.setblocking(False)
            command = [str(self.python), "-I", "-S", "-B", "-c", _SHIM, "--package-root", str(self.package_root)]
            for path in self.dependency_paths:
                command.extend(("--dependency-path", str(path)))
            options = {
                "parent": parent.pid,
                "address-space": self.max_address_space_bytes,
                "channel-fd": remote.fileno(),
                "startup-deadline": startup_deadline,
                "operation-deadline": operation_deadline,
                "launch-nonce": nonce.hex(),
                "implementation-sha256": self.implementation_sha256,
                "admission": self.admission.decode(),
            }
            for name, option_value in options.items():
                command.extend(("--" + name, str(option_value)))
            if time.monotonic() >= startup_deadline:
                raise ValueError(ERROR)
            child = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                pass_fds=(remote.fileno(),),
                close_fds=True,
                env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
            )
            custody.retain_process(child)
            expected = observe_process.TdsCoordinatorStartup(
                LinuxTdsProcess.identify(child.pid), self.implementation_sha256, str(self.package_root), nonce
            )
            retained = observe_process.SqlClientObserveProcess.from_custody(
                custody,
                local,
                expected,
                startup_deadline=startup_deadline,
                operation_deadline=operation_deadline,
                termination_timeout=termination_timeout,
            )
            custody.close_socket(remote)
            if LinuxTdsProcess.identify(os.getpid()) != parent:
                raise ValueError(ERROR)
            return retained
        except BaseException as error:
            custody.retain_acquisition(error)
            if retained is not None:
                retained._containment.request()
                raise TdsLaunchUnknown(_UnresolvedWriterObserver(retained)) from None
            if child is not None:
                raise TdsLaunchUnknown(UnresolvedPythonTdsLaunch.from_custody(custody)) from None
            custody.close()
            raise


__all__ = ("PythonSqlClientWriterObserverLauncher", "SqlClientWriterObserverProcessBackend")

"""Guarded OBSERVE launch with one dedicated local containment owner.

The containment thread exclusively acquires, signals, reaps and closes its
pidfd. It performs no journal work and cannot renew the first cleanup budget.
SQL remains entirely in the child; a stalled parent actor cannot delay expiry.
"""

from __future__ import annotations

import hashlib
import os
import secrets
import socket
import subprocess
import threading
import time
from pathlib import Path

from dpone.adapters.mssql_sqlclient_observe_transport import read_frame, require_quiet
from dpone.adapters.mssql_tds_child_process import (
    TdsChildContainmentExecutor,
    TdsChildProcess,
    UnresolvedPythonTdsLaunch,
)
from dpone.adapters.mssql_tds_coordinator_connection import decode_connection_admission, encode_connection_admission
from dpone.adapters.mssql_tds_installation import worker_installation_digest
from dpone.adapters.mssql_tds_launch_shim import SOURCE_SHIM
from dpone.adapters.mssql_tds_process import LinuxTdsProcess
from dpone.adapters.mssql_tds_python_admission import AdmittedPythonInputs
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup, decode_startup
from dpone.contracts.mssql_tds_validation import _hash, deadline_nanoseconds
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown

_ERROR = "mssql_native.sqlclient_observe_process_unknown"
_OBSERVE_SHIM = SOURCE_SHIM.replace("'mssql_tds_worker_bootstrap.py'", "'mssql_sqlclient_observe_bootstrap.py'")


class SqlClientObserveProcess:
    """Original parent owns protocol; dedicated owner independently contains."""

    def __init__(
        self,
        child: subprocess.Popen,
        channel: socket.socket,
        expected: TdsCoordinatorStartup,
        *,
        startup_deadline: float,
        operation_deadline: float,
        termination_timeout: float,
    ) -> None:
        resources = TdsChildProcess(child, None, ())
        resources.retain_socket(channel)
        self._configure(resources, channel, expected, startup_deadline, operation_deadline, termination_timeout)

    def _configure(
        self,
        resources: TdsChildProcess,
        channel: socket.socket,
        expected: TdsCoordinatorStartup,
        startup_deadline: float,
        operation_deadline: float,
        termination_timeout: float,
    ) -> None:
        self._resources = resources
        self.channel, self.child = channel, resources.process
        self.identity, self._expected = expected.process, expected
        self.startup_receipt: TdsCoordinatorStartup | None = None
        self.operation_deadline, self.startup_deadline = operation_deadline, startup_deadline
        self._owner = (os.getpid(), threading.current_thread())
        self._closed = self._startup_attempted = self._asserting = False
        self._containment = TdsChildContainmentExecutor(
            resources, expected.process, operation_deadline, termination_timeout
        )

    @classmethod
    def from_custody(
        cls,
        custody: TdsChildProcess,
        channel: socket.socket,
        expected: TdsCoordinatorStartup,
        *,
        startup_deadline: float,
        operation_deadline: float,
        termination_timeout: float,
    ) -> SqlClientObserveProcess:
        process = cls.__new__(cls)
        process._configure(custody, channel, expected, startup_deadline, operation_deadline, termination_timeout)
        return process

    @property
    def cleanup_deadline(self) -> float | None:
        return self._containment.cleanup_deadline

    def _local(self) -> None:
        if self._owner != (os.getpid(), threading.current_thread()):
            self._resources.poison()
            raise ValueError(_ERROR)

    def assert_current(self) -> None:
        self._local()
        if self._asserting:
            self._resources.poison()
            raise ValueError(_ERROR)
        self._asserting = True
        try:
            self._resources.check_owner()
            if (
                self._closed
                or self._containment.failed
                or self.cleanup_deadline is not None
                or time.monotonic() >= self.operation_deadline
                or LinuxTdsProcess.identify(self.identity.pid) != self.identity
            ):
                raise ValueError(_ERROR)
            self._resources.check_owner()
        except BaseException:
            self._resources.poison()
            self._containment.request()
            raise
        finally:
            self._asserting = False

    def startup(self) -> TdsCoordinatorStartup:
        self._local()
        if self._startup_attempted:
            self._resources.poison()
            raise ValueError(_ERROR)
        self._startup_attempted = True
        remaining = self.startup_deadline - time.monotonic()
        if remaining <= 0 or not self._containment.ready.wait(remaining):
            raise ValueError(_ERROR)
        self.assert_current()
        if self.startup_receipt is not None:
            raise ValueError(_ERROR)
        receipt = decode_startup(read_frame(self.channel, deadline=self.startup_deadline, limit=16384))
        if receipt != self._expected:
            raise ValueError(_ERROR)
        require_quiet(self.channel)
        self.startup_receipt = receipt
        self.assert_current()
        return receipt

    def contain(self, *, deadline: float | None = None) -> TdsChildExit:
        self._local()
        if deadline is not None:
            deadline_nanoseconds(deadline)
        end = self._containment.request(deadline)
        if not self._containment.done.wait(max(0, end - time.monotonic())):
            raise ValueError(_ERROR)
        if self._containment.failed or self._containment.exit is None or time.monotonic() >= end:
            raise ValueError(_ERROR)
        return self._containment.exit

    def close(self) -> None:
        self._local()
        if not self._containment.done.is_set():
            raise ValueError(_ERROR)
        self._resources.close()
        self._closed = True


class _UnresolvedObserveLaunch:
    """Existing unresolved port shape over the original dedicated owner."""

    def __init__(self, process: SqlClientObserveProcess) -> None:
        self.process = process

    def contain(self, *, deadline: float) -> None:
        self.process.contain(deadline=deadline)

    def close(self) -> None:
        self.process.close()


class PythonSqlClientObserveLauncher:
    """Reuse immutable launch admission, selecting only the fixed OBSERVE shim."""

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
        _hash(implementation_sha256)
        self._admission = encode_connection_admission(*decode_connection_admission(admission))
        if self._admission != admission:
            raise ValueError("mssql_native.sqlclient_departure_admission_noncanonical")
        if (
            not isinstance(python_executable, Path)
            or not isinstance(package_root, Path)
            or type(dependency_paths) is not tuple
            or any(not isinstance(path, Path) for path in dependency_paths)
        ):
            raise ValueError("mssql_native.sqlclient_departure_paths_invalid")
        if type(max_address_space_bytes) is not int or not 0 < max_address_space_bytes < 2**63:
            raise ValueError("mssql_native.tds_coordinator_address_space")
        inputs = AdmittedPythonInputs(python_executable, package_root, dependency_paths)
        self.python, self.package_root, self.dependency_paths = (
            inputs.python,
            inputs.package_root,
            inputs.dependency_paths,
        )
        if not self.package_root.is_dir():
            raise ValueError("mssql_native.sqlclient_departure_root_invalid")
        self.implementation_sha256 = implementation_sha256
        self.max_address_space_bytes = max_address_space_bytes

    @property
    def admission(self) -> bytes:
        return self._admission

    @property
    def admission_sha256(self) -> str:
        return hashlib.sha256(self._admission).hexdigest()

    def assert_installation(self) -> None:
        if not (self.package_root / "dpone/app/mssql_sqlclient_observe_bootstrap.py").is_file():
            raise ValueError(_ERROR)
        AdmittedPythonInputs(self.python, self.package_root, self.dependency_paths)
        if worker_installation_digest(self.package_root) != self.implementation_sha256:
            raise ValueError(_ERROR)

    def spawn(
        self, *, startup_deadline: float, operation_deadline: float, termination_timeout: float
    ) -> SqlClientObserveProcess:
        for value in (startup_deadline, operation_deadline, termination_timeout):
            deadline_nanoseconds(value)
        if startup_deadline > operation_deadline:
            raise ValueError(_ERROR)
        LinuxTdsProcess.admit()
        self.assert_installation()
        parent = LinuxTdsProcess.identify(os.getpid())
        nonce = secrets.token_bytes(32)
        custody = TdsChildProcess.launch()
        local, remote = socket.socketpair()
        custody.retain_socket(local)
        custody.retain_socket(remote)
        child = retained = None
        try:
            local.setblocking(False)
            remote.setblocking(False)
            command = [
                str(self.python),
                "-I",
                "-S",
                "-B",
                "-c",
                _OBSERVE_SHIM,
                "--package-root",
                str(self.package_root),
            ]
            for path in self.dependency_paths:
                command.extend(["--dependency-path", str(path)])
            options = {
                "parent": parent.pid,
                "address-space": self.max_address_space_bytes,
                "startup-deadline": startup_deadline,
                "operation-deadline": operation_deadline,
                "admission": self.admission.decode("utf-8"),
                "launch-nonce": nonce.hex(),
                "implementation-sha256": self.implementation_sha256,
                "channel-fd": remote.fileno(),
            }
            for name, option in options.items():
                command.extend(["--" + name, str(option)])
            if time.monotonic() >= startup_deadline:
                raise ValueError(_ERROR)
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
            identity = LinuxTdsProcess.identify(child.pid)
            expected = TdsCoordinatorStartup(identity, self.implementation_sha256, str(self.package_root), nonce)
            retained = SqlClientObserveProcess.from_custody(
                custody,
                local,
                expected,
                startup_deadline=startup_deadline,
                operation_deadline=operation_deadline,
                termination_timeout=termination_timeout,
            )
            custody.close_socket(remote)
            if LinuxTdsProcess.identify(os.getpid()) != parent:
                raise ValueError(_ERROR)
            return retained
        except BaseException as error:
            custody.retain_acquisition(error)
            if retained is not None:
                retained._containment.request()
                raise TdsLaunchUnknown(_UnresolvedObserveLaunch(retained)) from None
            if child is not None:
                raise TdsLaunchUnknown(UnresolvedPythonTdsLaunch.from_custody(custody)) from None
            custody.close()
            raise

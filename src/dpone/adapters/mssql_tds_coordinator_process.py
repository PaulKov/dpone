"""Fixed coordinator launch and five independent, bounded, one-shot pipes.

Immutable deployment and interpreter/SDK binary admission are upstream duties;
source hashing detects ordinary drift, not hostile filesystem replacement. OS
launch, identity and cleanup calls need an admitted local environment: deadline
checks do not make stalled synchronous kernel/filesystem calls interruptible.
"""

from __future__ import annotations

import hashlib
import math
import os
import secrets
import subprocess
import time
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory

from dpone.adapters.mssql_tds_channels import read_worker_message, write_worker_control
from dpone.adapters.mssql_tds_child_process import (
    TdsChildProcess,
    UnresolvedPythonTdsLaunch,
    _close_all,
    _close_fds,
    _existing_custody,
)
from dpone.adapters.mssql_tds_coordinator_connection import decode_connection_admission, encode_connection_admission
from dpone.adapters.mssql_tds_installation import worker_installation_digest
from dpone.adapters.mssql_tds_launch_shim import COORDINATOR_SOURCE_SHIM
from dpone.adapters.mssql_tds_process import LinuxTdsProcess
from dpone.adapters.mssql_tds_python_admission import AdmittedPythonInputs
from dpone.contracts.mssql_tds_api import encode_message
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup, decode_startup
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity, _hash
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown


def _deadline(value: float) -> float:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError("mssql_native.tds_coordinator_deadline")
    return value


class PythonTdsCoordinatorLauncher:
    """Construct with admitted immutable sources and explicit dependency paths.

    This is internal trusted composition, not a caller-selected executable API.
    Missing fixed bootstrap rejects admission; there is no fallback entrypoint.
    Credentials and SQL commands are never accepted by this launcher.
    """

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
        if type(max_address_space_bytes) is not int or not 0 < max_address_space_bytes < 2**63:
            raise ValueError("mssql_native.tds_coordinator_address_space")
        inputs = AdmittedPythonInputs(python_executable, package_root, dependency_paths)
        self.python, self.package_root, self.dependency_paths = (
            inputs.python,
            inputs.package_root,
            inputs.dependency_paths,
        )
        self.implementation_sha256 = implementation_sha256
        self.max_address_space_bytes = max_address_space_bytes

    @property
    def admission(self) -> bytes:
        """Canonical nonsecret build descriptor; decoding did not admit binaries."""
        return self._admission

    @property
    def admission_sha256(self) -> str:
        """Bind expected build/profile inputs in the trusted authentication producer."""
        return hashlib.sha256(self._admission).hexdigest()

    def assert_installation(self) -> None:
        """Verify fixed bootstrap presence and the admitted complete source digest."""
        if not (self.package_root / "dpone/app/mssql_tds_coordinator_bootstrap.py").is_file():
            raise ValueError("mssql_native.tds_coordinator_bootstrap_missing")
        AdmittedPythonInputs(self.python, self.package_root, self.dependency_paths)
        if worker_installation_digest(self.package_root) != self.implementation_sha256:
            raise ValueError("mssql_native.tds_implementation_changed")

    def spawn(self, *, startup_deadline: float, operation_deadline: float) -> PythonTdsCoordinatorProcess:
        startup_deadline = min(_deadline(startup_deadline), _deadline(operation_deadline))
        LinuxTdsProcess.admit()
        self.assert_installation()
        if time.monotonic() >= startup_deadline:
            raise ValueError("mssql_native.tds_coordinator_deadline")
        custody = TdsChildProcess.launch()
        parents: list[int] = []
        children: list[int] = []
        process = handle = cache = None
        nonce = secrets.token_bytes(32)
        try:
            # Startup/authority/result flow child->parent; credentials/grant reverse.
            for sending in (False, True, False, True, False):
                reader, writer = os.pipe()
                custody.retain_descriptors((reader, writer))
                parents.append(writer if sending else reader)
                children.append(reader if sending else writer)
                os.set_blocking(reader, False)
                os.set_blocking(writer, False)
            cache = TemporaryDirectory(prefix="dpone-tds-coordinator-pycache-")
            custody.retain_cache(cache)
            command = [
                str(self.python),
                "-I",
                "-S",
                "-B",
                "-X",
                "pycache_prefix=" + cache.name,
                "-c",
                COORDINATOR_SOURCE_SHIM,
                "--package-root",
                str(self.package_root),
            ]
            for path in self.dependency_paths:
                command.extend(["--dependency-path", str(path)])
            options = {
                "parent": os.getpid(),
                "address-space": self.max_address_space_bytes,
                "startup-deadline": startup_deadline,
                "operation-deadline": operation_deadline,
                "admission": self._admission.decode("utf-8"),
                "launch-nonce": nonce.hex(),
                "implementation-sha256": self.implementation_sha256,
                **dict(zip(("startup-fd", "credentials-fd", "authority-fd", "grant-fd", "result-fd"), children)),
            }
            for name, value in options.items():
                command.extend(["--" + name, str(value)])
            if time.monotonic() >= startup_deadline:
                raise ValueError("mssql_native.tds_coordinator_deadline")
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                pass_fds=tuple(children),
                close_fds=True,
                env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
            )
            custody.retain_process(process)
            detached, children = tuple(children), []
            custody.close_launch_descriptors(detached, _close_fds)
            handle = LinuxTdsProcess.acquire(LinuxTdsProcess.identify(process.pid))
            custody.retain_handle(handle)
            if time.monotonic() >= startup_deadline:
                raise ValueError("mssql_native.tds_coordinator_deadline")
            return PythonTdsCoordinatorProcess.from_custody(
                custody,
                package_root=self.package_root,
                implementation_sha256=self.implementation_sha256,
                launch_nonce=nonce,
                startup_deadline=startup_deadline,
                operation_deadline=operation_deadline,
            )
        except BaseException as error:
            custody.retain_acquisition(error)
            detached, children = tuple(children), []
            if process is not None:
                try:
                    custody.close_launch_descriptors(detached, _close_fds)
                finally:
                    raise TdsLaunchUnknown(UnresolvedPythonTdsLaunch.from_custody(custody)) from None
            _close_all((partial(custody.close_launch_descriptors, custody.descriptors, _close_fds), custody.close))
            raise


class PythonTdsCoordinatorProcess:
    """Own five phase channels through the shared exclusive child-resource owner.

    Phase is consumed before validation/I/O, and only successful completion opens
    the next phase. Errors preserve containment capability and forbid IPC reuse.
    The transport never treats returned raw bytes as validated grant or SQL facts.
    """

    def __init__(
        self,
        process: subprocess.Popen[bytes],
        handle: LinuxTdsProcess,
        descriptors: tuple[int, ...],
        *,
        package_root: Path,
        implementation_sha256: str,
        launch_nonce: bytes,
        startup_deadline: float,
        operation_deadline: float,
        cache: TemporaryDirectory[str] | None = None,
    ) -> None:
        self._resources = _existing_custody(process, handle, descriptors) or TdsChildProcess(
            process, handle, descriptors, cache
        )
        self._configure(
            self._resources, package_root, implementation_sha256, launch_nonce, startup_deadline, operation_deadline
        )

    def _configure(
        self,
        resources: TdsChildProcess,
        package_root: Path,
        implementation_sha256: str,
        launch_nonce: bytes,
        startup_deadline: float,
        operation_deadline: float,
    ) -> None:
        self._resources = resources
        self._descriptors = resources.descriptors
        self._expected = TdsCoordinatorStartup(
            resources.identity, implementation_sha256, str(package_root), launch_nonce
        )
        self._startup_deadline = min(_deadline(startup_deadline), _deadline(operation_deadline))
        self._operation_deadline = operation_deadline
        self._phase = 0
        self._startup_receipt: TdsCoordinatorStartup | None = None
        self._received_result: bytes | None = None

    @classmethod
    def from_custody(
        cls,
        custody: TdsChildProcess,
        *,
        package_root: Path,
        implementation_sha256: str,
        launch_nonce: bytes,
        startup_deadline: float,
        operation_deadline: float,
    ) -> PythonTdsCoordinatorProcess:
        process = cls.__new__(cls)
        process._configure(
            custody, package_root, implementation_sha256, launch_nonce, startup_deadline, operation_deadline
        )
        return process

    @property
    def identity(self) -> TdsProcessIdentity:
        return self._resources.identity

    @property
    def startup_receipt(self) -> TdsCoordinatorStartup | None:
        return self._startup_receipt

    @property
    def received_result(self) -> bytes | None:
        return self._received_result

    def _begin(self, phase: int, deadline: float) -> float:
        if self._phase != phase:
            self._resources.poison()
            raise ValueError("mssql_native.tds_coordinator_phase")
        self._resources.check_owner()
        self._phase = -1
        return min(_deadline(deadline), self._startup_deadline if phase == 0 else self._operation_deadline)

    def _complete(self, phase: int) -> None:
        self._resources.close_descriptor(self._descriptors[phase])
        self._phase = phase + 1

    def startup(self, *, deadline: float) -> TdsCoordinatorStartup:
        deadline = self._begin(0, deadline)
        body = read_worker_message(self._descriptors[0], deadline=deadline, max_payload=16384)
        receipt = decode_startup(body)
        if receipt != self._expected:
            raise ValueError("mssql_native.tds_coordinator_startup_binding")
        if time.monotonic() >= deadline:
            raise ValueError("mssql_native.tds_coordinator_deadline")
        self._startup_receipt = receipt
        self._complete(0)
        return receipt

    def _send(self, phase: int, body: bytes, deadline: float, limit: int) -> None:
        deadline = self._begin(phase, deadline)
        framed = encode_message(body, max_payload=limit)
        write_worker_control(self._descriptors[phase], framed, deadline=deadline, max_bytes=limit + 4)
        self._complete(phase)

    def deliver_credentials(self, body: bytes, *, deadline: float) -> None:
        self._send(1, body, deadline, 1024**2)

    def observe_authority(self, *, deadline: float) -> bytes:
        deadline = self._begin(2, deadline)
        body = read_worker_message(self._descriptors[2], deadline=deadline, max_payload=16384)
        self._complete(2)
        return body

    def deliver_grant(self, body: bytes, *, deadline: float) -> None:
        self._send(3, body, deadline, 16384)

    def receive_result(self, *, deadline: float) -> bytes:
        deadline = self._begin(4, deadline)
        body = read_worker_message(self._descriptors[4], deadline=deadline, max_payload=262144)
        self._received_result = body  # Preserve before a close or later decode can fail.
        self._complete(4)
        return body

    def wait(self, *, deadline: float) -> TdsChildExit:
        return self._resources.wait(deadline=min(_deadline(deadline), self._operation_deadline))

    def terminate(self, *, deadline: float) -> TdsChildExit:
        """Use the supervisor's separately captured absolute containment budget."""
        return self._resources.terminate(deadline=self._resources.capture_budget(deadline=_deadline(deadline)))

    def close(self) -> None:
        self._resources.close()

"""Fixed departure launch and three independent, bounded, one-shot pipes.

Immutable deployment and interpreter/SDK binary admission are upstream duties;
source hashing detects ordinary drift, not hostile filesystem replacement. OS
launch, identity and cleanup calls need an admitted local environment: deadline
checks do not make stalled synchronous kernel/filesystem calls interruptible.
"""

from __future__ import annotations

import hashlib
import os
import secrets
import subprocess
import time
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory

from dpone.adapters.mssql_sqlclient_departure_process import (
    SqlClientDepartureProcess,
    _admit_departure_launch,
    _canonical_departure_startup,
)
from dpone.adapters.mssql_tds_child_process import TdsChildProcess, UnresolvedPythonTdsLaunch, _close_all, _close_fds
from dpone.adapters.mssql_tds_coordinator_connection import decode_connection_admission, encode_connection_admission
from dpone.adapters.mssql_tds_installation import worker_installation_digest
from dpone.adapters.mssql_tds_launch_shim import DEPARTURE_SOURCE_SHIM
from dpone.adapters.mssql_tds_process import LinuxTdsProcess
from dpone.adapters.mssql_tds_python_admission import AdmittedPythonInputs
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown


class PythonSqlClientDepartureLauncher:
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
        _admit_departure_launch(implementation_sha256=implementation_sha256)
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
        """Canonical nonsecret build descriptor; decoding did not admit binaries."""
        return self._admission

    @property
    def admission_sha256(self) -> str:
        """Bind expected build/profile inputs in the trusted authentication producer."""
        return hashlib.sha256(self._admission).hexdigest()

    def assert_installation(self) -> None:
        """Verify fixed bootstrap presence and the admitted complete source digest."""
        if not (self.package_root / "dpone/app/mssql_sqlclient_departure_bootstrap.py").is_file():
            raise ValueError("mssql_native.tds_coordinator_bootstrap_missing")
        AdmittedPythonInputs(self.python, self.package_root, self.dependency_paths)
        if worker_installation_digest(self.package_root) != self.implementation_sha256:
            raise ValueError("mssql_native.tds_implementation_changed")

    def spawn(self, *, startup_deadline: float, operation_deadline: float) -> SqlClientDepartureProcess:
        _admit_departure_launch(startup_deadline=startup_deadline, operation_deadline=operation_deadline)
        LinuxTdsProcess.admit()
        self.assert_installation()
        if time.monotonic() >= startup_deadline:
            raise ValueError("mssql_native.tds_coordinator_deadline")
        parent = LinuxTdsProcess.identify(os.getpid())
        if time.monotonic() >= startup_deadline:
            raise ValueError("mssql_native.sqlclient_departure_deadline_invalid")
        custody = TdsChildProcess.launch()
        parents: list[int] = []
        children: list[int] = []
        process = handle = cache = None
        nonce = secrets.token_bytes(32)
        try:
            # Startup/result flow child->parent; the private request flows reverse.
            for sending in (False, True, False):
                reader, writer = os.pipe()
                custody.retain_descriptors((reader, writer))
                parents.append(writer if sending else reader)
                children.append(reader if sending else writer)
                os.set_blocking(reader, False)
                os.set_blocking(writer, False)
            cache = TemporaryDirectory(prefix="dpone-sqlclient-departure-pycache-")
            custody.retain_cache(cache)
            command = [
                str(self.python),
                "-I",
                "-S",
                "-B",
                "-X",
                "pycache_prefix=" + cache.name,
                "-c",
                DEPARTURE_SOURCE_SHIM,
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
                **dict(zip(("startup-fd", "request-fd", "result-fd"), children)),
            }
            for name, value in options.items():
                command.extend(["--" + name, str(value)])
            self.assert_installation()
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
            if type(process.pid) is not int or not 1 <= process.pid <= 2**31 - 1:
                raise ValueError("mssql_native.tds_invalid_integer")
            handle = LinuxTdsProcess.acquire(LinuxTdsProcess.identify(process.pid))
            custody.retain_handle(handle)
            actual = handle.identity
            expected = _canonical_departure_startup(
                actual,
                self.implementation_sha256,
                self.package_root,
                nonce,
            )
            current = LinuxTdsProcess.identify(os.getpid())
            if (
                actual.pid != process.pid
                or (actual.host_sha256, actual.boot_id) != (current.host_sha256, current.boot_id)
                or current != parent
            ):
                raise ValueError("mssql_native.sqlclient_departure_process_mismatch")
            if time.monotonic() >= startup_deadline:
                raise ValueError("mssql_native.tds_coordinator_deadline")
            return SqlClientDepartureProcess(
                custody,
                handle,
                tuple(parents),
                expected=expected,
                startup_deadline=startup_deadline,
                operation_deadline=operation_deadline,
                cache=cache,
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

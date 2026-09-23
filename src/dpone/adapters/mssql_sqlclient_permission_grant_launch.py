"""One fixed permission child launch; no credentials enter process metadata."""

from __future__ import annotations

import base64
import hashlib
import math
import os
import secrets
import socket
import subprocess
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Protocol, cast

from dpone.adapters import mssql_sqlclient_permission_grant_process as permission_process
from dpone.adapters import mssql_tds_child_process as child_process
from dpone.adapters import mssql_tds_coordinator_connection as connection_adapter
from dpone.adapters import mssql_tds_installation as installation
from dpone.adapters import mssql_tds_launch_shim as launch_shims
from dpone.adapters import mssql_tds_python_admission as python_admission

SqlClientPermissionGrantProcess = permission_process.SqlClientPermissionGrantProcess
TdsChildProcess = child_process.TdsChildProcess
UnresolvedPythonTdsLaunch = child_process.UnresolvedPythonTdsLaunch
TdsLaunchUnknown = child_process.TdsLaunchUnknown
LinuxTdsProcess = child_process.LinuxTdsProcess
AdmittedPythonInputs = python_admission.AdmittedPythonInputs
decode_connection_admission = connection_adapter.decode_connection_admission
encode_connection_admission = connection_adapter.encode_connection_admission
worker_installation_digest = installation.worker_installation_digest
deadline_nanoseconds = permission_process.deadline_nanoseconds
PermissionWireBinding = permission_process.permission_wire.PermissionWireBinding
TdsCoordinatorStartup = permission_process.permission_wire.TdsCoordinatorStartup

ERROR = "mssql_native.sqlclient_permission_launch_invalid"


class PermissionGrantLaunchInputs(Protocol):
    request: Any
    operation: Any
    execution_owner: Any
    profile: Any
    admission_sha256: str
    startup_deadline: float
    operation_deadline: float
    termination_timeout: float

    def public_payload(self) -> bytes: ...


class _CustodyReady(Protocol):
    def await_custody(self, *, deadline: float) -> None: ...


class PythonSqlClientPermissionGrantLauncher:
    """Launch the fixed bootstrap once from an admitted immutable installation."""

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
        if type(implementation_sha256) is not str or len(implementation_sha256) != 64:
            raise ValueError(ERROR)
        try:
            int(implementation_sha256, 16)
            canonical = encode_connection_admission(*decode_connection_admission(admission))
            inputs = AdmittedPythonInputs(python_executable, package_root, dependency_paths)
            if (
                canonical != admission
                or type(max_address_space_bytes) is not int
                or not 0 < max_address_space_bytes < 2**63
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise ValueError(ERROR) from None
        self.python, self.package_root, self.dependency_paths = (
            inputs.python,
            inputs.package_root,
            inputs.dependency_paths,
        )
        self.implementation_sha256 = implementation_sha256
        self.admission = canonical
        self.max_address_space_bytes = max_address_space_bytes

    def assert_installation(self) -> None:
        if not (self.package_root / "dpone/app/mssql_sqlclient_permission_grant_bootstrap.py").is_file():
            raise ValueError(ERROR)
        AdmittedPythonInputs(self.python, self.package_root, self.dependency_paths)
        if worker_installation_digest(self.package_root) != self.implementation_sha256:
            raise ValueError(ERROR)

    def launch(self, inputs: PermissionGrantLaunchInputs) -> SqlClientPermissionGrantProcess:
        """Spawn once, bind exact identity, and return only after pidfd custody."""
        try:
            public = inputs.public_payload()
            start_ns = deadline_nanoseconds(inputs.startup_deadline)
            operation_ns = deadline_nanoseconds(inputs.operation_deadline)
            if (
                start_ns > operation_ns
                or inputs.admission_sha256 != hashlib.sha256(self.admission).hexdigest()
                or inputs.profile is not decode_connection_admission(self.admission)[1]
                or inputs.operation.implementation_sha256 != self.implementation_sha256
                or type(inputs.termination_timeout) not in (int, float)
                or not math.isfinite(inputs.termination_timeout)
                or inputs.termination_timeout <= 0
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise ValueError(ERROR) from None
        LinuxTdsProcess.admit()
        self.assert_installation()
        if time.monotonic() >= inputs.startup_deadline:
            raise ValueError(ERROR)
        parent_identity = LinuxTdsProcess.identify(os.getpid())
        parent_channel, child_channel = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        custody = TdsChildProcess.launch()
        process = None
        cache = None
        nonce = secrets.token_bytes(32)
        try:
            parent_channel.setblocking(False)
            child_channel.setblocking(False)
            custody.retain_socket(child_channel)
            cache = TemporaryDirectory(prefix="dpone-sqlclient-permission-pycache-")
            custody.retain_cache(cache)
            command = [
                str(self.python),
                "-I",
                "-S",
                "-B",
                "-X",
                "pycache_prefix=" + cache.name,
                "-c",
                getattr(launch_shims, "PERMISSION_GRANT_SOURCE_SHIM"),
                "--package-root",
                str(self.package_root),
            ]
            for path in self.dependency_paths:
                command.extend(("--dependency-path", str(path)))
            options = {
                "parent": os.getpid(),
                "address-space": self.max_address_space_bytes,
                "startup-deadline": inputs.startup_deadline,
                "operation-deadline": inputs.operation_deadline,
                "channel-fd": child_channel.fileno(),
                "launch-nonce": nonce.hex(),
                "implementation-sha256": self.implementation_sha256,
                "admission": self.admission.decode("ascii"),
                "public-request": base64.urlsafe_b64encode(public).decode("ascii"),
            }
            for name, value in options.items():
                command.extend(("--" + name, str(value)))
            self.assert_installation()
            if time.monotonic() >= inputs.startup_deadline:
                raise ValueError(ERROR)
            process = subprocess.Popen(
                command,
                cwd=str(self.package_root),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                pass_fds=(child_channel.fileno(),),
                close_fds=True,
                env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
            )
            custody.retain_process(process)
            custody.close_socket(child_channel)
            if type(process.pid) is not int or not 1 <= process.pid <= 2**31 - 1:
                raise ValueError(ERROR)
            child_identity = LinuxTdsProcess.identify(process.pid)
            current = LinuxTdsProcess.identify(os.getpid())
            if (
                child_identity.pid != process.pid
                or current != parent_identity
                or (child_identity.host_sha256, child_identity.boot_id) != (current.host_sha256, current.boot_id)
            ):
                raise ValueError(ERROR)
            startup = TdsCoordinatorStartup(child_identity, self.implementation_sha256, str(self.package_root), nonce)
            binding = PermissionWireBinding(
                inputs.request, inputs.operation, startup, inputs.execution_owner, operation_ns
            )
            retained = SqlClientPermissionGrantProcess.adopt(
                custody,
                parent_channel,
                binding,
                startup_deadline=inputs.startup_deadline,
                operation_deadline=inputs.operation_deadline,
                termination_timeout=inputs.termination_timeout,
            )
            cast(_CustodyReady, retained).await_custody(deadline=inputs.startup_deadline)
            return retained
        except permission_process.PermissionGrantProcessUnknown:
            raise
        except BaseException:
            if process is not None:
                if not any(r.kind == "socket" and r.value is parent_channel for r in custody._resources):
                    custody.retain_socket(parent_channel)
                raise TdsLaunchUnknown(UnresolvedPythonTdsLaunch.from_custody(custody)) from None
            for channel in (parent_channel, child_channel):
                try:
                    channel.close()
                except BaseException:
                    pass
            if cache is not None:
                try:
                    cache.cleanup()
                except BaseException:
                    pass
            raise

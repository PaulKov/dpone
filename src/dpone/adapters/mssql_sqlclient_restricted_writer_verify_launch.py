"""Fixed P9a launch with existing child custody and one bounded cleanup path."""

from __future__ import annotations

import base64
import hashlib
import math
import os
import socket
import subprocess
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Protocol, cast

from dpone.adapters.mssql_sqlclient_permission_custody import await_permission_custody  # noqa: F401
from dpone.adapters.mssql_sqlclient_restricted_writer_verify_frames import (
    FRAMES,
)
from dpone.adapters.mssql_sqlclient_restricted_writer_verify_frames import (
    LIMIT as LIMIT,
)
from dpone.adapters.mssql_sqlclient_restricted_writer_verify_frames import _wait as _wait
from dpone.adapters.mssql_sqlclient_restricted_writer_verify_frames import read_frame as _read  # noqa: F401
from dpone.adapters.mssql_sqlclient_restricted_writer_verify_frames import write_frame as _write  # noqa: F401
from dpone.adapters.mssql_sqlclient_restricted_writer_verify_process import (
    RestrictedWriterVerifyProcess as RestrictedWriterVerifyProcess,
)
from dpone.adapters.mssql_sqlclient_restricted_writer_verify_process import (
    RestrictedWriterVerifyProcessUnknown as RestrictedWriterVerifyProcessUnknown,
)
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
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown

SHIM = SOURCE_SHIM.replace("'mssql_tds_worker_bootstrap.py'", "'mssql_sqlclient_restricted_writer_verify_bootstrap.py'")
ERROR = "mssql_native.sqlclient_restricted_writer_verify_invalid"

# Preserve the established public defining module after responsibility extraction.
RestrictedWriterVerifyProcess.__module__ = __name__
RestrictedWriterVerifyProcessUnknown.__module__ = __name__


class RestrictedWriterVerifyLaunchInputs(Protocol):
    request: object
    startup_deadline: float
    operation_deadline: float
    termination_timeout: float
    admission_sha256: str
    profile: object


class RestrictedWriterVerifyLaunchContract(Protocol):
    def validate_request(self, value: object) -> None: ...
    def decode_startup(self, payload: bytes) -> object: ...
    def registration(self, reservation: object, process: object) -> object: ...
    def decode_result(self, payload: bytes) -> object: ...
    def decode_opening(self, payload: bytes) -> object: ...
    def encode_authorization(self, opening_payload: bytes) -> bytes: ...


class PythonRestrictedWriterVerifyLauncher:
    def __init__(
        self,
        *,
        python_executable: Path,
        package_root: Path,
        contract: RestrictedWriterVerifyLaunchContract,
        implementation_sha256: str,
        admission: bytes,
        max_address_space_bytes: int,
        dependency_paths: tuple[Path, ...] = (),
    ) -> None:
        inputs = AdmittedPythonInputs(python_executable, package_root, dependency_paths)
        if not all(
            callable(getattr(contract, name, None))
            for name in (
                "validate_request",
                "decode_startup",
                "registration",
                "decode_opening",
                "encode_authorization",
                "decode_result",
            )
        ):
            raise ValueError(ERROR)
        canonical = encode_connection_admission(*decode_connection_admission(admission))
        if (
            canonical != admission
            or type(implementation_sha256) is not str
            or len(implementation_sha256) != 64
            or type(max_address_space_bytes) is not int
            or not 0 < max_address_space_bytes < 2**63
        ):
            raise ValueError(ERROR)
        self.python, self.package_root, self.dependency_paths = (
            inputs.python,
            inputs.package_root,
            inputs.dependency_paths,
        )
        self.implementation_sha256, self.admission = implementation_sha256, canonical
        self.max_address_space_bytes = max_address_space_bytes
        self.contract = contract

    def assert_installation(self) -> None:
        if not (self.package_root / "dpone/app/mssql_sqlclient_restricted_writer_verify_bootstrap.py").is_file():
            raise ValueError(ERROR)
        if worker_installation_digest(self.package_root) != self.implementation_sha256:
            raise ValueError(ERROR)

    def launch(
        self,
        inputs: RestrictedWriterVerifyLaunchInputs,
        reservation: Any,
        *,
        public_payload: bytes,
    ):
        self.contract.validate_request(inputs.request)
        request = cast(Any, inputs.request)
        if (
            reservation.operation_id != request.operation_id
            or request.implementation_sha256 != self.implementation_sha256
            or inputs.admission_sha256 != hashlib.sha256(self.admission).hexdigest()
            or inputs.profile is not decode_connection_admission(self.admission)[1]
            or type(public_payload) is not bytes
            or not public_payload
            or type(inputs.startup_deadline) is not float
            or type(inputs.operation_deadline) is not float
            or not math.isfinite(inputs.startup_deadline)
            or not math.isfinite(inputs.operation_deadline)
            or inputs.startup_deadline > inputs.operation_deadline
            or time.monotonic() >= inputs.startup_deadline
        ):
            raise ValueError(ERROR)
        LinuxTdsProcess.admit()
        self.assert_installation()
        custody = TdsChildProcess.launch()
        parent = child = cache = process = executor = None
        try:
            if time.monotonic() >= inputs.startup_deadline:
                raise TimeoutError(ERROR)
            parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
            custody.retain_socket(parent)
            custody.retain_socket(child)
            parent.setblocking(False)
            child.setblocking(False)
            cache = TemporaryDirectory(prefix="dpone-restricted-writer-verify-")
            custody.retain_cache(cache)
            command = [
                str(self.python),
                "-I",
                "-S",
                "-B",
                "-X",
                "pycache_prefix=" + cache.name,
                "-c",
                SHIM,
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
                "channel-fd": child.fileno(),
                "implementation-sha256": self.implementation_sha256,
                "admission": self.admission.decode("ascii"),
                "public-request": base64.urlsafe_b64encode(public_payload).decode("ascii"),
            }
            for name, value in options.items():
                command.extend(("--" + name, str(value)))
            if time.monotonic() >= inputs.startup_deadline:
                raise TimeoutError(ERROR)
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                pass_fds=(child.fileno(),),
                close_fds=True,
                env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
            )
            custody.process = process
            custody.retain_process(process)
            custody.close_socket(child)
            identity = LinuxTdsProcess.identify(process.pid)
            executor = TdsChildContainmentExecutor(
                custody, identity, inputs.operation_deadline, inputs.termination_timeout
            )
            retained = RestrictedWriterVerifyProcess(
                custody,
                executor,
                parent,
                reservation,
                inputs.startup_deadline,
                inputs.operation_deadline,
                self.contract,
                frames=FRAMES,
                custody_waiter=lambda custody, executor, identity, deadline: await_permission_custody(
                    custody, executor, identity, deadline
                ),
            )
            retained.await_custody()
            return retained
        except RestrictedWriterVerifyProcessUnknown:
            raise
        except BaseException as error:
            custody.retain_acquisition(error)
            if executor is not None:
                retained = RestrictedWriterVerifyProcess(
                    custody,
                    executor,
                    parent,
                    reservation,
                    inputs.startup_deadline,
                    inputs.operation_deadline,
                    self.contract,
                    frames=FRAMES,
                    custody_waiter=lambda custody, executor, identity, deadline: await_permission_custody(
                        custody, executor, identity, deadline
                    ),
                )
                try:
                    retained.cleanup()
                except BaseException:
                    pass
                raise RestrictedWriterVerifyProcessUnknown(retained) from None
            if process is not None:
                raise TdsLaunchUnknown(UnresolvedPythonTdsLaunch.from_custody(custody)) from None
            ambiguous_close = False
            for channel in (parent, child):
                if channel is None or any(
                    resource.kind == "socket" and resource.value is channel for resource in custody._resources
                ):
                    continue
                try:
                    channel.close()
                except BaseException as close_error:
                    custody.retain_socket(channel)
                    socket_resource = next(
                        resource
                        for resource in custody._resources
                        if resource.kind == "socket" and resource.value is channel
                    )
                    socket_resource.state = "CLOSE_UNKNOWN"
                    custody.retain_acquisition(close_error)
                    ambiguous_close = True
            if cache is not None and not any(
                resource.kind == "cache" and resource.value is cache for resource in custody._resources
            ):
                try:
                    cache.cleanup()
                except BaseException as close_error:
                    custody.retain_cache(cache)
                    cache_resource = next(
                        resource
                        for resource in custody._resources
                        if resource.kind == "cache" and resource.value is cache
                    )
                    cache_resource.state = "CLOSE_UNKNOWN"
                    custody.retain_acquisition(close_error)
                    ambiguous_close = True
            if ambiguous_close:
                raise TdsLaunchUnknown(UnresolvedPythonTdsLaunch.from_custody(custody)) from None
            try:
                custody.close(descriptors_first=True)
            except BaseException as close_error:
                custody.retain_acquisition(close_error)
                raise TdsLaunchUnknown(UnresolvedPythonTdsLaunch.from_custody(custody)) from None
            raise

"""Actual isolated Python-to-managed exec launch with retained failure capability.

Injected installation/input authority is internal trusted composition. Bootstrap
permission follows pidfd acquisition; it is never a credential or SQL grant.
"""

from __future__ import annotations

import math
import os
import stat
import subprocess
import time
import uuid
from dataclasses import replace
from tempfile import TemporaryDirectory

from dpone.adapters.mssql_sqlclient_input_admission import require_input_descriptor
from dpone.adapters.mssql_sqlclient_installation import AdmittedSqlClientInstallation
from dpone.adapters.mssql_sqlclient_launch_shim import SOURCE_SHIM
from dpone.adapters.mssql_sqlclient_process import SqlClientChildProcess
from dpone.adapters.mssql_tds_channels import write_worker_control, write_worker_message
from dpone.adapters.mssql_tds_child_process import TdsChildProcess, UnresolvedPythonTdsLaunch, _close_fds
from dpone.adapters.mssql_tds_process import LinuxTdsProcess
from dpone.contracts.mssql_tds_api import (
    SqlClientDescriptors,
    SqlClientInputDescriptor,
    SqlClientLaunch,
    encode_launch,
    input_descriptor_digest,
)
from dpone.contracts.mssql_tds_validation import _hash
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown


def _deadline_ns(value: float) -> int:
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value < (2**63 - 1) / 10**9:
        raise ValueError("mssql_native.sqlclient_deadline")
    # Round toward earlier admission, never extend an absolute float deadline.
    return int(math.nextafter(float(value), 0.0) * 10**9)


class SqlClientLauncher:
    """Spawn once with an already admitted readonly input descriptor and installation."""

    _input_binding: str | None

    def __init__(
        self,
        *,
        installation: AdmittedSqlClientInstallation,
        attempt_sha256: str,
        input_binding_sha256: str,
        input_fd: int,
        address_space_bytes: int,
    ) -> None:
        _hash(input_binding_sha256)
        self._initialize(installation, attempt_sha256, input_fd, address_space_bytes)
        self._input_binding = input_binding_sha256

    def _initialize(
        self, installation: AdmittedSqlClientInstallation, attempt_sha256: str, input_fd: int, address_space_bytes: int
    ) -> None:
        _hash(attempt_sha256)
        if type(installation) is not AdmittedSqlClientInstallation or type(input_fd) is not int or input_fd < 3:
            raise ValueError("mssql_native.sqlclient_launch_input")
        if type(address_space_bytes) is not int or not 8 * 1024**3 <= address_space_bytes <= 16 * 1024**3:
            raise ValueError("mssql_native.sqlclient_address_space")
        self._installation, self._attempt = installation, attempt_sha256
        self._input_binding = None
        self._input_descriptor: SqlClientInputDescriptor | None = None
        self._input_fd, self._limit = input_fd, address_space_bytes
        self._launched = False

    @classmethod
    def for_input_descriptor(
        cls,
        *,
        installation: AdmittedSqlClientInstallation,
        attempt_sha256: str,
        input_descriptor: SqlClientInputDescriptor,
        address_space_bytes: int,
    ) -> SqlClientLauncher:
        """Retain the frozen declaration without allocating or observing an OS FD.

        The caller lends the original FD exclusively until spawn finishes. dup
        shares its offset: no alias may read, seek, close or replace the original.
        The original stays open on failure. Spawn binds the actual inherited FD;
        only that child-numbered declaration is suitable for its subsequent job.
        """
        if type(input_descriptor) is not SqlClientInputDescriptor:
            raise ValueError("mssql_native.sqlclient_launch_input")
        launcher = cls.__new__(cls)
        launcher._initialize(installation, attempt_sha256, input_descriptor.fd, address_space_bytes)
        launcher._input_descriptor = input_descriptor
        return launcher

    @staticmethod
    def _require_bound_input(fd: int, original: SqlClientInputDescriptor) -> None:
        """Observe the actual duplicate without consuming bytes or resetting offset."""
        require_input_descriptor(fd, original)

    def spawn(self, *, startup_deadline: float, operation_deadline: float) -> SqlClientChildProcess:
        if self._launched:
            raise ValueError("mssql_native.sqlclient_launch_used")
        self._launched = True
        startup_ns, operation_ns = _deadline_ns(startup_deadline), _deadline_ns(operation_deadline)
        deadline_ns = min(startup_ns, operation_ns)
        LinuxTdsProcess.admit()
        import fcntl
        import resource

        info = os.fstat(self._input_fd)
        if not stat.S_ISREG(info.st_mode) or fcntl.fcntl(self._input_fd, fcntl.F_GETFL) & os.O_ACCMODE != os.O_RDONLY:
            raise ValueError("mssql_native.sqlclient_input_descriptor")
        if any(
            limit != resource.RLIM_INFINITY and limit < self._limit for limit in resource.getrlimit(resource.RLIMIT_AS)
        ):
            raise ValueError("mssql_native.sqlclient_inherited_limit")
        self._installation.assert_admitted(deadline_ns=deadline_ns)
        parents: list[int] = []
        children: list[int] = []
        custody = TdsChildProcess.launch()
        try:
            for sending in (False, True, False, True, False, True):
                reader, writer = os.pipe()
                custody.retain_descriptors((reader, writer))
                parents.append(writer if sending else reader)
                children.append(reader if sending else writer)
                os.set_blocking(reader, False)
                os.set_blocking(writer, False)
            input_copy = os.dup(self._input_fd)
            custody.retain_descriptors((input_copy,))
            children.append(input_copy)
            bound_input = None
            input_binding = self._input_binding
            if self._input_descriptor is not None:
                self._require_bound_input(children[6], self._input_descriptor)
                bound_input = replace(self._input_descriptor, fd=children[6])
                input_binding = input_descriptor_digest(bound_input)
            if input_binding is None:
                raise ValueError("mssql_native.sqlclient_launch_input")
            roles = SqlClientDescriptors(children[0], children[1], children[2], children[3], children[4], children[6])
            cache = TemporaryDirectory(prefix="dpone-sqlclient-pycache-")
            custody.retain_cache(cache)
            install = self._installation
            command = [
                str(install.python_executable),
                "-I",
                "-S",
                "-B",
                "-X",
                "pycache_prefix=" + cache.name,
                "-c",
                SOURCE_SHIM,
            ]
            values = {
                "package-root": install.package_root,
                "source-sha256": install.source_sha256,
                "dotnet-host": install.dotnet_host,
                "runtime-root": install.runtime_root,
                "companion-root": install.companion_root,
                "worker-assembly": install.worker_assembly,
                "build-manifest": install.build_manifest,
                "build-sha256": install.build_sha256,
                "parent": os.getpid(),
                "address-space": self._limit,
                "startup-deadline-ns": startup_ns,
                "operation-deadline-ns": operation_ns,
                "gate-fd": children[5],
                **dict(
                    zip(
                        ("startup-fd", "credentials-fd", "session-fd", "grant-fd", "result-fd", "input-fd"),
                        (roles.startup, roles.credentials, roles.session, roles.grant, roles.result, roles.input),
                    )
                ),
            }
            for key, value in values.items():
                command.extend(["--" + key, str(value)])
            if time.monotonic_ns() >= deadline_ns:
                raise ValueError("mssql_native.sqlclient_deadline")
            if bound_input is not None:
                self._require_bound_input(children[6], bound_input)
                if time.monotonic_ns() >= deadline_ns:
                    raise ValueError("mssql_native.sqlclient_deadline")
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                cwd="/",
                pass_fds=tuple(children),
                close_fds=True,
                env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
            )
            custody.retain_process(process)
            try:
                handle = LinuxTdsProcess.acquire(LinuxTdsProcess.identify(process.pid))
            except BaseException as error:
                custody.retain_acquisition(error)
                raise
            custody.retain_handle(handle)
            custody.close_launch_descriptors(tuple(children), _close_fds)
            launch = SqlClientLaunch(
                1,
                str(uuid.uuid4()),
                self._attempt,
                install.build_sha256,
                input_binding,
                handle.identity,
                os.getpid(),
                startup_ns,
                operation_ns,
                self._limit,
                roles,
            )
            write_worker_message(
                parents[5],
                encode_launch(launch),
                deadline=deadline_ns / 10**9,
                max_payload=16384,
                writer=write_worker_control,
            )
            custody.close_descriptor(parents[-1])
            parents.pop()
            return SqlClientChildProcess(custody, handle, tuple(parents), launch=launch, bound_input=bound_input)
        except BaseException:
            if custody.process is not None:
                raise TdsLaunchUnknown(UnresolvedPythonTdsLaunch.from_custody(custody)) from None
            try:
                custody.close(descriptors_first=True)
            except BaseException:
                raise TdsLaunchUnknown(UnresolvedPythonTdsLaunch.from_custody(custody)) from None
            raise

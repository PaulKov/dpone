"""Fixed Python worker launch and exclusive private-pipe lifecycle.

No credentials are inherited or accepted at launch. The application supervisor
must acknowledge durable registration before invoking send. Source installations
must remain immutable; checks detect ordinary deployment drift, not hostile
filesystem replacement between reads. SQL settlement remains a separate port.
"""

from __future__ import annotations

import hashlib
import math
import os
import subprocess
import time
from dataclasses import asdict
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
from dpone.adapters.mssql_tds_installation import worker_installation_digest
from dpone.adapters.mssql_tds_launch_shim import SOURCE_SHIM
from dpone.adapters.mssql_tds_process import LinuxTdsProcess
from dpone.adapters.mssql_tds_python_admission import AdmittedPythonInputs
from dpone.contracts.mssql_tds_api import (
    NativeBulkTransportPolicy,
    TdsInputReceipt,
    canonical_json_bytes,
    encode_message,
    strict_json_object,
)
from dpone.contracts.mssql_tds_result import TdsWorkerResult, attempt_identity_digest, decode_result_payload
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsChildExit, TdsProcessIdentity
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown


class PythonTdsWorkerLauncher:
    """Construct at the application root with admitted immutable installation."""

    def __init__(
        self,
        *,
        policy: NativeBulkTransportPolicy,
        identity: TdsAttemptIdentity,
        python_executable: Path,
        package_root: Path,
        dependency_paths: tuple[Path, ...] = (),
    ) -> None:
        if policy.backend != "mssql_python":
            raise ValueError("mssql_native.tds_backend_mismatch")
        self.policy, self.identity = policy, identity
        # Resolving a venv symlink would launch its base interpreter instead.
        inputs = AdmittedPythonInputs(python_executable, package_root, dependency_paths)
        self.python, self.package_root, self.dependency_paths = (
            inputs.python,
            inputs.package_root,
            inputs.dependency_paths,
        )
        if hashlib.sha256(canonical_json_bytes(policy.to_dict())).hexdigest() != identity.policy_sha256:
            raise ValueError("mssql_native.tds_policy_binding_invalid")

    def assert_installation(self) -> None:
        AdmittedPythonInputs(self.python, self.package_root, self.dependency_paths)
        if worker_installation_digest(self.package_root) != self.identity.implementation_sha256:
            raise ValueError("mssql_native.tds_implementation_changed")

    def spawn(self, *, startup_deadline: float, operation_deadline: float) -> PythonTdsWorker:
        if any(
            type(value) not in (int, float) or not math.isfinite(value)
            for value in (startup_deadline, operation_deadline)
        ):
            raise ValueError("mssql_native.tds_launch_deadline")
        LinuxTdsProcess.admit()
        self.assert_installation()
        if time.monotonic() >= min(startup_deadline, operation_deadline):
            raise ValueError("mssql_native.tds_launch_deadline")
        custody = TdsChildProcess.launch()
        cr, cw = os.pipe()
        custody.retain_descriptors((cr, cw))
        try:
            sr, sw = os.pipe()
            custody.retain_descriptors((sr, sw))
        except BaseException:
            custody.close_launch_descriptors((cr, cw), _close_fds)
            raise
        process = None
        handle = None
        child_fds: tuple[int, ...] = (cr, sw)
        cache = None
        try:
            for fd in (cr, cw, sr, sw):
                os.set_blocking(fd, False)
            cache = TemporaryDirectory(prefix="dpone-tds-pycache-")
            custody.retain_cache(cache)
            command = [
                str(self.python),
                "-I",
                "-S",
                "-B",
                "-X",
                "pycache_prefix=" + cache.name,
                "-c",
                SOURCE_SHIM,
                "--package-root",
                str(self.package_root),
            ]
            for path in self.dependency_paths:
                command.extend(["--dependency-path", str(path)])
            options = {
                "parent": os.getpid(),
                "address-space": self.policy.max_worker_address_space_bytes,
                "startup-deadline": startup_deadline,
                "operation-deadline": operation_deadline,
                "control-fd": cr,
                "startup-fd": sw,
            }
            for name, value in options.items():
                command.extend(["--" + name, str(value)])
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                pass_fds=(cr, sw),
                close_fds=True,
                env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "PYTHONPATH": str(self.package_root)},
            )
            custody.retain_process(process)
            identity = LinuxTdsProcess.identify(process.pid)
            handle = LinuxTdsProcess.acquire(identity)
            custody.retain_handle(handle)
            assert process.stdout is not None
            os.set_blocking(process.stdout.fileno(), False)
            detached, child_fds = child_fds, ()
            custody.close_launch_descriptors(detached, _close_fds)
            return PythonTdsWorker(self, custody, handle, cw, sr, cache)
        except BaseException as error:
            custody.retain_acquisition(error)
            detached, child_fds = child_fds, ()
            if process is not None:
                try:
                    custody.close_launch_descriptors(detached, _close_fds)
                finally:
                    raise TdsLaunchUnknown(UnresolvedPythonTdsLaunch.from_custody(custody)) from None
            _close_all((partial(custody.close_launch_descriptors, custody.descriptors, _close_fds), custody.close))
            raise


class PythonTdsWorker:
    """One supervisor thread owns the pidfd, pipes and exclusive child reaping."""

    def __init__(self, launcher, process, handle, control_fd: int, startup_fd: int, cache=None) -> None:
        self._launcher = launcher
        self._resources = _existing_custody(process, handle, (control_fd, startup_fd)) or TdsChildProcess(
            process, handle, (control_fd, startup_fd), cache
        )
        self._control, self._startup = control_fd, startup_fd
        self._started = self._startup_attempted = self._sent = self._received = False

    @property
    def identity(self) -> TdsProcessIdentity:
        return self._resources.identity

    def _check(self) -> None:
        self._resources.check_owner()

    def startup(self, *, deadline: float) -> None:
        self._resources.check_cleanup_owner()
        if self._startup_attempted:
            self._resources.poison()
            raise ValueError("mssql_native.tds_startup_already_observed")
        self._check()
        self._startup_attempted = True
        body = read_worker_message(self._startup, deadline=deadline, max_payload=16384)
        value = strict_json_object(body)
        if (
            set(value) != {"schema_version", "process", "implementation_sha256", "package_root"}
            or type(value["schema_version"]) is not int
            or value["schema_version"] != 1
        ):
            raise ValueError("mssql_native.tds_startup_invalid")
        if type(value["process"]) is not dict or set(value["process"]) != set(asdict(self.identity)):
            raise ValueError("mssql_native.tds_startup_invalid")
        if TdsProcessIdentity(**value["process"]) != self.identity:
            raise ValueError("mssql_native.tds_startup_identity_mismatch")
        if value["implementation_sha256"] != self._launcher.identity.implementation_sha256 or value[
            "package_root"
        ] != str(self._launcher.package_root):
            raise ValueError("mssql_native.tds_startup_implementation_mismatch")
        if time.monotonic() >= deadline:
            raise ValueError("mssql_native.tds_startup_deadline")
        self._check()
        self._started = True

    def send(self, body: bytes, *, deadline: float) -> None:
        self._resources.check_cleanup_owner()
        if not self._started or self._sent:
            if self._startup_attempted or self._sent:
                self._resources.poison()
            raise ValueError("mssql_native.tds_release_invalid")
        self._check()
        self._sent = True  # Never retry even a partial or rejected release.
        framed = encode_message(body, max_payload=1024**2)
        value = strict_json_object(body)
        if (
            value.get("identity") != asdict(self._launcher.identity)
            or value.get("policy") != self._launcher.policy.to_dict()
        ):
            raise ValueError("mssql_native.tds_release_binding_invalid")
        write_worker_control(self._control, framed, deadline=deadline, max_bytes=1024**2 + 4)
        control, self._control = self._control, -1
        self._resources.close_descriptor(control)

    def receive(
        self, *, expected_attempt_sha256: str, expected_input: TdsInputReceipt, deadline: float
    ) -> TdsWorkerResult:
        self._check()
        if (
            not self._sent
            or self._received
            or expected_attempt_sha256 != attempt_identity_digest(self._launcher.identity)
        ):
            self._resources.poison()
            raise ValueError("mssql_native.tds_receive_invalid")
        self._received = True
        output = self._resources.process.stdout
        assert output is not None
        body = read_worker_message(output.fileno(), deadline=deadline, max_payload=16 * 1024)
        result = decode_result_payload(
            body, expected_attempt_sha256=expected_attempt_sha256, expected_input=expected_input
        )
        if time.monotonic() >= deadline:
            raise ValueError("mssql_native.tds_result_deadline")
        return result

    def wait(self, *, deadline: float) -> TdsChildExit:
        return self._resources.wait(deadline=deadline)

    def terminate(self, *, deadline: float) -> TdsChildExit:
        return self._resources.terminate(deadline=deadline)

    def close(self) -> None:
        try:
            self._resources.close()
        finally:
            if not self._resources.descriptors:
                self._control = self._startup = -1

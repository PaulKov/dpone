"""Retained Python launch capability when authenticated custody is unresolved."""

from __future__ import annotations

import subprocess
from tempfile import TemporaryDirectory

from dpone.adapters.mssql_tds_process import LinuxTdsProcess
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown


class UnresolvedPythonTdsLaunch:
    """Retain launch resources; absent authenticated pidfd cannot be repaired."""

    def __init__(
        self,
        process: subprocess.Popen[bytes],
        descriptors: tuple[int, ...],
        handle: LinuxTdsProcess | None,
        cache: TemporaryDirectory[str] | None = None,
    ) -> None:
        from dpone.adapters.mssql_tds_child_process import TdsChildProcess, _existing_custody

        self._resources = _existing_custody(process, handle, descriptors) or TdsChildProcess(
            process, handle, descriptors, cache
        )

    @classmethod
    def from_custody(cls, custody):
        """Retain an already adopted launch without constructing another owner."""
        retained = cls.__new__(cls)
        retained._resources = custody
        return retained

    @property
    def process(self) -> subprocess.Popen[bytes]:
        return self._resources.process

    @property
    def descriptors(self) -> tuple[int, ...]:
        return self._resources.descriptors

    @property
    def handle(self) -> LinuxTdsProcess | None:
        return self._resources.handle

    def _check(self) -> None:
        try:
            self._resources.check_cleanup_owner()
        except ValueError:
            raise TdsLaunchUnknown(self) from None

    def contain(self, *, deadline: float) -> None:
        self._check()
        if self._resources.exit is not None:
            return
        try:
            self._resources.terminate(deadline=deadline)
        except BaseException:
            raise TdsLaunchUnknown(self) from None

    def close(self) -> None:
        self._check()
        if self._resources.exit is None:
            raise TdsLaunchUnknown(self)
        self._resources.close(descriptors_first=True)

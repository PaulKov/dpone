from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Protocol

from dpone.gitops.airflow_admission_check_models import (
    GitOpsAirflowAdmissionCommand,
    GitOpsAirflowAdmissionResult,
)


class AirflowAdmissionCheckRunner(Protocol):
    def run(self, *, command: GitOpsAirflowAdmissionCommand, cwd: Path) -> GitOpsAirflowAdmissionResult: ...


class StaticAirflowAdmissionCheckRunner:
    """Deterministic runner for credential-free tests."""

    def __init__(
        self,
        *,
        exit_codes: dict[str, int],
        stdout: dict[str, str] | None = None,
        stderr: dict[str, str] | None = None,
    ) -> None:
        self._exit_codes = dict(exit_codes)
        self._stdout = dict(stdout or {})
        self._stderr = dict(stderr or {})

    def run(self, *, command: GitOpsAirflowAdmissionCommand, cwd: Path) -> GitOpsAirflowAdmissionResult:
        _ = cwd
        return GitOpsAirflowAdmissionResult(
            name=command.name,
            command=command.command,
            exit_code=self._exit_codes.get(command.name, 0),
            stdout=self._stdout.get(command.name, ""),
            stderr=self._stderr.get(command.name, ""),
        )


class SubprocessAirflowAdmissionCheckRunner:
    """Run kubectl admission checks in opt-in live mode."""

    def run(self, *, command: GitOpsAirflowAdmissionCommand, cwd: Path) -> GitOpsAirflowAdmissionResult:
        started = time.monotonic()
        try:
            completed = subprocess.run(
                command.command,
                cwd=cwd,
                shell=True,
                check=False,
                text=True,
                capture_output=True,
                timeout=command.timeout_seconds,
            )
            return GitOpsAirflowAdmissionResult(
                name=command.name,
                command=command.command,
                exit_code=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
                duration_seconds=round(time.monotonic() - started, 3),
            )
        except subprocess.TimeoutExpired as exc:
            return GitOpsAirflowAdmissionResult(
                name=command.name,
                command=command.command,
                exit_code=124,
                stdout=exc.stdout or "",
                stderr=exc.stderr or "command timed out",
                duration_seconds=round(time.monotonic() - started, 3),
            )


__all__ = [
    "AirflowAdmissionCheckRunner",
    "StaticAirflowAdmissionCheckRunner",
    "SubprocessAirflowAdmissionCheckRunner",
]

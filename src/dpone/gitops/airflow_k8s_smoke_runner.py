from __future__ import annotations

import subprocess
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from dpone.gitops.airflow_k8s_smoke_models import GitOpsAirflowK8sCommandResult, GitOpsAirflowK8sSmokeCommand


class AirflowK8sSmokeRunner(Protocol):
    def run(self, *, command: GitOpsAirflowK8sSmokeCommand, cwd: Path) -> GitOpsAirflowK8sCommandResult: ...


@dataclass(frozen=True, slots=True)
class StaticAirflowK8sSmokeRunner:
    exit_codes: Mapping[str, int]

    def run(self, *, command: GitOpsAirflowK8sSmokeCommand, cwd: Path) -> GitOpsAirflowK8sCommandResult:
        _ = cwd
        return GitOpsAirflowK8sCommandResult(
            name=command.name,
            command=command.command,
            exit_code=int(self.exit_codes.get(command.name, 0)),
        )


class SubprocessAirflowK8sSmokeRunner:
    def run(self, *, command: GitOpsAirflowK8sSmokeCommand, cwd: Path) -> GitOpsAirflowK8sCommandResult:
        started = time.monotonic()
        try:
            completed = subprocess.run(
                command.command,
                cwd=cwd,
                shell=True,
                check=False,
                capture_output=True,
                text=True,
                timeout=command.timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            return GitOpsAirflowK8sCommandResult(
                name=command.name,
                command=command.command,
                exit_code=124,
                stdout=_decode_timeout_output(exc.stdout),
                stderr=_decode_timeout_output(exc.stderr) or f"Command timed out after {command.timeout_seconds}s",
                duration_seconds=round(time.monotonic() - started, 6),
            )
        return GitOpsAirflowK8sCommandResult(
            name=command.name,
            command=command.command,
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            duration_seconds=round(time.monotonic() - started, 6),
        )


def _decode_timeout_output(value: bytes | str | None) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value or ""


__all__ = [
    "AirflowK8sSmokeRunner",
    "StaticAirflowK8sSmokeRunner",
    "SubprocessAirflowK8sSmokeRunner",
]

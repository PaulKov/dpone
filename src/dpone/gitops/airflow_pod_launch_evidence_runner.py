from __future__ import annotations

import subprocess
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from dpone.gitops.airflow_pod_launch_evidence_models import (
    GitOpsAirflowPodLaunchEvidenceCommand,
    GitOpsAirflowPodLaunchEvidenceCommandResult,
)


class AirflowPodLaunchEvidenceRunner(Protocol):
    def run(
        self,
        *,
        command: GitOpsAirflowPodLaunchEvidenceCommand,
        cwd: Path,
    ) -> GitOpsAirflowPodLaunchEvidenceCommandResult: ...


@dataclass(frozen=True, slots=True)
class StaticAirflowPodLaunchEvidenceRunner:
    exit_codes: Mapping[str, int] = field(default_factory=dict)
    stdout_by_name: Mapping[str, str] = field(default_factory=dict)
    stderr_by_name: Mapping[str, str] = field(default_factory=dict)

    def run(
        self,
        *,
        command: GitOpsAirflowPodLaunchEvidenceCommand,
        cwd: Path,
    ) -> GitOpsAirflowPodLaunchEvidenceCommandResult:
        _ = cwd
        return GitOpsAirflowPodLaunchEvidenceCommandResult(
            name=command.name,
            command=command.command,
            exit_code=int(self.exit_codes.get(command.name, 0)),
            stdout=self.stdout_by_name.get(command.name, ""),
            stderr=self.stderr_by_name.get(command.name, ""),
        )


class SubprocessAirflowPodLaunchEvidenceRunner:
    def run(
        self,
        *,
        command: GitOpsAirflowPodLaunchEvidenceCommand,
        cwd: Path,
    ) -> GitOpsAirflowPodLaunchEvidenceCommandResult:
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
            return GitOpsAirflowPodLaunchEvidenceCommandResult(
                name=command.name,
                command=command.command,
                exit_code=124,
                stdout=_decode_timeout_output(exc.stdout),
                stderr=_decode_timeout_output(exc.stderr) or f"Command timed out after {command.timeout_seconds}s",
                duration_seconds=round(time.monotonic() - started, 6),
            )
        return GitOpsAirflowPodLaunchEvidenceCommandResult(
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
    "AirflowPodLaunchEvidenceRunner",
    "StaticAirflowPodLaunchEvidenceRunner",
    "SubprocessAirflowPodLaunchEvidenceRunner",
]

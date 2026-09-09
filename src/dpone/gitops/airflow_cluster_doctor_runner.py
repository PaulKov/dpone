from __future__ import annotations

import subprocess
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from dpone.gitops.airflow_cluster_doctor_models import (
    GitOpsAirflowClusterDoctorCommand,
    GitOpsAirflowClusterDoctorResult,
)


class AirflowClusterDoctorRunner(Protocol):
    def run(self, *, command: GitOpsAirflowClusterDoctorCommand, cwd: Path) -> GitOpsAirflowClusterDoctorResult: ...


@dataclass(frozen=True, slots=True)
class StaticAirflowClusterDoctorRunner:
    exit_codes: Mapping[str, int]
    stdout: Mapping[str, str] | None = None
    stderr: Mapping[str, str] | None = None

    def run(self, *, command: GitOpsAirflowClusterDoctorCommand, cwd: Path) -> GitOpsAirflowClusterDoctorResult:
        _ = cwd
        return GitOpsAirflowClusterDoctorResult(
            name=command.name,
            command=command.command,
            exit_code=int(self.exit_codes.get(command.name, 0)),
            stdout=(self.stdout or {}).get(command.name, _default_stdout(command)),
            stderr=(self.stderr or {}).get(command.name, ""),
        )


class SubprocessAirflowClusterDoctorRunner:
    def run(self, *, command: GitOpsAirflowClusterDoctorCommand, cwd: Path) -> GitOpsAirflowClusterDoctorResult:
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
            return GitOpsAirflowClusterDoctorResult(
                name=command.name,
                command=command.command,
                exit_code=124,
                stdout=_decode_timeout_output(exc.stdout),
                stderr=_decode_timeout_output(exc.stderr) or f"Command timed out after {command.timeout_seconds}s",
                duration_seconds=round(time.monotonic() - started, 6),
            )
        return GitOpsAirflowClusterDoctorResult(
            name=command.name,
            command=command.command,
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            duration_seconds=round(time.monotonic() - started, 6),
        )


def _default_stdout(command: GitOpsAirflowClusterDoctorCommand) -> str:
    if command.kind == "kubernetes_secret_keys":
        return "".join(f"{key}\n" for key in command.expected_keys)
    if command.kind == "kubernetes_external_secret":
        return '{"status":{"conditions":[{"type":"Ready","status":"True"}]}}\n'
    return ""


def _decode_timeout_output(value: bytes | str | None) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value or ""


__all__ = [
    "AirflowClusterDoctorRunner",
    "StaticAirflowClusterDoctorRunner",
    "SubprocessAirflowClusterDoctorRunner",
]

from __future__ import annotations

import subprocess
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from dpone.gitops.airflow_runtime_models import GitOpsAirflowRuntimeEvidence, GitOpsAirflowRuntimeStep


@dataclass(frozen=True, slots=True)
class AirflowCommandResult:
    exit_code: int
    stdout: str = ""
    stderr: str = ""


class AirflowCommandRunner(Protocol):
    def run(self, *, command: str, cwd: Path) -> AirflowCommandResult: ...


class SubprocessAirflowCommandRunner:
    """Run one run-spec command inside the custom dpone image."""

    def run(self, *, command: str, cwd: Path) -> AirflowCommandResult:
        completed = subprocess.run(
            command,
            cwd=cwd,
            shell=True,
            check=False,
            capture_output=True,
            text=True,
        )
        return AirflowCommandResult(
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )


@dataclass(frozen=True, slots=True)
class StaticAirflowCommandRunner:
    """Deterministic runner for unit tests and docs examples."""

    exit_codes: Mapping[str, int]

    def run(self, *, command: str, cwd: Path) -> AirflowCommandResult:
        _ = cwd
        return AirflowCommandResult(exit_code=int(self.exit_codes.get(command, 0)))


class GitOpsAirflowRunSpecExecutor:
    """Execute a run-spec sequentially and return runtime evidence."""

    def __init__(self, *, runner: AirflowCommandRunner | None = None) -> None:
        self._runner = runner or SubprocessAirflowCommandRunner()

    def execute(
        self,
        *,
        run_spec_path: str,
        run_spec: Mapping[str, Any],
        cwd: Path,
    ) -> GitOpsAirflowRuntimeEvidence:
        started_at = _now()
        started_monotonic = time.monotonic()
        steps: list[GitOpsAirflowRuntimeStep] = []
        for raw_step in _raw_steps(run_spec):
            step = self._execute_step(raw_step=raw_step, cwd=cwd)
            steps.append(step)
            if step.required and not step.passed:
                break
        finished_at = _now()
        status = "failed" if any(step.required and not step.passed for step in steps) else "passed"
        return GitOpsAirflowRuntimeEvidence(
            run_spec_path=run_spec_path,
            bundle_path=str(run_spec.get("bundle_path") or ""),
            image=str(run_spec.get("image") or ""),
            image_digest=_optional_str(run_spec.get("image_digest")),
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=round(time.monotonic() - started_monotonic, 6),
            steps=tuple(steps),
        )

    def _execute_step(self, *, raw_step: Mapping[str, Any], cwd: Path) -> GitOpsAirflowRuntimeStep:
        command = str(raw_step.get("command") or "").strip()
        started_at = _now()
        started_monotonic = time.monotonic()
        result = self._runner.run(command=command, cwd=cwd)
        finished_at = _now()
        status = "passed" if result.exit_code == 0 else "failed"
        return GitOpsAirflowRuntimeStep(
            name=str(raw_step.get("name") or ""),
            kind=str(raw_step.get("kind") or ""),
            command=command,
            required=bool(raw_step.get("required", True)),
            status=status,
            exit_code=result.exit_code,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=round(time.monotonic() - started_monotonic, 6),
            stdout=result.stdout,
            stderr=result.stderr,
            manifest=_optional_str(raw_step.get("manifest")),
        )


def _raw_steps(run_spec: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw_steps = run_spec.get("steps")
    if not isinstance(raw_steps, list):
        return ()
    return tuple(step for step in raw_steps if isinstance(step, Mapping))


def _now() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _optional_str(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


__all__ = [
    "AirflowCommandResult",
    "AirflowCommandRunner",
    "GitOpsAirflowRunSpecExecutor",
    "StaticAirflowCommandRunner",
    "SubprocessAirflowCommandRunner",
]

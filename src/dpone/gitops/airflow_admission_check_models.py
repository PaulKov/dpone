from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from dpone.gitops.models import GitOpsIssue

AIRFLOW_ADMISSION_CHECK_SOURCE = "dpone gitops airflow admission-check"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowAdmissionCommand:
    name: str
    kind: str
    command: str
    path: str
    required: bool
    timeout_seconds: int
    executed: bool = False

    def with_executed(self) -> GitOpsAirflowAdmissionCommand:
        return GitOpsAirflowAdmissionCommand(
            name=self.name,
            kind=self.kind,
            command=self.command,
            path=self.path,
            required=self.required,
            timeout_seconds=self.timeout_seconds,
            executed=True,
        )

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "command": self.command,
            "path": self.path,
            "required": self.required,
            "timeout_seconds": self.timeout_seconds,
            "executed": self.executed,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowAdmissionResult:
    name: str
    command: str
    exit_code: int
    stdout: str = ""
    stderr: str = ""
    duration_seconds: float = 0.0

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "command": self.command,
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "duration_seconds": self.duration_seconds,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowAdmissionReport:
    mode: str
    runner_policy: str
    artifact_dir: str
    manifest_path: str
    pod_spec_path: str
    timeout_seconds: int
    commands: tuple[GitOpsAirflowAdmissionCommand, ...]
    results: tuple[GitOpsAirflowAdmissionResult, ...] = ()
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_admission_check"
    schema_version: str = "1"
    producer: str = AIRFLOW_ADMISSION_CHECK_SOURCE

    @property
    def passed(self) -> bool:
        return not self.blockers

    def with_results(
        self,
        *,
        commands: tuple[GitOpsAirflowAdmissionCommand, ...],
        results: tuple[GitOpsAirflowAdmissionResult, ...],
        blockers: tuple[GitOpsIssue, ...],
        warnings: tuple[GitOpsIssue, ...] = (),
    ) -> GitOpsAirflowAdmissionReport:
        return GitOpsAirflowAdmissionReport(
            mode=self.mode,
            runner_policy=self.runner_policy,
            artifact_dir=self.artifact_dir,
            manifest_path=self.manifest_path,
            pod_spec_path=self.pod_spec_path,
            timeout_seconds=self.timeout_seconds,
            commands=commands,
            results=results,
            warnings=(*self.warnings, *warnings),
            blockers=(*self.blockers, *blockers),
        )

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "mode": self.mode,
            "runner_policy": self.runner_policy,
            "artifact_dir": self.artifact_dir,
            "manifest_path": self.manifest_path,
            "pod_spec_path": self.pod_spec_path,
            "timeout_seconds": self.timeout_seconds,
            "commands": [command.to_jsonable() for command in self.commands],
            "results": [result.to_jsonable() for result in self.results],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


def airflow_admission_check_issue(code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=AIRFLOW_ADMISSION_CHECK_SOURCE)


__all__ = [
    "AIRFLOW_ADMISSION_CHECK_SOURCE",
    "GitOpsAirflowAdmissionCommand",
    "GitOpsAirflowAdmissionReport",
    "GitOpsAirflowAdmissionResult",
    "airflow_admission_check_issue",
]

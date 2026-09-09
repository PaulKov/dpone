from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from dpone.gitops.airflow_pod_launch_evidence_checks import (
    AIRFLOW_POD_LAUNCH_EVIDENCE_SOURCE,
    GitOpsAirflowPodLaunchEvidenceCheck,
)
from dpone.gitops.models import GitOpsIssue


@dataclass(frozen=True, slots=True)
class GitOpsAirflowPodLaunchEvidenceCommand:
    name: str
    kind: str
    command: str
    required: bool
    timeout_seconds: int
    executed: bool = False

    def with_executed(self) -> GitOpsAirflowPodLaunchEvidenceCommand:
        return GitOpsAirflowPodLaunchEvidenceCommand(
            name=self.name,
            kind=self.kind,
            command=self.command,
            required=self.required,
            timeout_seconds=self.timeout_seconds,
            executed=True,
        )

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "command": self.command,
            "required": self.required,
            "timeout_seconds": self.timeout_seconds,
            "executed": self.executed,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowPodLaunchEvidenceCommandResult:
    name: str
    command: str
    exit_code: int
    stdout: str = ""
    stderr: str = ""
    duration_seconds: float = 0.0

    @property
    def passed(self) -> bool:
        return self.exit_code == 0

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
class GitOpsAirflowPodLaunchEvidenceReport:
    mode: str
    runner_policy: str
    runtime_profile_path: str
    pod_contract_path: str
    image_contract_path: str | None
    runtime_evidence_path: str | None
    xcom_summary_path: str | None
    pod_name: str
    namespace: str
    service_account: str
    image: str
    image_digest: str | None
    expected_phase: str
    timeout_seconds: int
    log_tail_lines: int
    checks: tuple[GitOpsAirflowPodLaunchEvidenceCheck, ...]
    commands: tuple[GitOpsAirflowPodLaunchEvidenceCommand, ...]
    results: tuple[GitOpsAirflowPodLaunchEvidenceCommandResult, ...] = ()
    observed_pod: Any | None = None
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_pod_launch_evidence"
    schema_version: str = "1"
    producer: str = AIRFLOW_POD_LAUNCH_EVIDENCE_SOURCE

    @property
    def passed(self) -> bool:
        checks_passed = all(check.passed or check.severity == "warning" for check in self.checks)
        return checks_passed and not self.blockers

    def with_results(
        self,
        *,
        commands: tuple[GitOpsAirflowPodLaunchEvidenceCommand, ...],
        results: tuple[GitOpsAirflowPodLaunchEvidenceCommandResult, ...],
        observed_pod: Any | None,
        checks: tuple[GitOpsAirflowPodLaunchEvidenceCheck, ...],
        warnings: tuple[GitOpsIssue, ...],
        blockers: tuple[GitOpsIssue, ...],
    ) -> GitOpsAirflowPodLaunchEvidenceReport:
        return GitOpsAirflowPodLaunchEvidenceReport(
            mode=self.mode,
            runner_policy=self.runner_policy,
            runtime_profile_path=self.runtime_profile_path,
            pod_contract_path=self.pod_contract_path,
            image_contract_path=self.image_contract_path,
            runtime_evidence_path=self.runtime_evidence_path,
            xcom_summary_path=self.xcom_summary_path,
            pod_name=self.pod_name,
            namespace=self.namespace,
            service_account=self.service_account,
            image=self.image,
            image_digest=self.image_digest,
            expected_phase=self.expected_phase,
            timeout_seconds=self.timeout_seconds,
            log_tail_lines=self.log_tail_lines,
            checks=(*self.checks, *checks),
            commands=commands,
            results=results,
            observed_pod=observed_pod,
            warnings=(*self.warnings, *warnings),
            blockers=(*self.blockers, *blockers),
            kind=self.kind,
            schema_version=self.schema_version,
            producer=self.producer,
        )

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "mode": self.mode,
            "runner_policy": self.runner_policy,
            "runtime_profile_path": self.runtime_profile_path,
            "pod_contract_path": self.pod_contract_path,
            "image_contract_path": self.image_contract_path,
            "runtime_evidence_path": self.runtime_evidence_path,
            "xcom_summary_path": self.xcom_summary_path,
            "pod_name": self.pod_name,
            "namespace": self.namespace,
            "service_account": self.service_account,
            "image": self.image,
            "image_digest": self.image_digest,
            "expected_phase": self.expected_phase,
            "timeout_seconds": self.timeout_seconds,
            "log_tail_lines": self.log_tail_lines,
            "checks": [check.to_jsonable() for check in self.checks],
            "commands": [command.to_jsonable() for command in self.commands],
            "results": [result.to_jsonable() for result in self.results],
            "observed_pod": self.observed_pod.to_jsonable() if self.observed_pod else None,
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


__all__ = [
    "AIRFLOW_POD_LAUNCH_EVIDENCE_SOURCE",
    "GitOpsAirflowPodLaunchEvidenceCheck",
    "GitOpsAirflowPodLaunchEvidenceCommand",
    "GitOpsAirflowPodLaunchEvidenceCommandResult",
    "GitOpsAirflowPodLaunchEvidenceReport",
]

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from dpone.gitops.models import GitOpsIssue

AIRFLOW_K8S_SMOKE_SOURCE = "dpone gitops airflow k8s-smoke"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowK8sSmokeCheck:
    name: str
    passed: bool
    severity: str
    message: str
    path: str
    source: str = AIRFLOW_K8S_SMOKE_SOURCE

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "severity": self.severity,
            "message": self.message,
            "path": self.path,
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowK8sSmokeCommand:
    name: str
    kind: str
    command: str
    required: bool
    timeout_seconds: int
    executed: bool = False

    def with_executed(self) -> GitOpsAirflowK8sSmokeCommand:
        return GitOpsAirflowK8sSmokeCommand(
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
class GitOpsAirflowK8sCommandResult:
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
class GitOpsAirflowK8sSmokeReport:
    mode: str
    runner_kind: str
    runner_policy: str
    run_spec_path: str
    runtime_profile_path: str
    pod_contract_path: str
    image_contract_path: str | None
    xcom_summary_path: str | None
    namespace: str
    service_account: str
    image: str
    image_digest: str | None
    image_ref: str
    smoke_name: str
    timeout_seconds: int
    checks: tuple[GitOpsAirflowK8sSmokeCheck, ...]
    commands: tuple[GitOpsAirflowK8sSmokeCommand, ...]
    results: tuple[GitOpsAirflowK8sCommandResult, ...] = ()
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_k8s_smoke"
    schema_version: str = "1"
    producer: str = AIRFLOW_K8S_SMOKE_SOURCE

    @property
    def passed(self) -> bool:
        checks_passed = all(check.passed or check.severity == "warning" for check in self.checks)
        return not self.blockers and checks_passed

    def with_results(
        self,
        *,
        commands: tuple[GitOpsAirflowK8sSmokeCommand, ...],
        results: tuple[GitOpsAirflowK8sCommandResult, ...],
        blockers: tuple[GitOpsIssue, ...],
    ) -> GitOpsAirflowK8sSmokeReport:
        return GitOpsAirflowK8sSmokeReport(
            mode=self.mode,
            runner_kind=self.runner_kind,
            runner_policy=self.runner_policy,
            run_spec_path=self.run_spec_path,
            runtime_profile_path=self.runtime_profile_path,
            pod_contract_path=self.pod_contract_path,
            image_contract_path=self.image_contract_path,
            xcom_summary_path=self.xcom_summary_path,
            namespace=self.namespace,
            service_account=self.service_account,
            image=self.image,
            image_digest=self.image_digest,
            image_ref=self.image_ref,
            smoke_name=self.smoke_name,
            timeout_seconds=self.timeout_seconds,
            checks=self.checks,
            commands=commands,
            results=results,
            warnings=self.warnings,
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
            "runner_kind": self.runner_kind,
            "runner_policy": self.runner_policy,
            "run_spec_path": self.run_spec_path,
            "runtime_profile_path": self.runtime_profile_path,
            "pod_contract_path": self.pod_contract_path,
            "image_contract_path": self.image_contract_path,
            "xcom_summary_path": self.xcom_summary_path,
            "namespace": self.namespace,
            "service_account": self.service_account,
            "image": self.image,
            "image_digest": self.image_digest,
            "image_ref": self.image_ref,
            "smoke_name": self.smoke_name,
            "timeout_seconds": self.timeout_seconds,
            "checks": [check.to_jsonable() for check in self.checks],
            "commands": [command.to_jsonable() for command in self.commands],
            "results": [result.to_jsonable() for result in self.results],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


__all__ = [
    "AIRFLOW_K8S_SMOKE_SOURCE",
    "GitOpsAirflowK8sCommandResult",
    "GitOpsAirflowK8sSmokeCheck",
    "GitOpsAirflowK8sSmokeCommand",
    "GitOpsAirflowK8sSmokeReport",
]

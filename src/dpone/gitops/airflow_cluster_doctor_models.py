from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from dpone.gitops.models import GitOpsIssue

AIRFLOW_CLUSTER_DOCTOR_SOURCE = "dpone gitops airflow cluster-doctor"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowClusterDoctorCheck:
    name: str
    passed: bool
    severity: str
    message: str
    path: str
    source: str = AIRFLOW_CLUSTER_DOCTOR_SOURCE

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
class GitOpsAirflowClusterSecretRef:
    name: str
    kind: str
    source: str
    required: bool
    required_keys: tuple[str, ...] = ()

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "source": self.source,
            "required": self.required,
            "required_keys": list(self.required_keys),
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowClusterExternalSecretRef:
    name: str
    source: str
    required: bool

    def to_jsonable(self) -> dict[str, Any]:
        return {"name": self.name, "source": self.source, "required": self.required}


@dataclass(frozen=True, slots=True)
class GitOpsAirflowClusterDoctorCommand:
    name: str
    kind: str
    command: str
    required: bool
    timeout_seconds: int
    expected_keys: tuple[str, ...] = ()
    executed: bool = False

    def with_executed(self) -> GitOpsAirflowClusterDoctorCommand:
        return GitOpsAirflowClusterDoctorCommand(
            name=self.name,
            kind=self.kind,
            command=self.command,
            required=self.required,
            timeout_seconds=self.timeout_seconds,
            expected_keys=self.expected_keys,
            executed=True,
        )

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "command": self.command,
            "required": self.required,
            "timeout_seconds": self.timeout_seconds,
            "expected_keys": list(self.expected_keys),
            "executed": self.executed,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowClusterDoctorResult:
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
class GitOpsAirflowClusterDoctorReport:
    mode: str
    runner_policy: str
    artifact_dir: str
    runtime_profile_path: str
    pod_contract_path: str
    connection_bridge_plan_path: str | None
    namespace: str
    service_account: str
    timeout_seconds: int
    secret_refs: tuple[GitOpsAirflowClusterSecretRef, ...]
    external_secret_refs: tuple[GitOpsAirflowClusterExternalSecretRef, ...]
    checks: tuple[GitOpsAirflowClusterDoctorCheck, ...]
    commands: tuple[GitOpsAirflowClusterDoctorCommand, ...]
    results: tuple[GitOpsAirflowClusterDoctorResult, ...] = ()
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_cluster_doctor"
    schema_version: str = "1"
    producer: str = AIRFLOW_CLUSTER_DOCTOR_SOURCE

    @property
    def passed(self) -> bool:
        checks_passed = all(check.passed or check.severity == "warning" for check in self.checks)
        return not self.blockers and checks_passed

    def with_results(
        self,
        *,
        commands: tuple[GitOpsAirflowClusterDoctorCommand, ...],
        results: tuple[GitOpsAirflowClusterDoctorResult, ...],
        warnings: tuple[GitOpsIssue, ...],
        blockers: tuple[GitOpsIssue, ...],
    ) -> GitOpsAirflowClusterDoctorReport:
        return GitOpsAirflowClusterDoctorReport(
            mode=self.mode,
            runner_policy=self.runner_policy,
            artifact_dir=self.artifact_dir,
            runtime_profile_path=self.runtime_profile_path,
            pod_contract_path=self.pod_contract_path,
            connection_bridge_plan_path=self.connection_bridge_plan_path,
            namespace=self.namespace,
            service_account=self.service_account,
            timeout_seconds=self.timeout_seconds,
            secret_refs=self.secret_refs,
            external_secret_refs=self.external_secret_refs,
            checks=self.checks,
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
            "runtime_profile_path": self.runtime_profile_path,
            "pod_contract_path": self.pod_contract_path,
            "connection_bridge_plan_path": self.connection_bridge_plan_path,
            "namespace": self.namespace,
            "service_account": self.service_account,
            "timeout_seconds": self.timeout_seconds,
            "secret_refs": [ref.to_jsonable() for ref in self.secret_refs],
            "external_secret_refs": [ref.to_jsonable() for ref in self.external_secret_refs],
            "checks": [check.to_jsonable() for check in self.checks],
            "commands": [command.to_jsonable() for command in self.commands],
            "results": [result.to_jsonable() for result in self.results],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


__all__ = [
    "AIRFLOW_CLUSTER_DOCTOR_SOURCE",
    "GitOpsAirflowClusterDoctorCheck",
    "GitOpsAirflowClusterDoctorCommand",
    "GitOpsAirflowClusterDoctorReport",
    "GitOpsAirflowClusterDoctorResult",
    "GitOpsAirflowClusterExternalSecretRef",
    "GitOpsAirflowClusterSecretRef",
]

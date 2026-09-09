from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from dpone.gitops.models import GitOpsIssue


@dataclass(frozen=True, slots=True)
class GitOpsAirflowArtifact:
    path: str
    kind: str
    required: bool
    exists: bool
    reason: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "kind": self.kind,
            "required": self.required,
            "exists": self.exists,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowCheck:
    name: str
    passed: bool
    severity: str
    message: str
    path: str
    source: str

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
class GitOpsAirflowImageContract:
    image: str
    tools: tuple[str, ...]
    image_digest: str | None = None
    dpone_version: str | None = None
    python_version: str | None = None
    airflow_provider_version: str | None = None
    user: str | None = None
    workdir: str | None = None
    entrypoint: str | None = None
    schema_version: str = "1"
    producer: str = "dpone gitops airflow image-contract"
    kind: str = "gitops.airflow_image_contract.v1"

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "image": self.image,
            "tools": list(self.tools),
        }
        optional = {
            "image_digest": self.image_digest,
            "dpone_version": self.dpone_version,
            "python_version": self.python_version,
            "airflow_provider_version": self.airflow_provider_version,
            "user": self.user,
            "workdir": self.workdir,
            "entrypoint": self.entrypoint,
        }
        payload.update({key: value for key, value in optional.items() if value is not None})
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowRenderReport:
    bundle_path: str
    output_dir: str
    image: str
    artifacts: tuple[GitOpsAirflowArtifact, ...]
    commands: tuple[str, ...]
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_render"

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "bundle_path": self.bundle_path,
            "output_dir": self.output_dir,
            "image": self.image,
            "artifacts": [artifact.to_jsonable() for artifact in self.artifacts],
            "commands": list(self.commands),
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowDoctorReport:
    bundle_path: str
    pod_template: str
    image_contract: str
    image: str | None
    runner_policy: str
    checks: tuple[GitOpsAirflowCheck, ...]
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_doctor"

    @property
    def passed(self) -> bool:
        return not self.blockers and all(check.passed or check.severity == "warning" for check in self.checks)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "bundle_path": self.bundle_path,
            "pod_template": self.pod_template,
            "image_contract": self.image_contract,
            "image": self.image,
            "runner_policy": self.runner_policy,
            "checks": [check.to_jsonable() for check in self.checks],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowImageContractReport:
    output_path: str
    contract: GitOpsAirflowImageContract
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_image_contract"

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "output_path": self.output_path,
            "contract": self.contract.to_jsonable(),
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


__all__ = [
    "GitOpsAirflowArtifact",
    "GitOpsAirflowCheck",
    "GitOpsAirflowDoctorReport",
    "GitOpsAirflowImageContract",
    "GitOpsAirflowImageContractReport",
    "GitOpsAirflowRenderReport",
]

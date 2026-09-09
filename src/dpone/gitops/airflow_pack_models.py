from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from dpone.gitops.models import GitOpsIssue

AIRFLOW_PACK_SOURCE = "dpone gitops airflow pack"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowHookRuntimeCommand:
    """Structured runtime command for one separately materialized hook."""

    hook_id: str
    process_selector: str | None
    argv: tuple[str, ...]

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "schema": "dpone.airflow-pre-hook-command.v1",
            "hook_id": self.hook_id,
            "process_selector": self.process_selector,
            "argv": list(self.argv),
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowPackArtifact:
    name: str
    path: str
    format: str
    expected_kind: str
    actual_kind: str | None
    required: bool
    exists: bool
    passed: bool
    reason: str
    sha256: str | None = None
    bytes: int | None = None

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": self.path,
            "format": self.format,
            "expected_kind": self.expected_kind,
            "actual_kind": self.actual_kind,
            "required": self.required,
            "exists": self.exists,
            "passed": self.passed,
            "reason": self.reason,
            "sha256": self.sha256,
            "bytes": self.bytes,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowPackStep:
    name: str
    phase: str
    command: str
    required: bool
    credential_required: bool
    produces: tuple[str, ...]
    reason: str
    depends_on: tuple[str, ...] = ()
    runtime_command: GitOpsAirflowHookRuntimeCommand | None = None

    def to_jsonable(self) -> dict[str, Any]:
        payload = {
            "name": self.name,
            "phase": self.phase,
            "command": self.command,
            "required": self.required,
            "credential_required": self.credential_required,
            "produces": list(self.produces),
            "reason": self.reason,
            "depends_on": list(self.depends_on),
        }
        if self.runtime_command is not None:
            payload["runtime_command"] = self.runtime_command.to_jsonable()
        return payload


@dataclass(frozen=True, slots=True)
class GitOpsAirflowPackReport:
    artifact_dir: str
    output_path: str
    bundle_path: str
    image: str | None
    image_digest: str | None
    mode: str
    runner_policy: str
    include_live_gates: bool
    artifacts: tuple[GitOpsAirflowPackArtifact, ...]
    steps: tuple[GitOpsAirflowPackStep, ...]
    next_actions: tuple[str, ...]
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_pack"
    schema_version: str = "1"
    producer: str = AIRFLOW_PACK_SOURCE

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "artifact_dir": self.artifact_dir,
            "output_path": self.output_path,
            "bundle_path": self.bundle_path,
            "image": self.image,
            "image_digest": self.image_digest,
            "mode": self.mode,
            "runner_policy": self.runner_policy,
            "include_live_gates": self.include_live_gates,
            "artifacts": [artifact.to_jsonable() for artifact in self.artifacts],
            "steps": [step.to_jsonable() for step in self.steps],
            "next_actions": list(self.next_actions),
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


def airflow_pack_issue(*, code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=AIRFLOW_PACK_SOURCE)


__all__ = [
    "AIRFLOW_PACK_SOURCE",
    "GitOpsAirflowPackArtifact",
    "GitOpsAirflowHookRuntimeCommand",
    "GitOpsAirflowPackReport",
    "GitOpsAirflowPackStep",
    "airflow_pack_issue",
]

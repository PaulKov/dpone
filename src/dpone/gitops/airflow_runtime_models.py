from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.gitops.airflow_interval_env import RUN_INTERVAL_ENV_NAMES
from dpone.gitops.models import GitOpsIssue

AIRFLOW_CONTEXT_ENV = (
    "AIRFLOW_CTX_DAG_ID",
    "AIRFLOW_CTX_TASK_ID",
    "AIRFLOW_CTX_RUN_ID",
    "AIRFLOW_CTX_TRY_NUMBER",
    *RUN_INTERVAL_ENV_NAMES,
)


@dataclass(frozen=True, slots=True)
class GitOpsAirflowRunSpecEntry:
    manifest: str
    plan_path: str
    verify_path: str
    passed: bool

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "manifest": self.manifest,
            "plan_path": self.plan_path,
            "verify_path": self.verify_path,
            "passed": self.passed,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowRunSpecStep:
    name: str
    kind: str
    command: str
    required: bool = True
    manifest: str | None = None
    depends_on: tuple[str, ...] = ()

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "kind": self.kind,
            "command": self.command,
            "required": self.required,
        }
        if self.manifest is not None:
            payload["manifest"] = self.manifest
        if self.depends_on:
            payload["depends_on"] = list(self.depends_on)
        return payload


@dataclass(frozen=True, slots=True)
class GitOpsAirflowRunSpec:
    bundle_path: str
    bundle_digest: str | None
    image: str
    image_digest: str | None
    worktree: str
    evidence_output: str
    entries: tuple[GitOpsAirflowRunSpecEntry, ...]
    steps: tuple[GitOpsAirflowRunSpecStep, ...]
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    airflow_context_env: tuple[str, ...] = AIRFLOW_CONTEXT_ENV
    kind: str = "gitops.airflow_run_spec"
    schema_version: str = "1"
    producer: str = "dpone gitops airflow run-spec"

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "bundle_path": self.bundle_path,
            "bundle_digest": self.bundle_digest,
            "image": self.image,
            "image_digest": self.image_digest,
            "worktree": self.worktree,
            "evidence_output": self.evidence_output,
            "airflow_context_env": list(self.airflow_context_env),
            "entries": [entry.to_jsonable() for entry in self.entries],
            "steps": [step.to_jsonable() for step in self.steps],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowRuntimeStep:
    name: str
    kind: str
    command: str
    required: bool
    status: str
    exit_code: int
    started_at: str
    finished_at: str
    duration_seconds: float
    stdout: str = ""
    stderr: str = ""
    manifest: str | None = None

    @property
    def passed(self) -> bool:
        return self.status == "passed" and self.exit_code == 0

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "kind": self.kind,
            "command": self.command,
            "required": self.required,
            "status": self.status,
            "exit_code": self.exit_code,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "stdout": self.stdout,
            "stderr": self.stderr,
        }
        if self.manifest is not None:
            payload["manifest"] = self.manifest
        return payload


@dataclass(frozen=True, slots=True)
class GitOpsAirflowRuntimeEvidence:
    run_spec_path: str
    bundle_path: str
    image: str
    image_digest: str | None
    status: str
    started_at: str
    finished_at: str
    duration_seconds: float
    steps: tuple[GitOpsAirflowRuntimeStep, ...]
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_runtime_evidence"
    schema_version: str = "1"
    producer: str = "dpone gitops airflow run-spec-exec"

    @property
    def passed(self) -> bool:
        required_steps_passed = all(step.passed for step in self.steps if step.required)
        return self.status == "passed" and required_steps_passed and not self.blockers

    def with_issues(
        self,
        *,
        warnings: tuple[GitOpsIssue, ...] | None = None,
        blockers: tuple[GitOpsIssue, ...] | None = None,
        status: str | None = None,
    ) -> GitOpsAirflowRuntimeEvidence:
        return GitOpsAirflowRuntimeEvidence(
            run_spec_path=self.run_spec_path,
            bundle_path=self.bundle_path,
            image=self.image,
            image_digest=self.image_digest,
            status=status or self.status,
            started_at=self.started_at,
            finished_at=self.finished_at,
            duration_seconds=self.duration_seconds,
            steps=self.steps,
            warnings=warnings if warnings is not None else self.warnings,
            blockers=blockers if blockers is not None else self.blockers,
            kind=self.kind,
            schema_version=self.schema_version,
            producer=self.producer,
        )

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "run_spec_path": self.run_spec_path,
            "bundle_path": self.bundle_path,
            "image": self.image,
            "image_digest": self.image_digest,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "steps": [step.to_jsonable() for step in self.steps],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


def read_airflow_runtime_evidence(payload: Mapping[str, Any]) -> GitOpsAirflowRuntimeEvidence:
    return GitOpsAirflowRuntimeEvidence(
        run_spec_path=str(payload.get("run_spec_path") or ""),
        bundle_path=str(payload.get("bundle_path") or ""),
        image=str(payload.get("image") or ""),
        image_digest=_optional_str(payload.get("image_digest")),
        status=str(payload.get("status") or "failed"),
        started_at=str(payload.get("started_at") or ""),
        finished_at=str(payload.get("finished_at") or ""),
        duration_seconds=_float(payload.get("duration_seconds")),
        steps=_read_steps(payload.get("steps")),
        warnings=_read_issues(payload.get("warnings")),
        blockers=_read_issues(payload.get("blockers")),
    )


def _read_steps(raw_steps: object) -> tuple[GitOpsAirflowRuntimeStep, ...]:
    if not isinstance(raw_steps, list):
        return ()
    steps: list[GitOpsAirflowRuntimeStep] = []
    for raw_step in raw_steps:
        if isinstance(raw_step, Mapping):
            steps.append(_read_step(raw_step))
    return tuple(steps)


def _read_step(raw_step: Mapping[str, Any]) -> GitOpsAirflowRuntimeStep:
    return GitOpsAirflowRuntimeStep(
        name=str(raw_step.get("name") or ""),
        kind=str(raw_step.get("kind") or ""),
        command=str(raw_step.get("command") or ""),
        required=bool(raw_step.get("required", True)),
        status=str(raw_step.get("status") or "failed"),
        exit_code=_int(raw_step.get("exit_code")),
        started_at=str(raw_step.get("started_at") or ""),
        finished_at=str(raw_step.get("finished_at") or ""),
        duration_seconds=_float(raw_step.get("duration_seconds")),
        stdout=str(raw_step.get("stdout") or ""),
        stderr=str(raw_step.get("stderr") or ""),
        manifest=_optional_str(raw_step.get("manifest")),
    )


def _read_issues(raw_issues: object) -> tuple[GitOpsIssue, ...]:
    if not isinstance(raw_issues, list):
        return ()
    issues: list[GitOpsIssue] = []
    for raw_issue in raw_issues:
        if isinstance(raw_issue, Mapping):
            issues.append(_read_issue(raw_issue))
    return tuple(issues)


def _read_issue(raw_issue: Mapping[str, Any]) -> GitOpsIssue:
    return GitOpsIssue(
        code=str(raw_issue.get("code") or ""),
        message=str(raw_issue.get("message") or ""),
        path=str(raw_issue.get("path") or ""),
        source=str(raw_issue.get("source") or "dpone gitops airflow evidence"),
    )


def _optional_str(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _float(value: object) -> float:
    if isinstance(value, int | float | str | bytes | bytearray):
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
    return 0.0


def _int(value: object) -> int:
    if isinstance(value, int | float | str | bytes | bytearray):
        try:
            return int(value)
        except (TypeError, ValueError):
            return 1
    return 1


__all__ = [
    "AIRFLOW_CONTEXT_ENV",
    "GitOpsAirflowRunSpec",
    "GitOpsAirflowRunSpecEntry",
    "GitOpsAirflowRunSpecStep",
    "GitOpsAirflowRuntimeEvidence",
    "GitOpsAirflowRuntimeStep",
    "read_airflow_runtime_evidence",
]

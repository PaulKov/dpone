"""Validate GitHub Actions workflow security controls."""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import yaml
from yaml.events import AliasEvent, NodeEvent

FULL_SHA = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)
USES_LINE = re.compile(r"^\s*(?:-\s*)?uses:\s*([^#\s]+)", re.MULTILINE)
UNTRUSTED_RUN_EXPRESSION = re.compile(r"\$\{\{\s*(?:inputs|github\.event|matrix|steps\.)\b", re.IGNORECASE)
REQUIRED_CONTROLS = """forbid_pull_request_target require_top_level_permissions require_pinned_external_actions
forbid_pr_workflow_secrets require_audit_artifact_retention require_hidden_ci_artifact_transport
isolate_dbt_execution_from_oidc require_platform_owned_prod_trust_policy""".split()
_SCOPES = frozenset(
    "actions artifact-metadata attestations checks code-quality contents deployments discussions id-token "
    "issues models packages pages pull-requests security-events statuses vulnerability-alerts".split()
)


def _invalid_permission_list(values: list[Any]) -> bool:
    strings = [value for value in values if isinstance(value, str)]
    return len(strings) != len(values) or len(strings) != len(set(strings)) or not set(strings) <= _SCOPES


def load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).resolve().with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))
_workflow_artifacts = load_sibling("dpone_agent_workflow_artifacts", "workflow_artifacts.py")
_workflow_security_dbt = load_sibling("dpone_agent_workflow_security_dbt", "workflow_security_dbt.py")
_workflow_privilege_expressions = load_sibling("dpone_agent_pr3b_expressions", "workflow_privilege_expressions.py")


@dataclass
class WorkflowSecurityValidationResult:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def extend(self, other: WorkflowSecurityValidationResult) -> None:
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)


@dataclass(frozen=True)
class WorkflowSecurityPolicy:
    allowed_write_permissions: dict[str, frozenset[str]]
    required_audit_artifacts: dict[str, dict[str, int]]
    allowed_job_write_permissions: dict[str, dict[str, frozenset[str]]] = field(default_factory=dict)

    @classmethod
    def empty(cls) -> WorkflowSecurityPolicy:
        return cls(allowed_write_permissions={}, required_audit_artifacts={}, allowed_job_write_permissions={})

    @classmethod
    def from_mapping(cls, payload: dict[str, Any], *, label: str) -> WorkflowSecurityPolicy:
        errors = _validate_policy_mapping(payload, label)
        if errors:
            raise ValueError("; ".join(errors))
        raw_allowed = cast(dict[str, dict[str, Any]], payload["allowed_workflow_write_permissions"])
        allowed = {
            workflow_name: frozenset(cast(list[str], entry["permissions"]))
            for workflow_name, entry in raw_allowed.items()
            if "permissions" in entry
        }
        allowed_jobs = {
            workflow_name: {
                job_name: frozenset(permissions)
                for job_name, permissions in cast(dict[str, list[str]], entry["jobs"]).items()
            }
            for workflow_name, entry in raw_allowed.items()
            if "jobs" in entry
        }
        return cls(allowed, _workflow_artifacts.required_audit_artifacts(payload), allowed_jobs)

    def allows_write(self, workflow_name: str, permission: str, *, job_name: str | None = None) -> bool:
        if job_name is None:
            return permission in self.allowed_write_permissions.get(workflow_name, frozenset())
        job_policy = self.allowed_job_write_permissions.get(workflow_name)
        if job_policy is not None:
            return permission in job_policy.get(job_name, frozenset())
        return permission in self.allowed_write_permissions.get(workflow_name, frozenset())


def load_policy(path: Path) -> WorkflowSecurityPolicy:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("workflow security policy must be a YAML mapping")
    return WorkflowSecurityPolicy.from_mapping(data, label=str(path))


def validate_repository(
    root: Path,
    *,
    policy_path: Path | None = None,
    workflows_dir: Path | None = None,
    semantic_service: Callable[[Path], Mapping[str, Any]] | None = None,
) -> WorkflowSecurityValidationResult:
    policy_file = policy_path or root / ".agents/policy/workflow-security.yml"
    workflows = workflows_dir or root / ".github/workflows"
    result = WorkflowSecurityValidationResult()
    try:
        policy = load_policy(policy_file)
    except (OSError, UnicodeError, ValueError, yaml.YAMLError, RecursionError):
        result.errors.append(f"{policy_file}: cannot load workflow security policy")
        policy = WorkflowSecurityPolicy.empty()

    if not workflows.is_dir():
        result.errors.append(f"{workflows}: workflows directory is missing")
    else:
        workflow_files = sorted({*workflows.glob("*.yml"), *workflows.glob("*.yaml")})
        for workflow in workflow_files:
            result.extend(validate_workflow_file(workflow, policy=policy))
    try:
        boundary = load_sibling("dpone_agent_release_privileged_boundary", "release_privileged_boundary.py")
    except Exception:
        boundary = None
    fixed_directory = root / ".github/workflows"
    candidates = sorted(
        fixed_directory / f"{stem}{suffix}" for stem in _PRIVILEGED_WORKFLOW_STEMS for suffix in (".yml", ".yaml")
    )
    for candidate in candidates:
        if not candidate.is_file():
            continue
        if boundary is None:
            result.errors.append(f"{candidate}: privileged-boundary scan UNVERIFIED")
            continue
        try:
            findings = boundary.find_privileged_checkouts(candidate)
        except Exception:
            result.errors.append(f"{candidate}: privileged-boundary scan UNVERIFIED")
        else:
            result.errors.extend(finding.message for finding in findings)
    _project_semantic_report(result, root, semantic_service=semantic_service)
    return result


_PRIVILEGED_WORKFLOW_STEMS = frozenset({"release", "runtime-image"})
_SEMANTIC_INTERNAL_ERROR = "semantic-pr-privilege-internal=PRIVILEGE_INTERNAL_REPORT_INVALID"


def _project_semantic_report(
    result: WorkflowSecurityValidationResult,
    root: Path,
    *,
    semantic_service: Callable[[Path], Mapping[str, Any]] | None = None,
    serialize_finding: Callable[[Mapping[str, Any]], str] | None = None,
) -> None:
    try:
        from tools.agent_policy.workflow_privilege_service import project_umbrella_findings as project_findings

        projected = project_findings(root, semantic_service=semantic_service, serialize_finding=serialize_finding)
    except Exception:
        projected = (_SEMANTIC_INTERNAL_ERROR,)
    result.errors.extend(projected)


def validate_workflow_file(path: Path, *, policy: WorkflowSecurityPolicy) -> WorkflowSecurityValidationResult:
    result = WorkflowSecurityValidationResult()
    try:
        text = path.read_text(encoding="utf-8")
        if any(
            isinstance(event, AliasEvent) or isinstance(event, NodeEvent) and event.anchor is not None
            for event in yaml.parse(text, Loader=yaml.BaseLoader)
        ):
            raise ValueError("workflow YAML aliases are unsupported")
        payload = yaml.safe_load(text)
    except (OSError, UnicodeError, ValueError, yaml.YAMLError, RecursionError):
        return WorkflowSecurityValidationResult(errors=[f"{path}: invalid workflow YAML"])
    if not isinstance(payload, dict):
        return WorkflowSecurityValidationResult(errors=[f"{path}: workflow must be a mapping"])

    top_permissions = payload.get("permissions")
    if not isinstance(top_permissions, dict):
        result.errors.append(f"{path}: top-level permissions must be a mapping")
    else:
        _validate_permissions(path, path.name, top_permissions, policy, "top-level", result)

    try:
        has_secret = _workflow_privilege_expressions.contains_secret_reference(payload)
    except RecursionError:
        return WorkflowSecurityValidationResult(errors=[f"{path}: invalid workflow YAML"])
    if _uses_event(payload, "pull_request_target"):
        result.errors.append(f"{path}: pull_request_target is forbidden for repository workflows")
    if _uses_event(payload, "pull_request") and has_secret:
        result.errors.append(f"{path}: pull_request workflows must not reference repository secrets")

    for match in USES_LINE.finditer(text):
        action_ref = match.group(1).strip("\"'")
        if action_ref.startswith(("./", "docker://")) or "@" not in action_ref:
            continue
        if not FULL_SHA.fullmatch(action_ref.rsplit("@", 1)[1]):
            result.errors.append(f"{path}: {action_ref} must pin external actions to a full commit SHA")

    artifact_requirements = policy.required_audit_artifacts
    result.errors.extend(_workflow_artifacts.validate_workflow_file(path, path.name, payload, artifact_requirements))

    jobs = payload.get("jobs", {})
    privileged_run = _uses_event(payload, "workflow_call") and (has_secret or _has_write_permission(payload))
    if isinstance(jobs, dict):
        for job_name, job in jobs.items():
            if not isinstance(job, dict):
                continue
            job_id = str(job_name)
            scope = f"job {job_id}"
            if isinstance(job.get("permissions"), dict):
                _validate_permissions(path, path.name, job["permissions"], policy, scope, result, job_name=job_id)
            steps = job.get("steps")
            if isinstance(steps, list):
                _validate_fail_closed_runtime_gate(path, job_id, steps, result)
                for index, step in enumerate(steps):
                    if not isinstance(step, dict):
                        continue
                    script = step.get("run")
                    if privileged_run and isinstance(script, str) and UNTRUSTED_RUN_EXPRESSION.search(script):
                        result.errors.append(
                            f"{path}: job {job_id} step {index} interpolates an untrusted "
                            "GitHub expression directly into run; bind it through env"
                        )
    result.errors.extend(_workflow_security_dbt.validate_dbt_workflow(path, payload, text))
    return result


def _validate_fail_closed_runtime_gate(
    path: Path,
    job_name: str,
    steps: list[Any],
    result: WorkflowSecurityValidationResult,
) -> None:
    for index, raw_gate in enumerate(steps):
        if not isinstance(raw_gate, dict):
            continue
        script = raw_gate.get("run")
        if not isinstance(script, str) or "DPONE_ARTIFACT_ATTESTATION_REQUIRED" not in script:
            continue
        if raw_gate.get("continue-on-error") not in (None, False, "false"):
            result.errors.append(f"{path}: job {job_name} attestation gate must not use continue-on-error")
        if raw_gate.get("if") is not None:
            result.errors.append(f"{path}: job {job_name} attestation gate must be unconditional")
        for later in steps[index + 1 :]:
            if not isinstance(later, dict) or not isinstance(later.get("run"), str):
                continue
            later_script = later["run"]
            condition = str(later.get("if") or "")
            publishes = any(
                command in later_script for command in ("dpone airflow publish", "dpone airflow cache-sync")
            )
            bypasses = any(status in condition for status in ("always()", "failure()", "!cancelled()"))
            if publishes and bypasses:
                result.errors.append(
                    f"{path}: job {job_name} publication/CAS step must not bypass the attestation gate"
                )


def _has_write_permission(payload: dict[str, Any]) -> bool:
    scopes = [payload.get("permissions")]
    jobs = payload.get("jobs")
    if isinstance(jobs, dict):
        scopes.extend(job.get("permissions") for job in jobs.values() if isinstance(job, dict))
    return any(isinstance(scope, dict) and any(value == "write" for value in scope.values()) for scope in scopes)


def _validate_policy_mapping(payload: dict[str, Any], label: str) -> list[str]:
    errors: list[str] = []
    if payload.get("schema_version") != 1:
        errors.append(f"{label}: schema_version must be 1")
    for field_name in ("owner", "last_reviewed", "purpose"):
        if not str(payload.get(field_name, "")).strip():
            errors.append(f"{label}: {field_name} must not be empty")
    controls = payload.get("required_controls")
    if not isinstance(controls, dict):
        errors.append(f"{label}: required_controls must be a mapping")
    else:
        for field_name in REQUIRED_CONTROLS:
            if controls.get(field_name) is not True:
                errors.append(f"{label}: required_controls.{field_name} must be true")

    allowed = payload.get("allowed_workflow_write_permissions")
    if not isinstance(allowed, dict):
        errors.append(f"{label}: allowed_workflow_write_permissions must be a mapping")
        return errors
    for workflow_name, entry in allowed.items():
        if not isinstance(workflow_name, str) or "/" in workflow_name or not workflow_name.endswith((".yml", ".yaml")):
            errors.append(f"{label}: workflow exception keys must be workflow filenames")
            continue
        if not isinstance(entry, dict):
            errors.append(f"{label}: {workflow_name} exception must be a mapping")
            continue
        shapes = ({"permissions", "reason"}, {"jobs", "reason"})
        if set(entry) not in shapes:
            errors.append(f"{label}: {workflow_name} must contain exactly permissions or jobs plus reason")
        permissions = entry.get("permissions", [])
        jobs = entry.get("jobs", {})
        if not isinstance(permissions, list):
            errors.append(f"{label}: {workflow_name}.permissions must be a list")
        elif permissions and _invalid_permission_list(permissions):
            errors.append(f"{label}: {workflow_name}.permissions must be unique known scopes")
        if not isinstance(jobs, dict):
            errors.append(f"{label}: {workflow_name}.jobs must be a mapping")
        else:
            for job_name, job_permissions in jobs.items():
                if not isinstance(job_name, str) or not job_name.strip():
                    errors.append(f"{label}: {workflow_name}.jobs keys must be non-empty strings")
                if not isinstance(job_permissions, list) or not job_permissions:
                    errors.append(f"{label}: {workflow_name}.jobs.{job_name} must be a non-empty list")
                elif _invalid_permission_list(job_permissions):
                    errors.append(f"{label}: {workflow_name}.jobs.{job_name} must contain unique known scopes")
        if not permissions and not jobs:
            errors.append(f"{label}: {workflow_name} must allow at least one scoped write permission")
        if len(str(entry.get("reason", "")).strip()) < 20:
            errors.append(f"{label}: {workflow_name}.reason must explain the exception")
    _workflow_artifacts.validate_policy_mapping(payload, label, errors)
    return errors


def _validate_permissions(
    path: Path,
    workflow_name: str,
    permissions: dict[Any, Any],
    policy: WorkflowSecurityPolicy,
    scope: str,
    result: WorkflowSecurityValidationResult,
    *,
    job_name: str | None = None,
) -> None:
    for permission, access in permissions.items():
        if not isinstance(permission, str):
            result.errors.append(f"{path}: {scope} permission keys must be strings")
            continue
        if access not in {"read", "write", "none"}:
            result.errors.append(f"{path}: {scope} permission {permission} must be read, write, or none")
            continue
        if access == "write" and not policy.allows_write(workflow_name, permission, job_name=job_name):
            result.errors.append(f"{path}: {scope} permission {permission}: write is not documented in workflow policy")


def _uses_event(payload: dict[str, Any], event_name: str) -> bool:
    events = payload.get("on", cast(dict[Any, Any], payload).get(True))
    if isinstance(events, str):
        return events == event_name
    return event_name in events if isinstance(events, (list, dict)) else False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "Overrides affect only legacy general lint; semantic inputs and fixed release/runtime "
            "boundaries remain below root. Relative overrides use the process working directory."
        ),
    )
    parser.add_argument("root", nargs="?", default=".", type=Path, help="repository root (default: .)")
    parser.add_argument("--policy", type=Path, help="legacy general-linter policy override")
    parser.add_argument("--workflows-dir", type=Path, help="legacy general-linter workflow-directory override")
    parser.add_argument("--format", choices=("text", "json"), default="text", help="output format (default: text)")
    args = parser.parse_args(argv)

    result = validate_repository(args.root, policy_path=args.policy, workflows_dir=args.workflows_dir)
    status = "failed" if result.errors else "passed"
    payload: dict[str, Any] = {"status": status, "errors": result.errors, "warnings": result.warnings}
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for prefix, messages in (("WARNING", result.warnings), ("ERROR", result.errors)):
            for message in messages:
                print(f"{prefix}: {message}")
        print(
            f"Workflow security validation: {payload['status'].upper()} "
            f"({len(result.errors)} errors, {len(result.warnings)} warnings)"
        )
    return 1 if result.errors else 0


if __name__ == "__main__":
    sys.exit(main())

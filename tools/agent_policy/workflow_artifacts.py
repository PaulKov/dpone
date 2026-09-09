"""Validate GitHub Actions audit artifact publication policy."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

UPLOAD_ARTIFACT_ACTION = "actions/upload-artifact"
_INPUT_EXPRESSION = re.compile(r"^\$\{\{\s*inputs\.([A-Za-z][A-Za-z0-9_-]{0,63})\s*\}\}$")


def required_audit_artifacts(payload: dict[str, Any]) -> dict[str, dict[str, int]]:
    """Extract required audit artifact retention settings from policy YAML."""

    artifacts: dict[str, dict[str, int]] = {}
    raw = payload.get("required_audit_artifacts", {})
    if not isinstance(raw, dict):
        return artifacts
    for workflow_name, workflow_artifacts in raw.items():
        if not isinstance(workflow_name, str) or not isinstance(workflow_artifacts, dict):
            continue
        parsed: dict[str, int] = {}
        for artifact_name, entry in workflow_artifacts.items():
            if not isinstance(artifact_name, str) or not isinstance(entry, dict):
                continue
            retention_days = parse_retention_days(entry.get("retention_days"))
            if retention_days is not None:
                parsed[artifact_name] = retention_days
        if parsed:
            artifacts[workflow_name] = parsed
    return artifacts


def validate_policy_mapping(payload: dict[str, Any], label: str, errors: list[str]) -> None:
    """Validate required audit artifact policy settings."""

    raw = payload.get("required_audit_artifacts")
    if not isinstance(raw, dict):
        errors.append(f"{label}: required_audit_artifacts must be a mapping")
        return
    for workflow_name, workflow_artifacts in raw.items():
        if not isinstance(workflow_name, str) or not workflow_name.endswith((".yml", ".yaml")):
            errors.append(f"{label}: required_audit_artifacts keys must be workflow filenames")
            continue
        if not isinstance(workflow_artifacts, dict):
            errors.append(f"{label}: required_audit_artifacts.{workflow_name} must be a mapping")
            continue
        _validate_workflow_artifacts(workflow_artifacts, label, workflow_name, errors)


def validate_workflow_file(
    path: Path,
    workflow_name: str,
    payload: dict[str, Any],
    required: dict[str, dict[str, int]],
) -> list[str]:
    """Validate required audit artifact uploads in one workflow file."""

    expected = required.get(workflow_name, {})
    if not expected:
        return []

    errors: list[str] = []
    observed: set[str] = set()
    input_defaults = _workflow_call_input_defaults(payload)
    jobs = payload.get("jobs", {})
    if not isinstance(jobs, dict):
        return errors
    for job in jobs.values():
        if not isinstance(job, dict):
            continue
        steps = job.get("steps", [])
        if not isinstance(steps, list):
            continue
        for step in steps:
            artifact_name, retention_days = upload_artifact_step(
                step,
                input_defaults=input_defaults,
            )
            if artifact_name is None or artifact_name not in expected:
                continue
            observed.add(artifact_name)
            expected_days = expected[artifact_name]
            if retention_days != expected_days:
                errors.append(f"{path}: upload artifact {artifact_name} retention-days must be {expected_days}")

    for artifact_name in sorted(set(expected) - observed):
        errors.append(f"{path}: required audit artifact {artifact_name} is not uploaded")
    return errors


def upload_artifact_step(
    step: Any,
    *,
    input_defaults: dict[str, str] | None = None,
) -> tuple[str | None, int | None]:
    """Return artifact name and retention for an upload-artifact step."""

    if not isinstance(step, dict):
        return None, None
    uses = str(step.get("uses", "")).strip("\"'")
    action_name = uses.split("@", 1)[0].lower()
    if action_name != UPLOAD_ARTIFACT_ACTION:
        return None, None
    with_args = step.get("with")
    if not isinstance(with_args, dict):
        return None, None
    artifact_name = with_args.get("name")
    if not isinstance(artifact_name, str) or not artifact_name.strip():
        return None, parse_retention_days(with_args.get("retention-days"))
    normalized = artifact_name.strip()
    if match := _INPUT_EXPRESSION.fullmatch(normalized):
        normalized = (input_defaults or {}).get(match.group(1), "")
    return normalized or None, parse_retention_days(with_args.get("retention-days"))


def parse_retention_days(value: Any) -> int | None:
    """Parse a GitHub Actions retention day value from YAML."""

    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _workflow_call_input_defaults(payload: dict[str, Any]) -> dict[str, str]:
    events = payload.get("on", payload.get(True))
    if not isinstance(events, dict):
        return {}
    workflow_call = events.get("workflow_call")
    if not isinstance(workflow_call, dict):
        return {}
    inputs = workflow_call.get("inputs")
    if not isinstance(inputs, dict):
        return {}
    defaults: dict[str, str] = {}
    for name, contract in inputs.items():
        if not isinstance(name, str) or not isinstance(contract, dict):
            continue
        default = contract.get("default")
        if isinstance(default, str) and default.strip():
            defaults[name] = default.strip()
    return defaults


def _validate_workflow_artifacts(
    workflow_artifacts: dict[Any, Any],
    label: str,
    workflow_name: str,
    errors: list[str],
) -> None:
    for artifact_name, entry in workflow_artifacts.items():
        if not isinstance(artifact_name, str) or not artifact_name.strip():
            errors.append(f"{label}: required_audit_artifacts.{workflow_name} artifact names must be non-empty")
            continue
        if not isinstance(entry, dict):
            errors.append(f"{label}: required_audit_artifacts.{workflow_name}.{artifact_name} must be a mapping")
            continue
        retention_days = parse_retention_days(entry.get("retention_days"))
        if retention_days is None or retention_days < 1:
            errors.append(
                f"{label}: required_audit_artifacts.{workflow_name}.{artifact_name}.retention_days "
                "must be a positive integer"
            )
        if len(str(entry.get("reason", "")).strip()) < 20:
            errors.append(
                f"{label}: required_audit_artifacts.{workflow_name}.{artifact_name}.reason "
                "must explain the retention requirement"
            )

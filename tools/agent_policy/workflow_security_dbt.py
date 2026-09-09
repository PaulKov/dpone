"""dbt self-service specific GitHub workflow security checks."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

DBT_EXECUTION = re.compile(r"\b(?:dpone\s+)?dbt\s+(?:parse|build|compile)\b")
PROD_TRUST_POLICY_PATH = "platform/runtime-artifact-trust-policy.json"
PROD_TRUST_POLICY_DIGEST = "${{ vars.DPONE_DBT_RUNTIME_TRUST_POLICY_SHA256 }}"
HIDDEN_DBT_ARTIFACT_WORKFLOWS = frozenset(
    (
        "dbt-self-service-dev.yml",
        "dbt-self-service-dev-activation.yml",
        "dbt-self-service-dev-evidence.yml",
        "dbt-self-service-prod.yml",
    )
)
SOURCE_FREE_DBT_ATTESTATION_WORKFLOWS = frozenset(
    (
        "dbt-self-service-dev.yml",
        "dbt-self-service-dev-evidence.yml",
    )
)


def validate_dbt_workflow(
    path: Path,
    payload: dict[str, Any],
    text: str,
) -> list[str]:
    """Return dbt-specific security violations for one parsed workflow."""

    errors: list[str] = []
    jobs = payload.get("jobs")
    workflow_executes_dbt = isinstance(jobs, dict) and any(
        _job_executes_dbt(job) for job in jobs.values() if isinstance(job, dict)
    )
    if isinstance(jobs, dict):
        for job_name, job in jobs.items():
            if not isinstance(job, dict):
                continue
            steps = job.get("steps")
            if not isinstance(steps, list):
                continue
            if path.name in HIDDEN_DBT_ARTIFACT_WORKFLOWS:
                _validate_hidden_ci_artifacts(
                    path,
                    str(job_name),
                    steps,
                    errors,
                )
            _validate_runtime_gates(path, str(job_name), steps, errors)
            effective_permissions = job.get(
                "permissions",
                payload.get("permissions"),
            )
            if _job_executes_dbt(job) and effective_permissions != {"contents": "read"}:
                errors.append(f"{path}: job {job_name} dbt execution must run with contents: read only")
            if workflow_executes_dbt and _uses_action(
                steps,
                "actions/attest",
            ):
                if _uses_action(steps, "actions/checkout"):
                    errors.append(f"{path}: job {job_name} attestation job must not checkout repository source")
                if _job_executes_dbt(job):
                    errors.append(f"{path}: job {job_name} attestation job must not execute dbt source")
            if path.name in SOURCE_FREE_DBT_ATTESTATION_WORKFLOWS and _uses_action(steps, "actions/attest"):
                _validate_source_free_attestation_job(
                    path,
                    str(job_name),
                    steps,
                    errors,
                )
    if path.name == "dbt-self-service-prod.yml":
        _validate_prod_trust_contract(path, payload, text, errors)
    return errors


def _validate_hidden_ci_artifacts(
    path: Path,
    job_name: str,
    steps: list[Any],
    errors: list[str],
) -> None:
    for step in steps:
        if not isinstance(step, dict):
            continue
        args = step.get("with") if isinstance(step.get("with"), dict) else {}
        uploads_ci = str(step.get("uses", "")).startswith("actions/upload-artifact@") and ".dpone-ci" in str(
            args.get("path", "")
        )
        if uploads_ci and args.get("include-hidden-files") is not True:
            errors.append(f"{path}: job {job_name} .dpone-ci upload include-hidden-files must be true")


def _job_executes_dbt(job: dict[str, Any]) -> bool:
    steps = job.get("steps")
    return isinstance(steps, list) and any(
        isinstance(step, dict) and isinstance(step.get("run"), str) and DBT_EXECUTION.search(step["run"]) is not None
        for step in steps
    )


def _validate_runtime_gates(
    path: Path,
    job_name: str,
    steps: list[Any],
    errors: list[str],
) -> None:
    for index, step in enumerate(steps):
        if not isinstance(step, dict):
            continue
        script = step.get("run")
        if isinstance(script, str) and "load_dpone_dags" in script and re.search(r"\bassert\b", script) is not None:
            errors.append(f"{path}: job {job_name} step {index} runtime gate must use an explicit exit, not assert")


def _validate_source_free_attestation_job(
    path: Path,
    job_name: str,
    steps: list[Any],
    errors: list[str],
) -> None:
    forbidden_actions = {"actions/checkout", "actions/setup-python"}
    for index, step in enumerate(steps):
        if not isinstance(step, dict):
            continue
        action = str(step.get("uses", "")).split("@", 1)[0]
        if action in forbidden_actions:
            errors.append(f"{path}: job {job_name} source-free attestation must not use {action}")
        script = step.get("run")
        if not isinstance(script, str):
            continue
        lowered = script.lower()
        if any(token in lowered for token in ("pip install", "dpone ", "dbt ", "python ")):
            errors.append(
                f"{path}: job {job_name} step {index} source-free "
                "attestation must not execute caller-controlled tooling"
            )


def _uses_action(steps: list[Any], action: str) -> bool:
    return any(isinstance(step, dict) and str(step.get("uses", "")).split("@", 1)[0] == action for step in steps)


def _validate_prod_trust_contract(
    path: Path,
    payload: dict[str, Any],
    text: str,
    errors: list[str],
) -> None:
    events = payload.get("on", payload.get(True))
    workflow_call = events.get("workflow_call") if isinstance(events, dict) else None
    inputs = workflow_call.get("inputs") if isinstance(workflow_call, dict) else None
    if (
        isinstance(inputs, dict)
        and {
            "trust-policy-path",
            "trust-policy-sha256",
        }
        & inputs.keys()
    ):
        errors.append(f"{path}: production trust policy path and digest must not be workflow_call inputs")
    jobs = payload.get("jobs")
    promote = jobs.get("promote") if isinstance(jobs, dict) else None
    env = promote.get("env") if isinstance(promote, dict) else None
    checks = (
        (
            isinstance(promote, dict) and promote.get("environment") == "production",
            "production trust digest must be scoped to the production environment",
        ),
        (
            isinstance(env, dict) and env.get("DPONE_TRUST_POLICY_PATH") == PROD_TRUST_POLICY_PATH,
            "production trust policy must use the fixed repository-relative path",
        ),
        (
            isinstance(env, dict) and env.get("DPONE_TRUST_POLICY_SHA256") == PROD_TRUST_POLICY_DIGEST,
            "production trust digest must come from the protected environment variable",
        ),
        (
            "--expected-trust-tier production" in text,
            "production attestation verification must require the production trust tier",
        ),
    )
    errors.extend(f"{path}: {message}" for valid, message in checks if not valid)


__all__ = ["validate_dbt_workflow"]

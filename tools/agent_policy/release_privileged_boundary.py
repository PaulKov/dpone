"""Detect privileged candidate-workflow jobs that still execute repository checkout (SS-47)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, NamedTuple

import yaml

PRIVILEGED_WRITE = frozenset({"attestations", "contents", "id-token", "packages"})


class PrivilegedCheckoutFinding(NamedTuple):
    workflow: str
    job: str
    permissions: tuple[str, ...]
    message: str


def find_privileged_checkouts(workflow_path: Path) -> tuple[PrivilegedCheckoutFinding, ...]:
    """Return jobs that hold privileged write/OIDC authority and still check out code."""

    payload = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{workflow_path}: workflow must be a mapping")
    jobs = payload.get("jobs")
    if not isinstance(jobs, dict):
        raise ValueError(f"{workflow_path}: jobs must be a mapping")
    top_writes = _write_keys(payload.get("permissions"))
    findings: list[PrivilegedCheckoutFinding] = []
    for job_name, job in jobs.items():
        if not isinstance(job_name, str) or not isinstance(job, dict):
            continue
        write_keys = _write_keys(job.get("permissions")) or top_writes
        privileged = tuple(sorted(write_keys & PRIVILEGED_WRITE))
        if not privileged or not _job_has_checkout(job):
            continue
        findings.append(
            PrivilegedCheckoutFinding(
                workflow=workflow_path.name,
                job=job_name,
                permissions=privileged,
                message=(
                    f"{workflow_path.name} job '{job_name}' holds write/OIDC "
                    f"permissions {list(privileged)} and executes actions/checkout; "
                    "SS-47 requires action-only privileged jobs without repository checkout."
                ),
            )
        )
    return tuple(findings)


def _write_keys(raw: Any) -> frozenset[str]:
    if not isinstance(raw, dict):
        return frozenset()
    return frozenset(
        key for key, value in raw.items() if isinstance(key, str) and isinstance(value, str) and value == "write"
    )


def _job_has_checkout(job: dict[str, Any]) -> bool:
    steps = job.get("steps")
    if not isinstance(steps, list):
        return False
    for step in steps:
        if isinstance(step, dict) and isinstance(step.get("uses"), str):
            if step["uses"].startswith("actions/checkout@"):
                return True
    return False

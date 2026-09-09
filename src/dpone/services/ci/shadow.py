"""Pure planning and outcome folding for CI shadow tooling."""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.contracts.ci_shadow import JOB_ORDER, JobName, ShadowIdentity, jobs_for, selected_jobs
from dpone.contracts.strict_json import canonical_json_bytes as canonical_bytes
from dpone.contracts.strict_json import strict_json_object


class ShadowContractError(ValueError):
    """A shadow input is malformed or cannot be safely interpreted."""


_SHA = re.compile(r"^[0-9a-f]{40}$")
_MAX_JSON_BYTES = 1_048_576
_MAX_JSON_DEPTH = 8
_PRODUCT_JOB_NAMES: dict[JobName, tuple[str, ...]] = {
    "static": ("PR Gate shadow static",),
    "contracts": ("PR Gate shadow contracts",),
    "docs": ("PR Gate shadow docs",),
    "python-3.11": ("PR Gate shadow Python 3.11",),
    "python-3.12": ("PR Gate shadow Python 3.12",),
    "packaging": ("PR Gate shadow packaging",),
    "postgresql": ("PR Gate shadow PostgreSQL XMin",),
    "airflow": (
        "PR Gate shadow Airflow 2.10.5 / py3.11",
        "PR Gate shadow Airflow 2.10.5 / py3.12",
        "PR Gate shadow Airflow 2.11.0 / py3.11",
        "PR Gate shadow Airflow 2.11.0 / py3.12",
        "PR Gate shadow Airflow 3.2.0 / py3.11",
        "PR Gate shadow Airflow 3.2.0 / py3.12",
        "PR Gate shadow Airflow 3.3.0 / py3.11",
        "PR Gate shadow Airflow 3.3.0 / py3.12",
    ),
    "runtime-wheel-smoke": ("PR Gate shadow runtime wheel smoke",),
}


def _sha256(value: object) -> str:
    return f"sha256:{hashlib.sha256(canonical_bytes(value)).hexdigest()}"


def _identity(raw: dict[str, Any]) -> ShadowIdentity:
    try:
        identity = ShadowIdentity(**{key: raw[key] for key in ShadowIdentity.__dataclass_fields__})
    except (KeyError, TypeError) as exc:
        raise ShadowContractError("event identity is incomplete") from exc
    if any(
        not isinstance(getattr(identity, key), int) or getattr(identity, key) <= 0
        for key in ("repository_id", "pr_number")
    ):
        raise ShadowContractError("event numeric identity is invalid")
    if any(
        not isinstance(getattr(identity, key), str) or _SHA.fullmatch(getattr(identity, key)) is None
        for key in ("base_sha", "head_sha", "merge_sha")
    ):
        raise ShadowContractError("event SHA identity is invalid")
    return identity


def build_plan(event: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    """Build a deterministic, closed plan from untrusted event/policy bytes."""

    allowed_event_keys = {
        "repository_id",
        "pr_number",
        "base_sha",
        "head_sha",
        "merge_sha",
        "changed_paths",
        "semantic_ambiguous",
    }
    if not set(event) <= allowed_event_keys:
        raise ShadowContractError("event has unknown fields")
    identity = _identity(event)
    paths = event.get("changed_paths")
    if not isinstance(paths, list) or any(
        not isinstance(path, str) or not path or "\x00" in path or path.startswith(("/", "../")) or "/../" in path
        for path in paths
    ):
        raise ShadowContractError("changed_paths must be a non-empty string list")
    semantic_ambiguous = event.get("semantic_ambiguous", False)
    if not isinstance(semantic_ambiguous, bool):
        raise ShadowContractError("semantic_ambiguous must be boolean")
    if (
        set(policy) != {"schema_version", "default_route"}
        or policy.get("schema_version") != "dpone.ci-shadow-route-policy.v1"
    ):
        raise ShadowContractError("unsupported route policy")
    if policy["default_route"] != "full":
        raise ShadowContractError("route policy must retain the full fallback")
    normalized = tuple(sorted(set(paths)))
    selected = jobs_for(changed_paths=normalized, semantic_ambiguous=semantic_ambiguous)
    plan = {
        "schema_version": "dpone.ci-change-plan.v1",
        "repository_id": identity.repository_id,
        "pr_number": identity.pr_number,
        "base_sha": identity.base_sha,
        "head_sha": identity.head_sha,
        "merge_sha": identity.merge_sha,
        "changed_paths": list(normalized),
        "route_policy_digest": _sha256(policy),
        "jobs": selected_jobs(selected),
    }
    return {**plan, "plan_digest": _sha256(plan)}


def changed_paths_from_git(*, repository_root: Path, base_sha: str, head_sha: str) -> list[str]:
    """Read the exact rename-disabled NUL-delimited ``B...H`` path set."""

    try:
        completed = subprocess.run(
            ["git", "diff", "--no-renames", "--name-only", "-z", f"{base_sha}...{head_sha}"],
            cwd=repository_root,
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ShadowContractError("exact B...H diff is unavailable") from exc
    try:
        paths = [item.decode("utf-8") for item in completed.stdout.split(b"\0") if item]
    except UnicodeDecodeError as exc:
        raise ShadowContractError("diff contains invalid UTF-8 path") from exc
    if any("\0" in path or not path for path in paths):
        raise ShadowContractError("diff contains invalid path")
    return paths


def event_from_pull_request_payload(payload: dict[str, Any], *, merge_sha: str) -> dict[str, Any]:
    """Extract only immutable PR-event identity needed by the producer."""

    repository = payload.get("repository")
    pull_request = payload.get("pull_request")
    if not isinstance(repository, dict) or not isinstance(pull_request, dict):
        raise ShadowContractError("pull_request event is incomplete")
    base = pull_request.get("base")
    head = pull_request.get("head")
    if not isinstance(base, dict) or not isinstance(head, dict):
        raise ShadowContractError("pull_request refs are incomplete")
    event: dict[str, Any] = {
        "repository_id": repository.get("id"),
        "pr_number": payload.get("number"),
        "base_sha": base.get("sha"),
        "head_sha": head.get("sha"),
        "merge_sha": merge_sha,
        "changed_paths": [],
    }
    _identity(event)
    return event


def build_claims(plan: dict[str, Any], job_results: dict[str, Any]) -> dict[str, Any]:
    """Fold provider job conclusions; absent or ambiguous work is UNVERIFIED."""

    if plan.get("schema_version") != "dpone.ci-change-plan.v1":
        raise ShadowContractError("unsupported plan")
    identity = _identity(plan)
    plan_digest = plan.get("plan_digest")
    if not isinstance(plan_digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", plan_digest):
        raise ShadowContractError("plan digest is invalid")
    selections = plan.get("jobs")
    if not isinstance(selections, dict) or set(selections) != set(JOB_ORDER):
        raise ShadowContractError("plan job vocabulary is not closed")
    if not isinstance(job_results, dict) or not set(job_results) <= set(JOB_ORDER):
        raise ShadowContractError("provider job vocabulary is not closed")
    jobs: dict[str, dict[str, str]] = {}
    outcomes: list[str] = []
    for job in JOB_ORDER:
        selection = selections[job]
        if selection == "N/A":
            outcome = "N/A"
        elif selection == "RUN":
            outcome = job_results.get(job, "UNVERIFIED")
            if outcome not in {"PASS", "FAIL", "UNVERIFIED"}:
                outcome = "UNVERIFIED"
        else:
            raise ShadowContractError("invalid plan selection")
        jobs[job] = {"selection": selection, "outcome": outcome}
        outcomes.append(outcome)
    status = "UNVERIFIED" if "UNVERIFIED" in outcomes else "FAIL" if "FAIL" in outcomes else "PASS"
    return {
        "schema_version": "dpone.pr-gate-shadow-evidence.v1",
        "repository_id": identity.repository_id,
        "pr_number": identity.pr_number,
        "base_sha": identity.base_sha,
        "head_sha": identity.head_sha,
        "merge_sha": identity.merge_sha,
        "plan_digest": plan_digest,
        "jobs": jobs,
        "status": status,
    }


def provider_outcomes(selections: dict[str, Any], provider_jobs: list[dict[str, Any]]) -> dict[str, str]:
    """Fold exact-attempt Jobs API rows without trusting product outputs.

    A selected product is a pass only when its complete, uniquely named job
    population has terminal ``success`` conclusions. Missing, duplicate,
    skipped, cancelled, timed-out, malformed or still-running rows are
    unverified; a terminal provider failure is preserved as ``FAIL``.
    """

    if set(selections) != set(JOB_ORDER) or any(value not in {"RUN", "N/A"} for value in selections.values()):
        raise ShadowContractError("plan job vocabulary is not closed")
    if not all(isinstance(job, dict) for job in provider_jobs):
        raise ShadowContractError("provider jobs must be objects")
    by_name: dict[str, list[dict[str, Any]]] = {}
    for job in provider_jobs:
        name = job.get("name")
        if isinstance(name, str):
            by_name.setdefault(name, []).append(job)
    outcomes: dict[str, str] = {}
    for product in JOB_ORDER:
        if selections[product] == "N/A":
            continue
        expected_names = _PRODUCT_JOB_NAMES[product]
        rows = [row for name in expected_names for row in by_name.get(name, [])]
        if not expected_names or len(rows) != len(expected_names):
            outcomes[product] = "UNVERIFIED"
            continue
        if any(row.get("status") != "completed" for row in rows):
            outcomes[product] = "UNVERIFIED"
            continue
        conclusions = {row.get("conclusion") for row in rows}
        if conclusions == {"success"}:
            outcomes[product] = "PASS"
        elif "failure" in conclusions:
            outcomes[product] = "FAIL"
        else:
            outcomes[product] = "UNVERIFIED"
    return outcomes


def audit_provider_decision(
    claimed_jobs: object,
    provider_jobs: list[dict[str, Any]],
    claimed_status: object,
) -> str:
    """Reconcile closed producer claims with exact provider-owned job rows."""

    if not isinstance(claimed_jobs, Mapping) or set(claimed_jobs) != set(JOB_ORDER):
        raise ShadowContractError("claims jobs do not use the closed vocabulary")
    selections: dict[str, Any] = {}
    claimed_outcomes: dict[str, Any] = {}
    for name in JOB_ORDER:
        entry = claimed_jobs.get(name)
        if not isinstance(entry, Mapping):
            raise ShadowContractError(f"claims.jobs.{name} must be an object")
        selections[name] = entry.get("selection")
        claimed_outcomes[name] = entry.get("outcome")
    outcomes = provider_outcomes(selections, provider_jobs)
    for product, outcome in outcomes.items():
        if claimed_outcomes[product] != outcome:
            raise ShadowContractError("claims outcome disagrees with provider jobs")
    values = tuple(outcomes.values())
    if "FAIL" in values:
        return "FAIL"
    if "UNVERIFIED" in values:
        return "UNVERIFIED"
    if claimed_status != "PASS":
        raise ShadowContractError("successful provider jobs disagree with claim status")
    return "PASS"


def read_object(path: Path) -> dict[str, Any]:
    payload = path.read_bytes()
    if len(payload) > _MAX_JSON_BYTES:
        raise ShadowContractError("JSON input exceeds 1 MiB")
    try:
        decoded = strict_json_object(payload)
    except ValueError as exc:
        raise ShadowContractError("JSON input is invalid") from exc
    _validate_json_depth(decoded)
    return decoded


def _validate_json_depth(value: object, *, depth: int = 1) -> None:
    """Reject deeply nested untrusted JSON before contract interpretation."""

    if depth > _MAX_JSON_DEPTH:
        raise ShadowContractError("JSON input exceeds nesting limit")
    if isinstance(value, dict):
        for child in value.values():
            _validate_json_depth(child, depth=depth + 1)
    elif isinstance(value, list):
        for child in value:
            _validate_json_depth(child, depth=depth + 1)


def write_create_new(path: Path, value: object) -> None:
    data = canonical_bytes(value) + b"\n"
    if len(data) > _MAX_JSON_BYTES:
        raise ShadowContractError("output exceeds 1 MiB")
    with path.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())

"""Pure semantic validation for observed pre-tag release evidence."""

from __future__ import annotations

import importlib.util
import re
import sys
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


codec = _load_sibling("dpone_release_candidate_codec_validation", "release_candidate_evidence_codec.py")
policy = _load_sibling("dpone_release_candidate_policy_validation", "release_candidate_evidence_policy.py")
route_validation = _load_sibling(
    "dpone_release_candidate_route_validation",
    "release_candidate_evidence_route_validation.py",
)
route_live_validation = _load_sibling(
    "dpone_release_candidate_route_live_validation",
    "release_candidate_route_live_validation.py",
)

BARE_SHA256 = re.compile(r"^[0-9a-f]{64}$")
JUNIT_KEYS = frozenset(
    {"schema_version", "status", "profile", "commit_sha", "junit", "gate", "totals", "cases", "message"}
)
PACKAGE_FILES = (
    Path("pyproject.toml"),
    Path("packages/dpone-native-accel/pyproject.toml"),
    Path("packages/dpone-airflow-pack/pyproject.toml"),
    Path("packages/apache-airflow-providers-dpone/pyproject.toml"),
)
EXACT_CHECK_OBSERVATION_KEYS = frozenset({"context", "source", "evidence_id", "state", "integration_id", "commit_sha"})


def validate_source(
    role: str,
    payload: dict[str, Any],
    *,
    raw: bytes,
    commit_sha: str,
    related_raw: Mapping[str, bytes] | None = None,
) -> dict[str, Any]:
    """Validate one frozen source role and return its compact projection."""

    if role in policy.JUNIT_CASES:
        return _validate_junit(role, payload, commit_sha=commit_sha)
    if role == "exact_commit_checks":
        return _validate_exact_checks(payload, commit_sha=commit_sha)
    if role == "merge_receipt":
        return _validate_merge_receipt(payload, commit_sha=commit_sha)
    if role.endswith("_execution"):
        route = role.removesuffix("_execution")
        return route_validation.validate_route_execution(payload, route=route)
    if role.endswith("_verification"):
        route = role.removesuffix("_verification")
        execution_role = f"{route}_execution"
        if related_raw is None or execution_role not in related_raw:
            raise ValueError(f"{role} requires exact {execution_role} bytes")
        return route_validation.validate_route_verification(
            payload,
            route=route,
            execution_raw=related_raw[execution_role],
        )
    if role == "stress_benchmark":
        return route_validation.validate_stress(payload)
    if role == "postgres_mssql_route_certification":
        return route_live_validation.validate_manifest(payload, commit_sha=commit_sha)
    if role == "postgres_mssql_route_artifact_binding":
        if related_raw is None or "postgres_mssql_route_certification" not in related_raw:
            raise ValueError("route-live provider binding requires exact manifest bytes")
        return route_live_validation.validate_provider_binding(
            payload,
            commit_sha=commit_sha,
            manifest_raw=related_raw["postgres_mssql_route_certification"],
        )
    raise ValueError(f"unknown release evidence role {role!r}")


def validate_project_release(root: Path, *, release: str) -> dict[str, str]:
    """Require all public packages, pins, and CHANGELOG to match the candidate."""

    if codec.RELEASE.fullmatch(release) is None:
        raise ValueError("release must use canonical stable form vX.Y.Z")
    version = release.removeprefix("v")
    projects: dict[str, Mapping[str, Any]] = {}
    versions: dict[str, str] = {}
    for path in PACKAGE_FILES:
        try:
            project = tomllib.loads((root / path).read_text(encoding="utf-8"))["project"]
            name = str(project["name"])
            current = str(project["version"])
        except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as exc:
            raise ValueError(f"invalid package metadata at {path}") from exc
        if current != version:
            raise ValueError(f"{name} version {current} does not match {version}")
        projects[name] = project
        versions[name] = current
    required_pins = (
        ("dpone", "dependencies", "dpone-airflow-pack"),
        ("dpone", "accel", "dpone-native-accel"),
        ("apache-airflow-providers-dpone", "dependencies", "dpone-airflow-pack"),
    )
    for owner, group, dependency in required_pins:
        project = projects[owner]
        if group == "dependencies":
            values = project.get("dependencies", [])
        else:
            values = _mapping(project.get("optional-dependencies"), f"{owner}.optional-dependencies").get(group, [])
        if f"{dependency}=={version}" not in values:
            raise ValueError(f"{owner} must pin {dependency}=={version}")
    try:
        changelog = (root / "CHANGELOG.md").read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError("CHANGELOG.md is unavailable") from exc
    if not any(line.startswith(f"## {version}") for line in changelog.splitlines()):
        raise ValueError(f"CHANGELOG.md has no {version} section")
    return dict(sorted(versions.items()))


def _validate_junit(role: str, payload: Mapping[str, Any], *, commit_sha: str) -> dict[str, Any]:
    codec.require_exact_keys(payload, JUNIT_KEYS, field=role)
    expected = policy.JUNIT_CASES[role]
    if (
        payload.get("schema_version") != 1
        or payload.get("status") != "PASS"
        or payload.get("profile") != policy.PROFILE
        or payload.get("commit_sha") != commit_sha
    ):
        raise ValueError(f"{role} identity or status is invalid")
    gate = _mapping(payload.get("gate"), f"{role}.gate")
    codec.require_exact_keys(gate, frozenset({"min_passed", "max_skipped"}), field=f"{role}.gate")
    if gate != {"min_passed": len(expected), "max_skipped": 0}:
        raise ValueError(f"{role} gate budgets are not frozen")
    totals = _mapping(payload.get("totals"), f"{role}.totals")
    codec.require_exact_keys(
        totals,
        frozenset({"tests", "passed", "skipped", "failures", "errors"}),
        field=f"{role}.totals",
    )
    if totals != {"tests": len(expected), "passed": len(expected), "skipped": 0, "failures": 0, "errors": 0}:
        raise ValueError(f"{role} totals are not an exact all-pass result")
    cases = _list(payload.get("cases"), f"{role}.cases")
    observed: list[str] = []
    for index, item in enumerate(cases):
        case = _mapping(item, f"{role}.cases[{index}]")
        codec.require_exact_keys(case, frozenset({"node_id", "status"}), field=f"{role}.cases[{index}]")
        if case.get("status") != "passed":
            raise ValueError(f"{role} contains a non-passing case")
        node_id = codec.require_string(case.get("node_id"), field=f"{role}.cases[{index}].node_id")
        observed.append(node_id)
    if tuple(sorted(observed)) != tuple(sorted(expected)):
        raise ValueError(f"{role} case inventory does not match frozen requirements")
    return {"status": "PASS", "passed_cases": len(expected)}


def _validate_exact_checks(payload: Mapping[str, Any], *, commit_sha: str) -> dict[str, Any]:
    codec.require_exact_keys(
        payload,
        frozenset(
            {
                "schema_version",
                "status",
                "decision",
                "repository",
                "commit_sha",
                "ruleset_id",
                "attempts",
                "contexts",
                "blockers",
                "policy_sha256",
                "policy_path",
            }
        ),
        field="exact_commit_checks",
    )
    required = {
        "schema_version": 1,
        "status": "PASS",
        "decision": "GO",
        "commit_sha": commit_sha,
    }
    if any(payload.get(key) != value for key, value in required.items()):
        raise ValueError("exact required-check report is not a PASS for the candidate")
    if payload.get("blockers") != []:
        raise ValueError("exact required-check report contains blockers")
    repository = codec.require_string(payload.get("repository"), field="exact_commit_checks.repository")
    if codec.REPOSITORY.fullmatch(repository) is None:
        raise ValueError("exact required-check repository is invalid")
    contexts = _list(payload.get("contexts"), "exact_commit_checks.contexts")
    if not contexts:
        raise ValueError("exact required-check report contains no contexts")
    codec.require_positive_int(payload.get("attempts"), field="exact_commit_checks.attempts")
    codec.require_positive_int(payload.get("ruleset_id"), field="exact_commit_checks.ruleset_id")
    policy_sha256 = payload.get("policy_sha256")
    if not isinstance(policy_sha256, str) or BARE_SHA256.fullmatch(policy_sha256) is None:
        raise ValueError("exact_commit_checks.policy_sha256 must be a bare lowercase SHA-256 digest")
    codec.require_string(payload.get("policy_path"), field="exact_commit_checks.policy_path")
    names: set[str] = set()
    for index, value in enumerate(contexts):
        context = _mapping(value, f"exact_commit_checks.contexts[{index}]")
        codec.require_exact_keys(
            context,
            frozenset(
                {
                    "blocker_codes",
                    "context",
                    "current_observations",
                    "integration_id",
                    "observed_count",
                    "status",
                }
            ),
            field=f"exact_commit_checks.contexts[{index}]",
        )
        if context.get("status") != "PASS" or context.get("blocker_codes") != []:
            raise ValueError("exact required-check report contains a non-success context")
        integration_id = codec.require_positive_int(
            context.get("integration_id"),
            field=f"exact_commit_checks.contexts[{index}].integration_id",
        )
        observed_count = codec.require_positive_int(
            context.get("observed_count"),
            field=f"exact_commit_checks.contexts[{index}].observed_count",
        )
        name = codec.require_string(
            context.get("context"),
            field=f"exact_commit_checks.contexts[{index}].context",
        )
        if name in names:
            raise ValueError("exact required-check report contains duplicate contexts")
        names.add(name)
        current = _list(
            context.get("current_observations"),
            f"exact_commit_checks.contexts[{index}].current_observations",
        )
        if not current:
            raise ValueError("exact required-check context has no current observation")
        for observation_index, observation in enumerate(current):
            field = f"exact_commit_checks.contexts[{index}].current_observations[{observation_index}]"
            current_observation = _mapping(observation, field)
            codec.require_exact_keys(current_observation, EXACT_CHECK_OBSERVATION_KEYS, field=field)
            codec.require_positive_int(
                current_observation.get("evidence_id"),
                field=f"{field}.evidence_id",
            )
            codec.require_positive_int(
                current_observation.get("integration_id"),
                field=f"{field}.integration_id",
            )
            expected_observation = {
                "context": name,
                "source": "check_run",
                "state": "success",
                "integration_id": integration_id,
                "commit_sha": commit_sha,
            }
            if any(current_observation.get(key) != value for key, value in expected_observation.items()):
                raise ValueError("exact required-check observation identity or state is invalid")
        # The producer reports every same-context observation in ``observed_count``.
        # An admissible PASS report may contain only current, provider-bound check
        # runs, so that inventory must close exactly over ``current_observations``.
        if observed_count != len(current):
            raise ValueError("exact required-check observed_count does not match current observations")
    return {"status": "PASS", "required_contexts": len(contexts), "repository": repository}


def _validate_merge_receipt(payload: Mapping[str, Any], *, commit_sha: str) -> dict[str, Any]:
    codec.require_exact_keys(
        payload,
        frozenset(
            {
                "schema_version",
                "status",
                "repository",
                "integration_commit_sha",
                "check_run_id",
                "github_app_id",
                "workflow_run_id",
                "workflow_run_attempt",
                "artifact_id",
                "artifact_digest",
                "artifact_size_bytes",
                "archive_sha256",
                "receipt_sha256",
                "binding_id",
            }
        ),
        field="merge_receipt",
    )
    expected = {
        "schema_version": 1,
        "status": "PASS",
        "integration_commit_sha": commit_sha,
    }
    if any(payload.get(key) != value for key, value in expected.items()):
        raise ValueError("merge receipt does not bind the candidate commit")
    repository = codec.require_string(payload.get("repository"), field="merge_receipt.repository")
    if codec.REPOSITORY.fullmatch(repository) is None:
        raise ValueError("merge receipt repository is invalid")
    for field in (
        "check_run_id",
        "workflow_run_id",
        "workflow_run_attempt",
        "artifact_id",
        "artifact_size_bytes",
    ):
        codec.require_positive_int(payload.get(field), field=f"merge_receipt.{field}")
    if payload.get("github_app_id") != policy.APP_ID:
        raise ValueError("merge receipt GitHub application is invalid")
    binding_id = codec.require_digest(payload.get("binding_id"), field="merge_receipt.binding_id")
    codec.require_digest(payload.get("artifact_digest"), field="merge_receipt.artifact_digest")
    codec.require_digest(payload.get("archive_sha256"), field="merge_receipt.archive_sha256")
    codec.require_digest(payload.get("receipt_sha256"), field="merge_receipt.receipt_sha256")
    return {"status": "PASS", "repository": repository, "binding_id": binding_id}


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a mapping")
    return value


def _list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{field} must be a list")
    return value


__all__ = ["validate_project_release", "validate_source"]

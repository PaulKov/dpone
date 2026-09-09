"""Validate the repository GitHub branch protection policy."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

BRANCH_PROTECTION_POLICY = ".agents/policy/github-branch-protection.yml"
REQUIRED_STATUS_CHECKS = (
    "Agent PR receipt",
    "Build Airflow distribution artifacts",
    "Airflow 2.10.5 / py3.11",
    "Airflow 2.10.5 / py3.12",
    "Airflow 2.11.0 / py3.11",
    "Airflow 2.11.0 / py3.12",
    "Airflow 3.2.0 / py3.11",
    "Airflow 3.2.0 / py3.12",
    "Airflow 3.3.0 / py3.11",
    "Airflow 3.3.0 / py3.12",
    "Runtime wheel smoke / py3.11",
    "Runtime wheel smoke / py3.12",
    "Analyze Python",
    "Build GitHub Pages documentation",
    "Dependency Review",
    "Doctor import Windows (3.11)",
    "Doctor import Windows (3.12)",
    "PostgreSQL XMin integration",
    "Quality checks (3.11)",
    "Quality checks (3.12)",
    "TruffleHog verified secrets",
)


@dataclass
class BranchProtectionValidationResult:
    """Validation result for the desired GitHub branch protection policy."""

    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def load_policy(path: Path) -> dict[str, Any]:
    """Load a branch protection policy YAML file."""

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("branch protection policy must be a YAML mapping")
    return data


def validate_file(path: Path) -> BranchProtectionValidationResult:
    """Validate a branch protection policy file."""

    try:
        payload = load_policy(path)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        return BranchProtectionValidationResult(errors=[f"{path}: cannot load branch protection policy: {exc}"])
    return validate_policy(payload, label=str(path))


def validate_policy(payload: dict[str, Any], *, label: str) -> BranchProtectionValidationResult:
    """Validate parsed branch protection policy data."""

    result = BranchProtectionValidationResult()
    schema_version = payload.get("schema_version")
    if schema_version == 2:
        return _validate_policy_v2(payload, label=label)
    if schema_version != 1:
        result.errors.append(f"{label}: schema_version must be 1 or 2")
        return result
    for field_name in ("owner", "last_reviewed", "purpose"):
        if not str(payload.get(field_name, "")).strip():
            result.errors.append(f"{label}: {field_name} must not be empty")

    mode = payload.get("mode")
    if mode not in {"solo_maintainer", "multi_reviewer"}:
        result.errors.append(f"{label}: mode must be solo_maintainer or multi_reviewer")

    ruleset = _mapping(payload, "ruleset", label, result)
    pull_request = _mapping(ruleset, "pull_request", label, result)
    status_checks = _mapping(ruleset, "required_status_checks", label, result)
    protected_actions = _mapping(ruleset, "protected_actions", label, result)
    bypass = _mapping(ruleset, "bypass", label, result)
    owner_attestation = _mapping(payload, "owner_attestation", label, result)

    _validate_ruleset(ruleset, label, result)
    _validate_pull_request(pull_request, label, result, solo_maintainer=mode == "solo_maintainer")
    _validate_status_checks(status_checks, label, result)
    _validate_protected_actions(protected_actions, label, result)
    _validate_bypass(bypass, label, result)
    _validate_owner_attestation(owner_attestation, label, result, solo_maintainer=mode == "solo_maintainer")
    return result


def _validate_policy_v2(payload: dict[str, Any], *, label: str) -> BranchProtectionValidationResult:
    """Validate governance-policy v2 through the strict parser and context contract."""

    result = BranchProtectionValidationResult()
    access = _load_sibling("dpone_agent_governance_policy_access_bp", "governance_policy_access.py")
    policy_v2 = _load_sibling("dpone_agent_governance_policy_v2_bp", "governance_policy_v2.py")
    try:
        parsed = policy_v2.parse_governance_policy(payload)
        contexts = access.required_context_names(payload)
    except (TypeError, ValueError) as exc:
        result.errors.append(f"{label}: invalid governance policy v2: {exc}")
        return result
    expected = set(REQUIRED_STATUS_CHECKS)
    observed = set(contexts)
    missing = sorted(expected - observed)
    extra = sorted(observed - expected)
    if missing:
        result.errors.append(f"{label}: required contexts missing {', '.join(missing)}")
    if extra:
        result.errors.append(f"{label}: required contexts contain unknown checks: {', '.join(extra)}")
    branch = parsed.branch_view
    if branch.mode not in {"solo_maintainer", "multi_reviewer"}:
        result.errors.append(f"{label}: branch_governance.mode is invalid")
    if branch.ruleset.target != "branch" or branch.ruleset.name != "protect-master":
        result.errors.append(f"{label}: branch ruleset name/target must remain protect-master/branch")
    if "refs/heads/master" not in branch.ruleset.include:
        result.errors.append(f"{label}: branch ruleset conditions.include must contain refs/heads/master")
    if not parsed.release_view.immutable_releases_required:
        result.errors.append(f"{label}: release_trust.immutable_releases.required must be true")
    return result


def _load_sibling(module_name: str, filename: str) -> Any:
    if module_name in sys.modules:
        return sys.modules[module_name]
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _validate_ruleset(
    ruleset: dict[str, Any],
    label: str,
    result: BranchProtectionValidationResult,
) -> None:
    ruleset_id = ruleset.get("id")
    if not isinstance(ruleset_id, int) or ruleset_id < 1:
        result.errors.append(f"{label}: ruleset.id must be a positive integer")
    version_id = ruleset.get("version_id")
    if isinstance(version_id, bool) or not isinstance(version_id, int) or version_id < 1:
        result.errors.append(f"{label}: ruleset.version_id must be a positive integer")
    updated_at = ruleset.get("updated_at")
    live_ruleset = _load_sibling("dpone_agent_governance_live_ruleset_for_policy", "governance_live_ruleset.py")
    if live_ruleset.canonical_ruleset_updated_at(updated_at) != updated_at:
        result.errors.append(f"{label}: ruleset.updated_at must be one canonical UTC timestamp")
    if ruleset.get("name") != "protect-master":
        result.errors.append(f"{label}: ruleset.name must be protect-master")
    if ruleset.get("target") != "branch":
        result.errors.append(f"{label}: ruleset.target must be branch")
    if ruleset.get("enforcement") != "active":
        result.errors.append(f"{label}: ruleset.enforcement must be active")
    conditions = _mapping(ruleset, "conditions", label, result)
    if _string_list(conditions, "include", label, result) != ["refs/heads/master"]:
        result.errors.append(f"{label}: ruleset.conditions.include must be exactly refs/heads/master")
    if _string_list(conditions, "exclude", label, result) != []:
        result.errors.append(f"{label}: ruleset.conditions.exclude must be empty")
    branches = _string_list(ruleset, "branches", label, result)
    if "master" not in branches:
        result.errors.append(f"{label}: ruleset.branches must include master")


def _validate_pull_request(
    pull_request: dict[str, Any],
    label: str,
    result: BranchProtectionValidationResult,
    *,
    solo_maintainer: bool,
) -> None:
    if pull_request.get("required") is not True:
        result.errors.append(f"{label}: pull_request.required must be true")
    if set(_string_list(pull_request, "allowed_merge_methods", label, result)) != {"merge", "squash"}:
        result.errors.append(f"{label}: pull_request.allowed_merge_methods must be merge and squash")
    if pull_request.get("required_review_thread_resolution") is not True:
        result.errors.append(f"{label}: pull_request.required_review_thread_resolution must be true")
    if pull_request.get("dismiss_stale_reviews_on_push") is not True:
        result.errors.append(f"{label}: pull_request.dismiss_stale_reviews_on_push must be true")
    if pull_request.get("require_extra_approval_for_unattributed_changes") is not True:
        result.errors.append(f"{label}: pull_request.require_extra_approval_for_unattributed_changes must be true")
    if pull_request.get("required_reviewers") != []:
        result.errors.append(f"{label}: pull_request.required_reviewers must be empty")
    if solo_maintainer:
        if pull_request.get("required_approving_review_count") != 0:
            result.errors.append(f"{label}: solo_maintainer mode requires required_approving_review_count=0")
        if pull_request.get("require_code_owner_review") is not False:
            result.errors.append(f"{label}: solo_maintainer mode requires require_code_owner_review=false")


def _validate_status_checks(
    status_checks: dict[str, Any],
    label: str,
    result: BranchProtectionValidationResult,
) -> None:
    if status_checks.get("strict") is not True:
        result.errors.append(f"{label}: required_status_checks.strict must be true")
    if status_checks.get("do_not_enforce_on_create") is not False:
        result.errors.append(f"{label}: required_status_checks.do_not_enforce_on_create must be false")
    integration_id = status_checks.get("integration_id")
    if isinstance(integration_id, bool) or not isinstance(integration_id, int) or integration_id <= 0:
        result.errors.append(f"{label}: required_status_checks.integration_id must be a positive integer")
    checks = set(_string_list(status_checks, "checks", label, result))
    expected = set(REQUIRED_STATUS_CHECKS)
    missing = sorted(expected - checks)
    extra = sorted(checks - expected)
    if missing:
        result.errors.append(f"{label}: required_status_checks.checks missing {', '.join(missing)}")
    if extra:
        result.errors.append(f"{label}: required_status_checks.checks contains unknown checks: {', '.join(extra)}")


def _validate_protected_actions(
    protected_actions: dict[str, Any],
    label: str,
    result: BranchProtectionValidationResult,
) -> None:
    if protected_actions.get("block_deletion") is not True:
        result.errors.append(f"{label}: protected_actions.block_deletion must be true")
    if protected_actions.get("block_force_push") is not True:
        result.errors.append(f"{label}: protected_actions.block_force_push must be true")


def _validate_bypass(
    bypass: dict[str, Any],
    label: str,
    result: BranchProtectionValidationResult,
) -> None:
    if _string_list(bypass, "allowed_repository_roles", label, result) != ["admin"]:
        result.errors.append(f"{label}: bypass.allowed_repository_roles must be exactly admin")
    if bypass.get("actors") != [{"actor_id": 5, "actor_type": "RepositoryRole", "bypass_mode": "always"}]:
        result.errors.append(f"{label}: bypass.actors must be exactly the repository admin role")
    disallowed = set(_string_list(bypass, "disallowed_for", label, result))
    if not {"agent", "automation", "broad_team"} <= disallowed:
        result.errors.append(f"{label}: bypass.disallowed_for must include agent, automation, and broad_team")


def _validate_owner_attestation(
    owner_attestation: dict[str, Any],
    label: str,
    result: BranchProtectionValidationResult,
    *,
    solo_maintainer: bool,
) -> None:
    if solo_maintainer and owner_attestation.get("required_when_no_independent_reviewer") is not True:
        result.errors.append(f"{label}: solo_maintainer mode requires owner attestation")
    if not _string_list(owner_attestation, "evidence_required", label, result):
        result.errors.append(f"{label}: owner_attestation.evidence_required must not be empty")


def _mapping(
    payload: dict[str, Any],
    field_name: str,
    label: str,
    result: BranchProtectionValidationResult,
) -> dict[str, Any]:
    value = payload.get(field_name)
    if not isinstance(value, dict):
        result.errors.append(f"{label}: {field_name} must be a mapping")
        return {}
    return value


def _string_list(
    payload: dict[str, Any],
    field_name: str,
    label: str,
    result: BranchProtectionValidationResult,
) -> list[str]:
    value = payload.get(field_name)
    if not isinstance(value, list):
        result.errors.append(f"{label}: {field_name} must be a list")
        return []
    strings = [item for item in value if isinstance(item, str) and item.strip()]
    if len(strings) != len(value):
        result.errors.append(f"{label}: {field_name} must contain only non-empty strings")
    return strings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("policy", type=Path)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args(argv)

    result = validate_file(args.policy)
    payload = {"status": "passed" if result.ok else "failed", "errors": result.errors, "warnings": result.warnings}
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for warning in result.warnings:
            print(f"WARNING: {warning}")
        for error in result.errors:
            print(f"ERROR: {error}")
        print(
            f"GitHub branch protection policy validation: {payload['status'].upper()} "
            f"({len(result.errors)} errors, {len(result.warnings)} warnings)"
        )
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())

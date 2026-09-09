"""Compare repository branch-protection policy with live GitHub settings."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

BRANCH_PROTECTION_POLICY = ".agents/policy/github-branch-protection.yml"


@dataclass
class GitHubSettingsDriftResult:
    """Validation result for live GitHub repository settings."""

    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def load_sibling(module_name: str, filename: str) -> Any:
    """Load a sibling agent-policy script even when executed by file path."""

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


def load_policy(path: Path) -> dict[str, Any]:
    """Load the desired GitHub branch protection policy."""

    branch_protection = load_sibling("dpone_agent_branch_protection_for_drift", "branch_protection.py")
    return branch_protection.load_policy(path)


def load_json(path: Path) -> dict[str, Any]:
    """Load a JSON object from disk."""

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path}: JSON payload must be an object")
    return data


def fetch_live_settings(
    *,
    repo: str,
    ruleset_id: int,
    branch: str,
    include_branch_protection: bool,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Fetch live GitHub ruleset and optional classic branch protection payloads."""

    ruleset = _gh_api(f"repos/{repo}/rulesets/{ruleset_id}")
    branch_protection = None
    if include_branch_protection:
        branch_protection = _gh_api(f"repos/{repo}/branches/{branch}/protection")
    return ruleset, branch_protection


def validate_live_settings(
    policy: dict[str, Any],
    *,
    ruleset_payload: dict[str, Any],
    branch_protection_payload: dict[str, Any] | None,
    label: str,
    require_branch_protection: bool = True,
) -> GitHubSettingsDriftResult:
    """Compare live GitHub settings with repository policy."""

    result = GitHubSettingsDriftResult()
    if policy.get("schema_version") == 2:
        return _validate_live_settings_v2(
            policy,
            ruleset_payload=ruleset_payload,
            branch_protection_payload=branch_protection_payload,
            label=label,
            require_branch_protection=require_branch_protection,
        )
    branch_protection = load_sibling("dpone_agent_branch_protection_for_drift_validation", "branch_protection.py")
    policy_result = branch_protection.validate_policy(policy, label=BRANCH_PROTECTION_POLICY)
    result.errors.extend(policy_result.errors)
    result.warnings.extend(policy_result.warnings)
    if policy_result.errors:
        return result

    ruleset = _mapping(policy, "ruleset", label, result)
    pull_request = _mapping(ruleset, "pull_request", label, result)
    status_checks = _mapping(ruleset, "required_status_checks", label, result)

    _validate_ruleset_payload(ruleset_payload, ruleset, pull_request, status_checks, label, result)
    if branch_protection_payload is None:
        if not require_branch_protection:
            return result
        result.warnings.append(f"{label}: classic branch protection drift is UNVERIFIED")
    else:
        _validate_branch_protection_payload(branch_protection_payload, policy, status_checks, label, result)
    return result


def _validate_live_settings_v2(
    policy: dict[str, Any],
    *,
    ruleset_payload: dict[str, Any],
    branch_protection_payload: dict[str, Any] | None,
    label: str,
    require_branch_protection: bool,
) -> GitHubSettingsDriftResult:
    """Compare live settings using SS-46 governance projection for policy v2."""

    result = GitHubSettingsDriftResult()
    policy_v2 = load_sibling("dpone_agent_governance_policy_v2_for_drift", "governance_policy_v2.py")
    projection = load_sibling("dpone_agent_governance_projection_for_drift", "governance_projection.py")
    live = load_sibling("dpone_agent_governance_live_ruleset_for_drift", "governance_live_ruleset.py")
    try:
        parsed = policy_v2.parse_governance_policy(policy)
        expected = projection.canonical_ruleset_projection(parsed.branch_view.ruleset)
        observed = live.live_ruleset_projection(ruleset_payload)
    except (TypeError, ValueError) as exc:
        result.errors.append(f"{label}: governance policy v2 compare failed: {exc}")
        return result
    for blocker in projection.compare_ruleset_projection(expected, observed):
        result.errors.append(f"{label}: {blocker.code}: {blocker.message}")
    if branch_protection_payload is None and require_branch_protection:
        result.warnings.append(f"{label}: classic branch protection drift is UNVERIFIED")
    return result


def _validate_ruleset_payload(
    payload: dict[str, Any],
    desired_ruleset: dict[str, Any],
    desired_pull_request: dict[str, Any],
    desired_status_checks: dict[str, Any],
    label: str,
    result: GitHubSettingsDriftResult,
) -> None:
    if payload.get("name") != desired_ruleset.get("name"):
        result.errors.append(f"{label}: ruleset.name drifted from policy")
    if payload.get("enforcement") != "active":
        result.errors.append(f"{label}: ruleset enforcement must be active")

    rules = payload.get("rules")
    if not isinstance(rules, list):
        result.errors.append(f"{label}: ruleset rules must be a list")
        return
    rule_by_type = {rule.get("type"): rule for rule in rules if isinstance(rule, dict)}
    for required_rule in ("deletion", "non_fast_forward", "pull_request", "required_status_checks"):
        if required_rule not in rule_by_type:
            result.errors.append(f"{label}: ruleset missing {required_rule} rule")

    pull_rule = rule_by_type.get("pull_request")
    if isinstance(pull_rule, dict):
        params = _mapping(pull_rule, "parameters", label, result)
        for field_name in (
            "dismiss_stale_reviews_on_push",
            "require_code_owner_review",
            "require_last_push_approval",
            "required_approving_review_count",
            "required_review_thread_resolution",
        ):
            if params.get(field_name) != desired_pull_request.get(field_name):
                result.errors.append(f"{label}: ruleset pull_request.{field_name} drifted from policy")
        if set(_list(params.get("allowed_merge_methods"))) != set(
            _list(desired_pull_request.get("allowed_merge_methods"))
        ):
            result.errors.append(f"{label}: ruleset pull_request.allowed_merge_methods drifted from policy")

    status_rule = rule_by_type.get("required_status_checks")
    if isinstance(status_rule, dict):
        params = _mapping(status_rule, "parameters", label, result)
        if params.get("strict_required_status_checks_policy") is not desired_status_checks.get("strict"):
            result.errors.append(f"{label}: ruleset required_status_checks strict mode drifted from policy")
        actual = _status_check_contexts(params.get("required_status_checks"))
        expected = set(_list(desired_status_checks.get("checks")))
        _compare_contexts(actual, expected, f"{label}: ruleset required_status_checks", result)


def _validate_branch_protection_payload(
    payload: dict[str, Any],
    policy: dict[str, Any],
    desired_status_checks: dict[str, Any],
    label: str,
    result: GitHubSettingsDriftResult,
) -> None:
    if policy.get("mode") == "solo_maintainer" and payload.get("required_pull_request_reviews") is not None:
        result.errors.append(f"{label}: classic branch protection must not require pull request reviews")
    if payload.get("required_conversation_resolution", {}).get("enabled") is not True:
        result.errors.append(f"{label}: classic branch protection must require conversation resolution")
    if payload.get("allow_force_pushes", {}).get("enabled") is not False:
        result.errors.append(f"{label}: classic branch protection must block force pushes")
    if payload.get("allow_deletions", {}).get("enabled") is not False:
        result.errors.append(f"{label}: classic branch protection must block deletions")

    status_checks = payload.get("required_status_checks")
    if not isinstance(status_checks, dict):
        result.errors.append(f"{label}: classic branch protection required_status_checks must be present")
        return
    if status_checks.get("strict") is not desired_status_checks.get("strict"):
        result.errors.append(f"{label}: classic branch protection status-check strict mode drifted from policy")
    actual = set(_list(status_checks.get("contexts")))
    expected = set(_list(desired_status_checks.get("checks")))
    _compare_contexts(actual, expected, f"{label}: classic branch protection required_status_checks", result)


def _compare_contexts(
    actual: set[str],
    expected: set[str],
    label: str,
    result: GitHubSettingsDriftResult,
) -> None:
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing:
        result.errors.append(f"{label} missing {', '.join(missing)}")
    if extra:
        result.errors.append(f"{label} contains unknown checks: {', '.join(extra)}")


def _status_check_contexts(value: Any) -> set[str]:
    contexts: set[str] = set()
    if not isinstance(value, list):
        return contexts
    for item in value:
        if isinstance(item, dict) and isinstance(item.get("context"), str):
            contexts.add(item["context"])
        elif isinstance(item, str):
            contexts.add(item)
    return contexts


def _mapping(payload: dict[str, Any], field_name: str, label: str, result: GitHubSettingsDriftResult) -> dict[str, Any]:
    value = payload.get(field_name)
    if not isinstance(value, dict):
        result.errors.append(f"{label}: {field_name} must be a mapping")
        return {}
    return value


def _list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _gh_api(endpoint: str) -> dict[str, Any]:
    completed = subprocess.run(("gh", "api", endpoint), check=True, text=True, capture_output=True)
    data = json.loads(completed.stdout)
    if not isinstance(data, dict):
        raise ValueError(f"GitHub API endpoint {endpoint} did not return a JSON object")
    return data


def _ruleset_id(policy: dict[str, Any], explicit: int | None) -> int:
    if explicit is not None:
        return explicit
    ruleset = policy.get("ruleset")
    if isinstance(ruleset, dict) and isinstance(ruleset.get("id"), int):
        return ruleset["id"]
    raise ValueError("ruleset id is required; pass --ruleset-id or set ruleset.id in policy")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", default=BRANCH_PROTECTION_POLICY, type=Path)
    parser.add_argument("--ruleset-json", type=Path)
    parser.add_argument("--branch-protection-json", type=Path)
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--ruleset-id", type=int)
    parser.add_argument("--branch", default="master")
    parser.add_argument("--skip-branch-protection", action="store_true")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args(argv)

    try:
        policy = load_policy(args.policy)
        if args.ruleset_json:
            ruleset_payload = load_json(args.ruleset_json)
            branch_payload = load_json(args.branch_protection_json) if args.branch_protection_json else None
        else:
            if not args.repo:
                raise ValueError("--repo or GITHUB_REPOSITORY is required when --ruleset-json is not provided")
            ruleset_payload, branch_payload = fetch_live_settings(
                repo=args.repo,
                ruleset_id=_ruleset_id(policy, args.ruleset_id),
                branch=args.branch,
                include_branch_protection=not args.skip_branch_protection,
            )
        result = validate_live_settings(
            policy,
            ruleset_payload=ruleset_payload,
            branch_protection_payload=branch_payload,
            label=args.repo or "captured GitHub settings",
            require_branch_protection=not args.skip_branch_protection,
        )
    except (OSError, ValueError, json.JSONDecodeError, subprocess.CalledProcessError) as exc:
        result = GitHubSettingsDriftResult(errors=[f"cannot validate GitHub settings drift: {exc}"])

    payload = {"status": "passed" if result.ok else "failed", "errors": result.errors, "warnings": result.warnings}
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for warning in result.warnings:
            print(f"WARNING: {warning}")
        for error in result.errors:
            print(f"ERROR: {error}")
        print(
            f"GitHub settings drift validation: {payload['status'].upper()} "
            f"({len(result.errors)} errors, {len(result.warnings)} warnings)"
        )
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


branch_protection = _load("dpone_agent_branch_protection_for_drift_tests", "tools/agent_policy/branch_protection.py")
github_settings_drift = _load(
    "dpone_agent_github_settings_drift",
    "tools/agent_policy/github_settings_drift.py",
)


def _policy() -> dict[str, object]:
    return branch_protection.load_policy(ROOT / ".agents/policy/github-branch-protection.yml")


def _ruleset_payload(policy: dict[str, object]) -> dict[str, object]:
    ruleset = policy["ruleset"]
    assert isinstance(ruleset, dict)
    pull_request = ruleset["pull_request"]
    status_checks = ruleset["required_status_checks"]
    assert isinstance(pull_request, dict)
    assert isinstance(status_checks, dict)
    return {
        "name": ruleset["name"],
        "enforcement": "active",
        "rules": [
            {"type": "deletion"},
            {"type": "non_fast_forward"},
            {
                "type": "pull_request",
                "parameters": {
                    "allowed_merge_methods": pull_request["allowed_merge_methods"],
                    "dismiss_stale_reviews_on_push": pull_request["dismiss_stale_reviews_on_push"],
                    "require_code_owner_review": pull_request["require_code_owner_review"],
                    "require_last_push_approval": pull_request["require_last_push_approval"],
                    "required_approving_review_count": pull_request["required_approving_review_count"],
                    "required_review_thread_resolution": pull_request["required_review_thread_resolution"],
                    "required_reviewers": [],
                },
            },
            {
                "type": "required_status_checks",
                "parameters": {
                    "strict_required_status_checks_policy": status_checks["strict"],
                    "required_status_checks": [{"context": check} for check in status_checks["checks"]],
                },
            },
        ],
    }


def _branch_protection_payload(policy: dict[str, object]) -> dict[str, object]:
    ruleset = policy["ruleset"]
    assert isinstance(ruleset, dict)
    status_checks = ruleset["required_status_checks"]
    assert isinstance(status_checks, dict)
    return {
        "required_pull_request_reviews": None,
        "required_conversation_resolution": {"enabled": True},
        "allow_force_pushes": {"enabled": False},
        "allow_deletions": {"enabled": False},
        "required_status_checks": {
            "strict": status_checks["strict"],
            "contexts": status_checks["checks"],
        },
    }


def test_live_github_settings_match_repository_policy() -> None:
    policy = _policy()
    result = github_settings_drift.validate_live_settings(
        policy,
        ruleset_payload=_ruleset_payload(policy),
        branch_protection_payload=_branch_protection_payload(policy),
        label="live",
    )

    assert result.errors == []


def test_live_github_settings_detect_missing_required_check() -> None:
    policy = _policy()
    ruleset = _ruleset_payload(policy)
    required_rule = next(rule for rule in ruleset["rules"] if rule["type"] == "required_status_checks")
    required_rule["parameters"]["required_status_checks"].pop()

    result = github_settings_drift.validate_live_settings(
        policy,
        ruleset_payload=ruleset,
        branch_protection_payload=_branch_protection_payload(policy),
        label="live",
    )

    assert any("ruleset required_status_checks missing" in error for error in result.errors)


def test_live_github_settings_detect_classic_review_deadlock() -> None:
    policy = _policy()
    branch_payload = _branch_protection_payload(policy)
    branch_payload["required_pull_request_reviews"] = {"required_approving_review_count": 1}

    result = github_settings_drift.validate_live_settings(
        policy,
        ruleset_payload=_ruleset_payload(policy),
        branch_protection_payload=branch_payload,
        label="live",
    )

    assert any("classic branch protection must not require pull request reviews" in error for error in result.errors)


def test_github_settings_drift_cli_reads_captured_json(tmp_path: Path, capsys) -> None:
    policy = _policy()
    ruleset_path = tmp_path / "ruleset.json"
    branch_path = tmp_path / "branch-protection.json"
    ruleset_path.write_text(json.dumps(_ruleset_payload(policy)), encoding="utf-8")
    branch_path.write_text(json.dumps(_branch_protection_payload(policy)), encoding="utf-8")

    code = github_settings_drift.main(
        [
            "--policy",
            str(ROOT / ".agents/policy/github-branch-protection.yml"),
            "--ruleset-json",
            str(ruleset_path),
            "--branch-protection-json",
            str(branch_path),
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "passed"


def test_github_settings_drift_ruleset_only_cli_does_not_warn_about_classic(
    tmp_path: Path,
    capsys,
) -> None:
    policy = _policy()
    ruleset_path = tmp_path / "ruleset.json"
    ruleset_path.write_text(json.dumps(_ruleset_payload(policy)), encoding="utf-8")

    code = github_settings_drift.main(
        [
            "--policy",
            str(ROOT / ".agents/policy/github-branch-protection.yml"),
            "--ruleset-json",
            str(ruleset_path),
            "--skip-branch-protection",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload == {"errors": [], "status": "passed", "warnings": []}

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


branch_protection = _load("dpone_agent_branch_protection", "tools/agent_policy/branch_protection.py")


def test_github_branch_protection_policy_is_valid() -> None:
    result = branch_protection.validate_file(ROOT / ".agents/policy/github-branch-protection.yml")

    assert result.errors == []


def test_solo_maintainer_policy_rejects_required_self_review() -> None:
    policy = branch_protection.load_policy(ROOT / ".agents/policy/github-branch-protection.yml")
    policy["ruleset"]["pull_request"]["required_approving_review_count"] = 1

    result = branch_protection.validate_policy(policy, label="policy.yml")

    assert any("solo_maintainer mode requires required_approving_review_count=0" in error for error in result.errors)


def test_solo_maintainer_policy_rejects_code_owner_review_deadlock() -> None:
    policy = branch_protection.load_policy(ROOT / ".agents/policy/github-branch-protection.yml")
    policy["ruleset"]["pull_request"]["require_code_owner_review"] = True

    result = branch_protection.validate_policy(policy, label="policy.yml")

    assert any("solo_maintainer mode requires require_code_owner_review=false" in error for error in result.errors)


def test_branch_protection_schema_is_closed_json_contract() -> None:
    data = json.loads((ROOT / "evals/agent/github-branch-protection.schema.json").read_text(encoding="utf-8"))

    assert data["type"] == "object"
    assert data["additionalProperties"] is False
    assert "ruleset" in data["required"]


def test_branch_protection_cli_reports_json(capsys) -> None:
    code = branch_protection.main([str(ROOT / ".agents/policy/github-branch-protection.yml"), "--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "passed"


def test_governance_policy_v2_fixture_fails_incomplete_context_contract() -> None:
    import yaml

    payload = yaml.safe_load(
        (ROOT / "tests/fixtures/agent_policy/governance_policy_v2_fixture.yml").read_text(encoding="utf-8")
    )
    result = branch_protection.validate_policy(payload, label="v2-fixture.yml")
    assert result.errors
    assert any("required contexts missing" in error for error in result.errors)

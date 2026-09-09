from __future__ import annotations

import importlib.util
import json
import sys
import tomllib
from pathlib import Path
from types import ModuleType

import yaml

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


select_checks = _load("dpone_agent_select_checks", "tools/agent_policy/select_checks.py")
validate_setup = _load("dpone_agent_validate_setup", "tools/agent_policy/validate_setup.py")
permissions = _load("dpone_agent_permissions", "tools/agent_policy/permissions.py")


def test_agent_setup_is_valid() -> None:
    errors, _warnings = validate_setup.validate(ROOT)
    assert errors == []


def test_agent_toml_and_skill_metadata_are_parseable() -> None:
    names: set[str] = set()
    for path in (ROOT / ".codex/agents").glob("*.toml"):
        data = tomllib.loads(path.read_text(encoding="utf-8"))
        assert data["name"] not in names
        names.add(data["name"])
        assert data["description"]
        assert data["developer_instructions"]

    skill_names: set[str] = set()
    for path in (ROOT / ".agents/skills").glob("*/SKILL.md"):
        _, raw, _ = path.read_text(encoding="utf-8").split("---", 2)
        data = yaml.safe_load(raw)
        assert data["name"] == path.parent.name
        assert data["name"] not in skill_names
        skill_names.add(data["name"])
        assert data["description"]


def test_check_selection_covers_high_risk_surfaces() -> None:
    assert "cli" in select_checks.categories_for_path("src/dpone/commands/run.py")
    assert "nested_identity" in select_checks.categories_for_path("src/dpone/runtime/nested_normalization.py")
    assert "airflow" in select_checks.categories_for_path("packages/dpone-airflow-pack/src/provider.py")
    assert "airflow" in select_checks.categories_for_path(
        "packages/apache-airflow-providers-dpone/src/airflow/providers/dpone/__init__.py"
    )
    payload = select_checks.plan(
        [
            "src/dpone/commands/run.py",
            "src/dpone/runtime/nested_normalization.py",
            "docs/run.md",
        ]
    )
    commands = payload["commands"]
    assert any("pytest" in command and "cli" in command for command in commands)
    assert "uv run mkdocs build --strict" in commands
    root_module_gate = next(
        command for command in commands if "check-module-size --baseline docs/module_size_baseline.json" in command
    )
    assert 'HEAD_SHA="$(git rev-parse HEAD)"' in root_module_gate
    assert 'BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"' in root_module_gate
    assert 'if [ "$BASE_SHA" = "$HEAD_SHA" ]' in root_module_gate
    assert '--base-ref "$BASE_SHA" --head-ref "$HEAD_SHA"' in root_module_gate
    assert payload["live_evidence_review_required"] is False


def test_airflow_validation_builds_reader_and_formal_provider() -> None:
    payload = select_checks.plan(["packages/apache-airflow-providers-dpone/src/airflow/providers/dpone/__init__.py"])

    assert "uv build packages/dpone-airflow-pack --out-dir dist" in payload["commands"]
    assert "uv build packages/apache-airflow-providers-dpone --out-dir dist" in payload["commands"]
    assert "uv run dpone docs check-airflow-public-contracts" in payload["commands"]


def test_nested_agent_instructions_do_not_trigger_package_builds() -> None:
    payload = select_checks.plan(["packages/dpone-airflow-pack/AGENTS.md"])
    assert "agent_policy" in payload["categories"]
    assert "packaging" not in payload["categories"]
    assert not any(command.startswith("uv build") for command in payload["commands"])


def test_workflow_security_is_a_manual_review_not_a_shell_command() -> None:
    payload = select_checks.plan([".github/workflows/ci.yml"])
    assert payload["manual_reviews"]
    assert all(not command.startswith("Review ") for command in payload["commands"])
    assert "R8 packaging/security/supply chain" in payload["release_gates"]


def test_agent_governance_documents_are_required_and_routed() -> None:
    expected = {
        ".agents/policy/agent-permission-profile.yml",
        ".agents/policy/github-branch-protection.yml",
        ".agents/policy/mcp-connector-onboarding.yml",
        ".agents/policy/tool-registry.yml",
        ".agents/policy/workflow-security.yml",
        ".agents/policy/workflow-security-privileged.yml",
        ".agents/policy/workflow-security-privileged-v2.yml",
        ".agents/policy/workflow-security-privileged-v3.yml",
        ".github/workflows/agent-governance-drift.yml",
        ".github/workflows/agent-pr-receipt.yml",
        "docs/agent-governance.md",
        "docs/agent-pr-merge-receipt-runbook.md",
        "docs/agent-mcp-connectors.md",
        "docs/agent-permissions.md",
        "docs/agent-task-contracts.md",
        "docs/agent-risk-register.md",
        "docs/agent-security-mapping.md",
        "docs/feature-design-agent-task-contract-gate.md",
        "docs/feature-design-agent-permission-tool-registry.md",
        "docs/feature-design-mcp-connector-onboarding-gate.md",
        "evals/agent/agent-task-contract.schema.json",
        "evals/agent/audit-manifest.schema.json",
        "docs/github-branch-protection.md",
        "docs/supply-chain-slsa.md",
        "evals/agent/agent-permission-profile.schema.json",
        "evals/agent/github-branch-protection.schema.json",
        "evals/agent/mcp-connector-onboarding.schema.json",
        "evals/agent/tool-registry.schema.json",
        "evals/agent/workflow-security.schema.json",
        "evals/agent/workflow-security-privileged-policy.schema.json",
        "evals/agent/workflow-security-privileged-policy-v2.schema.json",
        "evals/agent/workflow-security-privileged-policy-v3.schema.json",
        "evals/agent/workflow-security-privileged-report.schema.json",
        "evals/agent/governance-drift-summary.schema.json",
        "evals/agent/pr-merge-receipt.schema.json",
        "evals/agent/pr-receipt.schema.json",
        "tools/agent_policy/audit_manifest.py",
        "tools/agent_policy/github_settings_drift.py",
        "tools/agent_policy/governance_artifact_archive.py",
        "tools/agent_policy/governance_artifact_attestation.py",
        "tools/agent_policy/governance_artifact_content.py",
        "tools/agent_policy/governance_drift_summary.py",
        "tools/agent_policy/control_surface.py",
        "tools/agent_policy/pr_body.py",
        "tools/agent_policy/pr_merge_check.py",
        "tools/agent_policy/pr_merge_binding.py",
        "tools/agent_policy/release_merge_receipt_gate.py",
        "tools/agent_policy/release_merge_receipt_reconcile.py",
        "tools/agent_policy/pr_merge_identity.py",
        "tools/agent_policy/pr_merge_receipt.py",
        "tools/agent_policy/pr_merge_receipt_contract.py",
        "tools/agent_policy/path_evidence.py",
        "tools/agent_policy/pr_receipt.py",
        "tools/agent_policy/pr_receipt_wait.py",
        "tools/agent_policy/pr_receipt_github.py",
        "tools/agent_policy/pr_receipt_github_attestation.py",
        "tools/agent_policy/pr_receipt_github_validation.py",
        "tools/agent_policy/pr_receipt_source.py",
        "tools/agent_policy/pr_receipt_source_archive.py",
        "tools/agent_policy/pr_traceability.py",
        "tools/agent_policy/workflow_artifacts.py",
        "tools/agent_policy/workflow_security.py",
        "tools/agent_policy/workflow_privilege_contracts.py",
        "tools/agent_policy/workflow_privilege_composition.py",
        "tools/agent_policy/workflow_privilege_snapshot.py",
        "tools/agent_policy/workflow_privilege_policy_selection.py",
        "tools/agent_policy/workflow_privilege_parser.py",
        "tools/agent_policy/workflow_privilege_graph.py",
        "tools/agent_policy/workflow_privilege_expressions.py",
        "tools/agent_policy/workflow_privilege_permissions.py",
        "tools/agent_policy/workflow_privilege_profiles.py",
        "tools/agent_policy/workflow_privilege_profile_support.py",
        "tools/agent_policy/workflow_privilege_report.py",
        "tools/agent_policy/workflow_privilege_service.py",
        "tools/agent_policy/workflow_security_privileged.py",
    }

    assert expected <= set(validate_setup.REQUIRED_FILES)

    payload = select_checks.plan(["docs/agent-security-mapping.md"])
    assert {"agent_policy", "docs"} <= set(payload["categories"])
    assert any(
        command.startswith("uv run python tools/agent_policy/governance_gate.py") for command in payload["commands"]
    )
    assert any(
        command.startswith("uv run python tools/agent_policy/branch_protection.py") for command in payload["commands"]
    )
    assert any(
        command.startswith("uv run python tools/agent_policy/workflow_security.py") for command in payload["commands"]
    )
    assert any("check-module-size --package tools/agent_policy" in command for command in payload["commands"])
    assert "uv run python tools/agent_policy/validate_setup.py ." in payload["commands"]
    assert "uv run mkdocs build --strict" in payload["commands"]


def test_agent_permission_policy_is_valid_and_mapped_to_tools() -> None:
    result = permissions.validate_all(ROOT)

    assert result.errors == []
    profile = permissions.load_permission_profile(ROOT / ".agents/policy/agent-permission-profile.yml")
    registry = permissions.load_tool_registry(ROOT / ".agents/policy/tool-registry.yml")
    profile_ids = {entry["id"] for entry in profile["profiles"]}
    tool_ids = {entry["id"] for entry in registry["tools"]}

    assert permissions.REQUIRED_PROFILE_IDS <= profile_ids
    assert "integrator" in profile_ids
    assert "mcp_connector" in tool_ids
    assert all("secrets.read" in entry["forbidden_tools"] for entry in profile["profiles"])
    assert all(set(entry["allowed_tools"]) <= tool_ids for entry in profile["profiles"])


def test_mcp_connector_onboarding_is_valid_and_mapped_to_registry() -> None:
    result = permissions.validate_all(ROOT)

    assert result.errors == []
    registry = permissions.load_tool_registry(ROOT / ".agents/policy/tool-registry.yml")
    onboarding = permissions.load_mcp_connector_onboarding(ROOT / ".agents/policy/mcp-connector-onboarding.yml")
    mcp_tool_ids = {entry["id"] for entry in registry["tools"] if entry["category"] == "mcp"}
    onboarded_tool_ids = {entry["tool_registry_id"] for entry in onboarding["connectors"]}

    assert permissions.REQUIRED_MCP_ONBOARDING_CONTROLS <= set(onboarding["required_controls"])
    assert mcp_tool_ids <= onboarded_tool_ids
    assert all(entry["status"] == "approved" for entry in onboarding["connectors"])
    assert all(entry["connector_kind"] in permissions.VALID_CONNECTOR_KINDS for entry in onboarding["connectors"])
    assert all("prompt injection boundary" in entry["required_controls"] for entry in onboarding["connectors"])


def test_mcp_connector_onboarding_rejects_scope_expansion(tmp_path: Path) -> None:
    profile = permissions.load_permission_profile(ROOT / ".agents/policy/agent-permission-profile.yml")
    registry = permissions.load_tool_registry(ROOT / ".agents/policy/tool-registry.yml")
    onboarding = permissions.load_mcp_connector_onboarding(ROOT / ".agents/policy/mcp-connector-onboarding.yml")
    onboarding["connectors"][0]["allowed_scopes"].append("unreviewed admin scope")
    policy_dir = tmp_path / ".agents/policy"
    policy_dir.mkdir(parents=True)
    (policy_dir / "agent-permission-profile.yml").write_text(yaml.safe_dump(profile), encoding="utf-8")
    (policy_dir / "tool-registry.yml").write_text(yaml.safe_dump(registry), encoding="utf-8")
    (policy_dir / "mcp-connector-onboarding.yml").write_text(yaml.safe_dump(onboarding), encoding="utf-8")

    result = permissions.validate_all(tmp_path)

    assert any("allowed_scopes exceed tool registry" in error for error in result.errors)


def test_mcp_connector_onboarding_rejects_missing_required_controls(tmp_path: Path) -> None:
    profile = permissions.load_permission_profile(ROOT / ".agents/policy/agent-permission-profile.yml")
    registry = permissions.load_tool_registry(ROOT / ".agents/policy/tool-registry.yml")
    onboarding = permissions.load_mcp_connector_onboarding(ROOT / ".agents/policy/mcp-connector-onboarding.yml")
    onboarding["connectors"][0]["required_controls"].remove("prompt injection boundary")
    policy_dir = tmp_path / ".agents/policy"
    policy_dir.mkdir(parents=True)
    (policy_dir / "agent-permission-profile.yml").write_text(yaml.safe_dump(profile), encoding="utf-8")
    (policy_dir / "tool-registry.yml").write_text(yaml.safe_dump(registry), encoding="utf-8")
    (policy_dir / "mcp-connector-onboarding.yml").write_text(yaml.safe_dump(onboarding), encoding="utf-8")

    result = permissions.validate_all(tmp_path)

    assert any("missing required MCP onboarding controls" in error for error in result.errors)


def test_agent_permission_policy_rejects_allowed_forbidden_overlap(tmp_path: Path) -> None:
    profile = permissions.load_permission_profile(ROOT / ".agents/policy/agent-permission-profile.yml")
    registry = permissions.load_tool_registry(ROOT / ".agents/policy/tool-registry.yml")
    profile["profiles"][0]["allowed_tools"].append("secrets.read")
    policy_dir = tmp_path / ".agents/policy"
    policy_dir.mkdir(parents=True)
    (policy_dir / "agent-permission-profile.yml").write_text(yaml.safe_dump(profile), encoding="utf-8")
    (policy_dir / "tool-registry.yml").write_text(yaml.safe_dump(registry), encoding="utf-8")

    result = permissions.validate_all(tmp_path)

    assert any("cannot be both allowed and forbidden" in error for error in result.errors)


def test_admin_bypass_issue_template_is_required_and_owner_reviewed() -> None:
    relative = ".github/ISSUE_TEMPLATE/admin_bypass.yml"
    path = ROOT / relative

    assert relative in validate_setup.REQUIRED_FILES
    assert path.is_file()
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert payload["name"] == "Admin bypass / break-glass"
    assert any(
        field.get("id") == "rollback_plan" and field["validations"]["required"] is True for field in payload["body"]
    )
    codeowners = (ROOT / ".github/CODEOWNERS").read_text(encoding="utf-8")
    assert "/.github/ISSUE_TEMPLATE/" in codeowners


def test_agent_red_team_scenarios_are_required_and_documented() -> None:
    expected = {
        "connector scope escalation",
        "prompt injection",
        "secret disclosure",
        "workflow tampering",
        "verification laundering",
        "excessive agency",
        "unbounded consumption",
        "unregistered tool use",
        "github settings drift",
    }

    assert expected == set(validate_setup.REQUIRED_RED_TEAM_SCENARIOS)

    combined = "\n".join(
        (ROOT / relative).read_text(encoding="utf-8").lower()
        for relative in (
            "docs/agent-governance.md",
            "docs/agent-risk-register.md",
            "docs/agent-security-mapping.md",
        )
    )
    for scenario in expected:
        assert scenario in combined


def test_agent_red_team_scenarios_are_executable_policy_inputs() -> None:
    scenario_path = ROOT / "evals/agent/red_team_scenarios.yml"

    assert "evals/agent/red_team_scenarios.yml" in validate_setup.REQUIRED_FILES
    assert scenario_path.is_file()
    payload = yaml.safe_load(scenario_path.read_text(encoding="utf-8"))
    assert payload["version"] == 1
    assert isinstance(payload["scenarios"], list)
    assert len(payload["scenarios"]) >= len(validate_setup.REQUIRED_RED_TEAM_SCENARIOS)

    red_team = _load("dpone_agent_red_team", "tools/agent_policy/red_team.py")
    report = red_team.validate_file(scenario_path)
    assert report.errors == []
    scenario_text = "\n".join(
        f"{scenario['id']} {scenario['title']} {scenario['risk']}".lower() for scenario in payload["scenarios"]
    )
    for scenario in validate_setup.REQUIRED_RED_TEAM_SCENARIOS:
        assert scenario in scenario_text


def test_changed_paths_keeps_deleted_files_in_scope(monkeypatch) -> None:
    def fake_git(*args: str) -> list[str]:
        if args == ("merge-base", "origin/master", "HEAD"):
            return ["base"]
        if args == ("diff", "--name-only", "--diff-filter=ACDMRT", "base...HEAD"):
            return ["src/dpone/commands/deleted.py"]
        if args in {
            ("diff", "--name-only", "--diff-filter=ACDMRT"),
            ("diff", "--cached", "--name-only", "--diff-filter=ACDMRT"),
            ("ls-files", "--others", "--exclude-standard"),
        }:
            return []
        return []

    monkeypatch.setattr(select_checks, "_git", fake_git)

    assert select_checks.changed_paths("origin/master") == ["src/dpone/commands/deleted.py"]


def test_agent_result_schema_is_json() -> None:
    data = json.loads((ROOT / "evals/agent/result.schema.json").read_text(encoding="utf-8"))
    assert data["type"] == "object"
    assert "status" in data["required"]


def test_agent_governance_gate_schema_is_json() -> None:
    data = json.loads((ROOT / "evals/agent/governance-gate.schema.json").read_text(encoding="utf-8"))
    assert data["type"] == "object"
    assert "checks" in data["required"]


def test_agent_permission_and_tool_registry_schemas_are_json() -> None:
    for relative in (
        "evals/agent/agent-permission-profile.schema.json",
        "evals/agent/agent-task-contract.schema.json",
        "evals/agent/github-branch-protection.schema.json",
        "evals/agent/mcp-connector-onboarding.schema.json",
        "evals/agent/tool-registry.schema.json",
    ):
        data = json.loads((ROOT / relative).read_text(encoding="utf-8"))
        assert data["type"] == "object"
        assert data["additionalProperties"] is False

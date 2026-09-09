from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


governance_gate = _load("dpone_agent_governance_gate", "tools/agent_policy/governance_gate.py")


def test_governance_gate_passes_on_current_control_surface() -> None:
    report = governance_gate.build_report(
        ROOT,
        [
            "docs/agent-governance.md",
            ".agents/policy/agent-permission-profile.yml",
            ".agents/policy/mcp-connector-onboarding.yml",
            ".agents/policy/tool-registry.yml",
            "tools/agent_policy/governance_gate.py",
            "docs/supply-chain-slsa.md",
        ],
    )

    assert report["status"] == "PASS"
    assert report["control_surface_changed"] is True
    checks = {check["name"]: check for check in report["checks"]}
    assert checks["agent_policy_setup"]["status"] == "PASS"
    assert checks["agent_red_team_catalog"]["status"] == "PASS"
    assert checks["changed_control_surface_red_team"]["status"] == "PASS"
    assert checks["release_artifact_attestations"]["status"] == "PASS"
    assert checks["ossf_scorecard"]["status"] == "PASS"
    assert checks["slsa_self_assessment"]["status"] == "PASS"
    assert checks["agent_permission_profile"]["status"] == "PASS"
    assert checks["agent_tool_registry"]["status"] == "PASS"
    assert checks["agent_mcp_connector_onboarding"]["status"] == "PASS"
    assert checks["agent_task_contract_template"]["status"] == "PASS"
    assert checks["github_branch_protection_policy"]["status"] == "PASS"
    assert checks["github_workflow_security_policy"]["status"] == "PASS"


def test_governance_gate_marks_non_control_surface_as_not_applicable() -> None:
    report = governance_gate.build_report(ROOT, ["docs/run.md"])
    checks = {check["name"]: check for check in report["checks"]}

    assert report["status"] == "PASS"
    assert report["control_surface_changed"] is False
    assert checks["changed_control_surface_red_team"]["status"] == "N/A"


def test_governance_gate_fails_when_slsa_self_assessment_is_missing(monkeypatch) -> None:
    monkeypatch.setattr(governance_gate, "SLSA_SELF_ASSESSMENT_DOC", "docs/missing-slsa.md")

    report = governance_gate.build_report(ROOT, [])
    checks = {check["name"]: check for check in report["checks"]}

    assert report["status"] == "FAIL"
    assert checks["slsa_self_assessment"]["status"] == "FAIL"
    assert "docs/missing-slsa.md" in checks["slsa_self_assessment"]["details"]


def test_governance_gate_fails_when_release_attestation_workflow_is_missing(monkeypatch) -> None:
    monkeypatch.setattr(governance_gate, "RELEASE_WORKFLOW", ".github/workflows/missing-release.yml")

    report = governance_gate.build_report(ROOT, [])
    checks = {check["name"]: check for check in report["checks"]}

    assert report["status"] == "FAIL"
    assert checks["release_artifact_attestations"]["status"] == "FAIL"


def test_governance_gate_fails_when_permission_profile_is_missing(monkeypatch) -> None:
    monkeypatch.setattr(governance_gate, "PERMISSION_PROFILE", ".agents/policy/missing-permissions.yml")

    report = governance_gate.build_report(ROOT, [])
    checks = {check["name"]: check for check in report["checks"]}

    assert report["status"] == "FAIL"
    assert checks["agent_permission_profile"]["status"] == "FAIL"
    assert ".agents/policy/missing-permissions.yml" in checks["agent_permission_profile"]["details"]


def test_governance_gate_fails_when_tool_registry_is_missing(monkeypatch) -> None:
    monkeypatch.setattr(governance_gate, "TOOL_REGISTRY", ".agents/policy/missing-tools.yml")

    report = governance_gate.build_report(ROOT, [])
    checks = {check["name"]: check for check in report["checks"]}

    assert report["status"] == "FAIL"
    assert checks["agent_tool_registry"]["status"] == "FAIL"
    assert ".agents/policy/missing-tools.yml" in checks["agent_tool_registry"]["details"]


def test_governance_gate_fails_when_mcp_connector_onboarding_is_missing(monkeypatch) -> None:
    monkeypatch.setattr(governance_gate, "MCP_CONNECTOR_ONBOARDING", ".agents/policy/missing-mcp-onboarding.yml")

    report = governance_gate.build_report(ROOT, [])
    checks = {check["name"]: check for check in report["checks"]}

    assert report["status"] == "FAIL"
    assert checks["agent_mcp_connector_onboarding"]["status"] == "FAIL"
    assert ".agents/policy/missing-mcp-onboarding.yml" in checks["agent_mcp_connector_onboarding"]["details"]


def test_governance_gate_writes_json_receipt(tmp_path: Path) -> None:
    report = governance_gate.build_report(ROOT, ["AGENTS.md"], base_ref="origin/master")
    output = tmp_path / "agent_governance_gate.json"

    governance_gate.write_report(report, output)
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert payload["schema_version"] == 1
    assert payload["status"] == "PASS"
    assert payload["base_ref"] == "origin/master"
    assert payload["changed_paths"] == ["AGENTS.md"]
    assert any(standard["name"] == "SLSA Build Track" for standard in payload["standards"])
    assert any(standard["name"] == "Model Context Protocol Security" for standard in payload["standards"])


def test_governance_gate_can_bind_report_to_reviewed_head() -> None:
    reviewed_head = "a" * 40

    report = governance_gate.build_report(
        ROOT,
        ["tools/agent_policy/governance_gate.py"],
        head_commit=reviewed_head,
    )

    assert report["head_commit"] == reviewed_head


@pytest.mark.parametrize("invalid_head", ["", "a" * 39, "a" * 41, "A" * 40])
def test_governance_gate_rejects_invalid_explicit_head(invalid_head: str) -> None:
    with pytest.raises(ValueError, match="40-character lowercase Git SHA"):
        governance_gate.build_report(ROOT, [], head_commit=invalid_head)

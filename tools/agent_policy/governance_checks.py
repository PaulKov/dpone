"""Focused control checks used by the agent governance gate."""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

Status = Literal["PASS", "FAIL", "N/A"]


@dataclass(frozen=True)
class ControlCheck:
    """Single machine-readable governance control result."""

    name: str
    status: Status
    details: str
    artifact: str | None = None


def load_sibling(module_name: str, filename: str) -> Any:
    """Load a sibling agent-policy script even when files are imported by path."""

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


def check_agent_setup(root: Path) -> ControlCheck:
    validate_setup = load_sibling("dpone_agent_validate_setup_gate", "validate_setup.py")
    errors, warnings = validate_setup.validate(root)
    if errors:
        return ControlCheck(
            name="agent_policy_setup",
            status="FAIL",
            details="; ".join(errors),
            artifact="tools/agent_policy/validate_setup.py",
        )
    warning_text = f" Warnings: {'; '.join(warnings)}" if warnings else ""
    return ControlCheck(
        name="agent_policy_setup",
        status="PASS",
        details=f"Repository-local agent control-plane inventory is valid.{warning_text}",
        artifact="tools/agent_policy/validate_setup.py",
    )


def check_red_team_catalog(root: Path) -> ControlCheck:
    red_team = load_sibling("dpone_agent_red_team_gate", "red_team.py")
    catalog = root / "evals/agent/red_team_scenarios.yml"
    report = red_team.validate_file(catalog)
    if report.errors:
        return ControlCheck(
            name="agent_red_team_catalog",
            status="FAIL",
            details="; ".join(report.errors),
            artifact="evals/agent/red_team_scenarios.yml",
        )
    return ControlCheck(
        name="agent_red_team_catalog",
        status="PASS",
        details=(
            "Required connector-scope-escalation, prompt-injection, secret-disclosure, "
            "workflow-tampering, verification-laundering, excessive-agency, "
            "unbounded-consumption, unregistered-tool-use, and GitHub-settings-drift "
            "scenarios are valid."
        ),
        artifact="evals/agent/red_team_scenarios.yml",
    )


def check_permission_profile(
    root: Path,
    *,
    permission_profile: str,
    tool_registry: str,
) -> ControlCheck:
    permissions = load_sibling("dpone_agent_permissions_gate", "permissions.py")
    result = permissions.validate_permission_profile(
        root,
        permission_profile=permission_profile,
        tool_registry=tool_registry,
    )
    if result.errors:
        return ControlCheck(
            name="agent_permission_profile",
            status="FAIL",
            details="; ".join(result.errors),
            artifact=permission_profile,
        )
    warning_text = f" Warnings: {'; '.join(result.warnings)}" if result.warnings else ""
    return ControlCheck(
        name="agent_permission_profile",
        status="PASS",
        details=(
            "Agent permission profiles define role owners, allowed tools, forbidden tools, protected "
            f"actions, escalation, and required evidence.{warning_text}"
        ),
        artifact=permission_profile,
    )


def check_tool_registry(
    root: Path,
    *,
    permission_profile: str,
    tool_registry: str,
) -> ControlCheck:
    permissions = load_sibling("dpone_agent_permissions_gate", "permissions.py")
    result = permissions.validate_tool_registry(
        root,
        permission_profile=permission_profile,
        tool_registry=tool_registry,
    )
    if result.errors:
        return ControlCheck(
            name="agent_tool_registry",
            status="FAIL",
            details="; ".join(result.errors),
            artifact=tool_registry,
        )
    warning_text = f" Warnings: {'; '.join(result.warnings)}" if result.warnings else ""
    return ControlCheck(
        name="agent_tool_registry",
        status="PASS",
        details=(
            "Agent tool registry defines tool provenance, risk tier, allowed profiles, approval "
            f"requirements, scopes, and evidence.{warning_text}"
        ),
        artifact=tool_registry,
    )


def check_mcp_connector_onboarding(
    root: Path,
    *,
    permission_profile: str,
    tool_registry: str,
    mcp_connector_onboarding: str,
) -> ControlCheck:
    permissions = load_sibling("dpone_agent_permissions_gate", "permissions.py")
    result = permissions.validate_mcp_connector_onboarding(
        root,
        permission_profile=permission_profile,
        tool_registry=tool_registry,
        mcp_connector_onboarding=mcp_connector_onboarding,
    )
    if result.errors:
        return ControlCheck(
            name="agent_mcp_connector_onboarding",
            status="FAIL",
            details="; ".join(result.errors),
            artifact=mcp_connector_onboarding,
        )
    warning_text = f" Warnings: {'; '.join(result.warnings)}" if result.warnings else ""
    return ControlCheck(
        name="agent_mcp_connector_onboarding",
        status="PASS",
        details=(
            "MCP/connector onboarding defines provenance, scopes, tenant boundaries, approval gates, "
            f"red-team coverage, and evidence receipts.{warning_text}"
        ),
        artifact=mcp_connector_onboarding,
    )


def check_task_contract_template(root: Path, *, task_contract_template: str) -> ControlCheck:
    path = root / task_contract_template
    if not path.is_file():
        return ControlCheck(
            name="agent_task_contract_template",
            status="FAIL",
            details=f"Missing {task_contract_template}.",
            artifact=task_contract_template,
        )
    task_contract = load_sibling("dpone_agent_task_contract_gate", "task_contract.py")
    try:
        result = task_contract.validate_file(path, template=True)
    except Exception as exc:  # pragma: no cover - surfaced by governance output
        return ControlCheck(
            name="agent_task_contract_template",
            status="FAIL",
            details=f"Cannot validate task contract template: {exc}",
            artifact=task_contract_template,
        )
    if result.errors:
        return ControlCheck(
            name="agent_task_contract_template",
            status="FAIL",
            details="; ".join(result.errors),
            artifact=task_contract_template,
        )
    warning_text = f" Warnings: {'; '.join(result.warnings)}" if result.warnings else ""
    return ControlCheck(
        name="agent_task_contract_template",
        status="PASS",
        details=(
            "Agent task contract template preserves schema, protected paths, status vocabulary, "
            f"and stop conditions.{warning_text}"
        ),
        artifact=task_contract_template,
    )


def check_branch_protection_policy(root: Path, *, branch_protection_policy: str) -> ControlCheck:
    path = root / branch_protection_policy
    if not path.is_file():
        return ControlCheck(
            name="github_branch_protection_policy",
            status="FAIL",
            details=f"Missing {branch_protection_policy}.",
            artifact=branch_protection_policy,
        )
    branch_protection = load_sibling("dpone_agent_branch_protection_gate", "branch_protection.py")
    result = branch_protection.validate_file(path)
    if result.errors:
        return ControlCheck(
            name="github_branch_protection_policy",
            status="FAIL",
            details="; ".join(result.errors),
            artifact=branch_protection_policy,
        )
    warning_text = f" Warnings: {'; '.join(result.warnings)}" if result.warnings else ""
    return ControlCheck(
        name="github_branch_protection_policy",
        status="PASS",
        details=(
            "GitHub branch protection policy preserves solo-maintainer PR flow, strict required checks, "
            f"conversation resolution, and protected branch actions.{warning_text}"
        ),
        artifact=branch_protection_policy,
    )


def check_workflow_security_policy(
    root: Path,
    *,
    workflow_security_policy: str,
    workflows_dir: str,
) -> ControlCheck:
    workflow_security = load_sibling("dpone_agent_workflow_security_gate", "workflow_security.py")
    result = workflow_security.validate_repository(
        root,
        policy_path=root / workflow_security_policy,
        workflows_dir=root / workflows_dir,
    )
    if result.errors:
        return ControlCheck(
            name="github_workflow_security_policy",
            status="FAIL",
            details="; ".join(result.errors),
            artifact=workflow_security_policy,
        )
    warning_text = f" Warnings: {'; '.join(result.warnings)}" if result.warnings else ""
    return ControlCheck(
        name="github_workflow_security_policy",
        status="PASS",
        details=(
            "GitHub Actions workflows define top-level permissions, pin external actions, avoid "
            f"pull_request_target, and document write scopes.{warning_text}"
        ),
        artifact=workflow_security_policy,
    )


def check_control_surface_gate(
    changed_paths: list[str],
    red_team_check: ControlCheck,
    *,
    control_surface_changed: bool,
) -> ControlCheck:
    if not control_surface_changed:
        return ControlCheck(
            name="changed_control_surface_red_team",
            status="N/A",
            details="No agent-control, workflow, prompt, or release-governance paths changed.",
        )
    if red_team_check.status != "PASS":
        return ControlCheck(
            name="changed_control_surface_red_team",
            status="FAIL",
            details="Agent-control paths changed but the red-team catalog is not valid.",
            artifact=red_team_check.artifact,
        )
    return ControlCheck(
        name="changed_control_surface_red_team",
        status="PASS",
        details="Agent-control paths changed and the executable red-team catalog validates.",
        artifact=red_team_check.artifact,
    )

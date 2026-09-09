"""Build an executable governance receipt for dpone agent-control changes."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

Status = Literal["PASS", "FAIL", "N/A"]

SLSA_SELF_ASSESSMENT_DOC = "docs/supply-chain-slsa.md"
RELEASE_WORKFLOW = ".github/workflows/release.yml"
SCORECARD_WORKFLOW = ".github/workflows/scorecard.yml"
PERMISSION_PROFILE = ".agents/policy/agent-permission-profile.yml"
TOOL_REGISTRY = ".agents/policy/tool-registry.yml"
MCP_CONNECTOR_ONBOARDING = ".agents/policy/mcp-connector-onboarding.yml"
TASK_CONTRACT_TEMPLATE = "docs/agent-templates/agent-task-contract.yml"
BRANCH_PROTECTION_POLICY = ".agents/policy/github-branch-protection.yml"
WORKFLOW_SECURITY_POLICY = ".agents/policy/workflow-security.yml"
WORKFLOWS_DIR = ".github/workflows"
FULL_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")


def load_sibling(module_name: str, filename: str) -> Any:
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


_checks = load_sibling("dpone_agent_governance_checks", "governance_checks.py")
_supply_chain_checks = load_sibling("dpone_agent_supply_chain_checks", "supply_chain_checks.py")
_control_surface = load_sibling("dpone_agent_control_surface_gate", "control_surface.py")
ControlCheck = _checks.ControlCheck


def _git(root: Path, *args: str) -> str:
    completed = subprocess.run(("git", *args), cwd=root, check=True, text=True, capture_output=True)
    return completed.stdout.strip()


def _current_commit(root: Path) -> str:
    try:
        return _git(root, "rev-parse", "HEAD")
    except subprocess.CalledProcessError:
        return "unknown"


def _control_surface_changed(changed_paths: list[str]) -> bool:
    return _control_surface.control_surface_changed(changed_paths)


def _check_agent_setup(root: Path) -> ControlCheck:
    return _checks.check_agent_setup(root)


def _check_red_team_catalog(root: Path) -> ControlCheck:
    return _checks.check_red_team_catalog(root)


def _check_permission_profile(root: Path) -> ControlCheck:
    return _checks.check_permission_profile(
        root,
        permission_profile=PERMISSION_PROFILE,
        tool_registry=TOOL_REGISTRY,
    )


def _check_tool_registry(root: Path) -> ControlCheck:
    return _checks.check_tool_registry(
        root,
        permission_profile=PERMISSION_PROFILE,
        tool_registry=TOOL_REGISTRY,
    )


def _check_mcp_connector_onboarding(root: Path) -> ControlCheck:
    return _checks.check_mcp_connector_onboarding(
        root,
        permission_profile=PERMISSION_PROFILE,
        tool_registry=TOOL_REGISTRY,
        mcp_connector_onboarding=MCP_CONNECTOR_ONBOARDING,
    )


def _check_task_contract_template(root: Path) -> ControlCheck:
    return _checks.check_task_contract_template(root, task_contract_template=TASK_CONTRACT_TEMPLATE)


def _check_branch_protection_policy(root: Path) -> ControlCheck:
    return _checks.check_branch_protection_policy(root, branch_protection_policy=BRANCH_PROTECTION_POLICY)


def _check_workflow_security_policy(root: Path) -> ControlCheck:
    return _checks.check_workflow_security_policy(
        root,
        workflow_security_policy=WORKFLOW_SECURITY_POLICY,
        workflows_dir=WORKFLOWS_DIR,
    )


def _check_control_surface_gate(changed_paths: list[str], red_team_check: ControlCheck) -> ControlCheck:
    return _checks.check_control_surface_gate(
        changed_paths,
        red_team_check,
        control_surface_changed=_control_surface_changed(changed_paths),
    )


def _check_release_attestations(root: Path) -> ControlCheck:
    return _supply_chain_checks.check_release_attestations(root, release_workflow=RELEASE_WORKFLOW)


def _check_scorecard(root: Path) -> ControlCheck:
    return _supply_chain_checks.check_scorecard(root, scorecard_workflow=SCORECARD_WORKFLOW)


def _check_slsa_self_assessment(root: Path) -> ControlCheck:
    return _supply_chain_checks.check_slsa_self_assessment(root, slsa_self_assessment_doc=SLSA_SELF_ASSESSMENT_DOC)


def build_report(
    root: Path,
    changed_paths: list[str],
    base_ref: str = "origin/master",
    *,
    head_commit: str | None = None,
) -> dict[str, Any]:
    """Build the governance gate report without writing it to disk."""

    report_head_commit = _current_commit(root) if head_commit is None else head_commit
    if FULL_GIT_SHA.fullmatch(report_head_commit) is None:
        raise ValueError("head_commit must be a full 40-character lowercase Git SHA")
    standards = load_sibling("dpone_agent_governance_standards_gate", "governance_standards.py")
    normalized_paths = sorted({path.replace("\\", "/") for path in changed_paths if path})
    setup_check = _check_agent_setup(root)
    red_team_check = _check_red_team_catalog(root)
    checks = [
        setup_check,
        red_team_check,
        _check_permission_profile(root),
        _check_tool_registry(root),
        _check_mcp_connector_onboarding(root),
        _check_task_contract_template(root),
        _check_branch_protection_policy(root),
        _check_workflow_security_policy(root),
        _check_control_surface_gate(normalized_paths, red_team_check),
        _check_release_attestations(root),
        _check_scorecard(root),
        _check_slsa_self_assessment(root),
    ]
    status: Status = "FAIL" if any(check.status == "FAIL" for check in checks) else "PASS"
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),  # noqa: UP017
        "status": status,
        "base_ref": base_ref,
        "head_commit": report_head_commit,
        "changed_paths": normalized_paths,
        "control_surface_changed": _control_surface_changed(normalized_paths),
        "checks": [asdict(check) for check in checks],
        "standards": standards.agent_governance_standards(),
        "risks": [
            "AG-001",
            "AG-002",
            "AG-005",
            "AG-007",
            "AG-008",
            "AG-009",
            "AG-010",
            "AG-011",
            "AG-012",
            "AG-013",
            "AG-014",
            "AG-015",
            "AG-016",
        ],
    }


def write_report(report: dict[str, Any], output: Path) -> None:
    """Write a deterministic JSON governance receipt."""

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _changed_paths(root: Path, base_ref: str, explicit_paths: list[str]) -> list[str]:
    if explicit_paths:
        return explicit_paths
    select_checks = load_sibling("dpone_agent_select_checks_gate", "select_checks.py")
    cwd = Path.cwd()
    try:
        # select_checks shells out without a cwd argument, so run from the requested root.
        os.chdir(root)
        return select_checks.changed_paths(base_ref)
    finally:
        os.chdir(cwd)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", help="Changed paths. Defaults to git diff against --base-ref.")
    parser.add_argument("--root", default=".", type=Path)
    parser.add_argument("--base-ref", default="origin/master")
    parser.add_argument(
        "--head-commit",
        help="Reviewed head commit represented by the report. Defaults to the checked-out commit.",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args()

    root = args.root.resolve()
    paths = _changed_paths(root, args.base_ref, args.paths)
    report = build_report(root, paths, base_ref=args.base_ref, head_commit=args.head_commit)
    if args.output:
        write_report(report, args.output)

    if args.format == "json":
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"Agent governance gate: {report['status']}{f' -> {args.output}' if args.output else ''}")
        for check in report["checks"]:
            print(f"- {check['status']}: {check['name']} - {check['details']}")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())

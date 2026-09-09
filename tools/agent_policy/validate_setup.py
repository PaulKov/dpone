"""Validate dpone's repository-local agent control plane."""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

import yaml


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


_inventory = _load_sibling("dpone_agent_setup_inventory", "setup_inventory.py")

MAX_INSTRUCTION_BYTES = _inventory.MAX_INSTRUCTION_BYTES
SKILL_NAME = re.compile(_inventory.SKILL_NAME_PATTERN)
REQUIRED_FILES = _inventory.REQUIRED_FILES
REQUIRED_RED_TEAM_SCENARIOS = _inventory.REQUIRED_RED_TEAM_SCENARIOS
RED_TEAM_DOCS = _inventory.RED_TEAM_DOCS
NESTED_INSTRUCTIONS = _inventory.NESTED_INSTRUCTIONS


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _frontmatter(path: Path) -> dict[str, Any]:
    text = _read(path)
    if not text.startswith("---\n"):
        raise ValueError("missing YAML frontmatter")
    try:
        _, raw, _ = text.split("---", 2)
    except ValueError as exc:
        raise ValueError("unterminated YAML frontmatter") from exc
    data = yaml.safe_load(raw) or {}
    if not isinstance(data, dict):
        raise ValueError("frontmatter must be a mapping")
    return data


def validate(root: Path) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    max_instruction_bytes = MAX_INSTRUCTION_BYTES

    for relative in REQUIRED_FILES:
        if not (root / relative).is_file():
            errors.append(f"missing required file: {relative}")

    config = root / ".codex/config.toml"
    if config.is_file():
        try:
            parsed = tomllib.loads(_read(config))
            configured_budget = parsed.get("project_doc_max_bytes", MAX_INSTRUCTION_BYTES)
            if not isinstance(configured_budget, int) or configured_budget < 1:
                errors.append(".codex/config.toml: project_doc_max_bytes must be positive")
            else:
                max_instruction_bytes = configured_budget
            if parsed.get("features", {}).get("multi_agent") is not True:
                errors.append(".codex/config.toml: features.multi_agent must be true")
            agents = parsed.get("agents", {})
            if not isinstance(agents.get("max_threads"), int) or agents["max_threads"] < 1:
                errors.append(".codex/config.toml: agents.max_threads must be positive")
            if agents.get("max_depth") != 1:
                warnings.append(".codex/config.toml: max_depth should remain 1 unless recursion is intentional")
            if not isinstance(agents.get("interrupt_message"), bool):
                errors.append(".codex/config.toml: agents.interrupt_message must be boolean")
        except (tomllib.TOMLDecodeError, OSError) as exc:
            errors.append(f"invalid .codex/config.toml: {exc}")

    agent_names: set[str] = set()
    for path in sorted((root / ".codex/agents").glob("*.toml")):
        try:
            data = tomllib.loads(_read(path))
        except tomllib.TOMLDecodeError as exc:
            errors.append(f"invalid agent TOML {path.relative_to(root)}: {exc}")
            continue
        for field in ("name", "description", "developer_instructions"):
            if not str(data.get(field, "")).strip():
                errors.append(f"{path.relative_to(root)}: missing {field}")
        name = str(data.get("name", ""))
        if name in agent_names:
            errors.append(f"duplicate agent name: {name}")
        agent_names.add(name)
        sandbox_mode = data.get("sandbox_mode")
        if sandbox_mode not in {"read-only", "workspace-write", "danger-full-access"}:
            errors.append(f"{path.relative_to(root)}: invalid sandbox_mode {sandbox_mode!r}")

    skill_names: set[str] = set()
    for path in sorted((root / ".agents/skills").glob("*/SKILL.md")):
        try:
            data = _frontmatter(path)
        except (ValueError, yaml.YAMLError) as exc:
            errors.append(f"invalid skill {path.relative_to(root)}: {exc}")
            continue
        name = str(data.get("name", "")).strip()
        description = str(data.get("description", "")).strip()
        if not name or not description:
            errors.append(f"{path.relative_to(root)}: name and description are required")
        elif not SKILL_NAME.fullmatch(name):
            errors.append(f"{path.relative_to(root)}: invalid skill name {name!r}")
        if description and len(description) < 40:
            errors.append(f"{path.relative_to(root)}: description must identify a clear trigger")
        if name in skill_names:
            errors.append(f"duplicate skill name: {name}")
        skill_names.add(name)
        if name and name != path.parent.name:
            warnings.append(f"{path.relative_to(root)}: skill name differs from directory")

    root_agents = root / "AGENTS.md"
    if root_agents.is_file():
        root_size = root_agents.stat().st_size
        if root_size >= max_instruction_bytes:
            errors.append("AGENTS.md alone exceeds the configured instruction budget")
        for relative in NESTED_INSTRUCTIONS:
            nested = root / relative
            if nested.is_file():
                combined = root_size + nested.stat().st_size
                if combined >= max_instruction_bytes:
                    errors.append(f"instruction chain exceeds the configured budget: AGENTS.md + {relative}")

    for relative in (
        ".github/ISSUE_TEMPLATE/feature_design.yml",
        ".github/ISSUE_TEMPLATE/release_readiness.yml",
        ".github/ISSUE_TEMPLATE/admin_bypass.yml",
        ".github/workflows/dependency-review.yml",
        "docs/agent-templates/agent-task-contract.yml",
        ".agents/policy/github-branch-protection.yml",
        ".agents/policy/agent-permission-profile.yml",
        ".agents/policy/mcp-connector-onboarding.yml",
        ".agents/policy/tool-registry.yml",
        ".agents/policy/workflow-security.yml",
        ".agents/policy/workflow-security-privileged.yml",
        ".github/workflows/agent-governance-drift.yml",
        ".github/workflows/agent-pr-receipt.yml",
        ".github/workflows/ci.yml",
    ):
        path = root / relative
        if not path.is_file():
            continue
        try:
            yaml.safe_load(_read(path))
        except yaml.YAMLError as exc:
            errors.append(f"invalid YAML {relative}: {exc}")

    schema = root / "evals/agent/result.schema.json"
    if schema.is_file():
        try:
            data = json.loads(_read(schema))
            if data.get("type") != "object" or not data.get("required"):
                errors.append("agent result schema must define an object and required fields")
        except json.JSONDecodeError as exc:
            errors.append(f"invalid agent result JSON schema: {exc}")

    for relative in (
        "evals/agent/agent-permission-profile.schema.json",
        "evals/agent/agent-task-contract.schema.json",
        "evals/agent/github-branch-protection.schema.json",
        "evals/agent/governance-gate.schema.json",
        "evals/agent/governance-drift-summary.schema.json",
        "evals/agent/mcp-connector-onboarding.schema.json",
        "evals/agent/pr-merge-receipt.schema.json",
        "evals/agent/pr-receipt.schema.json",
        "evals/agent/tool-registry.schema.json",
        "evals/agent/workflow-security.schema.json",
        "evals/agent/workflow-security-privileged-policy.schema.json",
        "evals/agent/workflow-security-privileged-report.schema.json",
    ):
        path = root / relative
        if path.is_file():
            try:
                data = json.loads(_read(path))
                if data.get("type") != "object" or data.get("additionalProperties") is not False:
                    errors.append(f"{relative}: schema must define a closed object contract")
            except json.JSONDecodeError as exc:
                errors.append(f"invalid JSON schema {relative}: {exc}")

    task_contract_template = root / "docs/agent-templates/agent-task-contract.yml"
    task_contract_validator = root / "tools/agent_policy/task_contract.py"
    if task_contract_template.is_file() and task_contract_validator.is_file():
        try:
            report = _load_sibling("dpone_agent_task_contract_runtime", "task_contract.py").validate_file(
                task_contract_template, template=True
            )
        except Exception as exc:  # pragma: no cover - surfaced by validation output
            errors.append(f"cannot validate agent task contract template: {exc}")
        else:
            errors.extend(f"invalid agent task contract template: {error}" for error in report.errors)
            warnings.extend(report.warnings)

    branch_protection_policy = root / ".agents/policy/github-branch-protection.yml"
    branch_protection_validator = root / "tools/agent_policy/branch_protection.py"
    if branch_protection_policy.is_file() and branch_protection_validator.is_file():
        try:
            report = _load_sibling("dpone_agent_branch_protection_runtime", "branch_protection.py").validate_file(
                branch_protection_policy
            )
        except Exception as exc:  # pragma: no cover - surfaced by validation output
            errors.append(f"cannot validate GitHub branch protection policy: {exc}")
        else:
            errors.extend(f"invalid GitHub branch protection policy: {error}" for error in report.errors)
            warnings.extend(report.warnings)

    workflow_security_policy = root / ".agents/policy/workflow-security.yml"
    workflow_security_validator = root / "tools/agent_policy/workflow_security.py"
    if workflow_security_policy.is_file() and workflow_security_validator.is_file():
        try:
            report = _load_sibling("dpone_agent_workflow_security_runtime", "workflow_security.py").validate_repository(
                root,
                policy_path=workflow_security_policy,
                workflows_dir=root / ".github/workflows",
            )
        except Exception as exc:  # pragma: no cover - surfaced by validation output
            errors.append(f"cannot validate workflow security policy: {exc}")
        else:
            errors.extend(f"invalid workflow security policy: {error}" for error in report.errors)
            warnings.extend(report.warnings)

    permission_profile = root / ".agents/policy/agent-permission-profile.yml"
    tool_registry = root / ".agents/policy/tool-registry.yml"
    mcp_onboarding = root / ".agents/policy/mcp-connector-onboarding.yml"
    if permission_profile.is_file() and tool_registry.is_file() and mcp_onboarding.is_file():
        try:
            policy_result = _load_sibling("dpone_agent_permissions_runtime", "permissions.py").validate_all(root)
        except Exception as exc:  # pragma: no cover - surfaced by validation output
            errors.append(f"cannot validate agent permission policy: {exc}")
        else:
            errors.extend(f"invalid agent permission policy: {error}" for error in policy_result.errors)
            warnings.extend(policy_result.warnings)

    red_team_catalog = root / "evals/agent/red_team_scenarios.yml"
    red_team_validator = root / "tools/agent_policy/red_team.py"
    if red_team_catalog.is_file() and red_team_validator.is_file():
        try:
            report = _load_sibling("dpone_agent_red_team_runtime", "red_team.py").validate_file(red_team_catalog)
        except Exception as exc:  # pragma: no cover - surfaced by validation output
            errors.append(f"cannot validate agent red-team catalog: {exc}")
        else:
            errors.extend(f"invalid agent red-team catalog: {error}" for error in report.errors)

    for relative in REQUIRED_FILES:
        path = root / relative
        if path.is_file() and any(marker in _read(path) for marker in ("FILL_ME", "<TODO>")):
            errors.append(f"unresolved policy placeholder in {relative}")

    red_team_text = "\n".join(
        _read(root / relative).lower() for relative in RED_TEAM_DOCS if (root / relative).is_file()
    )
    for scenario in REQUIRED_RED_TEAM_SCENARIOS:
        if scenario not in red_team_text:
            errors.append(f"missing agent red-team scenario: {scenario}")

    if len(agent_names) < 4:
        warnings.append("fewer than four custom agents: parallel role coverage may be incomplete")
    if len(skill_names) < 4:
        warnings.append("fewer than four repository skills: repeated workflows may remain prompt-only")

    return errors, warnings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", default=".", type=Path)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args()
    errors, warnings = validate(args.root.resolve())
    status = "failed" if errors else "passed"
    payload = {"status": status, "errors": errors, "warnings": warnings}
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for warning in warnings:
            print(f"WARNING: {warning}")
        for error in errors:
            print(f"ERROR: {error}")
        print(f"Agent policy validation: {status.upper()} ({len(errors)} errors, {len(warnings)} warnings)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())

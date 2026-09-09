"""Validate agent permission profile policy documents."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any


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


_constants = _load_sibling("dpone_agent_policy_constants", "policy_constants.py")
_support = _load_sibling("dpone_agent_policy_support", "policy_support.py")

PERMISSION_PROFILE = _constants.PERMISSION_PROFILE
TOOL_REGISTRY = _constants.TOOL_REGISTRY
READ_ONLY_FORBIDDEN = _constants.READ_ONLY_FORBIDDEN
REQUIRED_FORBIDDEN_FOR_ALL = _constants.REQUIRED_FORBIDDEN_FOR_ALL
REQUIRED_PROFILE_IDS = _constants.REQUIRED_PROFILE_IDS
REQUIRED_PROTECTED_ACTIONS = _constants.REQUIRED_PROTECTED_ACTIONS

PolicyValidationResult = _support.PolicyValidationResult


def load_permission_profile(path: Path) -> dict[str, Any]:
    """Load the permission profile YAML file."""

    return _support.load_mapping(path)


def validate_permission_profile(
    root: Path,
    *,
    permission_profile: str = PERMISSION_PROFILE,
    tool_registry: str = TOOL_REGISTRY,
) -> Any:
    """Validate the agent permission profile and its references to registered tools."""

    profile_path = root / permission_profile
    if not profile_path.is_file():
        return PolicyValidationResult([f"missing required permission profile: {permission_profile}"], [])

    errors: list[str] = []
    warnings: list[str] = []
    profile = _support.load_with_errors(profile_path, errors, permission_profile)
    if profile is None:
        return PolicyValidationResult(errors, warnings)

    tools = _support.load_optional_mapping(root / tool_registry, tool_registry, errors)
    tool_ids = _support.ids_from_items(tools.get("tools", [])) if tools else set()
    _support.validate_document_header(profile, permission_profile, errors)
    profiles = _support.required_list(profile, "profiles", permission_profile, errors)
    profile_ids = _support.ids_from_items(profiles)
    missing_profiles = sorted(REQUIRED_PROFILE_IDS - profile_ids)
    if missing_profiles:
        errors.append(f"{permission_profile}: missing required profiles: {', '.join(missing_profiles)}")

    for index, entry in enumerate(profiles):
        if not isinstance(entry, dict):
            errors.append(f"{permission_profile}: profiles[{index}] must be a mapping")
            continue
        _validate_profile(entry, index, permission_profile, tool_ids, errors, warnings)

    return PolicyValidationResult(errors, warnings)


def _validate_profile(
    entry: dict[str, Any],
    index: int,
    label: str,
    tool_ids: set[str],
    errors: list[str],
    warnings: list[str],
) -> None:
    prefix = _support.entry_prefix(label, "profiles", index, entry)
    profile_id = _support.required_string(entry, "id", prefix, errors)
    for field in ("owner", "description"):
        _support.required_string(entry, field, prefix, errors)
    allowed_tools = set(_support.required_string_list(entry, "allowed_tools", prefix, errors))
    forbidden_tools = set(_support.required_string_list(entry, "forbidden_tools", prefix, errors))
    protected_actions = set(_support.required_string_list(entry, "protected_actions", prefix, errors))
    sandbox_modes = set(_support.required_string_list(entry, "sandbox_modes", prefix, errors))
    evidence = _support.required_string_list(entry, "evidence_required", prefix, errors)
    escalation = _support.required_string_list(entry, "escalation", prefix, errors)

    overlap = sorted(allowed_tools & forbidden_tools)
    if overlap:
        errors.append(f"{prefix}: tools cannot be both allowed and forbidden: {', '.join(overlap)}")
    unknown_tools = sorted(allowed_tools - tool_ids)
    if tool_ids and unknown_tools:
        errors.append(f"{prefix}: allowed_tools reference unregistered tools: {', '.join(unknown_tools)}")
    missing_forbidden = sorted(REQUIRED_FORBIDDEN_FOR_ALL - forbidden_tools)
    if missing_forbidden:
        errors.append(f"{prefix}: forbidden_tools must include: {', '.join(missing_forbidden)}")
    missing_protected = sorted(REQUIRED_PROTECTED_ACTIONS - protected_actions)
    if missing_protected:
        errors.append(f"{prefix}: protected_actions must include: {', '.join(missing_protected)}")
    if not evidence:
        errors.append(f"{prefix}: evidence_required must not be empty")
    if not escalation:
        errors.append(f"{prefix}: escalation must not be empty")
    if profile_id in {"read_only_reviewer", "release_auditor"}:
        _validate_read_only_profile(prefix, sandbox_modes, forbidden_tools, allowed_tools, errors)
    if profile_id == "integrator" and "filesystem_write_owned" not in allowed_tools:
        warnings.append(f"{prefix}: integrator should keep filesystem_write_owned for shared-file reconciliation")


def _validate_read_only_profile(
    prefix: str,
    sandbox_modes: set[str],
    forbidden_tools: set[str],
    allowed_tools: set[str],
    errors: list[str],
) -> None:
    if sandbox_modes != {"read-only"}:
        errors.append(f"{prefix}: read-only profiles must use only read-only sandbox mode")
    missing = sorted(READ_ONLY_FORBIDDEN - forbidden_tools)
    if missing:
        errors.append(f"{prefix}: read-only profile must forbid: {', '.join(missing)}")
    write_like_tools = sorted(tool for tool in allowed_tools if "write" in tool or tool in {"git_remote", "github_pr"})
    if write_like_tools:
        errors.append(f"{prefix}: read-only profile allows write-capable tools: {', '.join(write_like_tools)}")

"""Validate agent tool registry policy documents."""

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
REQUIRED_TOOL_IDS = _constants.REQUIRED_TOOL_IDS
SCOPE_REVIEW_CATEGORIES = _constants.SCOPE_REVIEW_CATEGORIES
VALID_RISK_TIERS = _constants.VALID_RISK_TIERS
VALID_TOOL_CATEGORIES = _constants.VALID_TOOL_CATEGORIES

PolicyValidationResult = _support.PolicyValidationResult


def load_tool_registry(path: Path) -> dict[str, Any]:
    """Load the tool registry YAML file."""

    return _support.load_mapping(path)


def validate_tool_registry(
    root: Path,
    *,
    permission_profile: str = PERMISSION_PROFILE,
    tool_registry: str = TOOL_REGISTRY,
) -> Any:
    """Validate the tool registry and references to known permission profiles."""

    registry_path = root / tool_registry
    if not registry_path.is_file():
        return PolicyValidationResult([f"missing required tool registry: {tool_registry}"], [])

    errors: list[str] = []
    warnings: list[str] = []
    registry = _support.load_with_errors(registry_path, errors, tool_registry)
    if registry is None:
        return PolicyValidationResult(errors, warnings)

    profile = _support.load_optional_mapping(root / permission_profile, permission_profile, errors)
    profile_ids = _support.ids_from_items(profile.get("profiles", [])) if profile else set()
    _support.validate_document_header(registry, tool_registry, errors)
    tools = _support.required_list(registry, "tools", tool_registry, errors)
    tool_ids = _support.ids_from_items(tools)
    missing_tools = sorted(REQUIRED_TOOL_IDS - tool_ids)
    if missing_tools:
        errors.append(f"{tool_registry}: missing required tools: {', '.join(missing_tools)}")

    for index, entry in enumerate(tools):
        if not isinstance(entry, dict):
            errors.append(f"{tool_registry}: tools[{index}] must be a mapping")
            continue
        _validate_tool(entry, index, tool_registry, profile_ids, errors)

    return PolicyValidationResult(errors, warnings)


def _validate_tool(
    entry: dict[str, Any],
    index: int,
    label: str,
    profile_ids: set[str],
    errors: list[str],
) -> None:
    prefix = _support.entry_prefix(label, "tools", index, entry)
    for field in ("id", "owner", "description", "category", "provenance", "risk_tier"):
        _support.required_string(entry, field, prefix, errors)
    category = str(entry.get("category", ""))
    risk_tier = str(entry.get("risk_tier", ""))
    allowed_profiles = set(_support.required_string_list(entry, "allowed_profiles", prefix, errors))
    allowed_actions = set(_support.required_string_list(entry, "allowed_actions", prefix, errors))
    forbidden_actions = set(_support.required_string_list(entry, "forbidden_actions", prefix, errors))
    evidence = _support.required_string_list(entry, "evidence_required", prefix, errors)
    approval_required_for = _support.required_string_list(entry, "approval_required_for", prefix, errors)

    if category and category not in VALID_TOOL_CATEGORIES:
        errors.append(f"{prefix}: invalid category {category!r}")
    if risk_tier and risk_tier not in VALID_RISK_TIERS:
        errors.append(f"{prefix}: invalid risk_tier {risk_tier!r}")
    if "*" in allowed_profiles:
        errors.append(f"{prefix}: wildcard allowed_profiles are not permitted")
    unknown_profiles = sorted(allowed_profiles - profile_ids)
    if profile_ids and unknown_profiles:
        errors.append(f"{prefix}: allowed_profiles reference unknown profiles: {', '.join(unknown_profiles)}")
    action_overlap = sorted(allowed_actions & forbidden_actions)
    if action_overlap:
        errors.append(f"{prefix}: actions cannot be both allowed and forbidden: {', '.join(action_overlap)}")
    if risk_tier in {"high", "critical"} and not approval_required_for:
        errors.append(f"{prefix}: high and critical risk tools require approval_required_for")
    if category in SCOPE_REVIEW_CATEGORIES and not _support.required_string_list(
        entry, "allowed_scopes", prefix, errors
    ):
        errors.append(f"{prefix}: {category} tools require explicit allowed_scopes")
    if not evidence:
        errors.append(f"{prefix}: evidence_required must not be empty")

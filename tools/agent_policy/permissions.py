"""Validate dpone agent permission profiles and tool registry policy.

This module is intentionally a thin compatibility facade. Focused validators
live in sibling modules so agent-control policy remains easy to review and each
file stays inside the repository module-size budget.
"""

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
_permission_profiles = _load_sibling("dpone_agent_permission_profiles", "permission_profiles.py")
_tool_registry = _load_sibling("dpone_agent_tool_registry", "tool_registry.py")
_mcp_onboarding = _load_sibling("dpone_agent_mcp_onboarding", "mcp_onboarding.py")

MCP_CONNECTOR_ONBOARDING = _constants.MCP_CONNECTOR_ONBOARDING
PERMISSION_PROFILE = _constants.PERMISSION_PROFILE
READ_ONLY_FORBIDDEN = _constants.READ_ONLY_FORBIDDEN
REQUIRED_FORBIDDEN_FOR_ALL = _constants.REQUIRED_FORBIDDEN_FOR_ALL
REQUIRED_MCP_ONBOARDING_CONTROLS = _constants.REQUIRED_MCP_ONBOARDING_CONTROLS
REQUIRED_PROFILE_IDS = _constants.REQUIRED_PROFILE_IDS
REQUIRED_PROTECTED_ACTIONS = _constants.REQUIRED_PROTECTED_ACTIONS
REQUIRED_TOOL_IDS = _constants.REQUIRED_TOOL_IDS
SCOPE_REVIEW_CATEGORIES = _constants.SCOPE_REVIEW_CATEGORIES
TOOL_REGISTRY = _constants.TOOL_REGISTRY
VALID_CONNECTOR_KINDS = _constants.VALID_CONNECTOR_KINDS
VALID_CONNECTOR_STATUSES = _constants.VALID_CONNECTOR_STATUSES
VALID_RISK_TIERS = _constants.VALID_RISK_TIERS
VALID_TOOL_CATEGORIES = _constants.VALID_TOOL_CATEGORIES

PolicyValidationResult = _support.PolicyValidationResult
load_mcp_connector_onboarding = _mcp_onboarding.load_mcp_connector_onboarding
load_permission_profile = _permission_profiles.load_permission_profile
load_tool_registry = _tool_registry.load_tool_registry
validate_mcp_connector_onboarding = _mcp_onboarding.validate_mcp_connector_onboarding
validate_permission_profile = _permission_profiles.validate_permission_profile
validate_tool_registry = _tool_registry.validate_tool_registry


def validate_all(
    root: Path,
    *,
    permission_profile: str = PERMISSION_PROFILE,
    tool_registry: str = TOOL_REGISTRY,
    mcp_connector_onboarding: str = MCP_CONNECTOR_ONBOARDING,
) -> Any:
    """Validate permission, registry, and MCP onboarding policy files together."""

    profile_result = validate_permission_profile(
        root, permission_profile=permission_profile, tool_registry=tool_registry
    )
    registry_result = validate_tool_registry(root, permission_profile=permission_profile, tool_registry=tool_registry)
    onboarding_result = validate_mcp_connector_onboarding(
        root,
        permission_profile=permission_profile,
        tool_registry=tool_registry,
        mcp_connector_onboarding=mcp_connector_onboarding,
    )
    return PolicyValidationResult(
        errors=[*profile_result.errors, *registry_result.errors, *onboarding_result.errors],
        warnings=[*profile_result.warnings, *registry_result.warnings, *onboarding_result.warnings],
    )

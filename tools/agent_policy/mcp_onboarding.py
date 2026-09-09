"""Validate MCP and connector onboarding policy documents."""

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

MCP_CONNECTOR_ONBOARDING = _constants.MCP_CONNECTOR_ONBOARDING
PERMISSION_PROFILE = _constants.PERMISSION_PROFILE
REQUIRED_MCP_ONBOARDING_CONTROLS = _constants.REQUIRED_MCP_ONBOARDING_CONTROLS
TOOL_REGISTRY = _constants.TOOL_REGISTRY
VALID_CONNECTOR_KINDS = _constants.VALID_CONNECTOR_KINDS
VALID_CONNECTOR_STATUSES = _constants.VALID_CONNECTOR_STATUSES

PolicyValidationResult = _support.PolicyValidationResult


def load_mcp_connector_onboarding(path: Path) -> dict[str, Any]:
    """Load the MCP/connector onboarding YAML file."""

    return _support.load_mapping(path)


def validate_mcp_connector_onboarding(
    root: Path,
    *,
    permission_profile: str = PERMISSION_PROFILE,
    tool_registry: str = TOOL_REGISTRY,
    mcp_connector_onboarding: str = MCP_CONNECTOR_ONBOARDING,
) -> Any:
    """Validate MCP/connector onboarding records against the tool registry."""

    onboarding_path = root / mcp_connector_onboarding
    if not onboarding_path.is_file():
        return PolicyValidationResult([f"missing required MCP connector onboarding: {mcp_connector_onboarding}"], [])

    errors: list[str] = []
    warnings: list[str] = []
    onboarding = _support.load_with_errors(onboarding_path, errors, mcp_connector_onboarding)
    if onboarding is None:
        return PolicyValidationResult(errors, warnings)

    profile = _support.load_optional_mapping(root / permission_profile, permission_profile, errors)
    profile_ids = _support.ids_from_items(profile.get("profiles", [])) if profile else set()
    registry = _support.load_optional_mapping(root / tool_registry, tool_registry, errors)
    registry_tools = registry.get("tools", []) if registry else []
    registry_by_id = _support.items_by_id(registry_tools)
    mcp_tool_ids = {
        entry["id"]
        for entry in registry_tools
        if isinstance(entry, dict) and entry.get("category") == "mcp" and isinstance(entry.get("id"), str)
    }

    _support.validate_document_header(onboarding, mcp_connector_onboarding, errors)
    document_controls = set(
        _support.required_string_list(onboarding, "required_controls", mcp_connector_onboarding, errors)
    )
    missing_document_controls = sorted(REQUIRED_MCP_ONBOARDING_CONTROLS - document_controls)
    if missing_document_controls:
        errors.append(
            f"{mcp_connector_onboarding}: missing required MCP onboarding controls: "
            + ", ".join(missing_document_controls)
        )

    connectors = _support.required_list(onboarding, "connectors", mcp_connector_onboarding, errors)
    onboarded_tool_ids: set[str] = set()
    for index, entry in enumerate(connectors):
        if not isinstance(entry, dict):
            errors.append(f"{mcp_connector_onboarding}: connectors[{index}] must be a mapping")
            continue
        tool_id = _validate_mcp_connector(
            entry,
            index,
            mcp_connector_onboarding,
            profile_ids,
            registry_by_id,
            errors,
        )
        if tool_id:
            onboarded_tool_ids.add(tool_id)

    missing_tools = sorted(mcp_tool_ids - onboarded_tool_ids)
    if missing_tools:
        errors.append(f"{mcp_connector_onboarding}: mcp tools require onboarding records: {', '.join(missing_tools)}")

    return PolicyValidationResult(errors, warnings)


def _validate_mcp_connector(
    entry: dict[str, Any],
    index: int,
    label: str,
    profile_ids: set[str],
    registry_by_id: dict[str, dict[str, Any]],
    errors: list[str],
) -> str:
    prefix = _support.entry_prefix(label, "connectors", index, entry)
    for field in (
        "id",
        "owner",
        "description",
        "tool_registry_id",
        "connector_kind",
        "status",
        "data_classification",
        "tenant_boundary",
        "token_storage",
    ):
        _support.required_string(entry, field, prefix, errors)

    tool_id = str(entry.get("tool_registry_id", ""))
    connector_kind = str(entry.get("connector_kind", ""))
    status = str(entry.get("status", ""))
    allowed_profiles = set(_support.required_string_list(entry, "allowed_profiles", prefix, errors))
    allowed_operations = set(_support.required_string_list(entry, "allowed_operations", prefix, errors))
    denied_operations = set(_support.required_string_list(entry, "denied_operations", prefix, errors))
    allowed_scopes = set(_support.required_string_list(entry, "allowed_scopes", prefix, errors))
    approval_required_for = set(_support.required_string_list(entry, "approval_required_for", prefix, errors))
    evidence_required = set(_support.required_string_list(entry, "evidence_required", prefix, errors))
    required_controls = set(_support.required_string_list(entry, "required_controls", prefix, errors))
    red_team_scenarios = set(_support.required_string_list(entry, "red_team_scenarios", prefix, errors))

    if connector_kind and connector_kind not in VALID_CONNECTOR_KINDS:
        errors.append(f"{prefix}: invalid connector_kind {connector_kind!r}")
    if status and status not in VALID_CONNECTOR_STATUSES:
        errors.append(f"{prefix}: invalid status {status!r}")
    if profile_ids:
        unknown_profiles = sorted(allowed_profiles - profile_ids)
        if unknown_profiles:
            errors.append(f"{prefix}: allowed_profiles reference unknown profiles: {', '.join(unknown_profiles)}")
    operation_overlap = sorted(allowed_operations & denied_operations)
    if operation_overlap:
        errors.append(f"{prefix}: operations cannot be both allowed and denied: {', '.join(operation_overlap)}")

    missing_controls = sorted(REQUIRED_MCP_ONBOARDING_CONTROLS - required_controls)
    if missing_controls:
        errors.append(f"{prefix}: missing required MCP onboarding controls: {', '.join(missing_controls)}")
    if "connector scope escalation" not in red_team_scenarios:
        errors.append(f"{prefix}: red_team_scenarios must include connector scope escalation")
    if any("write" in operation.lower() for operation in allowed_operations) and (
        "connector write operation" not in approval_required_for
    ):
        errors.append(f"{prefix}: write-capable connectors require connector write operation approval")

    registry_tool = registry_by_id.get(tool_id)
    if registry_by_id and registry_tool is None:
        errors.append(f"{prefix}: tool_registry_id references unregistered tool: {tool_id}")
        return tool_id
    if registry_tool is None:
        return tool_id
    if registry_tool.get("category") != "mcp":
        errors.append(f"{prefix}: tool_registry_id must reference a category=mcp tool")

    _validate_registry_subset(prefix, "allowed_profiles", allowed_profiles, registry_tool, errors)
    _validate_registry_subset(prefix, "allowed_scopes", allowed_scopes, registry_tool, errors)
    _validate_registry_coverage(prefix, "approval_required_for", approval_required_for, registry_tool, errors)
    _validate_registry_coverage(prefix, "evidence_required", evidence_required, registry_tool, errors)
    return tool_id


def _validate_registry_subset(
    prefix: str,
    field: str,
    onboarding_values: set[str],
    registry_tool: dict[str, Any],
    errors: list[str],
) -> None:
    expanded = sorted(onboarding_values - set(_support.string_items(registry_tool.get(field))))
    if expanded:
        errors.append(f"{prefix}: {field} exceed tool registry: {', '.join(expanded)}")


def _validate_registry_coverage(
    prefix: str,
    field: str,
    onboarding_values: set[str],
    registry_tool: dict[str, Any],
    errors: list[str],
) -> None:
    missing = sorted(set(_support.string_items(registry_tool.get(field))) - onboarding_values)
    if missing:
        errors.append(f"{prefix}: {field} must cover tool registry: {', '.join(missing)}")

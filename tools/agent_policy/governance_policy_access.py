"""Shared read helpers for v1 and v2 governance policy payloads."""

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


def required_context_names(payload: dict[str, Any]) -> tuple[str, ...]:
    """Return required check context names from a v1 or v2 policy mapping."""

    if not isinstance(payload, dict):
        raise ValueError("governance policy must be a mapping")
    if payload.get("schema_version") == 2:
        policy_v2 = _load_sibling("dpone_agent_governance_policy_v2_access", "governance_policy_v2.py")
        return policy_v2.parse_governance_policy(payload).branch_view.context_names
    ruleset = payload.get("ruleset")
    if not isinstance(ruleset, dict):
        raise ValueError("v1 policy must define ruleset")
    status = ruleset.get("required_status_checks")
    if not isinstance(status, dict):
        raise ValueError("v1 policy must define required_status_checks")
    checks = status.get("checks")
    if not isinstance(checks, list) or not checks or not all(isinstance(item, str) and item.strip() for item in checks):
        raise ValueError("v1 required_status_checks.checks must be a non-empty string list")
    return tuple(checks)


def ruleset_id(payload: dict[str, Any]) -> int:
    """Return the branch ruleset id from a v1 or v2 policy mapping."""

    if not isinstance(payload, dict):
        raise ValueError("governance policy must be a mapping")
    if payload.get("schema_version") == 2:
        branch = payload.get("branch_governance")
        ruleset = branch.get("ruleset") if isinstance(branch, dict) else None
    else:
        ruleset = payload.get("ruleset")
    if not isinstance(ruleset, dict):
        raise ValueError("policy must define a branch ruleset")
    value = ruleset.get("id")
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("ruleset.id must be a positive integer")
    return value

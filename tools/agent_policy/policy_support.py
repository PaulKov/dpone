"""Reusable YAML and field-validation helpers for agent policy files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class PolicyValidationResult:
    """Validation result for repository-local agent policy files."""

    errors: list[str]
    warnings: list[str]


def load_mapping(path: Path) -> dict[str, Any]:
    """Load a YAML mapping from disk."""

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected a YAML mapping")
    return data


def load_with_errors(path: Path, errors: list[str], label: str) -> dict[str, Any] | None:
    """Load a YAML mapping, appending a policy-style error on failure."""

    try:
        return load_mapping(path)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        errors.append(f"{label}: invalid YAML policy: {exc}")
        return None


def load_optional_mapping(path: Path, label: str, errors: list[str]) -> dict[str, Any] | None:
    """Load an optional YAML mapping when it exists."""

    if not path.is_file():
        return None
    return load_with_errors(path, errors, label)


def validate_document_header(data: dict[str, Any], label: str, errors: list[str]) -> None:
    """Validate common policy document metadata."""

    if data.get("schema_version") != 1:
        errors.append(f"{label}: schema_version must be 1")
    for field in ("owner", "last_reviewed"):
        if not str(data.get(field, "")).strip():
            errors.append(f"{label}: missing {field}")


def ids_from_items(items: Any) -> set[str]:
    """Extract string ids from a list of mapping items."""

    ids: set[str] = set()
    if not isinstance(items, list):
        return ids
    for item in items:
        if isinstance(item, dict) and isinstance(item.get("id"), str):
            ids.add(item["id"])
    return ids


def items_by_id(items: Any) -> dict[str, dict[str, Any]]:
    """Index mapping items by their string id."""

    by_id: dict[str, dict[str, Any]] = {}
    if not isinstance(items, list):
        return by_id
    for item in items:
        if isinstance(item, dict) and isinstance(item.get("id"), str):
            by_id[item["id"]] = item
    return by_id


def required_string(entry: dict[str, Any], field: str, prefix: str, errors: list[str]) -> str:
    """Require a non-empty string field and return it when present."""

    value = entry.get(field)
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{prefix}: missing {field}")
        return ""
    return value


def required_list(data: dict[str, Any], field: str, label: str, errors: list[str]) -> list[Any]:
    """Require a list field and return an empty list on validation failure."""

    value = data.get(field)
    if not isinstance(value, list):
        errors.append(f"{label}: {field} must be a list")
        return []
    return value


def required_string_list(entry: dict[str, Any], field: str, prefix: str, errors: list[str]) -> list[str]:
    """Require a non-empty list of non-empty strings."""

    value = entry.get(field)
    if not isinstance(value, list) or not value:
        errors.append(f"{prefix}: {field} must be a non-empty list")
        return []
    invalid = [item for item in value if not isinstance(item, str) or not item.strip()]
    if invalid:
        errors.append(f"{prefix}: {field} must contain only non-empty strings")
        return []
    return value


def entry_prefix(label: str, collection: str, index: int, entry: dict[str, Any]) -> str:
    """Build a stable error prefix for an item in a policy collection."""

    entry_id = entry.get("id")
    suffix = entry_id if isinstance(entry_id, str) and entry_id else str(index)
    return f"{label}: {collection}[{suffix}]"


def string_items(value: Any) -> list[str]:
    """Return only string entries from a YAML list-like field."""

    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]

"""Small normalization helpers shared by closed workflow-profile checks."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def nested_mapping(workflow: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    """Return a mapping child or the canonical empty mapping."""

    value = workflow.get(key, {})
    return value if isinstance(value, Mapping) else {}

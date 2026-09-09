"""Small helper for stable lazy public facades."""

from __future__ import annotations

from collections.abc import MutableMapping
from importlib import import_module
from typing import Any


def resolve_export(
    name: str,
    *,
    exports: dict[str, str],
    namespace: MutableMapping[str, Any],
    module_name: str,
) -> Any:
    """Resolve and cache a lazy ``module:attribute`` facade export."""
    target = exports.get(name)
    if target is None:
        raise AttributeError(f"module {module_name!r} has no attribute {name!r}")
    source_module, attr = target.split(":", 1)
    value = getattr(import_module(source_module), attr)
    namespace[name] = value
    return value


def exported_dir(namespace: MutableMapping[str, Any], exports: dict[str, str]) -> list[str]:
    """Return a stable ``dir()`` view for a lazy facade."""
    return sorted(set(namespace) | set(exports))


__all__ = ["exported_dir", "resolve_export"]

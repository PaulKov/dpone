"""Credentials & factories for runtime.

IMPORTANT
---------
This package may require optional dependencies (e.g. GCP SDKs) depending on the
selected connection types.

To keep lightweight CLI tooling usable in minimal environments, package-level
exports are **lazy**.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "SourceFactory",
    "SinkFactory",
]

_EXPORTS: dict[str, str] = {
    "SourceFactory": "dpone.runtime.credentials.factory:SourceFactory",
    "SinkFactory": "dpone.runtime.credentials.factory:SinkFactory",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if not target:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    mod = import_module(module_name)
    value = getattr(mod, attr)
    globals()[name] = value  # cache
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(__all__)))

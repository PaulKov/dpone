# ruff: noqa: F822
"""Manifest domain layer."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "BatchYamlManifestLoader",
    "LoadedManifest",
    "ManifestLoader",
    "ProcessSpec",
    "SingleYamlManifestLoader",
    "apply_conventions",
    "apply_registries",
]

_EXPORTS: dict[str, str] = {
    "BatchYamlManifestLoader": "dpone.manifest.batch_loader:BatchYamlManifestLoader",
    "LoadedManifest": "dpone.manifest.models:LoadedManifest",
    "ManifestLoader": "dpone.manifest.loader:ManifestLoader",
    "ProcessSpec": "dpone.manifest.models:ProcessSpec",
    "SingleYamlManifestLoader": "dpone.manifest.loader:SingleYamlManifestLoader",
    "apply_conventions": "dpone.manifest.conventions:apply_conventions",
    "apply_registries": "dpone.manifest.registry:apply_registries",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value

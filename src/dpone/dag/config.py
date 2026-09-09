# ruff: noqa: F822
"""Backward-compatible public facade for ETL process config parsing."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "DependencyParser",
    "ETLProcessConfig",
    "ETLProcessConfigParser",
    "LoadConfigBuilder",
    "ManifestRef",
    "load_via_manifest_loader",
    "split_manifest_ref",
]

_EXPORTS: dict[str, str] = {
    "DependencyParser": "dpone.dag.dependency_parser:DependencyParser",
    "ETLProcessConfig": "dpone.dag.config_models:ETLProcessConfig",
    "ETLProcessConfigParser": "dpone.dag.process_config_parser:ETLProcessConfigParser",
    "LoadConfigBuilder": "dpone.dag.load_config_builder:LoadConfigBuilder",
    "ManifestRef": "dpone.dag.config_refs:ManifestRef",
    "load_via_manifest_loader": "dpone.dag.config_refs:load_via_manifest_loader",
    "split_manifest_ref": "dpone.dag.config_refs:split_manifest_ref",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value

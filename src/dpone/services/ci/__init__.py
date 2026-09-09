"""CI-oriented helper services used by pipeline tooling."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "PromoteSnapshotResult",
    "PromoteSnapshotSettings",
    "build_promote_branch_name",
    "load_snapshot_values",
    "promote_snapshot_to_argocd",
    "build_snapshot_version",
    "build_snapshot_version_from_pyproject",
    "read_project_version",
]

_EXPORTS: dict[str, str] = {
    "PromoteSnapshotResult": "dpone.services.ci.argocd_promote:PromoteSnapshotResult",
    "PromoteSnapshotSettings": "dpone.services.ci.argocd_promote:PromoteSnapshotSettings",
    "build_promote_branch_name": "dpone.services.ci.argocd_promote:build_promote_branch_name",
    "load_snapshot_values": "dpone.services.ci.argocd_promote:load_snapshot_values",
    "promote_snapshot_to_argocd": "dpone.services.ci.argocd_promote:promote_snapshot_to_argocd",
    "build_snapshot_version": "dpone.services.ci.snapshot_version:build_snapshot_version",
    "build_snapshot_version_from_pyproject": "dpone.services.ci.snapshot_version:build_snapshot_version_from_pyproject",
    "read_project_version": "dpone.services.ci.snapshot_version:read_project_version",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if not target:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    mod = import_module(module_name)
    value = getattr(mod, attr)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(__all__)))

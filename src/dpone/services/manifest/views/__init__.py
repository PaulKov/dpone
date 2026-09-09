"""Typed manifest CLI view-model facade."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ManifestViewMeta",
    "build_meta",
    "ManifestExplainView",
    "ManifestListRow",
    "ManifestListView",
    "ManifestMigrateView",
    "ManifestRegistryLintView",
    "ManifestRenderView",
    "RenderedProcessDoc",
    "ManifestSparsePathsView",
    "ManifestStatsView",
    "ManifestValidateView",
    "ManifestVerifyView",
]

_EXPORTS: dict[str, str] = {
    "ManifestViewMeta": "dpone.services.manifest.views.common:ManifestViewMeta",
    "build_meta": "dpone.services.manifest.views.common:build_meta",
    "ManifestExplainView": "dpone.services.manifest.views.explain:ManifestExplainView",
    "ManifestListRow": "dpone.services.manifest.views.list:ManifestListRow",
    "ManifestListView": "dpone.services.manifest.views.list:ManifestListView",
    "ManifestMigrateView": "dpone.services.manifest.views.migrate:ManifestMigrateView",
    "ManifestRegistryLintView": "dpone.services.manifest.views.registry_lint:ManifestRegistryLintView",
    "ManifestRenderView": "dpone.services.manifest.views.render:ManifestRenderView",
    "RenderedProcessDoc": "dpone.services.manifest.views.render:RenderedProcessDoc",
    "ManifestSparsePathsView": "dpone.services.manifest.views.sparse_paths:ManifestSparsePathsView",
    "ManifestStatsView": "dpone.services.manifest.views.stats:ManifestStatsView",
    "ManifestValidateView": "dpone.services.manifest.views.validate:ManifestValidateView",
    "ManifestVerifyView": "dpone.services.manifest.views.verify:ManifestVerifyView",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":", 1)
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))

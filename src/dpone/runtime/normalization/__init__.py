"""Nested normalization public runtime API."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "NestedNormalizationOptions": "dpone.runtime.normalization.options",
    "NestedAtomicityOptions": "dpone.runtime.normalization.options",
    "NestedChildQualityOptions": "dpone.runtime.normalization.options",
    "RawLandingOptions": "dpone.runtime.normalization.options",
    "NestedNormalizationService": "dpone.runtime.normalization.normalizer",
    "NormalizationResult": "dpone.runtime.normalization.models",
    "NormalizedTable": "dpone.runtime.normalization.models",
    "PathPolicy": "dpone.runtime.normalization.policies",
    "PathPolicyResolver": "dpone.runtime.normalization.policies",
    "NormalizationGuardrails": "dpone.runtime.normalization.guardrails",
    "HierarchyContract": "dpone.runtime.normalization.contracts",
    "TableContract": "dpone.runtime.normalization.contracts",
    "HierarchyBuilderService": "dpone.runtime.normalization.hierarchy_builder",
    "NormalizationPreviewService": "dpone.runtime.normalization.preview",
    "ChildDeleteFinalizerPlan": "dpone.runtime.normalization.finalizers",
    "ChildDeleteFinalizerService": "dpone.runtime.normalization.finalizers",
    "ChildSnapshotStore": "dpone.runtime.normalization.snapshot_store",
    "JsonFileChildSnapshotStore": "dpone.runtime.normalization.snapshot_store",
    "SqlChildSnapshotStore": "dpone.runtime.normalization.snapshot_sql",
    "ChildSnapshotSqlExecutor": "dpone.runtime.normalization.snapshot_sql",
    "ChildSnapshotStoreFactory": "dpone.runtime.normalization.snapshot_factory",
    "ChildSnapshotStoreOptions": "dpone.runtime.normalization.snapshot_factory",
    "ChildSnapshotRuntimeService": "dpone.runtime.normalization.snapshot_runtime",
    "ChildSnapshotStage": "dpone.runtime.normalization.snapshot_runtime",
    "NestedSpillFastPathDecision": "dpone.runtime.normalization.fast_path",
    "NestedSpillFastPathPlanner": "dpone.runtime.normalization.fast_path",
    "NestedSpillArtifactFactory": "dpone.runtime.normalization.fast_path",
    "NestedLoadPackageCoordinator": "dpone.runtime.normalization.load_package",
    "ChildQualityResult": "dpone.runtime.normalization.child_quality",
    "ChildQualityService": "dpone.runtime.normalization.child_quality",
    "ChildQualityViolationError": "dpone.runtime.normalization.child_quality",
    "NativeSpillWriter": "dpone.runtime.normalization.native_spill",
    "SpilledNormalizationResult": "dpone.runtime.normalization.spill",
    "SpillToDiskNormalizationService": "dpone.runtime.normalization.spill",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    module_path = _EXPORTS.get(name)
    if module_path is None:
        raise AttributeError(name)
    value = getattr(import_module(module_path), name)
    globals()[name] = value
    return value

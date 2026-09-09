"""Managed-like UX services for self-service dpone operations.

This compatibility facade keeps the historical ``dpone.readiness.managed`` API
stable while concrete services live in focused modules by responsibility.
"""

from __future__ import annotations

from typing import Any

from dpone.lazy_exports import exported_dir, resolve_export

_EXPORTS: dict[str, str] = {
    "ConnectorScaffoldService": "dpone.readiness.managed_scaffold:ConnectorScaffoldService",
    "ExecutionPlanService": "dpone.readiness.managed_planning:ExecutionPlanService",
    "InitBundleResult": "dpone.readiness.managed_models:InitBundleResult",
    "ManagedFormat": "dpone.readiness.managed_models:ManagedFormat",
    "ManagedRenderer": "dpone.readiness.managed_rendering:ManagedRenderer",
    "PerformanceAdvisor": "dpone.readiness.managed_performance:PerformanceAdvisor",
    "PerformanceRecommendation": "dpone.readiness.managed_models:PerformanceRecommendation",
    "QualityCheckOutcome": "dpone.readiness.managed_models:QualityCheckOutcome",
    "QualityRunResult": "dpone.readiness.managed_models:QualityRunResult",
    "QualityService": "dpone.readiness.managed_quality:QualityService",
    "RunArtifactPaths": "dpone.readiness.managed_models:RunArtifactPaths",
    "RunArtifactWriter": "dpone.readiness.managed_artifacts:RunArtifactWriter",
    "StateInspectorService": "dpone.readiness.managed_state:StateInspectorService",
    "temporary_artifact_dir": "dpone.readiness.managed_utils:temporary_artifact_dir",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_export(name, exports=_EXPORTS, namespace=globals(), module_name=__name__)


def __dir__() -> list[str]:
    return exported_dir(globals(), _EXPORTS)

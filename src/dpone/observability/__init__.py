"""Runtime observability compatibility facade."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "MetricPoint",
    "RuntimeMetricsExtractor",
    "MetricsArtifactIndex",
    "MetricsArtifactIndexService",
    "MetricsArtifactItem",
    "RuntimeMetricsExportReport",
    "RuntimeMetricsExportService",
]

_EXPORTS: dict[str, str] = {
    "MetricPoint": "dpone.observability.metrics:MetricPoint",
    "RuntimeMetricsExtractor": "dpone.observability.metrics:RuntimeMetricsExtractor",
    "MetricsArtifactIndex": "dpone.observability.artifacts:MetricsArtifactIndex",
    "MetricsArtifactIndexService": "dpone.observability.artifacts:MetricsArtifactIndexService",
    "MetricsArtifactItem": "dpone.observability.artifacts:MetricsArtifactItem",
    "RuntimeMetricsExportReport": "dpone.observability.export:RuntimeMetricsExportReport",
    "RuntimeMetricsExportService": "dpone.observability.export:RuntimeMetricsExportService",
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

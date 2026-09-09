"""Compatibility facade for columnar fast-path runtime contracts."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [  # noqa: F822 - exports are resolved lazily via __getattr__.
    "COLUMNAR_CHUNKS_SCHEMA",
    "COLUMNAR_DECISION_SCHEMA",
    "ColumnarFastPathDecision",
    "ColumnarFastPathPlanner",
    "LocalColumnarChunk",
    "LocalColumnarStagingManifest",
    "ObjectStorageChunk",
    "ObjectStorageChunkWindow",
    "ObjectStorageColumnarChunkedArtifact",
    "ObjectStorageStagingManifest",
]

_EXPORTS: dict[str, str] = {
    "COLUMNAR_CHUNKS_SCHEMA": "dpone.runtime.columnar_fast_path_models",
    "COLUMNAR_DECISION_SCHEMA": "dpone.runtime.columnar_fast_path_models",
    "ColumnarFastPathDecision": "dpone.runtime.columnar_fast_path_models",
    "ColumnarFastPathPlanner": "dpone.runtime.columnar_fast_path_planner",
    "LocalColumnarChunk": "dpone.runtime.columnar_fast_path_models",
    "LocalColumnarStagingManifest": "dpone.runtime.columnar_fast_path_models",
    "ObjectStorageChunk": "dpone.runtime.columnar_fast_path_models",
    "ObjectStorageChunkWindow": "dpone.runtime.columnar_object_storage_windows",
    "ObjectStorageColumnarChunkedArtifact": "dpone.runtime.columnar_object_storage_windows",
    "ObjectStorageStagingManifest": "dpone.runtime.columnar_fast_path_models",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))

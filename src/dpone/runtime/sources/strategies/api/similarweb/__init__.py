"""Similarweb strategy compatibility facade."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "SimilarwebBaseStrategy",
    "SimilarwebKeywordsIncrementalAppendStrategy",
    "SimilarwebRowTransformer",
    "SimilarwebDQValidator",
]

_EXPORTS: dict[str, str] = {
    "SimilarwebBaseStrategy": "dpone.runtime.sources.strategies.api.similarweb.base:SimilarwebBaseStrategy",
    "SimilarwebKeywordsIncrementalAppendStrategy": "dpone.runtime.sources.strategies.api.similarweb.incremental_append_extract:SimilarwebKeywordsIncrementalAppendStrategy",
    "SimilarwebRowTransformer": "dpone.runtime.sources.strategies.api.similarweb.transformer:SimilarwebRowTransformer",
    "SimilarwebDQValidator": "dpone.runtime.sources.strategies.api.similarweb.validator:SimilarwebDQValidator",
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

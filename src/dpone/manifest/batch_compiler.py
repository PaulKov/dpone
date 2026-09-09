# ruff: noqa: F822
"""Compatibility facade for the batch manifest compiler."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "BatchManifestCompiler",
    "CompiledProcess",
    "TemplateRenderer",
    "deep_merge",
    "_RESERVED_VARS",
    "_RU_TRANSLIT",
    "_is_mapping",
    "_normalize_depends_on",
    "_to_identifier",
    "_to_snake",
    "_translit_ru",
    "_validate_unique_names",
]

_EXPORTS: dict[str, str] = {
    "BatchManifestCompiler": "dpone.manifest.batch_compiler_impl:BatchManifestCompiler",
    "CompiledProcess": "dpone.manifest.batch_models:CompiledProcess",
    "TemplateRenderer": "dpone.manifest.batch_rendering:TemplateRenderer",
    "deep_merge": "dpone.manifest.batch_merge:deep_merge",
    "_RESERVED_VARS": "dpone.manifest.batch_models:_RESERVED_VARS",
    "_RU_TRANSLIT": "dpone.manifest.batch_naming:_RU_TRANSLIT",
    "_is_mapping": "dpone.manifest.batch_merge:_is_mapping",
    "_normalize_depends_on": "dpone.manifest.batch_dependencies:_normalize_depends_on",
    "_to_identifier": "dpone.manifest.batch_naming:_to_identifier",
    "_to_snake": "dpone.manifest.batch_naming:_to_snake",
    "_translit_ru": "dpone.manifest.batch_naming:_translit_ru",
    "_validate_unique_names": "dpone.manifest.batch_dependencies:_validate_unique_names",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value

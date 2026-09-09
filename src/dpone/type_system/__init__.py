# ruff: noqa: F822
"""Portable type inference API."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ColumnProfile",
    "ContractDiagnostic",
    "ContractEnforcementResult",
    "ContractEnforcementService",
    "InferredColumn",
    "SampleTypeProfiler",
    "TypeInferenceOptions",
    "TypeInferenceReport",
    "TypeInferenceService",
]

_EXPORTS: dict[str, str] = {
    "ColumnProfile": "dpone.type_system.models:ColumnProfile",
    "ContractDiagnostic": "dpone.type_system.enforcement:ContractDiagnostic",
    "ContractEnforcementResult": "dpone.type_system.enforcement:ContractEnforcementResult",
    "ContractEnforcementService": "dpone.type_system.enforcement:ContractEnforcementService",
    "InferredColumn": "dpone.type_system.models:InferredColumn",
    "SampleTypeProfiler": "dpone.type_system.profiler:SampleTypeProfiler",
    "TypeInferenceOptions": "dpone.type_system.models:TypeInferenceOptions",
    "TypeInferenceReport": "dpone.type_system.models:TypeInferenceReport",
    "TypeInferenceService": "dpone.type_system.inference:TypeInferenceService",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value

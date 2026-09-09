"""Production profile defaults."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any


class ProductionProfileService:
    """Apply named fail-closed profile defaults to manifest-like mappings."""

    def apply(self, manifest: Mapping[str, Any]) -> dict[str, Any]:
        result = deepcopy(dict(manifest))
        profile = str(result.get("profile", "")).strip().lower()
        if profile != "production_safe":
            return result
        _deep_defaults(
            result,
            {
                "schema_contract": {"enforcement": "strict", "columns": {}},
                "sink": {
                    "options": {
                        "schema_contract": {"enforcement": "strict", "columns": {}},
                        "type_inference": {"enabled": True, "conflict_policy": "fail"},
                        "schema_evolution": {"enabled": True},
                        "physical_design": {"enabled": True, "apply": "online"},
                        "runtime_evidence": {"enabled": True, "output_dir": ".dpone/evidence/latest"},
                        "lineage": {"enabled": True, "preset": "standard"},
                    }
                },
            },
        )
        return result


def _deep_defaults(target: dict[str, Any], defaults: Mapping[str, Any]) -> None:
    for key, value in defaults.items():
        if key not in target:
            target[key] = deepcopy(value)
            continue
        if isinstance(target[key], dict) and isinstance(value, Mapping):
            _deep_defaults(target[key], value)


__all__ = ["ProductionProfileService"]

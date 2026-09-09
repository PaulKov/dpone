"""Composable lineage option resolution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.technical_columns import TechnicalColumnCatalog, TechnicalColumnRole

_PRESET_FEATURES: Mapping[str, frozenset[str]] = {
    "off": frozenset(),
    "minimal": frozenset({"identity"}),
    "bulk_standard": frozenset({"run_identity", "identity", "extraction_time"}),
    "standard": frozenset({"identity", "row_identity"}),
    "debug": frozenset({"identity", "row_identity", "operations", "diagnostics"}),
    "hierarchical": frozenset({"identity", "row_identity", "hierarchy"}),
}

_FEATURE_ROLES: Mapping[str, tuple[TechnicalColumnRole, ...]] = {
    "run_identity": (TechnicalColumnRole.RUN_ID,),
    "identity": (TechnicalColumnRole.LOAD_ID, TechnicalColumnRole.LOADED_AT),
    "extraction_time": (TechnicalColumnRole.EXTRACTED_AT,),
    "row_identity": (TechnicalColumnRole.ROW_ID, TechnicalColumnRole.EXTRACTED_AT),
    "hierarchy": (
        TechnicalColumnRole.PARENT_ROW_ID,
        TechnicalColumnRole.ROOT_ROW_ID,
        TechnicalColumnRole.LIST_INDEX,
    ),
    "operations": (TechnicalColumnRole.OP,),
    "diagnostics": (TechnicalColumnRole.META,),
}

_VALID_FEATURES = frozenset((*_FEATURE_ROLES.keys(), "quarantine"))


@dataclass(frozen=True, slots=True)
class LineageOptions:
    """Resolved composable lineage settings for one load."""

    enabled: bool = True
    preset: str = "standard"
    features: frozenset[str] = frozenset({"identity", "row_identity", "quarantine"})

    @classmethod
    def from_config(cls, raw: Mapping[str, Any] | bool | None) -> LineageOptions:
        if raw is False:
            return cls(enabled=False, preset="off", features=frozenset())
        if raw is True or raw is None:
            raw = {}
        if not isinstance(raw, Mapping):
            raise ValueError("lineage config must be an object or boolean")

        enabled = bool(raw.get("enabled", True))
        if not enabled:
            return cls(enabled=False, preset="off", features=frozenset())

        preset = str(raw.get("preset", "standard")).strip().lower() or "standard"
        if preset not in _PRESET_FEATURES:
            raise ValueError(f"lineage.preset must be one of: {', '.join(sorted(_PRESET_FEATURES))}")

        features = set(_PRESET_FEATURES[preset])
        explicit_features = raw.get("features", {})
        if explicit_features is None:
            explicit_features = {}
        if not isinstance(explicit_features, Mapping):
            raise ValueError("lineage.features must be an object")

        if preset == "standard" and "quarantine" not in explicit_features:
            features.add("quarantine")

        for feature, enabled_flag in explicit_features.items():
            normalized = str(feature).strip().lower()
            if normalized not in _VALID_FEATURES:
                raise ValueError(f"unsupported lineage feature: {feature!r}")
            if bool(enabled_flag):
                features.add(normalized)
            else:
                features.discard(normalized)

        return cls(enabled=True, preset=preset, features=frozenset(features))

    def has_feature(self, feature: str) -> bool:
        return str(feature).strip().lower() in self.features

    def target_roles(self) -> tuple[TechnicalColumnRole, ...]:
        if not self.enabled:
            return ()
        roles: list[TechnicalColumnRole] = []
        for feature in (
            "run_identity",
            "identity",
            "extraction_time",
            "row_identity",
            "hierarchy",
            "operations",
            "diagnostics",
        ):
            if feature not in self.features:
                continue
            roles.extend(_FEATURE_ROLES[feature])
        return tuple(dict.fromkeys(roles))

    def target_column_names(self) -> tuple[str, ...]:
        catalog = TechnicalColumnCatalog()
        return tuple(catalog.name(role) for role in self.target_roles())

    def target_schema_columns(self) -> list[tuple[str, str]]:
        return TechnicalColumnCatalog().schema_columns(self.target_roles())

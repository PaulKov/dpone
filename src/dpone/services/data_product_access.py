"""File-IO facade for data product access governance commands."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class DataProductAccessFacade:
    """Thin CLI facade; access decisions live in readiness modules."""

    def classify(self, *, manifest_path: str, schema_contract_path: str | None = None) -> dict[str, Any]:
        return (
            _access()
            .AccessClassificationBuilder()
            .build(
                manifest=_read_mapping(manifest_path),
                schema_contract=_read_optional(schema_contract_path),
            )
        )

    def entitlements_plan(
        self,
        *,
        manifest_path: str,
        classification_path: str,
        consumer_matrix_path: str | None = None,
    ) -> dict[str, Any]:
        return (
            _access()
            .EntitlementPlanBuilder()
            .build(
                manifest=_read_mapping(manifest_path),
                classification=_read_mapping(classification_path),
                consumer_matrix=_read_optional(consumer_matrix_path),
            )
        )

    def privacy_assess(
        self,
        *,
        manifest_path: str,
        entitlement_plan_path: str,
        authority_gate_path: str | None = None,
    ) -> dict[str, Any]:
        return (
            _access()
            .PrivacyImpactAssessor()
            .assess(
                manifest=_read_mapping(manifest_path),
                entitlement_plan=_read_mapping(entitlement_plan_path),
                authority_gate=_read_optional(authority_gate_path),
            )
        )

    def gate(self, *, entitlement_plan_path: str, privacy_impact_path: str, profile: str) -> dict[str, Any]:
        return (
            _access()
            .AccessGateEvaluator()
            .evaluate(
                entitlement_plan=_read_mapping(entitlement_plan_path),
                privacy_impact=_read_mapping(privacy_impact_path),
                profile=profile,
            )
        )

    def report(self, *, gate_path: str) -> dict[str, Any]:
        return _access().AccessGovernanceRenderer().report(gate=_read_mapping(gate_path))


def _read_optional(path: str | None) -> dict[str, Any] | None:
    return _read_mapping(path) if path else None


def _read_mapping(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    raw = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _access() -> Any:
    return import_module("dpone.readiness.data_product_access")


__all__ = ["DataProductAccessFacade"]

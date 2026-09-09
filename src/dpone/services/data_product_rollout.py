"""File-IO facade for data product progressive delivery commands."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class DataProductRolloutFacade:
    """Thin CLI facade; rollout decisions live in readiness modules."""

    def plan(self, *, manifest_path: str, bundle_path: str | None = None, evidence_dir: str | None = None) -> dict:
        return (
            _rollout()
            .RolloutPlanBuilder()
            .plan(
                manifest=_read_mapping(manifest_path),
                bundle=_read_optional(bundle_path),
                evidence=_evidence_dir(evidence_dir),
            )
        )

    def shadow_validate(
        self,
        *,
        plan_path: str,
        runtime_artifact_path: str,
        baseline_runtime_artifact_path: str | None = None,
    ) -> dict:
        return (
            _rollout()
            .ShadowValidationEvaluator()
            .evaluate(
                plan=_read_mapping(plan_path),
                runtime_artifact=_read_mapping(runtime_artifact_path),
                baseline_runtime_artifact=_read_optional(baseline_runtime_artifact_path),
            )
        )

    def ring_gate(
        self,
        *,
        plan_path: str,
        ring: str,
        evidence_dir: str | None = None,
        shadow_validation_path: str | None = None,
        promotion_paths: tuple[str, ...] = (),
    ) -> dict:
        return (
            _rollout()
            .RingGateEvaluator()
            .evaluate(
                plan=_read_mapping(plan_path),
                ring_id=ring,
                evidence=_evidence_dir(evidence_dir),
                shadow_validation=_read_optional(shadow_validation_path),
                promotions=tuple(_read_mapping(path) for path in promotion_paths),
            )
        )

    def promote(
        self,
        *,
        ring_gate_path: str,
        target_ring: str,
        authority_gate_path: str | None = None,
    ) -> dict:
        return (
            _rollout()
            .RolloutPromotionService()
            .promote(
                ring_gate=_read_mapping(ring_gate_path),
                target_ring=target_ring,
                authority_gate=_read_optional(authority_gate_path),
            )
        )

    def report(self, *, promotion_path: str) -> dict:
        return _renderer().RolloutRenderer().report(promotion=_read_mapping(promotion_path))


def _evidence_dir(path: str | None) -> dict[str, dict[str, Any]]:
    if not path:
        return {}
    root = Path(path)
    if not root.exists():
        return {}
    evidence: dict[str, dict[str, Any]] = {}
    for source in sorted([*root.glob("*.json"), *root.glob("*.yaml"), *root.glob("*.yml")]):
        payload = _read_mapping(str(source))
        kind = str(payload.get("kind") or source.stem)
        evidence[kind] = payload
    return evidence


def _read_optional(path: str | None) -> dict[str, Any]:
    return _read_mapping(path) if path else {}


def _read_mapping(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    raw = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _rollout() -> Any:
    return import_module("dpone.readiness.data_product_rollout")


def _renderer() -> Any:
    return import_module("dpone.readiness.data_product_rollout_rendering")


__all__ = ["DataProductRolloutFacade"]

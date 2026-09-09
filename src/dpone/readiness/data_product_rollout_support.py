"""Support helpers for data product progressive delivery contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.readiness import data_product_compliance_support as base_support
from dpone.readiness.data_product_rollout_schemas import (
    RING_GATE_SCHEMA,
    ROLLOUT_PLAN_SCHEMA,
    ROLLOUT_PROMOTION_SCHEMA,
    ROLLOUT_REPORT_SCHEMA,
    SHADOW_VALIDATION_SCHEMA,
)
from dpone.readiness.migration_control import stable_fingerprint

ALLOWED_GATE_STATUSES = frozenset({"allowed", "warning", "waived", "certified", "passed", "ready"})


@dataclass(frozen=True, slots=True)
class ProgressiveDeliveryOptions:
    enabled: bool
    mode: str
    profile: str
    stale_evidence_policy: str
    rings: tuple[dict[str, Any], ...]
    shadow_enabled: bool
    shadow_required_for: tuple[str, ...]
    row_count_delta_max: float | None
    null_key_delta_max: int | None
    duplicate_key_delta_max: int | None
    typed_hash_policy: str
    auto_hold_on: tuple[str, ...]
    require_authority_gate: bool

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]) -> ProgressiveDeliveryOptions:
        raw = _progressive_options(manifest)
        shadow = _mapping(raw.get("shadow_validation"))
        compare = _mapping(shadow.get("compare"))
        rollback = _mapping(raw.get("rollback"))
        return cls(
            enabled=base_support.bool_value(raw.get("enabled"), False),
            mode=str(raw.get("mode") or "gate"),
            profile=str(raw.get("profile") or "prod_strict"),
            stale_evidence_policy=str(raw.get("stale_evidence_policy") or "block"),
            rings=tuple(_normalize_ring(item) for item in base_support.mappings(raw.get("rings"))),
            shadow_enabled=base_support.bool_value(shadow.get("enabled"), False),
            shadow_required_for=base_support.strings(shadow.get("required_for")),
            row_count_delta_max=_float_or_none(compare.get("row_count_delta_max")),
            null_key_delta_max=base_support.optional_int(compare.get("null_key_delta_max")),
            duplicate_key_delta_max=base_support.optional_int(compare.get("duplicate_key_delta_max")),
            typed_hash_policy=str(compare.get("typed_hash") or "warning"),
            auto_hold_on=base_support.strings(rollback.get("auto_hold_on")),
            require_authority_gate=base_support.bool_value(rollback.get("require_authority_gate"), False),
        )

    @classmethod
    def from_plan(cls, plan: Mapping[str, Any]) -> ProgressiveDeliveryOptions:
        raw = _mapping(plan.get("options"))
        return cls(
            enabled=plan.get("status") != "disabled",
            mode=str(plan.get("mode") or "gate"),
            profile=str(plan.get("profile") or "prod_strict"),
            stale_evidence_policy=str(raw.get("stale_evidence_policy") or "block"),
            rings=tuple(dict(item) for item in base_support.mappings(plan.get("rings"))),
            shadow_enabled=base_support.bool_value(raw.get("shadow_enabled"), False),
            shadow_required_for=base_support.strings(raw.get("shadow_required_for")),
            row_count_delta_max=_float_or_none(raw.get("row_count_delta_max")),
            null_key_delta_max=base_support.optional_int(raw.get("null_key_delta_max")),
            duplicate_key_delta_max=base_support.optional_int(raw.get("duplicate_key_delta_max")),
            typed_hash_policy=str(raw.get("typed_hash_policy") or "warning"),
            auto_hold_on=base_support.strings(raw.get("auto_hold_on")),
            require_authority_gate=base_support.bool_value(raw.get("require_authority_gate"), False),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "stale_evidence_policy": self.stale_evidence_policy,
            "shadow_enabled": self.shadow_enabled,
            "shadow_required_for": list(self.shadow_required_for),
            "row_count_delta_max": self.row_count_delta_max,
            "null_key_delta_max": self.null_key_delta_max,
            "duplicate_key_delta_max": self.duplicate_key_delta_max,
            "typed_hash_policy": self.typed_hash_policy,
            "auto_hold_on": list(self.auto_hold_on),
            "require_authority_gate": self.require_authority_gate,
        }


def product_ref(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return base_support.product_ref(base_support.product(manifest))


def payload_id(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    result = dict(payload)
    result[key] = stable_fingerprint(result)
    return result


def apply_profile(
    *,
    blockers: Sequence[str],
    warnings: Sequence[str],
    profile: str,
    mode: str = "gate",
) -> tuple[list[str], list[str]]:
    unique_blockers = list(dict.fromkeys(str(item) for item in blockers if str(item)))
    unique_warnings = list(dict.fromkeys(str(item) for item in warnings if str(item)))
    if profile == "advisory" or mode == "observe":
        return [], list(dict.fromkeys([*unique_warnings, *unique_blockers]))
    return unique_blockers, unique_warnings


def status(blockers: Sequence[str], warnings: Sequence[str], success: str) -> str:
    if blockers:
        return "blocked"
    if warnings:
        return "warning"
    return success


def mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


def strings(raw: Any) -> tuple[str, ...]:
    return base_support.strings(raw)


def mappings(raw: Any) -> tuple[Mapping[str, Any], ...]:
    return base_support.mappings(raw)


def number(raw: Any) -> float:
    try:
        return float(raw or 0)
    except (TypeError, ValueError):
        return 0.0


def ring_by_id(plan: Mapping[str, Any], ring_id: str) -> Mapping[str, Any] | None:
    for ring in base_support.mappings(plan.get("rings")):
        if ring.get("id") == ring_id:
            return ring
    return None


def previous_rings(plan: Mapping[str, Any], ring_id: str) -> tuple[Mapping[str, Any], ...]:
    selected = ring_by_id(plan, ring_id)
    if not selected:
        return ()
    selected_order = int(selected.get("order") or 0)
    return tuple(
        ring for ring in base_support.mappings(plan.get("rings")) if int(ring.get("order") or 0) < selected_order
    )


def evidence_id(payload: Mapping[str, Any]) -> str | None:
    return base_support.evidence_id(payload)


def _progressive_options(manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    product = base_support.product(manifest)
    return _mapping(product.get("progressive_delivery"))


def _normalize_ring(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": str(raw.get("id") or ""),
        "order": int(raw.get("order") or 0),
        "required_gates": list(base_support.strings(raw.get("required_gates"))),
        "max_consumers": base_support.optional_int(raw.get("max_consumers")),
        "consumer_selector": dict(_mapping(raw.get("consumer_selector"))),
    }


def _mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


def _float_or_none(raw: Any) -> float | None:
    if raw in {None, ""}:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


__all__ = [
    "ALLOWED_GATE_STATUSES",
    "ProgressiveDeliveryOptions",
    "RING_GATE_SCHEMA",
    "ROLLOUT_PLAN_SCHEMA",
    "ROLLOUT_PROMOTION_SCHEMA",
    "ROLLOUT_REPORT_SCHEMA",
    "SHADOW_VALIDATION_SCHEMA",
    "apply_profile",
    "evidence_id",
    "mapping",
    "mappings",
    "number",
    "payload_id",
    "previous_rings",
    "product_ref",
    "ring_by_id",
    "status",
    "strings",
]

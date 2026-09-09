"""Provider-neutral data product progressive delivery contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness import data_product_rollout_support as support
from dpone.readiness.data_product_rollout_support import ProgressiveDeliveryOptions


class RolloutPlanBuilder:
    """Builds product and bundle-bound rollout plans."""

    def plan(
        self,
        *,
        manifest: Mapping[str, Any],
        bundle: Mapping[str, Any] | None = None,
        evidence: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        options = ProgressiveDeliveryOptions.from_manifest(manifest)
        product = support.product_ref(manifest)
        if not options.enabled:
            return _plan_payload(options, product, bundle or {}, (), (), "disabled", ())
        blockers, warnings = _ring_config_findings(options.rings)
        if not product.get("id"):
            blockers.append("data_product_rollout.product_id_missing")
        if options.profile == "regulated" and not product.get("owner"):
            blockers.append("data_product_rollout.owner_missing")
        if bundle and bundle.get("status") == "blocked":
            blockers.append("data_product_rollout.bundle_blocked")
        blockers, warnings = support.apply_profile(
            blockers=blockers,
            warnings=warnings,
            profile=options.profile,
            mode=options.mode,
        )
        return _plan_payload(
            options,
            product,
            bundle or {},
            _evidence_refs(evidence or {}),
            blockers,
            support.status(blockers, warnings, "ready"),
            warnings,
        )


class ShadowValidationEvaluator:
    """Compares baseline and candidate run evidence for shadow validation."""

    def evaluate(
        self,
        *,
        plan: Mapping[str, Any],
        runtime_artifact: Mapping[str, Any] | None = None,
        baseline_runtime_artifact: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if plan.get("status") == "disabled":
            return _shadow_payload(plan, {}, {}, "disabled", (), ())
        options = ProgressiveDeliveryOptions.from_plan(plan)
        candidate = support.mapping(runtime_artifact)
        baseline = support.mapping(baseline_runtime_artifact)
        blockers: list[str] = []
        warnings: list[str] = []
        if not baseline:
            blockers.append("data_product_rollout.shadow_baseline_missing")
        if candidate.get("status") == "blocked" or baseline.get("status") == "blocked":
            blockers.append("data_product_rollout.shadow_runtime_blocked")
        blockers.extend(_row_count_blockers(options, candidate, baseline))
        blockers.extend(_key_delta_blockers(options, candidate, baseline))
        _typed_hash_findings(options, candidate, baseline, blockers, warnings)
        blockers, warnings = support.apply_profile(
            blockers=blockers,
            warnings=warnings,
            profile=str(plan.get("profile") or "prod_strict"),
            mode=str(plan.get("mode") or "gate"),
        )
        return _shadow_payload(
            plan, candidate, baseline, support.status(blockers, warnings, "passed"), blockers, warnings
        )


class RingGateEvaluator:
    """Profile-aware gate for a rollout ring."""

    def evaluate(
        self,
        *,
        plan: Mapping[str, Any],
        ring_id: str,
        evidence: Mapping[str, Mapping[str, Any]] | None = None,
        shadow_validation: Mapping[str, Any] | None = None,
        promotions: Sequence[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        if plan.get("status") == "disabled":
            return _ring_gate_payload(plan, {}, ring_id, "allowed", (), (), shadow_validation)
        options = ProgressiveDeliveryOptions.from_plan(plan)
        ring = support.ring_by_id(plan, ring_id)
        blockers = list(support.strings(plan.get("blockers")))
        warnings = list(support.strings(plan.get("warnings")))
        if not ring:
            blockers.append(f"data_product_rollout.unknown_ring:{ring_id}")
            ring = {"id": ring_id, "required_gates": []}
        blockers.extend(_previous_ring_blockers(plan, ring_id, promotions, options.profile))
        blockers.extend(_required_gate_blockers(ring, evidence or {}, plan))
        blockers.extend(_shadow_blockers(options, ring_id, shadow_validation))
        blockers, warnings = support.apply_profile(
            blockers=blockers,
            warnings=warnings,
            profile=options.profile,
            mode=options.mode,
        )
        return _ring_gate_payload(
            plan,
            ring,
            ring_id,
            support.status(blockers, warnings, "allowed"),
            blockers,
            warnings,
            shadow_validation,
        )


class RolloutPromotionService:
    """Emits deterministic evidence-only ring promotion receipts."""

    def promote(
        self,
        *,
        ring_gate: Mapping[str, Any],
        target_ring: str,
        authority_gate: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        blockers = list(support.strings(ring_gate.get("blockers")))
        warnings = list(support.strings(ring_gate.get("warnings")))
        source_ring = str(ring_gate.get("ring_id") or "")
        options = support.mapping(ring_gate.get("options"))
        if ring_gate.get("status") not in {"allowed", "warning"}:
            blockers.append("data_product_rollout.source_ring_gate_blocked")
        if options.get("require_authority_gate") and support.mapping(authority_gate).get("status") not in {
            "allowed",
            "warning",
        }:
            blockers.append("data_product_rollout.authority_gate_missing")
        status = "held" if blockers else "promoted"
        payload = {
            "schema_version": support.ROLLOUT_PROMOTION_SCHEMA,
            "status": status,
            "decision": "hold" if blockers else "promote",
            "product": support.mapping(ring_gate.get("product")),
            "product_id": ring_gate.get("product_id"),
            "rollout_plan_id": ring_gate.get("rollout_plan_id"),
            "ring_gate_id": ring_gate.get("ring_gate_id"),
            "from_ring": source_ring,
            "to_ring": target_ring,
            "target_ring": target_ring,
            "pack_id": ring_gate.get("pack_id"),
            "bundle_id": ring_gate.get("bundle_id"),
            "authority_gate_id": support.evidence_id(support.mapping(authority_gate)),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _recommendations(status),
        }
        return support.payload_id(payload, "rollout_promotion_id")


def _plan_payload(
    options: ProgressiveDeliveryOptions,
    product: Mapping[str, Any],
    bundle: Mapping[str, Any],
    evidence_refs: Sequence[Mapping[str, Any]],
    blockers: Sequence[str],
    status: str,
    warnings: Sequence[str],
) -> dict[str, Any]:
    payload = {
        "schema_version": support.ROLLOUT_PLAN_SCHEMA,
        "status": status,
        "mode": options.mode,
        "profile": options.profile,
        "product": dict(product),
        "product_id": product.get("id"),
        "pack_id": bundle.get("pack_id"),
        "bundle_id": bundle.get("bundle_id"),
        "rings": list(options.rings),
        "options": options.to_mapping(),
        "evidence_refs": list(evidence_refs),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "recommendations": _recommendations(status),
    }
    return support.payload_id(payload, "rollout_plan_id")


def _shadow_payload(
    plan: Mapping[str, Any],
    candidate: Mapping[str, Any],
    baseline: Mapping[str, Any],
    status: str,
    blockers: Sequence[str],
    warnings: Sequence[str],
) -> dict[str, Any]:
    payload = {
        "schema_version": support.SHADOW_VALIDATION_SCHEMA,
        "status": status,
        "product": support.mapping(plan.get("product")),
        "product_id": plan.get("product_id"),
        "rollout_plan_id": plan.get("rollout_plan_id"),
        "pack_id": plan.get("pack_id"),
        "bundle_id": plan.get("bundle_id"),
        "candidate": _runtime_summary(candidate),
        "baseline": _runtime_summary(baseline),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "recommendations": _recommendations(status),
    }
    return support.payload_id(payload, "shadow_validation_id")


def _ring_gate_payload(
    plan: Mapping[str, Any],
    ring: Mapping[str, Any],
    ring_id: str,
    status: str,
    blockers: Sequence[str],
    warnings: Sequence[str],
    shadow_validation: Mapping[str, Any] | None,
) -> dict[str, Any]:
    payload = {
        "schema_version": support.RING_GATE_SCHEMA,
        "status": status,
        "product": support.mapping(plan.get("product")),
        "product_id": plan.get("product_id"),
        "rollout_plan_id": plan.get("rollout_plan_id"),
        "ring_id": ring_id,
        "ring": dict(ring),
        "pack_id": plan.get("pack_id"),
        "bundle_id": plan.get("bundle_id"),
        "shadow_validation_id": support.evidence_id(support.mapping(shadow_validation)),
        "options": {} if plan.get("status") == "disabled" else dict(support.mapping(plan.get("options"))),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "recommendations": _recommendations(status),
    }
    return support.payload_id(payload, "ring_gate_id")


def _ring_config_findings(rings: Sequence[Mapping[str, Any]]) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    seen: set[str] = set()
    previous_order = -1
    for ring in rings:
        ring_id = str(ring.get("id") or "")
        order = int(ring.get("order") or 0)
        if ring_id in seen:
            blockers.append(f"data_product_rollout.duplicate_ring:{ring_id}")
        seen.add(ring_id)
        if order <= previous_order:
            blockers.append(f"data_product_rollout.ring_order_not_increasing:{ring_id}")
        previous_order = order
    return blockers, []


def _evidence_refs(evidence: Mapping[str, Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    return tuple(
        {"kind": kind, "evidence_id": evidence_id}
        for kind, payload in sorted(evidence.items())
        if (evidence_id := support.evidence_id(payload))
    )


def _row_count_blockers(
    options: ProgressiveDeliveryOptions, candidate: Mapping[str, Any], baseline: Mapping[str, Any]
) -> list[str]:
    if options.row_count_delta_max is None or not baseline:
        return []
    base = support.number(baseline.get("row_count") or baseline.get("rows"))
    current = support.number(candidate.get("row_count") or candidate.get("rows"))
    delta = abs(current - base) / base if base else 0.0
    return ["data_product_rollout.shadow_row_count_delta_exceeded"] if delta > options.row_count_delta_max else []


def _key_delta_blockers(
    options: ProgressiveDeliveryOptions, candidate: Mapping[str, Any], baseline: Mapping[str, Any]
) -> list[str]:
    blockers: list[str] = []
    null_delta = support.number(candidate.get("null_keys")) - support.number(baseline.get("null_keys"))
    dup_delta = support.number(candidate.get("duplicate_keys")) - support.number(baseline.get("duplicate_keys"))
    if options.null_key_delta_max is not None and null_delta > options.null_key_delta_max:
        blockers.append("data_product_rollout.shadow_null_key_delta_exceeded")
    if options.duplicate_key_delta_max is not None and dup_delta > options.duplicate_key_delta_max:
        blockers.append("data_product_rollout.shadow_duplicate_key_delta_exceeded")
    return blockers


def _typed_hash_findings(
    options: ProgressiveDeliveryOptions,
    candidate: Mapping[str, Any],
    baseline: Mapping[str, Any],
    blockers: list[str],
    warnings: list[str],
) -> None:
    if options.typed_hash_policy == "disabled" or not candidate.get("typed_hash") or not baseline.get("typed_hash"):
        return
    if candidate.get("typed_hash") == baseline.get("typed_hash"):
        return
    target = blockers if options.typed_hash_policy == "strict" else warnings
    target.append("data_product_rollout.shadow_typed_hash_mismatch")


def _previous_ring_blockers(
    plan: Mapping[str, Any], ring_id: str, promotions: Sequence[Mapping[str, Any]], profile: str
) -> list[str]:
    if profile in {"advisory", "stage"}:
        return []
    promoted = {str(item.get("from_ring")) for item in promotions if item.get("status") == "promoted"}
    return [
        f"data_product_rollout.previous_ring_not_promoted:{ring.get('id')}"
        for ring in support.previous_rings(plan, ring_id)
        if ring.get("id") not in promoted
    ]


def _required_gate_blockers(
    ring: Mapping[str, Any], evidence: Mapping[str, Mapping[str, Any]], plan: Mapping[str, Any]
) -> list[str]:
    blockers: list[str] = []
    for kind in support.strings(ring.get("required_gates")):
        payload = support.mapping(evidence.get(kind))
        if not payload:
            blockers.append(f"data_product_rollout.required_gate_missing:{kind}")
            continue
        if payload.get("status") not in support.ALLOWED_GATE_STATUSES:
            blockers.append(f"data_product_rollout.required_gate_blocked:{kind}")
        if payload.get("pack_id") not in {None, plan.get("pack_id")}:
            blockers.append(f"data_product_rollout.required_gate_pack_id_mismatch:{kind}")
        if payload.get("bundle_id") not in {None, plan.get("bundle_id")}:
            blockers.append(f"data_product_rollout.required_gate_bundle_id_mismatch:{kind}")
        blockers.extend(str(item) for item in payload.get("blockers", []) if str(item))
    return blockers


def _shadow_blockers(
    options: ProgressiveDeliveryOptions,
    ring_id: str,
    shadow_validation: Mapping[str, Any] | None,
) -> list[str]:
    if not options.shadow_enabled or ring_id not in set(options.shadow_required_for):
        return []
    shadow = support.mapping(shadow_validation)
    if not shadow:
        return [f"data_product_rollout.shadow_validation_missing:{ring_id}"]
    if shadow.get("status") == "blocked":
        return ["data_product_rollout.shadow_validation_blocked"]
    return []


def _runtime_summary(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in {
            "run_id": raw.get("run_id"),
            "status": raw.get("status"),
            "row_count": raw.get("row_count") or raw.get("rows"),
            "null_keys": raw.get("null_keys"),
            "duplicate_keys": raw.get("duplicate_keys"),
            "typed_hash": raw.get("typed_hash"),
        }.items()
        if value is not None
    }


def _recommendations(status: str) -> list[str]:
    if status in {"blocked", "held"}:
        return ["Hold rollout, resolve blockers, and rerun ring gate before promotion."]
    if status == "warning":
        return ["Review rollout warnings with the product owner before promotion."]
    return ["Record rollout evidence in the migration evidence registry."]


__all__ = [
    "ProgressiveDeliveryOptions",
    "RingGateEvaluator",
    "RolloutPlanBuilder",
    "RolloutPromotionService",
    "ShadowValidationEvaluator",
]

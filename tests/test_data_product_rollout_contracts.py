from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_rollout import (
    RingGateEvaluator,
    RolloutPlanBuilder,
    RolloutPromotionService,
    ShadowValidationEvaluator,
)
from dpone.readiness.data_product_rollout_rendering import RolloutRenderer


def test_disabled_progressive_delivery_emits_noop_artifacts() -> None:
    plan = RolloutPlanBuilder().plan(manifest=_manifest(enabled=False), bundle={}, evidence={})
    shadow = ShadowValidationEvaluator().evaluate(plan=plan, runtime_artifact={}, baseline_runtime_artifact={})
    gate = RingGateEvaluator().evaluate(plan=plan, ring_id="staging", evidence={}, shadow_validation=None)
    promotion = RolloutPromotionService().promote(ring_gate=gate, target_ring="prod_full", authority_gate=None)
    report = RolloutRenderer().report(promotion=promotion)

    assert plan["schema_version"] == "dpone.data_product_rollout_plan.v1"
    assert plan["status"] == "disabled"
    assert shadow["status"] == "disabled"
    assert gate["status"] == "allowed"
    assert promotion["status"] == "promoted"
    assert report["status"] == "promoted"


def test_rollout_plan_rejects_duplicate_and_non_increasing_rings() -> None:
    manifest = _manifest()
    manifest["sink"]["options"]["data_product"]["progressive_delivery"]["rings"] = [
        {"id": "staging", "order": 10, "required_gates": []},
        {"id": "staging", "order": 10, "required_gates": []},
        {"id": "prod", "order": 5, "required_gates": []},
    ]

    plan = RolloutPlanBuilder().plan(manifest=manifest, bundle=_bundle(), evidence={})

    assert plan["status"] == "blocked"
    assert "data_product_rollout.duplicate_ring:staging" in plan["blockers"]
    assert "data_product_rollout.ring_order_not_increasing:prod" in plan["blockers"]


def test_shadow_validation_blocks_row_count_and_strict_hash_delta() -> None:
    manifest = _manifest()
    manifest["sink"]["options"]["data_product"]["progressive_delivery"]["shadow_validation"]["compare"][
        "typed_hash"
    ] = "strict"
    plan = RolloutPlanBuilder().plan(manifest=manifest, bundle=_bundle(), evidence=_green_staging_evidence())
    shadow = ShadowValidationEvaluator().evaluate(
        plan=plan,
        runtime_artifact={
            "run_id": "candidate",
            "row_count": 1200,
            "null_keys": 2,
            "duplicate_keys": 1,
            "typed_hash": "sha256:new",
            "status": "passed",
        },
        baseline_runtime_artifact={
            "run_id": "baseline",
            "row_count": 1000,
            "null_keys": 0,
            "duplicate_keys": 0,
            "typed_hash": "sha256:old",
            "status": "passed",
        },
    )

    assert shadow["schema_version"] == "dpone.data_product_shadow_validation.v1"
    assert shadow["status"] == "blocked"
    assert "data_product_rollout.shadow_row_count_delta_exceeded" in shadow["blockers"]
    assert "data_product_rollout.shadow_null_key_delta_exceeded" in shadow["blockers"]
    assert "data_product_rollout.shadow_duplicate_key_delta_exceeded" in shadow["blockers"]
    assert "data_product_rollout.shadow_typed_hash_mismatch" in shadow["blockers"]


def test_canary_ring_gate_requires_previous_promotion_required_gates_and_shadow_validation() -> None:
    plan = RolloutPlanBuilder().plan(manifest=_manifest(), bundle=_bundle(), evidence=_green_canary_evidence())

    gate = RingGateEvaluator().evaluate(
        plan=plan,
        ring_id="canary",
        evidence=_green_canary_evidence(),
        shadow_validation=None,
        promotions=(),
    )

    assert gate["schema_version"] == "dpone.data_product_ring_gate.v1"
    assert gate["status"] == "blocked"
    assert "data_product_rollout.previous_ring_not_promoted:staging" in gate["blockers"]
    assert "data_product_rollout.shadow_validation_missing:canary" in gate["blockers"]


def test_advisory_ring_gate_converts_blockers_to_warnings() -> None:
    manifest = _manifest()
    manifest["sink"]["options"]["data_product"]["progressive_delivery"]["profile"] = "advisory"
    plan = RolloutPlanBuilder().plan(manifest=manifest, bundle=_bundle(), evidence={})

    gate = RingGateEvaluator().evaluate(plan=plan, ring_id="staging", evidence={}, shadow_validation=None)

    assert gate["status"] == "warning"
    assert "data_product_rollout.required_gate_missing:schema_contract_gate" in gate["warnings"]
    assert gate["blockers"] == []


def test_promotion_and_report_emit_hold_when_authority_is_required_and_missing() -> None:
    plan = RolloutPlanBuilder().plan(manifest=_manifest(), bundle=_bundle(), evidence=_green_canary_evidence())
    shadow = ShadowValidationEvaluator().evaluate(
        plan=plan,
        runtime_artifact=_runtime(row_count=1000, typed_hash="sha256:ok"),
        baseline_runtime_artifact=_runtime(row_count=1000, typed_hash="sha256:ok"),
    )
    staging_promotion = {"from_ring": "staging", "to_ring": "canary", "status": "promoted"}
    gate = RingGateEvaluator().evaluate(
        plan=plan,
        ring_id="canary",
        evidence=_green_canary_evidence(),
        shadow_validation=shadow,
        promotions=(staging_promotion,),
    )

    promotion = RolloutPromotionService().promote(ring_gate=gate, target_ring="prod_full", authority_gate=None)
    report = RolloutRenderer().report(promotion=promotion)

    assert gate["status"] == "allowed"
    assert promotion["schema_version"] == "dpone.data_product_rollout_promotion.v1"
    assert promotion["status"] == "held"
    assert promotion["decision"] == "hold"
    assert "data_product_rollout.authority_gate_missing" in promotion["blockers"]
    assert "# Data Product Rollout Report" in report["markdown"]


def test_rollout_artifacts_validate_json_schemas() -> None:
    plan = RolloutPlanBuilder().plan(manifest=_manifest(), bundle=_bundle(), evidence=_green_canary_evidence())
    shadow = ShadowValidationEvaluator().evaluate(
        plan=plan,
        runtime_artifact=_runtime(row_count=1000, typed_hash="sha256:ok"),
        baseline_runtime_artifact=_runtime(row_count=1000, typed_hash="sha256:ok"),
    )
    gate = RingGateEvaluator().evaluate(
        plan=plan,
        ring_id="staging",
        evidence=_green_staging_evidence(),
        shadow_validation=None,
    )
    promotion = RolloutPromotionService().promote(
        ring_gate=gate,
        target_ring="canary",
        authority_gate={"status": "allowed", "authority_gate_id": "sha256:authority"},
    )
    report = RolloutRenderer().report(promotion=promotion)

    for name, payload in (
        ("data-product-rollout-plan.schema.json", plan),
        ("data-product-shadow-validation.schema.json", shadow),
        ("data-product-ring-gate.schema.json", gate),
        ("data-product-rollout-promotion.schema.json", promotion),
        ("data-product-rollout-report.schema.json", report),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


def _manifest(*, enabled: bool = True) -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "progressive_delivery": {
                        "enabled": enabled,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "stale_evidence_policy": "block",
                        "rings": [
                            {
                                "id": "staging",
                                "order": 10,
                                "required_gates": [
                                    "schema_contract_gate",
                                    "data_product_assertion_gate",
                                    "data_product_cost_gate",
                                ],
                            },
                            {
                                "id": "canary",
                                "order": 20,
                                "max_consumers": 3,
                                "consumer_selector": {
                                    "owners": ["data-platform"],
                                    "criticality": ["low", "medium"],
                                },
                                "required_gates": [
                                    "data_product_slo_gate",
                                    "data_product_access_gate",
                                    "data_product_connection_rotation_gate",
                                ],
                            },
                            {
                                "id": "prod_full",
                                "order": 30,
                                "required_gates": [
                                    "data_product_release_closeout_gate",
                                    "data_product_compliance_gate",
                                    "data_product_policy_gate",
                                ],
                            },
                        ],
                        "shadow_validation": {
                            "enabled": True,
                            "required_for": ["canary", "prod_full"],
                            "compare": {
                                "row_count_delta_max": 0.001,
                                "null_key_delta_max": 0,
                                "duplicate_key_delta_max": 0,
                                "typed_hash": "warning",
                            },
                        },
                        "rollback": {
                            "auto_hold_on": [
                                "shadow_failed",
                                "ring_gate_blocked",
                                "slo_blocked",
                                "assertion_blocked",
                                "cost_blocked",
                            ],
                            "require_authority_gate": True,
                        },
                    },
                }
            }
        }
    }


def _bundle() -> dict:
    return {
        "bundle_id": "sha256:bundle",
        "pack_id": "sha256:pack",
        "status": "attested",
        "summary": {"changes_count": 1},
    }


def _green_staging_evidence() -> dict[str, dict]:
    return {
        "schema_contract_gate": _gate("contract_gate_id"),
        "data_product_assertion_gate": _gate("assertion_gate_id"),
        "data_product_cost_gate": _gate("cost_gate_id"),
    }


def _green_canary_evidence() -> dict[str, dict]:
    return {
        **_green_staging_evidence(),
        "data_product_slo_gate": _gate("slo_gate_id"),
        "data_product_access_gate": _gate("access_gate_id"),
        "data_product_connection_rotation_gate": _gate("connection_rotation_gate_id"),
    }


def _gate(id_key: str, *, status: str = "allowed") -> dict:
    return {
        "schema_version": f"dpone.test.{id_key}.v1",
        "status": status,
        id_key: f"sha256:{id_key}",
        "pack_id": "sha256:pack",
        "bundle_id": "sha256:bundle",
        "blockers": [],
        "warnings": [],
    }


def _runtime(*, row_count: int, typed_hash: str) -> dict:
    return {
        "status": "passed",
        "row_count": row_count,
        "null_keys": 0,
        "duplicate_keys": 0,
        "typed_hash": typed_hash,
    }
